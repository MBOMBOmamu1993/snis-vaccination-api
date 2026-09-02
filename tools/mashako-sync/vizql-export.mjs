/** MOTEUR D'EXPORT PAR SESSION VizQL (02/09/2026)
 *
 *  Remplace, pour les feuilles « unitaires » du classeur ZS (sans colonne de
 *  localisation en export groupé), les 519 exports .csv par URL — chacun ouvre
 *  une session VizQL neuve et rend tout le dashboard (~33 s, 120 s sous charge).
 *
 *  Ici : UNE session par (dashboard, période) et par tranche de zones ; pour
 *  chaque zone, categorical-filter (~10-15 s, coût du live query côté Tableau)
 *  puis export « données résumé » de la feuille (~0,7 s) — le MÊME CSV que
 *  l'export par URL (vérifié ligne à ligne le 02/09 sur CDF_HZ_P1/Aketi ;
 *  seule différence : booléens localisés « Vrai/Faux » comme dans les archives
 *  produites par sync.mjs).
 *
 *  Faits établis (à ne pas réapprendre) :
 *   · la feuille exportée par l'URL .csv d'un dashboard = la feuille dont les
 *     colonnes correspondent à celles des archives (zs_expected_columns.json) ;
 *   · une session ne sert QUE son dashboard : demander une feuille d'un autre
 *     dashboard tue la session (503 puis 410) ;
 *   · la feuille visée par categorical-filter doit appartenir au dashboard,
 *     sinon la commande répond 200 sans rien changer ;
 *   · l'export résumé ignore les colonnes ajoutées ; les tableaux par aire sont
 *     paginés à 20 lignes → pas d'export multi-zones attribuable.
 */
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { launch, openSession, setZone, crosstabSheets, viewDataColumns, summaryCsv } from "./vizql-lib.mjs";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const norm = (c) => String(c).trim().toLowerCase();

export function parseCsv(text) {
  const rows = []; let row = [], field = "", inQ = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQ) { if (c === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else inQ = false; } else field += c; }
    else if (c === '"') inQ = true;
    else if (c === ",") { row.push(field); field = ""; }
    else if (c === "\n" || c === "\r") {
      if (c === "\r" && text[i + 1] === "\n") i++;
      row.push(field); field = "";
      if (row.length > 1 || row[0] !== "") rows.push(row);
      row = [];
    } else field += c;
  }
  if (field !== "" || row.length) { row.push(field); rows.push(row); }
  return rows;
}

const EXPECTED = {};
/* colonnes de référence = celles des archives publiées (août 2026), par classeur */
function expectedColumns(urlName, wb) {
  const f = /Antenne/i.test(wb || "") ? "ant_expected_columns.json" : "zs_expected_columns.json";
  if (!EXPECTED[f]) { try { EXPECTED[f] = JSON.parse(readFileSync(path.join(HERE, f), "utf8")); } catch (e) { EXPECTED[f] = {}; } }
  return EXPECTED[f][urlName] || null;
}

/** Choisit, parmi les feuilles du dashboard, celle qui reproduit l'export .csv par URL. */
const slugify = (label) => String(label || "").normalize("NFD").replace(/[̀-ͯ]/g, "").replace(/[^A-Za-z0-9_-]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 60);
async function choisirFeuille(s, urlName, log, slugArchive) {
  /* clé des colonnes attendues = slug d'archive (« Performance_Resume_HZ »),
     pas l'urlName Tableau (« PerformanceRsum_HZ ») */
  const attendues = expectedColumns(slugArchive, s.wb) || expectedColumns(urlName, s.wb) || expectedColumns(slugify(s.dashboard), s.wb);
  const sheets = s.sheets && s.sheets.length ? s.sheets : await crosstabSheets(s, s.thumbs);
  if (!sheets.length) throw new Error("aucune feuille listée pour ce dashboard (dialogue vide et bootstrap muet)");
  let best = null;
  const ordre = attendues ? sheets : sheets.slice().sort((a, b) => (a.name < b.name ? -1 : 1));
  for (const sh of ordre) {
    if (/^_PAGE_TITLE/i.test(sh.name)) continue;
    let vd = null, err = null;
    for (let essai = 1; essai <= 3 && !vd; essai++) {
      try { vd = await viewDataColumns(s, sh.name); } catch (e) { err = e; await sleep(1500 * essai); }
    }
    if (!vd) { log(`  ⚠ ${sh.name} : ${String(err && err.message).replace(/\s+/g, " ").slice(0, 100)}`); continue; }
    if (!vd.columns.length) continue;
    if (!attendues) { best = { sh, vd, score: 1 }; break; } // sans référence : 1re feuille (ordre ASCII) — règle de l'export .csv
    const hit = attendues.filter((c) => vd.captions.map(norm).includes(norm(c))).length;
    const score = hit / attendues.length;
    if (!best || score > best.score) best = { sh, vd, score };
    if (score >= 0.999) break;
  }
  if (!best) throw new Error("aucune feuille exportable");
  if (attendues && best.score < 0.5) log(`  ⚠ ${urlName} : feuille « ${best.sh.name} » ne couvre que ${Math.round(best.score * 100)} % des colonnes attendues.`);
  return best;
}

/**
 * Exporte une feuille « unitaire » pour toutes les zones, par sessions VizQL.
 * @param o { wb, urlName, label, month, year, zones: [valeurs composées], antLabel: {comp→court},
 *            sessions (déf. 6), log, limit (test), profile,
 *            ctx (contexte Playwright déjà ouvert — sync.mjs — réutilisé, jamais fermé ici),
 *            deadline (ms epoch : on ne prend plus de zone au-delà → budgetEpuise=true) }
 * @returns { columns:[…], records:[{Antenne, …}], zonesOk, zonesVides, zonesEchec, sheet, budgetEpuise, minutes }
 */
export async function exportParSessions(o) {
  const log = o.log || ((m) => console.log(m));
  const P = Math.max(1, Math.min(Number(o.sessions || process.env.MASHAKO_SESSIONS || 6), 12));
  const zones = (o.limit ? o.zones.slice(0, o.limit) : o.zones).slice();
  const per = `_PARAM_month=${encodeURIComponent(o.month)}&_PARAM_year=${encodeURIComponent(o.year)}`;
  const thumbsFile = /Antenne/i.test(o.wb || "") ? "thumb-uris.json" : "thumb-uris-zs.json";
  const thumbs = (() => { try { return readFileSync(path.join(HERE, thumbsFile), "utf8"); } catch (e) { return "[]"; } })();
  const ownCtx = !o.ctx;
  const ctx = o.ctx || await launch({ profile: o.profile });
  const out = { columns: [], records: [], zonesOk: 0, zonesVides: 0, zonesEchec: 0, sheet: null, budgetEpuise: false };
  const colSet = new Set();
  const t0 = Date.now();
  let idx = 0; // curseur partagé : chaque session prend la zone suivante
  const prochaine = () => {
    if (o.deadline && Date.now() > o.deadline) { if (!out.budgetEpuise) { out.budgetEpuise = true; log(`  ⏱ ${o.label} : budget temps épuisé — ${idx}/${zones.length} zones traitées, le reste au prochain run.`); } return null; }
    return idx < zones.length ? zones[idx++] : null;
  };

  let choisie = null; // { name, columns } — même feuille pour toutes les sessions du run
  /* Certaines feuilles (Supervision_HZ_P3 : texte + index()) renvoient un export
     VIDE (même sans en-tête) après categorical-filter, alors que l'export juste
     après le bootstrap est bon (constaté 02/09). Pour elles on rouvre une session
     par zone (paramètre d'URL) : plus lent (~30 s/zone) mais juste. Le mode est
     détecté au premier export vide-sans-en-tête et partagé entre les sessions. */
  let reouverture = false;
  async function ouvrir(zone) {
    const s = await openSession(ctx, o.wb, o.urlName, `${per}&_SELECTED_location_level=${encodeURIComponent(zone)}`, { timeout: 240000 });
    s.thumbs = thumbs;
    await crosstabSheets(s, thumbs); // s.sheets : nécessaire à setZone (feuille du dashboard)
    let f;
    if (choisie) f = { sh: { name: choisie.name }, vd: { columns: choisie.columns } };
    else {
      f = await choisirFeuille(s, o.urlName, log, o.slug);
      choisie = { name: f.sh.name, columns: f.vd.columns };
      out.sheet = f.sh.name;
      log(`  ↳ ${o.label} : feuille « ${f.sh.name} » (${f.vd.columns.length} colonnes, session ${(s.ms / 1000).toFixed(0)} s)`);
    }
    return { s, f, courante: zone, zoneOuverture: zone, exportsDepuisFiltre: 0 };
  }

  async function travailleur(n) {
    /* ouvertures DÉCALÉES : trois bootstraps simultanés → deux « session non
       capturée » (constaté 02/09) ; 8 s d'écart suffisent. */
    if (n > 1) await sleep((n - 1) * 8000);
    let w = null, zone = prochaine();
    let echecsConsecutifs = 0;
    while (zone) {
      try {
        if (!w) { w = await ouvrir(zone); }
        else if (w.courante !== zone) {
          if (reouverture) { await w.s.close(); w = await ouvrir(zone); }
          else { await setZone(w.s, zone); w.courante = zone; }
        }
        const csv = await summaryCsv(w.s, w.f.sh.name, w.f.vd.columns);
        const rows = parseCsv(csv.replace(/^﻿/, ""));
        if (!rows.length && !reouverture && w.courante === zone && zone !== w.zoneOuverture) {
          /* export sans en-tête après un changement de zone = le filtre en session
             ne rafraîchit pas cette feuille → on rejoue la zone en rouvrant. */
          reouverture = true;
          log(`  ↺ ${o.label} : export vide après changement de zone — passage en mode « une session par zone » (plus lent, ~30 s/zone).`);
          await w.s.close(); w = null;
          continue;
        }
        const hdr = rows[0] || [];
        const vide = rows.length < 2 || (hdr.length === 1 && /_LABEL$/i.test(String(hdr[0] || "")));
        if (vide) out.zonesVides++;
        else {
          const court = (o.antLabel && o.antLabel[zone]) || zone;
          for (const c of hdr) colSet.add(c);
          for (const rr of rows.slice(1)) {
            const rec = { Antenne: court };
            hdr.forEach((c, i) => { rec[c] = rr[i] ?? ""; });
            out.records.push(rec);
          }
          out.zonesOk++;
        }
        echecsConsecutifs = 0;
        const fait = out.zonesOk + out.zonesVides + out.zonesEchec;
        if (fait % 50 === 0) log(`  … ${o.label} : ${fait}/${zones.length} zones (${((Date.now() - t0) / 60000).toFixed(1)} min, ${P} sessions)`);
        zone = prochaine();
      } catch (e) {
        const msg = String(e.message || e);
        echecsConsecutifs++;
        log(`  ⟳ ${o.label} · ${((o.antLabel && o.antLabel[zone]) || zone).slice(0, 30)} : ${msg.slice(0, 120)} (essai ${echecsConsecutifs})`);
        if (w) { await w.s.close(); w = null; } // session perdue (410/503) ou état douteux → on rouvre
        if (echecsConsecutifs >= 3) { out.zonesEchec++; echecsConsecutifs = 0; zone = prochaine(); }
        await sleep(3000 * echecsConsecutifs);
      }
    }
    if (w) await w.s.close();
  }

  try {
    const n = Math.min(P, zones.length);
    await Promise.all(Array.from({ length: n }, (_, i) => travailleur(i + 1)));
  } finally {
    if (ownCtx) await ctx.close().catch(() => { });
  }
  out.columns = ["Antenne", ...colSet];
  out.minutes = (Date.now() - t0) / 60000;
  log(`  ✓ ${o.label} : ${out.records.length} lignes, ${out.zonesOk} zones avec données, ${out.zonesVides} vides, ${out.zonesEchec} en échec — ${out.minutes.toFixed(1)} min par sessions VizQL`);
  return out;
}
