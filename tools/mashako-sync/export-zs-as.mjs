#!/usr/bin/env node
/** DÉTAIL PAR AIRE DE SANTÉ du classeur ZS (Mashako3_0RapportdelaZone).
 *
 *  Dans l'original Tableau, chaque dashboard ZS affiche pour la zone choisie
 *  une ligne de synthèse puis le tableau de ses aires de santé. Le CSV de
 *  synthèse ne porte que l'agrégat : ce détail ne s'obtient que par l'export
 *  « Tableau croisé » (dialogue → Excel), feuille par feuille.
 *
 *  ── Trois pièges, tous vérifiés le 28/07 ───────────────────────────────────
 *  ① Le filtre _SELECTED_location_level attend la valeur COMPOSÉE
 *    « bu Aketi Zone de Santé », pas le nom court. Avec une valeur invalide le
 *    dashboard ne rend rien et le dialogue répond 200 avec une liste de
 *    feuilles VIDE — ce qui laisse croire que la voie crosstab est morte.
 *    Table : zs_filter_values.json (extract-zs-filter-values.mjs).
 *  ② Le détail AS est paginé à 20 lignes et ne porte pas de colonne « zone de
 *    santé » : un paquet multi-ZS ne rend que les 20 premières aires, sans
 *    moyen de les rattacher. D'où UNE SESSION PAR ZONE, la zone étant estampée
 *    depuis le filtre (_ZS). Les pages suivantes sont les dashboards _P2.
 *  ③ Inutile d'attendre le rendu du canvas (~5 min) : les identifiants VizQL
 *    suffisent (~8 s) et le dialogue est tout aussi complet.
 *
 *  Reprenable : chaque couple (dashboard, zone) traité est inscrit dans
 *  zs_as_ledger.json ; un run interrompu reprend où il s'est arrêté.
 *
 *  Sortie : out-zs/views/<urlName>_AS.json
 *      { name, urlName, source, columns, rows } — chaque ligne porte
 *      _ZS (zone de santé), _ROLE ('AS' détail / 'HZ' synthèse) et _SHEET.
 *
 *  Usage : node export-zs-as.mjs [Mois] [Année]
 *  Env   : MASHAKO_ONLY (urlNames, virgules), MASHAKO_ZS (zones, virgules),
 *          MASHAKO_SHARD ("1/4" pour paralléliser), MASHAKO_MINUTES (déf. 300),
 *          MASHAKO_REPRISE=0 pour ignorer le journal.
 */
import { chromium } from "playwright";
import { readFileSync, writeFileSync, mkdirSync, statSync, rmSync, existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { readXlsx } from "./xlsx-lite.mjs";
import { surveiller, bailAutre } from "./cloud/lease.mjs";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PROFILE = process.env.MASHAKO_PROFILE || path.join(HERE, "browser-profile");
const OUT = process.env.MASHAKO_AS_OUT || path.join(HERE, "out-zs"); // MASHAKO_AS_OUT : backfill d'un mois passé dans son propre dossier
const DIAG = path.join(HERE, "diag-sheets");
const SERVER = "https://eu-west-1a.online.tableau.com";
const SITE = "axdata";
const WB = "Mashako3_0RapportdelaZone";
/* Période : mois CALENDAIRE COURANT par défaut, comme sync.mjs — sans quoi la
   tâche quotidienne resterait figée sur le mois où elle a été écrite et ne
   collecterait jamais le mois suivant. Un mois est explicitable en argument
   (backfill d'une archive). */
const MOIS_FR = ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet", "Août", "Septembre", "Octobre", "Novembre", "Décembre"];
const _maintenant = new Date();
const MONTH = process.argv[2] || process.env.MASHAKO_MONTH || MOIS_FR[_maintenant.getMonth()];
const YEAR = process.argv[3] || process.env.MASHAKO_YEAR || String(_maintenant.getFullYear());
const PER = `_PARAM_month=${encodeURIComponent(MONTH)}&_PARAM_year=${encodeURIComponent(YEAR)}`;
const MAX_MINUTES = Number(process.env.MASHAKO_MINUTES || 300);
const ONLY = (process.env.MASHAKO_ONLY || "").split(",").map((s) => s.trim()).filter(Boolean);
const ZS_ONLY = (process.env.MASHAKO_ZS || "").split(",").map((s) => s.trim()).filter(Boolean);
const SHARD = /^(\d+)\/(\d+)$/.exec(process.env.MASHAKO_SHARD || "");
const REPRISE = process.env.MASHAKO_REPRISE !== "0";
const log = (m) => console.log(`[${new Date().toISOString()}] ${m}`);

/* Pages sans détail AS : titre, navigation, valeurs de filtre, et les vues
   d'ensemble (Résumé, Heatmap, Carte) qui sont des agrégats par zone. */
const IGNORE = /^(Cover Page|Contacts Page|FILTER_VALUES|KPI Manuel|Configuration|Performance |Carte de Supervision)/;
const urlnames = JSON.parse(readFileSync(path.join(HERE, "urlnames-zs.json"), "utf8"));
const THUMBS = readFileSync(path.join(HERE, "thumb-uris-zs.json"), "utf8");
let VUES = Object.entries(urlnames).filter(([k]) => !IGNORE.test(k));
if (ONLY.length) VUES = VUES.filter(([k, u]) => ONLY.includes(u) || ONLY.includes(k));

/* Feuilles à exporter : le détail par aire de santé et sa ligne de synthèse.
   Les tables brutes _TABLE_… du classeur ZS portent l'agrégat de la zone
   sélectionnée — on les prend aussi, elles alimentent les bandeaux. */
/* « _Airesante_supervision_monitoring » (constats et recommandations du
   superviseur, dashboard Supervision_HZ_P3) s'écrit sans espaces. */
const EST_CIBLE = (n) => /aire\s*de\s*sant|airesante|_TABLE|_HZ_total/i.test(n);
const EST_TOTAL = (n) => /_HZ_total|_total$|_TABLE/i.test(n);

const FILTRES = JSON.parse(readFileSync(path.join(HERE, "zs_filter_values.json"), "utf8"));
let ZONES = ZS_ONLY.length ? ZS_ONLY.filter((z) => FILTRES[z]) : Object.keys(FILTRES).sort((a, b) => a.localeCompare(b, "fr"));
if (!ZONES.length) { log("✗ aucune zone de santé exploitable — lancer extract-zs-filter-values.mjs"); process.exit(1); }
if (SHARD) {
  const [, i, n] = SHARD.map(Number);
  ZONES = ZONES.filter((_, k) => k % n === (i - 1) % n);
  log(`Tranche ${i}/${n} : ${ZONES.length} zone(s)`);
}

/* Chaque tranche écrit ses propres fichiers : sans cela, quatre processus
   réécrivent le même JSON et se perdent mutuellement des zones. La fusion se
   fait ensuite (fusion-zs-as.mjs). */
const SUF = SHARD ? `_s${SHARD[1]}` : "";
/* Journal de reprise : « <urlName>|<zone> » → horodatage. */
const LEDGER = path.join(process.env.MASHAKO_AS_OUT ? OUT : HERE, `zs_as_ledger${SUF}.json`); // journal dans le dossier du mois en backfill
let fait = {};
if (REPRISE && existsSync(LEDGER)) { try { fait = JSON.parse(readFileSync(LEDGER, "utf8")); } catch (e) { } }
const cle = (u, z) => `${u}|${z}|${MONTH}-${YEAR}`;

/* Reprise des données déjà exportées, sinon un run repart de zéro. */
const acc = {};
for (const [label, urlName] of VUES) {
  const f = path.join(OUT, "views", `${urlName}_AS${SUF}.json`);
  if (!existsSync(f)) continue;
  try {
    const j = JSON.parse(readFileSync(f, "utf8"));
    if (j.period === `${MONTH} ${YEAR}`) acc[urlName] = { name: label, columns: j.columns || [], rows: j.rows || [] };
  } catch (e) { }
}
const dejaFait = VUES.reduce((n, [, u]) => n + ZONES.filter((z) => fait[cle(u, z)]).length, 0);
log(`${VUES.length} dashboard(s) × ${ZONES.length} zone(s) = ${VUES.length * ZONES.length} exports (${dejaFait} déjà au journal)`);

/* Le profil peut être encore verrouillé par le processus précédent qui se
   ferme (Chrome sort alors en code 21) : trois essais espacés. */
let ctx = null;
for (let essai = 1; essai <= 3 && !ctx; essai++) {
  try {
    ctx = await chromium.launchPersistentContext(PROFILE, {
      channel: "chrome", headless: true, ignoreDefaultArgs: ["--enable-automation"],
      viewport: { width: 1400, height: 900 }, args: ["--no-first-run", "--no-default-browser-check"],
    });
  } catch (e) {
    if (essai === 3) throw e;
    log(`⟳ Chrome n'a pas démarré (essai ${essai}/3) — nouvel essai dans 20 s`);
    await new Promise((r) => setTimeout(r, 20000));
  }
}
/* Session Tableau : les profils d'export (browser-profile-as*) perdent leur session
   Google/Tableau bien avant le profil principal. On injecte la session vivante
   entretenue par reconnecter.mjs (cookies-tableau.json, format Playwright) —
   MASHAKO_INJECT_COOKIES=0 pour s'en passer. Ajouté le 06/09/2026 après
   « session non capturée » sur les 3 profils d'export. */
if (process.env.MASHAKO_INJECT_COOKIES !== "0") {
  const cf = path.join(HERE, "cookies-tableau.json");
  if (existsSync(cf)) {
    try {
      const ck = JSON.parse(readFileSync(cf, "utf8")).filter((c) => c.name && c.value && c.domain)
        .map((c) => ({ name: c.name, value: c.value, domain: c.domain, path: c.path || "/", secure: !!c.secure, httpOnly: !!c.httpOnly,
          sameSite: ["Strict", "Lax", "None"].includes(c.sameSite) ? c.sameSite : "Lax", ...(c.expires && c.expires > 0 ? { expires: c.expires } : {}) }));
      await ctx.addCookies(ck);
      log(`Session Tableau injectée depuis cookies-tableau.json (${ck.length} cookies).`);
    } catch (e) { log(`⚠ Injection des cookies impossible (${String(e.message).slice(0, 80)}) — on continue avec la session du profil.`); }
  }
}
const page = ctx.pages()[0] || await ctx.newPage();
let SID = null, GSH = null, XSRF = null, TVER = null;
/* ⚠ Le trafic de la vue PRÉCÉDENTE est encore en vol quand on navigue vers la
   suivante : sans filtrer, on capture son identifiant de session et le
   dialogue renvoie alors les feuilles de l'autre dashboard (constaté 28/07 :
   Infirmier_HZ_P1 répondait « Aire de Santé Taux d'Abandon »). On n'accepte
   donc que le trafic portant le nom de la vue attendue. */
let VUE_ATTENDUE = "";
const pourVueCourante = (u) => VUE_ATTENDUE && (u.includes(`/v/${VUE_ATTENDUE}/`) || u.includes(`/views/${VUE_ATTENDUE}?`) || u.includes(`/views/${VUE_ATTENDUE}/`));
page.on("response", (r) => {
  try {
    const u = r.url();
    if (!u.includes(`/t/${SITE}/`) || !pourVueCourante(u)) return;
    const h = r.headers();
    if (h["x-session-id"]) SID = h["x-session-id"];
    if (h["global-session-header"]) GSH = h["global-session-header"];
  } catch (e) { }
});
page.on("request", (r) => {
  try {
    const u = r.url();
    const m = /\/sessions\/([0-9A-F]+-\d+:\d+)/i.exec(u);
    if (m && !SID && pourVueCourante(u)) SID = m[1];
    const h = r.headers();
    if (!XSRF && h["x-xsrf-token"]) XSRF = h["x-xsrf-token"];
    if (!TVER && h["x-tableau-version"]) TVER = h["x-tableau-version"];
  } catch (e) { }
});
const mkHeaders = (accept, tab) => {
  const h = {
    accept: accept || "text/javascript", "global-session-header": GSH, "x-xsrf-token": XSRF,
    "x-tableau-version": TVER || "2026.2", "x-requested-with": "XMLHttpRequest",
  };
  if (tab) h["x-tsi-active-tab"] = encodeURIComponent(tab);
  return h;
};
const telemetryId = () => `${Date.now().toString(36)}$${Math.random().toString(36).slice(2, 8)}`;
const post = (u, fields, tab) => page.evaluate(async (a) => {
  const fd = new FormData();
  for (const [k, v] of Object.entries(a.fields)) fd.append(k, v);
  try {
    const r = await fetch(a.u, { method: "POST", body: fd, credentials: "include", headers: a.headers });
    return { st: r.status, txt: (await r.text()).slice(0, 400000) };
  } catch (e) { return { st: 0, err: String(e).slice(0, 140) }; }
}, { u, fields, headers: mkHeaders(null, tab) });

const t0 = Date.now();
/* Verrou partagé avec sync.mjs : un seul processus par profil Chrome. */
const LOCK = path.join(HERE, "out", ".sync.lock");
const partage = PROFILE === path.join(HERE, "browser-profile");
if (partage) {
  try {
    const st = statSync(LOCK);
    if (Date.now() - st.mtimeMs < 2 * 3600 * 1000) { log("⏭ Synchro en cours (verrou récent) — abandon."); await ctx.close(); process.exit(0); }
  } catch (e) { }
  writeFileSync(LOCK, `${new Date().toISOString()} pid=${process.pid} export-zs-as`);
}
const lockTimer = partage ? setInterval(() => { try { writeFileSync(LOCK, `${new Date().toISOString()} pid=${process.pid} export-zs-as`); } catch (e) { } }, 20 * 60000) : null;

/* Bail « as » (cloud/lease.mjs) : coordonne PC ↔ relais cloud (VM/Actions) —
   un seul export AS à la fois, sinon Tableau bride le compte et les deux runs
   échouent. Fail-open : GitHub muet → on travaille quand même. */
const autre = bailAutre("as");
if (autre) { log(`⏭ ${autre.titulaire} collecte déjà le détail AS (battement il y a ${autre.age_min} min) — abandon.`); await ctx.close(); process.exit(0); }
const bail = surveiller("as", { note: `export AS${SHARD ? " " + SHARD[1] + "/" + SHARD[2] : ""}`, tache: "sync" });
if (!bail) log("⚠ Bail « as » non posé (GitHub muet) — on continue (fail-open).");

/* Un tableau croisé Tableau éclate la feuille en plusieurs sous-tables, une
   par bloc de mesures : la même aire de santé revient donc sur 4 à 8 lignes
   ne portant chacune que quelques colonnes. On les fusionne par (zone, aire)
   pour publier UNE ligne complète par aire — ce que le rendu attend. */
const EST_COL_AS = (c) => /·\s*aire de sant/i.test(c);
function consolider(v) {
  const groupes = new Map();
  const ordre = [];
  for (const r of v.rows) {
    /* Le nom de l'aire peut venir de n'importe quel bloc : on prend le premier
       renseigné. Les lignes de synthèse n'en ont pas — une ligne par zone. */
    let nom = r._AS || "";
    if (!nom) for (const [c, val] of Object.entries(r)) { if (EST_COL_AS(c) && String(val ?? "").trim()) { nom = String(val).trim(); break; } }
    const k = r._ROLE === "AS" && nom ? `AS|${r._ZS}|${nom.toLowerCase()}` : `${r._ROLE}|${r._ZS}`;
    let g = groupes.get(k);
    if (!g) { g = { _ZS: r._ZS, _ROLE: r._ROLE, _AS: nom }; groupes.set(k, g); ordre.push(g); }
    for (const [c, val] of Object.entries(r)) {
      if (c === "_SHEET" || c === "_ZS" || c === "_ROLE" || c === "_BLOC" || c === "_AS") continue;
      const s = String(val ?? "").trim();
      if (s !== "" && (g[c] === undefined || g[c] === "")) g[c] = s;
    }
  }
  return ordre;
}

const sauver = () => {
  for (const [u, v] of Object.entries(acc)) {
    const rows = consolider(v);
    writeFileSync(path.join(OUT, "views", `${u}_AS${SUF}.json`), JSON.stringify({
      name: `${v.name} — détail par aire de santé`, urlName: `${u}_AS`, source: u,
      period: `${MONTH} ${YEAR}`, generated_at: new Date().toISOString(),
      zones: [...new Set(rows.map((r) => r._ZS))].sort((a, b) => a.localeCompare(b, "fr")),
      columns: v.columns, rows,
    }));
  }
  writeFileSync(LEDGER, JSON.stringify(fait));
};

let nOk = 0, nVide = 0, nEchec = 0;
const aires = {}; // « <urlName>|<zone> » → nombre d'aires trouvées (pilote le saut des _P2)
try {
  mkdirSync(path.join(OUT, "views"), { recursive: true });
  mkdirSync(DIAG, { recursive: true });

  boucle:
  for (const zone of ZONES) {
    for (const [label, urlName] of VUES) {
      if ((Date.now() - t0) / 60000 > MAX_MINUTES) { log(`⚠ Garde-fou ${MAX_MINUTES} min — arrêt (la progression est au journal).`); break boucle; }
      if (fait[cle(urlName, zone)]) continue;
      /* Les dashboards _P2 sont la 2ᵉ page du détail (20 aires par page) : pour
         une zone dont la page 1 n'est pas pleine, ils n'ont rien à montrer.
         On évite ainsi ~5 sessions inutiles par zone. */
      const p1 = /_P2$/.test(urlName) ? urlName.replace(/_P2$/, "_P1") : null;
      if (p1 && aires[`${p1}|${zone}`] !== undefined && aires[`${p1}|${zone}`] < 20) {
        fait[cle(urlName, zone)] = "page1-suffit";
        continue;
      }
      const tz = Date.now();
      SID = null; GSH = null; VUE_ATTENDUE = urlName;
      await page.goto("about:blank").catch(() => { });
      await page.goto(`${SERVER}/#/site/${SITE}/views/${WB}/${urlName}?${PER}&_SELECTED_location_level=${encodeURIComponent(FILTRES[zone])}`,
        { waitUntil: "domcontentloaded", timeout: 120000 }).catch(() => { });
      const dl = Date.now() + 120000;
      while (Date.now() < dl && !(SID && GSH && XSRF)) await page.waitForTimeout(1500);
      if (!(SID && GSH && XSRF)) { log(`✗ ${zone} · ${label} : session non capturée`); nEchec++; continue; }
      const BASE = `${SERVER}/vizql/t/${SITE}/w/${WB}/v/${urlName}/sessions/${SID}/commands/tabsrv`;

      /* ① dialogue — le serveur peut répondre vide tant que la feuille n'est
         pas chargée : on relance à intervalle court plutôt qu'une longue
         attente fixe. */
      let items = [];
      for (let essai = 1; essai <= 15 && !items.length; essai++) {
        const d = await post(`${BASE}/export-crosstab-server-dialog`, { thumbnailUris: THUMBS, telemetryCommandId: telemetryId() }, label);
        items = [...(d.txt || "").matchAll(/"sheetName"\s*:\s*"([^"]+)"\s*,\s*"sheetdocId"\s*:\s*"([^"]+)"/g)].map((m) => ({ name: m[1], id: m[2] }));
        if (!items.length) await page.waitForTimeout(4000);
      }
      /* Un dialogue TOTALEMENT vide est transitoire (feuilles pas encore
         chargées côté serveur, d'autant plus sous charge) : ne pas l'inscrire
         au journal, sinon la zone serait définitivement perdue. En revanche un
         dialogue qui répond sans feuille de détail est un fait établi. */
      if (!items.length) { log(`✗ ${zone} · ${label} : dialogue vide après 15 essais — sera repris`); nEchec++; continue; }
      const cibles = items.filter((s) => EST_CIBLE(s.name));
      if (!cibles.length) {
        log(`· ${zone} · ${label} : pas de détail par aire (${items.map((s) => s.name).join(" | ")})`);
        fait[cle(urlName, zone)] = "vide"; nVide++;
        continue;
      }

      let lignes = 0;
      const a = acc[urlName] || (acc[urlName] = { name: label, columns: [], rows: [] });
      /* Un ré-export de la même zone remplace ses lignes (pas de doublon). */
      a.rows = a.rows.filter((r) => r._ZS !== zone);
      for (const cible of cibles) {
        const x = await post(`${BASE}/export-crosstab-to-excel-server`,
          { sheetdocId: cible.id, useTabs: "true", sendNotifications: "true", telemetryCommandId: telemetryId() }, label);
        const key = (/"resultKey"\s*:\s*"?([^",}]+)/.exec(x.txt || "") || [])[1];
        if (!key) { log(`  ✗ ${zone} · ${label} · ${cible.name} : export HTTP ${x.st}`); continue; }
        /* Le fichier temporaire n'est pas prêt à l'instant où la commande rend
           sa clé — d'autant moins avec plusieurs exports en parallèle. */
        let b64 = null;
        for (let essai = 1; essai <= 4 && !b64; essai++) {
          b64 = await page.evaluate(async (a2) => {
            const r = await fetch(a2.u, { credentials: "include", headers: a2.h });
            if (!r.ok) return null;
            const b = new Uint8Array(await r.arrayBuffer());
            let s = ""; const C = 0x8000;
            for (let i = 0; i < b.length; i += C) s += String.fromCharCode.apply(null, b.subarray(i, i + C));
            return btoa(s);
          }, { u: `${SERVER}/vizql/t/${SITE}/w/${WB}/v/${urlName}/tempfile/sessions/${SID}?key=${key}&keepfile=yes&attachment=yes`, h: mkHeaders("*/*", label) });
          if (!b64) await page.waitForTimeout(3000);
        }
        if (!b64) { log(`  ✗ ${zone} · ${label} · ${cible.name} : téléchargement refusé (4 essais)`); continue; }

        const buf = Buffer.from(b64, "base64");
        if (process.env.MASHAKO_XLSX) {
          writeFileSync(path.join(DIAG, `AS_${urlName}_${zone}_${cible.name.replace(/[^A-Za-z0-9]+/g, "_").slice(0, 40)}.xlsx`), buf);
        }
        const role = EST_TOTAL(cible.name) ? "HZ" : "AS";
        /* Chaque sous-table du classeur exporté est un PIVOT : le nom de
           colonne porte la VALEUR (« Oui », « Vrai », « Qualité satisfaisante »)
           et le critère n'est identifié que par le rang de la sous-table. Sans
           le conserver, deux critères booléens différents se confondent tous
           les deux sous « Vrai ». On indexe donc les colonnes par bloc. */
        let bloc = 0;
        for (const sh of readXlsx(buf)) {
          if (sh.rows.length < 2) continue;
          bloc++;
          const hdr = sh.rows[0].map((h) => String(h || "").trim());
          for (const rr of sh.rows.slice(1)) {
            if (!rr.some((v) => String(v || "").trim())) continue;
            const o = { _ZS: zone, _ROLE: role, _SHEET: cible.name, _BLOC: bloc };
            hdr.forEach((c, i) => { if (c) o[`b${bloc}·${c}`] = rr[i] ?? ""; });
            a.rows.push(o); lignes++;
          }
          for (const c of hdr) if (c && a.columns.indexOf(`b${bloc}·${c}`) < 0) a.columns.push(`b${bloc}·${c}`);
        }
      }
      aires[`${urlName}|${zone}`] = new Set(a.rows.filter((r) => r._ZS === zone && r._ROLE === "AS")
        .map((r) => { for (const [c, val] of Object.entries(r)) if (/·\s*aire de sant/i.test(c) && String(val ?? "").trim()) return String(val).trim().toLowerCase(); return ""; })
        .filter(Boolean)).size;
      /* Un export qui n'a rien ramené alors que des feuilles étaient annoncées
         est un ÉCHEC (téléchargement refusé, session migrée…) : ne pas
         l'inscrire au journal, sinon la zone ne serait jamais reprise. */
      if (!lignes) { log(`✗ ${zone} · ${label} : ${cibles.length} feuille(s) annoncée(s), 0 ligne — sera repris`); nEchec++; continue; }
      fait[cle(urlName, zone)] = new Date().toISOString();
      nOk++;
      log(`✓ ${zone} · ${label} : ${cibles.length} feuille(s), ${lignes} lignes (${Math.round((Date.now() - tz) / 1000)} s) [${nOk}/${VUES.length * ZONES.length - dejaFait}]`);
      if (nOk % 5 === 0) sauver();
    }
    sauver();
  }
} finally {
  sauver();
  clearInterval(lockTimer);
  await ctx.close().catch(() => { });
  if (partage) rmSync(LOCK, { force: true });
}
log("— Bilan —");
for (const [u, v] of Object.entries(acc)) {
  const z = new Set(v.rows.map((r) => r._ZS));
  const nAS = v.rows.filter((r) => r._ROLE === "AS").length;
  log(`   ${u}_AS${SUF}.json : ${z.size} zone(s), ${v.rows.length} lignes (${nAS} détail AS), ${v.columns.length} colonnes`);
}
log(`— ${nOk} export(s), ${nVide} sans détail, ${nEchec} échec(s) en ${Math.round((Date.now() - t0) / 60000)} min —`);
