#!/usr/bin/env node
/**
 * ARBITRE — le cerveau de la machine de secours.
 *
 * Tourne toutes les 30 min sur la VM (timer systemd). Pour chaque rapport, il
 * se pose trois questions dans cet ORDRE :
 *
 *   1. La synchro du jour est-elle DÉJÀ PUBLIÉE ?        → oui : rien à faire.
 *   2. Un AUTRE titulaire détient-il un bail frais ?      → oui : le PC (ou la
 *      VM) travaille, on ne touche à rien.
 *   3. L'heure prévue + le délai de grâce sont-ils passés ? → non : on laisse
 *      sa chance au PC de Felly, qui reste prioritaire.
 *
 * Si les trois réponses autorisent l'action, la VM prend la main et lance
 * exactement le même sync.mjs que le PC.
 *
 * Cas « PC éteint en pleine synchro » : son bail cesse d'être renouvelé, il
 * périme au bout de 25 min, et le tick suivant de l'arbitre reprend le travail
 * au lieu de tout perdre jusqu'au lendemain.
 *
 * Usage :
 *   node cloud/arbitre.mjs                 → examine ANT puis ZS, agit
 *   node cloud/arbitre.mjs --dry           → explique sans rien lancer
 *   node cloud/arbitre.mjs --canal zs      → un seul rapport
 *   node cloud/arbitre.mjs --tache backfill --canal ant
 *   node cloud/arbitre.mjs --force         → ignore l'heure et la publication
 */
import { execFileSync, spawn } from "node:child_process";
import { appendFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { bailAutre, surveiller, liberer, lire, TITULAIRE, FRAIS_MS } from "./lease.mjs";

const ICI = path.dirname(fileURLToPath(import.meta.url));
const RACINE = path.resolve(ICI, "..");
const REPO = process.env.MASHAKO_REPO || "repos/MBOMBOmamu1993/snis-vaccination-api";
const TZ = process.env.MASHAKO_TZ || "Africa/Kinshasa";
const LOG = path.join(RACINE, "arbitre.log");

/* Horaires des tâches planifiées du PC (heure de Kinshasa). La VM attend
   GRACE minutes de plus : le PC garde toujours la priorité.
   « as » = détail par aire de santé : il dépend du classeur ZS ; on laisse
   au PC jusqu'à 14h45 (sa passe ZS finit vers 13h, ses workers AS suivent). */
const HORAIRES = { ant: 7 * 60, zs: 10 * 60 + 30, as: 14 * 60 };
const HORAIRES_BACKFILL = { ant: 20 * 60, zs: 23 * 60 + 30 };
const GRACE_MIN = Number(process.env.MASHAKO_GRACE_MIN || 45);

const args = process.argv.slice(2);
const opt = (n, d) => { const i = args.indexOf(n); return i >= 0 ? args[i + 1] : d; };
/* --decider : comme --dry, mais écrit MASHAKO_A_FAIRE=0/1 dans $GITHUB_ENV.
   Sert au workflow à ne réveiller Chrome et Tableau QUE s'il y a du travail :
   une ronde de rattrapage qui n'a rien à faire coûte alors 5 secondes et
   n'ajoute aucune charge sur le compte Tableau. */
const DECIDER = args.includes("--decider");
const DRY = args.includes("--dry") || DECIDER;
const FORCE = args.includes("--force");
const TACHE = opt("--tache", "sync");
/* 03/09/2026 : sans --canal, le BACKFILL examine la ZS d'abord — c'est elle qui
   a 13 périodes à compléter (7 feuilles unitaires × 519 zones), alors que la
   nuit du 02/09 les runs bihoraires ont dépensé leurs créneaux sur l'Antenne
   (Vaccine_expiration_ANT_P2, feuille vide sur Tableau) pendant que juillet ZS
   attendait. La synchro quotidienne garde l'ordre Antenne → ZS. */
const CANAUX = opt("--canal") ? [opt("--canal")] : (TACHE === "backfill" ? ["zs", "ant", "as"] : ["ant", "zs", "as"]);

function log(msg) {
  const ligne = `[${new Date().toISOString()}] ${msg}`;
  console.log(ligne);
  try { appendFileSync(LOG, ligne + "\n"); } catch (_) { }
}

function gh(args) {
  return execFileSync("gh", ["api", ...args], { encoding: "utf8", maxBuffer: 8 * 1024 * 1024, stdio: ["ignore", "pipe", "pipe"] });
}

/* ── Heure locale de Kinshasa, indépendamment du fuseau de la VM ────────── */
function partsKinshasa(d = new Date()) {
  const f = new Intl.DateTimeFormat("fr-FR", {
    timeZone: TZ, year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", hour12: false,
  }).formatToParts(d).reduce((o, p) => (o[p.type] = p.value, o), {});
  return { jour: `${f.year}-${f.month}-${f.day}`, minutes: Number(f.hour) * 60 + Number(f.minute) };
}

/**
 * Date (jour de Kinshasa) de la dernière publication du rapport, lue dans le
 * meta.json publié sur la branche mashako-data. On passe par l'API GitHub et
 * non par raw.githubusercontent, dont le CDN sert jusqu'à 5 min de retard.
 */
function jourDernierePublication(canal) {
  /* AS : pas de meta.json — la preuve est le generated_at du détail fusionné
     (zs/views/Supervision_HZ_P1_AS.json, toujours produit par la publication). */
  const chemin = canal === "zs" ? "zs/meta.json" : canal === "as" ? "zs/views/Supervision_HZ_P1_AS.json" : "meta.json";
  try {
    const brut = gh([`${REPO}/contents/${chemin}?ref=mashako-data`, "--jq", ".content"]);
    const meta = JSON.parse(Buffer.from(brut.trim(), "base64").toString("utf8"));
    if (!meta.generated_at) return null;
    const p = partsKinshasa(new Date(meta.generated_at));
    return { jour: p.jour, minutes: p.minutes, quand: meta.generated_at, feuilles: (meta.views || []).length, zones: (meta.zones || []).length };
  } catch (e) {
    log(`⚠ meta.json (${canal}) illisible : ${String(e.stderr || e.message).slice(0, 200)}`);
    return null; /* dans le doute on considère « pas publié » → la VM tentera */
  }
}

/* 03/09/2026 : spawnSync BLOQUAIT la boucle d'événements → le setInterval de
   surveiller() ne tirait jamais, le bail périmait après 25 min alors que le
   cloud travaillait (bail « périmé depuis 247 min » constaté en plein run) et un
   PC allumé pouvait reprendre le canal en parallèle. Lancement asynchrone. */
function attendre(cmd, args, opts) {
  return new Promise((resolve) => {
    const p = spawn(cmd, args, { cwd: opts.cwd, env: opts.env, stdio: "inherit" });
    const t = opts.timeout ? setTimeout(() => { try { p.kill("SIGTERM"); } catch (_) { } }, opts.timeout) : null;
    p.on("exit", (code, sig) => { if (t) clearTimeout(t); resolve({ status: code == null ? (sig ? 137 : 1) : code }); });
    p.on("error", (e) => { if (t) clearTimeout(t); log(`✖ lancement impossible : ${e.message}`); resolve({ status: 1 }); });
  });
}

async function lancer(canal, tache) {
  /* AS : export reprenable (journal zs_as_ledger*.json, porté par le cache
     Actions) plafonné à 4 h 30, puis fusion + publication — même enchaîné que
     la tâche locale de 06h15. */
  if (canal === "as") {
    log(`▶ Prise de relais AS : export-zs-as.mjs puis publish-zs-as.mjs — titulaire ${TITULAIRE}`);
    const bail = surveiller(canal, { note: "arbitre détail AS", tache: "sync" });
    if (!bail) log("⚠ Bail non posé (GitHub muet) — on lance quand même (fail-open).");
    let r = await attendre(process.execPath, [path.join(RACINE, "export-zs-as.mjs")], {
      cwd: RACINE, env: { ...process.env, MASHAKO_ROLE: process.env.MASHAKO_ROLE || "vm", MASHAKO_HEADLESS: "1", MASHAKO_MINUTES: "270" }, stdio: "inherit", timeout: 300 * 60 * 1000,
    });
    /* On publie même un export partiel : le journal de reprise le complètera
       au prochain relais ; chaque zone publiée est gagnée pour le dashboard. */
    const p = await attendre(process.execPath, [path.join(RACINE, "publish-zs-as.mjs"), "--fusion"], { cwd: RACINE, env: process.env, stdio: "inherit", timeout: 30 * 60 * 1000 });
    const code = r.status === 0 ? p.status : r.status;
    liberer(canal, code === 0 ? "succès" : `code ${code}`);
    log(`■ Fin détail AS — export ${r.status}, publication ${p.status}`);
    return code;
  }
  const script = tache === "backfill" ? "backfill-periods.mjs" : "sync.mjs";
  const env = {
    ...process.env,
    MASHAKO_ROLE: process.env.MASHAKO_ROLE || "vm",
    MASHAKO_HEADLESS: "1",
    ...(canal === "zs" ? { MASHAKO_CFG: "zs" } : {}),
  };
  log(`▶ Prise de relais : ${script} (${canal}) — titulaire ${TITULAIRE}`);
  /* Le bail est posé ici et renouvelé toutes les 10 min par surveiller() tant que
     l'enfant tourne ; s'il meurt, notre libération à la sortie remet le canal libre. */
  const bail = surveiller(canal, { note: `arbitre ${tache}`, tache });
  if (!bail) log("⚠ Bail non posé (GitHub muet) — on lance quand même (fail-open).");
  const r = await attendre(process.execPath, [path.join(RACINE, script), "--background"], {
    cwd: RACINE, env, stdio: "inherit", timeout: 20 * 3600 * 1000,
  });
  liberer(canal, r.status === 0 ? "succès" : `code ${r.status}`);
  log(`■ Fin ${script} (${canal}) — code ${r.status}`);
  return r.status;
}

async function examiner(canal) {
  const { jour, minutes } = partsKinshasa();
  const heure = (TACHE === "backfill" ? HORAIRES_BACKFILL : HORAIRES)[canal];
  const prefixe = `${canal.toUpperCase()} ${TACHE}`;
  if (heure == null) { log(`${prefixe} — pas de créneau ${TACHE} pour ce canal — ignoré.`); return "ignore"; }

  /* 1. Déjà fait aujourd'hui ? (le backfill n'a pas de marqueur : il s'auto-
        suspend tout seul avec « rien à faire ».) */
  if (TACHE === "sync" && !FORCE) {
    const pub = jourDernierePublication(canal);
    /* ⚠ Une passe ZS dure jusqu'à 17 h : lancée à 10h30, elle publie vers 4 h
       du matin LE LENDEMAIN. Une publication datée d'aujourd'hui mais ANTÉRIEURE
       à l'heure prévue est donc le fruit du run de la veille — elle ne dispense
       pas de la synchro du jour. ⚠ AS : l'inverse — la publication quotidienne
       a lieu tôt (06h15, avant l'heure de grâce) et compte pour AUJOURD'HUI. */
    const deja = canal === "as" ? (pub && pub.jour === jour) : (pub && pub.jour === jour && pub.minutes >= heure);
    if (deja) {
      log(`${prefixe} — déjà publié aujourd'hui (${pub.feuilles || pub.zones || "?"} ${canal === "as" ? "zones" : "feuilles"}, ${pub.quand}). Rien à faire.`);
      return "deja";
    }
    if (pub && pub.jour === jour) {
      log(`${prefixe} — dernière publication ${pub.quand}, antérieure à l'heure prévue : c'est le run de la veille qui vient de finir.`);
    }
  }

  /* 2. Quelqu'un d'autre travaille ? */
  const autre = bailAutre(canal);
  if (autre) {
    log(`${prefixe} — ${autre.titulaire} travaille (battement il y a ${autre.age_min} min). On n'y touche pas.`);
    return "occupe";
  }

  /* 3. Le PC a-t-il eu sa chance ? */
  if (!FORCE && minutes < heure + GRACE_MIN) {
    const reste = heure + GRACE_MIN - minutes;
    log(`${prefixe} — prévu à ${String(Math.floor(heure / 60)).padStart(2, "0")}h${String(heure % 60).padStart(2, "0")}, on laisse ${reste} min de plus au PC.`);
    return "attente";
  }

  if (DRY) { log(`${prefixe} — ✅ il y a du travail : le secours prendrait la main (rien lancé).`); return "dry"; }
  return (await lancer(canal, TACHE)) === 0 ? "fait" : "echec";
}

/**
 * Cette machine est-elle DÉJÀ en train de synchroniser ?
 * Indispensable : le timer du backfill peut se déclencher pendant qu'une passe
 * ZS de 17 h tourne encore (deux unités systemd distinctes). Deux Chrome sur le
 * même profil = profil corrompu et exports perdus. Mon propre bail, renouvelé
 * toutes les 10 min par le processus qui travaille, fait office de verrou.
 */
function occupeParMoiMeme() {
  for (const c of ["ant", "zs"]) {
    const b = lire(c);
    if (b && b.etat === "actif" && b.titulaire === TITULAIRE
      && Date.now() - Date.parse(b.battement || b.debut || 0) < FRAIS_MS) return b;
  }
  return null;
}

log(`— Arbitre (${TITULAIRE}) : ${CANAUX.join(", ")} / ${TACHE}${DRY ? " [dry]" : ""}${FORCE ? " [force]" : ""} —`);
const moi = occupeParMoiMeme();
if (moi && !DRY) {
  log(`⏭ Cette machine synchronise déjà « ${moi.canal} » (${moi.note || "en cours"}, battement ${moi.battement}) — on ne lance rien d'autre.`);
  if (DECIDER && process.env.GITHUB_ENV) appendFileSync(process.env.GITHUB_ENV, "MASHAKO_A_FAIRE=0\n");
} else {
  const verdicts = [];
  for (const c of CANAUX) {
    try { verdicts.push(await examiner(c)); } catch (e) { log(`✖ ${c} : ${e.stack || e}`); }
  }
  if (DECIDER && process.env.GITHUB_ENV) {
    const aFaire = verdicts.includes("dry") ? "1" : "0";
    /* Le canal est transmis au workflow : inutile de relancer les deux quand
       un seul a du travail. */
    const canalUtile = CANAUX[verdicts.indexOf("dry")] || "";
    appendFileSync(process.env.GITHUB_ENV, `MASHAKO_A_FAIRE=${aFaire}\nMASHAKO_CANAL_UTILE=${canalUtile}\n`);
    log(`→ décision transmise au workflow : ${aFaire === "1" ? "AGIR sur " + canalUtile : "rien à faire"}`);
  }
}
