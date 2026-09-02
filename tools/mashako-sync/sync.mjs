#!/usr/bin/env node
/**
 * Synchro locale « Plan Mashako 3.0 » v3 — toutes les feuilles + filtre Antenne.
 *
 * Sans PAT (désactivés par l'admin axdata) : réutilise la session Google SSO du
 * profil dédié browser-profile/ (connexion unique via login.cmd, Chrome normal).
 *
 * Chaque exécution :
 *   1. ouvre le classeur → keep-alive licence ;
 *   2. liste les feuilles via la barre d'onglets Tableau ;
 *   3. IMAGES PROPRES : export PNG natif de Tableau (fetch dans la page, avec la
 *      session) — pas de barre d'outils, pas de capture pendant chargement.
 *      Le nom d'URL de chaque feuille est deviné depuis son libellé et validé ;
 *      en dernier recours, capture d'écran (avec attente de fin de rendu) ;
 *   4. FILTRE ANTENNE : liste des antennes lue dans FILTER_VALUES_ANT, champ de
 *      filtre validé sur une feuille test (les hashes doivent différer), puis
 *      export de chaque feuille × chaque antenne via `?<champ>=<antenne>` ;
 *   5. publie le tout sur la branche `mashako-data` (UN commit, ref forcée —
 *      l'historique ne grossit pas) → lu par l'onglet via raw.githubusercontent.
 *
 * Usage : node sync.mjs
 */
import { chromium } from "playwright";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync, appendFileSync, rmSync, statSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PROFILE = path.join(HERE, "browser-profile");

/* ── DEUX CLASSEURS, UN SEUL SCRIPT : ANT (défaut) et ZS (env MASHAKO_CFG=zs).
   Le rapport Zone de Santé est publié sous le préfixe zs/ de la même branche
   mashako-data ; mêmes règles (mois courant forcé, archives periods/ figées). ── */
const IS_ZS = process.env.MASHAKO_CFG === "zs";
const OUT = path.join(HERE, IS_ZS ? "out-zs" : "out");
const LOG = path.join(HERE, IS_ZS ? "sync-zs.log" : "sync.log");

const SERVER = "https://eu-west-1a.online.tableau.com";
const SITE = "axdata";
/* ⚠ Classeur ZS = « Mashako 3.0 Rapport de la Zone » (jumeau exact du rapport
   Antenne : 30 feuilles, mêmes visuels, suffixe _HZ au lieu de _ANT). Ne pas
   confondre avec « RapportdeZonedeSantPlanMashako » (rapport PDF de supervision,
   utilisé par erreur le 25/07 → écarté par Felly). */
const WORKBOOK = IS_ZS ? "Mashako3_0RapportdelaZone" : "Mashako3_0RapportdelAntenne";
const MAIN_VIEW = IS_ZS ? "PerformanceRsum_HZ" : "HZScores_ANT";
const PFX = IS_ZS ? "zs/" : "";
const URLCACHE_FILE = path.join(HERE, IS_ZS ? "urlnames-zs.json" : "urlnames.json");
const BEST_COUNT_FILE = path.join(HERE, IS_ZS ? "best_count_zs.json" : "best_count.json");
/* Aucune feuille écartée : les deux classeurs sont pris en entier (demande Felly
   du 25/07 — « considérer toutes les feuilles de ce lien »). */
const SKIP_SHEETS = null;
const UI_URL = `${SERVER}/#/site/${SITE}/views/${WORKBOOK}/${MAIN_VIEW}`;
const REPO = "repos/MBOMBOmamu1993/snis-vaccination-api";
const DATA_BRANCH = "mashako-data";
/* Feuilles sans intérêt par localisation (pages fixes / techniques) — couvre les
   deux classeurs (FILTER_VALUES_ANT côté ANT, FILTERpage côté ZS) */
const NO_ANT = /^(Cover ?Page|Contacts ?Page|FILTER|Configuration)/i;
/* Feuilles dont le CSV est inexploitable (texte/notes non exportés par Tableau) :
   PAS d'export data — image PAR ANTENNE à la place, pour que le filtre marche.
   ⚠ Dispo_vaccins_ANT et Vaccine_expiration_ANT_P1 en ont été RETIRÉS le 26/07
   (demande Felly : ces deux feuilles doivent être des tableaux vivants, pas des
   images) → leur CSV est de nouveau exporté par antenne.
   KPI Manuel_* (P1/P2/P3, ANT comme HZ) ajoutés le 28/07 (précision Felly) : ce
   sont des NOTES EXPLICATIVES d'interprétation des indicateurs, il n'y a AUCUN
   tableau ni donnée derrière — les « aucune donnée pour cette période » des runs
   étaient donc normaux, et chaque export CSV tenté était du temps perdu. */
const FORCE_IMG = /^(Supervision_ANT_P2|KPI\s*Manuel)/i;
/* Feuilles ABSENTES de la barre d'onglets mais bien publiées (accessibles par
   URL) : Ranking_ANT est le classement officiel des antennes — il était
   RECONSTRUIT à partir de la Heatmap, d'où des scores vides en mois courant
   (constaté 26/07). On l'exporte désormais tel quel. Ces feuilles sont globales
   (pas de filtre localisation) : un seul export, pas un par antenne. */
const EXTRA_SHEETS = IS_ZS ? {} : { "Ranking_ANT": "Ranking_ANT" };
/* Garde-fou durée totale. ANT : phase data groupée (multi-valeurs, ~15 min) +
   images par antenne (~1 h 30) — budget large pour ne plus couper la phase
   images (la coupure à 180 min provoquait le crash ENOENT du 26/07).
   ZS : couverture complète groupée en un run. Réglable par MASHAKO_MINUTES. */
const MAX_MINUTES = Number(process.env.MASHAKO_MINUTES || (IS_ZS ? 300 : 300));
/* Concurrence de la phase DONNÉES. Les rendus d'images saturent Tableau à 3,
   mais les CSV sont bien plus légers : côté ZS (519 zones, une passe complète à
   3 en parallèle = 32 h) on monte à 6, ce qui divise le temps par deux environ. */
const DATA_PAR = Number(process.env.MASHAKO_PAR || (IS_ZS ? 6 : 3));

const md5 = (b) => createHash("md5").update(b).digest("hex");

function log(msg) {
  const line = `[${new Date().toISOString()}]${IS_ZS ? " [ZS]" : ""} ${msg}`;
  console.log(line);
  try { appendFileSync(LOG, line + "\n"); } catch (e) { }
}
/* Ne plus JAMAIS mourir en silence : le 26/07, quatre runs ont disparu sans
   aucune trace après le pré-contrôle. Toute erreur non attrapée est
   journalisée (et donc visible au prochain diagnostic) avant de quitter. */
process.on("unhandledRejection", (e) => { try { log(`✖ ERREUR NON GÉRÉE (promesse) : ${(e && e.stack) || e}`); } catch (_) { } });
process.on("uncaughtException", (e) => { try { log(`✖ ERREUR NON GÉRÉE : ${(e && e.stack) || e}`); } catch (_) { } });
function notify(text) {
  try {
    execFileSync("powershell", ["-NoProfile", "-Command",
      `(New-Object -ComObject Wscript.Shell).Popup('${text.replace(/'/g, "''")}', 120, 'Synchro Plan Mashako 3.0', 48)`],
      { timeout: 125000 });
  } catch (e) { }
}
/* ⚠ Un appel API GitHub sur des centaines peut hoqueter (réseau instable) :
   « error connecting » (25/07), « TLS handshake timeout » (26/07), « malformed
   request » 400 transitoire (27/07 — vérifié rejouable à la main). Sans rejeu,
   un seul hoquet jetait des heures d'exports. On réessaie donc les erreurs de
   transport avec pause croissante (un vrai 4xx se reproduirait à l'identique). */
const GH_RETRIABLE = /timeout|connection|connect|reset|EOF|handshake|temporarily|502|503|504|malformed|400/i;
function gh(args, inputFile) {
  const a = ["api", ...args];
  if (inputFile) a.push("--input", inputFile);
  let last;
  for (let essai = 1; essai <= 5; essai++) {
    try {
      return execFileSync("gh", a, { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
    } catch (e) {
      last = e;
      const msg = String((e.stderr || "") + (e.message || ""));
      if (!GH_RETRIABLE.test(msg) || essai === 5) throw e;
      const pause = 2000 * essai;
      log(`  ⟳ appel API GitHub en échec (${msg.trim().slice(0, 80)}) — nouvel essai ${essai + 1}/5 dans ${pause / 1000} s`);
      const fin = Date.now() + pause; while (Date.now() < fin) { }
    }
  }
  throw last;
}
function slug(label) {
  return label.normalize("NFD").replace(/[̀-ͯ]/g, "")
    .replace(/[^A-Za-z0-9_-]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 60) || "feuille";
}
function parseCsv(text) {
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
function vizFrame(page) {
  return page.frames().find((f) => f.url().includes(`/t/${SITE}/views/`)) || null;
}
/* Variantes possibles du nom d'URL Tableau d'une feuille (espaces/apostrophes
   retirés ; avec puis sans accents). */
function urlNameCandidates(label) {
  const noSpace = label.replace(/['\s]+/g, "");
  const noAccents = noSpace.normalize("NFD").replace(/[̀-ͯ]/g, "");
  /* Tableau supprime souvent entièrement les lettres accentuées du nom d'URL
     (« Résumé » → « Rsum ») */
  const stripped = noSpace.replace(/[^\x20-\x7E]/g, "");
  /* Tableau retire aussi la ponctuation : « Seances: réalisation_1 » →
     « Seancesralisation_1 » (constaté 25/07 sur le classeur ZS). */
  const alnum = stripped.replace(/[^\w]/g, "");
  return [...new Set([noSpace, noAccents, stripped, alnum, noAccents.replace(/[^\w]/g, "")])];
}

/* --background (ou variable MASHAKO_HEADLESS=1) → Chrome invisible : la tâche
   planifiée quotidienne tourne ainsi sans fenêtre à l'écran. La session vit
   dans le profil, donc headless réutilise les mêmes cookies. */
const HEADLESS = process.argv.includes("--background") || process.env.MASHAKO_HEADLESS === "1";

/* ── PÉRIODE CIBLE = MOIS CALENDAIRE COURANT, forcée sur TOUS les exports.
   Le classeur garde l'ancien mois en vue par défaut plusieurs jours après la
   bascule (constaté 25/07 : défaut = Juin alors que Juillet a des données) →
   sans _PARAM explicite la synchro quotidienne resterait sur le mois passé.
   Les mois passés sont FIGÉS dans periods/<AAAA-MM>/ (backfill) ; la synchro
   quotidienne ne rafraîchit que le mois en cours. ── */
const MOIS_FR = ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet", "Août", "Septembre", "Octobre", "Novembre", "Décembre"];
const _now = new Date();
/* Mois/année de référence : figés si MASHAKO_MONTH/YEAR sont fournis (backfill),
   sinon MOIS CALENDAIRE COURANT — pour les deux classeurs (demande Felly du
   25/07 : le rapport ZS doit afficher le mois en cours par défaut, comme
   l'Antenne). ⚠ Un mois en cours est partiellement saisi : côté ZS beaucoup de
   feuilles de détail renvoient alors un CSV VIDE (1 octet, HTTP 200 — ce n'est
   PAS un refus serveur : vérifié le 25/07, Aba = 13 lignes en Juillet contre 97
   en Juin sur CDF_HZ_P1, et certaines ZS comme Adi n'ont aucune donnée même en
   Juin). Les mois complets restent accessibles via les archives periods/. */
let CUR_MONTH = process.env.MASHAKO_MONTH || MOIS_FR[_now.getMonth()];
let CUR_YEAR = process.env.MASHAKO_YEAR || String(_now.getFullYear());
const PERIOD_FIXED = !!(process.env.MASHAKO_MONTH || process.env.MASHAKO_YEAR);
let PERIOD_QS = `_PARAM_month=${encodeURIComponent(CUR_MONTH)}&_PARAM_year=${encodeURIComponent(CUR_YEAR)}`;
function setPeriod(month, year) {
  CUR_MONTH = month; CUR_YEAR = String(year);
  PERIOD_QS = `_PARAM_month=${encodeURIComponent(CUR_MONTH)}&_PARAM_year=${encodeURIComponent(CUR_YEAR)}`;
}

/* Verrou PARTAGÉ entre ANT, ZS et backfills : un seul processus utilise le
   profil Chrome à la fois. */
const LOCK = path.join(HERE, "out", ".sync.lock");

async function main() {
  /* Verrou : une seule synchro à la fois (sinon le rmSync de démarrage de l'une
     efface les fichiers de l'autre → ENOENT / publications corrompues). */
  mkdirSync(OUT, { recursive: true });
  try {
    const st = statSync(LOCK);
    if (Date.now() - st.mtimeMs < 2 * 3600 * 1000) {
      log("⏭ Une autre synchro est déjà en cours (verrou récent) — abandon.");
      return;
    }
  } catch (e) { /* pas de verrou */ }

  /* Déjà réussi aujourd'hui ? → on ne re-synchronise pas (évite de recharger
     Tableau et de risquer un nouveau throttling). Les essais de rattrapage
     matinaux s'auto-annulent une fois qu'un a abouti. */
  try {
    const b = JSON.parse(readFileSync(BEST_COUNT_FILE, "utf8"));
    const today = new Date().toISOString().slice(0, 10);
    /* MASHAKO_FORCE=1 : relance manuelle le même jour (plus besoin d'antidater
       best_count.json à la main pour compléter un run interrompu). */
    if (process.env.MASHAKO_FORCE === "1") log("⚙ MASHAKO_FORCE=1 — garde « déjà synchronisé aujourd'hui » ignorée.");
    else if (b.count >= 20 && String(b.at).slice(0, 10) === today) {
      log(`⏭ Déjà synchronisé aujourd'hui (${b.count} feuilles le ${b.at}) — rien à faire.`);
      return;
    }
  } catch (e) { /* pas encore de succès enregistré */ }

  writeFileSync(LOCK, new Date().toISOString() + " pid=" + process.pid);

  /* Heartbeat du verrou : ce run dure souvent ~10 h, or les concurrents
     (backfill, autre synchro) considèrent le verrou périmé après 2 h → ils
     lançaient Chrome sur le profil occupé et plantaient (crashs 27-29/07).
     On retouche le verrou toutes les 15 min pour signaler que ce run vit. */
  const heartbeat = setInterval(() => {
    try { writeFileSync(LOCK, new Date().toISOString() + " pid=" + process.pid); } catch (e) { }
  }, 15 * 60000);
  heartbeat.unref?.();

  rmSync(path.join(OUT, "views"), { recursive: true, force: true });
  mkdirSync(path.join(OUT, "views"), { recursive: true });
  log(`— Début synchro Mashako 3.0 v3 (feuilles + antennes)${HEADLESS ? " [arrière-plan]" : ""} —`);
  const t0 = Date.now();
  /* ⏸ VEILLE DU PC EXCLUE DU BUDGET (crash du 28/07) : le PC s'est mis en
     veille 6,5 h pendant la phase data ZS → au réveil, le garde-fou 300 min
     (basé sur l'heure horloge) a coupé la synchro alors qu'il restait ~5 h de
     travail utile. On mesure désormais le temps ACTIF : tout trou > 30 min
     entre deux itérations est compté comme une veille et exclu du budget. */
  let veilleMs = 0, dernierTick = Date.now();
  const tick = () => {
    const n = Date.now(), gap = n - dernierTick; dernierTick = n;
    if (gap > 30 * 60000) { veilleMs += gap; log(`⏸ Pause de ${Math.round(gap / 60000)} min détectée (veille ?) — exclue du budget temps.`); }
  };
  const minutesActives = () => (Date.now() - t0 - veilleMs) / 60000;

  /* Le profil Chrome peut être encore verrouillé par la synchro précédente en
     cours de fermeture (crash exitCode 21 du 28/07 à 06:40, 19 s après la fin
     du run ZS) : 3 essais espacés de 30 s avant d'abandonner. */
  let ctx = null, launchErr = null;
  for (let essai = 1; essai <= 3 && !ctx; essai++) {
    try {
      ctx = await chromium.launchPersistentContext(PROFILE, {
        channel: "chrome", headless: HEADLESS, ignoreDefaultArgs: ["--enable-automation"],
        viewport: { width: 1600, height: 950 }, acceptDownloads: true,
        args: ["--window-position=10,10", "--window-size=1680,1050", "--no-first-run", "--no-default-browser-check"],
      });
    } catch (e) {
      launchErr = e;
      log(`  ⟳ Lancement de Chrome impossible (essai ${essai}/3 : ${String(e.message || e).slice(0, 80)})${essai < 3 ? " — nouvel essai dans 30 s…" : ""}`);
      if (essai < 3) await new Promise((r) => setTimeout(r, 30000));
    }
  }
  if (!ctx) { rmSync(LOCK, { force: true }); throw launchErr; }
  const page = ctx.pages()[0] || await ctx.newPage();

  try {
    await page.goto(UI_URL, { waitUntil: "domcontentloaded", timeout: 120000 }).catch(() => { });
    /* Attente ACTIVE de la vue Tableau (l'iframe /t/axdata/views met souvent
       20-40 s à apparaître) — ne pas conclure « session expirée » trop tôt.
       Vraie expiration = redirection vers une page de connexion. */
    /* ⚠ La ré-authentification SILENCIEUSE passe TRANSITOIREMENT par
       accounts.google.com : voir cette URL une fois ne veut PAS dire que la
       session est morte (faux positif constaté le 26/07 à 14h33 — la vue se
       chargeait en 37 s et les exports répondaient 200 juste après). On exige
       donc que la page de connexion PERSISTE (5 relevés = 15 s) avant de
       conclure. */
    let frame = null, loginSeen = 0;
    const vizDeadline = Date.now() + 120000;
    while (Date.now() < vizDeadline) {
      await page.waitForTimeout(3000);
      frame = vizFrame(page);
      if (frame) break;
      const u = page.url();
      if (/signin|\/login|accounts\.google|identity|SAMLRequest/i.test(u)) {
        if (++loginSeen >= 5) {
          notify("Session Tableau expiree. Lance login.cmd, connecte-toi avec Google, FERME la fenetre avec la croix, puis relance.");
          throw new Error("Redirigé vers la connexion pendant 15 s — session expirée (refaire login.cmd).");
        }
      } else loginSeen = 0;
    }
    if (!frame) {
      notify("Dashboard Tableau trop long a charger (ou session incertaine). Reessai a la prochaine synchro.");
      throw new Error("Vue Tableau introuvable après 90 s (chargement lent ou session).");
    }
    log("✓ Session valide — licence maintenue.");

    /* fetch binaire DANS la page (cookies + referer natifs) → base64 ou null.
       Double garde-temps : AbortController côté page + course Playwright côté
       Node (si la page se fige, frame.evaluate ne rend jamais la main → le
       timer Node tranche pour que le script ne gèle JAMAIS). */
    /* ── CAUSES D'ÉCHEC ────────────────────────────────────────────────────
       fetchBin renvoyait `null` quelle que soit la raison : refus HTTP, page
       d'erreur HTML, délai dépassé, ou iframe de la viz détachée. Faute de
       distinction, le 26/07 à 10h49 on a conclu au « bridage Tableau » alors
       que 10 feuilles × 51 antennes sont tombées en 3,7 s — impossible pour
       des refus réseau (408 requêtes) : c'était l'iframe de la viz qui s'était
       détachée, et `frame.evaluate` rejetait instantanément. Vérifié : 45 min
       plus tard, les mêmes exports répondaient 200 en 30-45 s.
       On compte donc les causes, on les journalise, et on répare l'iframe. ── */
    const ERRS = {};
    const errReset = () => { for (const k of Object.keys(ERRS)) delete ERRS[k]; };
    const errSummary = () => Object.keys(ERRS).sort((a, b) => ERRS[b] - ERRS[a]).map((k) => `${k}×${ERRS[k]}`).join(", ");
    let frameLost = 0;
    async function reviveFrame() {
      log("  ⟳ contexte perdu — rechargement de la page…");
      try {
        await page.goto(UI_URL, { waitUntil: "domcontentloaded", timeout: 120000 });
      } catch (e) { /* le rechargement peut expirer sans être fatal */ }
      /* ⚠ 26/07 16h00 : se contenter de « l'iframe existe » relançait la boucle
         toutes les 16 s — la frame réapparaissait en 3 s mais la viz n'était pas
         encore bootstrapée, donc chaque fetch mourait aussitôt. On attend le
         RENDU RÉEL (canvas présent, plus de modal de chargement). */
      const dlF = Date.now() + 180000;
      while (Date.now() < dlF) {
        await page.waitForTimeout(3000);
        const f2 = vizFrame(page);
        if (!f2) continue;
        const st = await f2.evaluate(() => ({
          load: /Ouverture du classeur|Processus en cours/i.test(document.body.innerText || ""),
          canvas: !!document.querySelector("canvas"),
        })).catch(() => null);
        if (st && st.canvas && !st.load) {
          frame = f2; frameLost = 0;
          log("  ✓ vue rechargée et rendue — reprise.");
          return true;
        }
      }
      log("  ✖ vue toujours pas rendue après rechargement.");
      return false;
    }
    /* ── OÙ PART LE FETCH ? LA PAGE, PAS L'IFRAME (corrigé 26/07 17h) ────────
       L'iframe de la viz se détache toute seule (Tableau la recycle) et emporte
       le contexte d'exécution : c'est l'origine des deux effondrements du 26/07
       (« inconnu×102 », puis la boucle perdue/retrouvée toutes les 16 s).
       Or la PAGE DE TÊTE est sur le MÊME ORIGINE que l'URL d'export, donc elle
       envoie exactement les mêmes cookies. Mesuré (probe-pagefetch.mjs) sur la
       même session : Taux d'abandon_ANT = 1363 octets dans les deux cas, 19 s
       depuis la page contre 23 s depuis l'iframe. On fetch donc depuis la page,
       l'iframe ne servant plus que de secours et de témoin de session. ── */
    async function fetchBin(url, timeout) {
      const scope = page;
      frame = vizFrame(page) || frame;
      const evalP = scope.evaluate(async (args) => {
        const ctrl = new AbortController();
        const to = setTimeout(() => ctrl.abort(), args.timeout);
        try {
          const r = await fetch(args.url, { credentials: "include", signal: ctrl.signal });
          clearTimeout(to);
          const ct = (r.headers.get("content-type") || "").toLowerCase();
          if (!r.ok) return { fail: `http-${r.status}` };
          if (ct.includes("html")) return { fail: "page-html" };
          const buf = new Uint8Array(await r.arrayBuffer());
          let bin = ""; const CH = 0x8000;
          for (let i = 0; i < buf.length; i += CH) bin += String.fromCharCode.apply(null, buf.subarray(i, i + CH));
          return { b64: btoa(bin), ct, n: buf.length };
        } catch (e) { clearTimeout(to); return { fail: /abort/i.test(String(e)) ? "delai-fetch" : "exception-fetch" }; }
      }, { url, timeout }).catch((e) => ({
        fail: /detached|destroyed|Execution context|Target closed|Frame was/i.test(String(e)) ? "contexte-detruit" : "eval-ko",
      }));
      const guard = new Promise((res) => setTimeout(() => res({ fail: "delai-garde" }), timeout + 20000));
      const r = await Promise.race([evalP, guard]);
      /* ⚠ UNE RÉPONSE VIDE EST UNE RÉPONSE (corrigé 26/07 19h) ────────────────
         `if (r && r.b64)` prenait une chaîne base64 VIDE pour un échec : les
         feuilles statiques (KPI Manuel_ANT_P1/P2/P3, Contacts, Configuration)
         exportent un CSV de 0 octet, donc b64 === "" — falsy. D'où les « 51
         refus, 0 vides » et les causes fantômes « inconnu×102 » puis
         « eval-vide×102 » : le serveur répondait 200 à chaque fois.
         Sonde probe-evalvide.mjs : KPI Manuel_ANT_P1 → 200 text/csv, 0 octet,
         12 s (seul comme à 3 en parallèle) ; HZ Scores_ANT → 1733 octets, 31 s.
         On teste donc le TYPE, pas la vérité : "" est un contenu valide. */
      if (r && typeof r.b64 === "string") { frameLost = 0; return r; }
      /* `evaluate` peut aussi RÉSOUDRE avec undefined quand le contexte est
         détruit pendant l'appel : ce n'est pas un rejet, le .catch ne le voit
         pas. Cas réel mais rare — à distinguer du vide ci-dessus. */
      const why = (r && r.fail) || "eval-vide";
      ERRS[why] = (ERRS[why] || 0) + 1;
      if (why === "contexte-detruit" || why === "eval-ko" || why === "eval-vide") {
        if (++frameLost >= 3) await reviveFrame();
      }
      return null;
    }
    const exportUrl = (urlName, ext, params) =>
      `${SERVER}/t/${SITE}/views/${WORKBOOK}/${encodeURIComponent(urlName)}.${ext}?${PERIOD_QS}` +
      (params ? "&" + params : "");
    log(`→ Période cible : ${CUR_MONTH} ${CUR_YEAR} (forcée sur tous les exports).`);

    /* Exécution par petits lots (2 fetches simultanés MAX) : Tableau sérialise
       les rendus d'une même session — à 6 en parallèle, tout part en file
       d'attente et dépasse les délais (constaté : 100 % de timeouts). */
    async function runBatch(items, size, fn) {
      const out = [];
      for (let i = 0; i < items.length; i += size) {
        const chunk = items.slice(i, i + size); /* batchs de 2 */
        out.push(...await Promise.all(chunk.map(fn)));
      }
      return out;
    }

    // ── PRÉ-CONTRÔLE THROTTLING : un seul export test (feuille principale).
    //    S'il échoue, le quota Tableau est épuisé → on abandonne en ~90 s au lieu
    //    de marteler 15 min de requêtes vouées à l'échec. Rend les essais de
    //    rattrapage rapides et sans surcharge. ──
    {
      let probeName = MAIN_VIEW, probeExt = "png", isImg = true;
      let uc = null;
      try { uc = JSON.parse(readFileSync(URLCACHE_FILE, "utf8")); } catch (e) { }
      if (uc && !IS_ZS && uc["HZ Scores_ANT"]) probeName = uc["HZ Scores_ANT"];
      /* Sonder en CSV sur une feuille LÉGÈRE plutôt qu'en PNG sur la vue
         principale : le rendu image du tableau de bord ZS prend ~150 s (mesuré
         25/07), soit le délai lui-même → « seconde tentative » systématique et
         5 min perdues à chaque run. Le CSV de FILTER_VALUES répond en ~20 s et
         prouve exactement la même chose (session + exports disponibles). */
      const light = uc && (uc["FILTER_VALUES"] || uc["FILTER_VALUES_ANT"]);
      if (light) { probeName = light; probeExt = "csv"; isImg = false; }
      const okProbe = (p) => p && (isImg ? p.ct.includes("image") : !p.ct.includes("html"));
      let probe = await fetchBin(exportUrl(probeName, probeExt), 170000);
      if (!okProbe(probe)) {
        log("… Pré-contrôle lent — seconde tentative.");
        probe = await fetchBin(exportUrl(probeName, probeExt), 170000);
      }
      if (!okProbe(probe)) {
        notify("Tableau bride encore les exports (quota). Nouvel essai automatique plus tard — rien a faire.");
        throw new Error("Pré-contrôle : exports Tableau indisponibles (quota/throttle) — abandon rapide, réessai à la prochaine synchro.");
      }
      log("✓ Pré-contrôle OK — les exports Tableau répondent.");
    }

    // ── Feuilles (barre d'onglets), attente patiente ──
    let tabs = [];
    const dl = Date.now() + 90000;
    while (Date.now() < dl && !tabs.length) {
      frame = vizFrame(page) || frame;
      tabs = await frame.evaluate(() =>
        [...document.querySelectorAll(".tabLabel[id^='tableauTabbedNavigation_tab_']")]
          .map((el) => ({ id: el.id, label: (el.getAttribute("value") || el.textContent || "").trim() }))
          .filter((t) => t.label)
      ).catch(() => []);
      if (!tabs.length) await page.waitForTimeout(4000);
    }
    if (!tabs.length) throw new Error("Aucun onglet détecté après 90 s.");
    if (SKIP_SHEETS) tabs = tabs.filter((t) => !SKIP_SHEETS.test(t.label));
    log(`→ ${tabs.length} feuilles${SKIP_SHEETS ? " (OVM/surveillance/annexe exclues)" : ""}.`);

    // ── Résolution PILOTÉE PAR LE CACHE, sans navigation (les clics d'onglets
    //    figeaient le navigateur). Nom en cache → 1 requête PNG directe ; sinon
    //    on essaie quelques variantes du libellé. Aucune feuille ne bloque le run.
    const CACHE_FILE = URLCACHE_FILE;
    let urlCache = {};
    try { urlCache = JSON.parse(readFileSync(CACHE_FILE, "utf8")); } catch (e) { }
    const sheets = await runBatch(tabs, 3, async (t) => {
      const s = slug(t.label);
      let resolved = null, png = null;
      /* Le nom en cache d'abord, mais on garde les variantes en secours : un
         export peut échouer parce que Tableau était occupé (constaté 25/07 :
         PerformanceRsum_HZ refusé en parallèle puis accepté seul), et une
         entrée de cache erronée ne doit pas condamner la feuille. */
      const cands = urlCache[t.label]
        ? [urlCache[t.label], ...urlNameCandidates(t.label).filter((c) => c !== urlCache[t.label])]
        : urlNameCandidates(t.label);
      /* ── VALIDATION PAR CSV, PAS PAR IMAGE ────────────────────────────────
         Le rendu image d'un tableau de bord Tableau prend 150 s et échoue
         régulièrement ; le CSV répond en ~20-45 s. Tant que la survie d'une
         feuille dépendait de son image, un simple ralentissement la faisait
         DISPARAÎTRE du dashboard avec toutes ses données (constaté 25-26/07 :
         Performance Résumé_ANT, Livraison_ANT_P1, Vaccine_dispo_HZ_P1/P2).
         Désormais le CSV valide la feuille ; l'image n'est qu'un aperçu, et
         reste le seul recours pour les pages sans données (KPI Manuel,
         Contacts Page, qui n'exportent aucun CSV). ── */
      for (const cand of (urlCache[t.label] ? [urlCache[t.label]] : cands.slice(0, 2))) {
        const c = await fetchBin(exportUrl(cand, "csv", ":refresh=yes"), 120000);
        if (c && !c.ct.includes("html")) { resolved = cand; break; }
      }
      for (const cand of (resolved ? [resolved] : cands)) {
        /* 150 s : à 90 s, les nuits où le serveur est lent, la moitié des
           feuilles partait en timeout (constaté 25/07). */
        const r = await fetchBin(exportUrl(cand, "png"), 150000);
        if (r && r.ct.includes("image")) { resolved = resolved || cand; png = Buffer.from(r.b64, "base64"); break; }
      }
      if (resolved) urlCache[t.label] = resolved;
      log(`  ${resolved ? "✓" : "✗"} ${t.label}${resolved ? "" : " : à rattraper"}`);
      return { label: t.label, tabId: t.id, slug: s, urlName: resolved, defaultPng: png };
    });
    /* RATTRAPAGE SÉQUENTIEL : Tableau refuse parfois un rendu lourd quand 3
       exports tournent en parallèle sur la même session (constaté 25/07 :
       « Performance Résumé_HZ » sort seul au pré-contrôle mais échoue en lot).
       On rejoue les feuilles manquantes UNE PAR UNE avant de les abandonner. */
    const missing = sheets.filter((s) => !s.urlName);
    if (missing.length) {
      log(`→ Rattrapage séquentiel de ${missing.length} feuille(s) : ${missing.map((s) => s.label).join(", ")}…`);
      for (const sh of missing) {
        const cands = urlCache[sh.label]
          ? [urlCache[sh.label], ...urlNameCandidates(sh.label).filter((c) => c !== urlCache[sh.label])]
          : urlNameCandidates(sh.label);
        /* LE CSV D'ABORD : il répond en ~20 s là où le rendu image demande 150 s
           et échoue régulièrement. Une feuille validée en CSV est sauvée même si
           son image ne sort pas — c'est ce qui a fait disparaître
           « Performance Résumé_ANT » et « Livraison_ANT_P1 » du mois de juillet
           (constaté 26/07 : 29 feuilles en juin, 27 en juillet). */
        for (const cand of cands) {
          const c = await fetchBin(exportUrl(cand, "csv", ":refresh=yes"), 120000);
          if (c && !c.ct.includes("html")) { sh.urlName = cand; urlCache[sh.label] = cand; break; }
        }
        for (const cand of (sh.urlName ? [sh.urlName] : cands)) {
          const r = await fetchBin(exportUrl(cand, "png"), 170000);
          if (r && r.ct.includes("image")) {
            sh.urlName = cand; sh.defaultPng = Buffer.from(r.b64, "base64");
            urlCache[sh.label] = cand;
            break;
          }
        }
        /* Certaines feuilles ne sortent JAMAIS en image (rendu trop lourd :
           Vaccine_dispo_HZ_P1/P2 dépassent 170 s) alors que leur CSV répond très
           bien. Le nom d'URL est ce qui compte : on le valide en CSV et la
           feuille rejoint la synchro sans image par défaut — le dashboard la
           rend de toute façon en HTML à partir des données. */
        if (!sh.urlName) {
          for (const cand of cands) {
            const r = await fetchBin(exportUrl(cand, "csv", ":refresh=yes"), 170000);
            if (r && !r.ct.includes("html")) {
              sh.urlName = cand; urlCache[sh.label] = cand;
              log(`  ✓ ${sh.label} (rattrapée en CSV — pas d'image, rendu HTML uniquement)`);
              break;
            }
          }
          if (sh.urlName) continue;
        }
        log(`  ${sh.urlName ? "✓" : "✗"} ${sh.label}${sh.urlName ? " (rattrapée)" : " : non exportable (ignorée)"}`);
      }
    }
    writeFileSync(CACHE_FILE, JSON.stringify(urlCache, null, 2));
    {
      /* Contrôle de complétude : toute feuille déjà connue (cache des noms) et
         absente de ce run est signalée — c'est le symptôme des disparitions. */
      const got = new Set(sheets.filter((s) => s.urlName).map((s) => s.label));
      const abs = Object.keys(urlCache).filter((k) => !got.has(k));
      if (abs.length) log("⚠ Feuilles connues absentes de ce run : " + abs.join(", "));
    }

    /* ── Feuilles hors barre d'onglets (EXTRA_SHEETS) : publiées mais non
       listées par Tableau. Elles sont GLOBALES (aucun filtre localisation) →
       un seul export, pas un par antenne. ── */
    for (const [label, urlName] of Object.entries(EXTRA_SHEETS)) {
      if (sheets.some((s) => s.label === label)) continue;
      const r = await fetchBin(exportUrl(urlName, "png"), 170000);
      const png = r && r.ct.includes("image") ? Buffer.from(r.b64, "base64") : null;
      let ok = !!png;
      if (!ok) {
        const c = await fetchBin(exportUrl(urlName, "csv", ":refresh=yes"), 170000);
        ok = !!(c && !c.ct.includes("html"));
      }
      if (!ok) { log(`  ✗ ${label} (hors barre d'onglets) : non exportable`); continue; }
      sheets.push({ label, tabId: null, slug: slug(label), urlName, defaultPng: png, global: true });
      log(`  ✓ ${label} (hors barre d'onglets, feuille globale)`);
    }

    // ── Localisations : liste depuis les feuilles FILTER_VALUES* (CSV).
    //    ANT : une seule feuille (FILTER_VALUES_ANT, 51 antennes).
    //    ZS  : TROIS feuilles (FILTER_VALUES, _2, _3) qui se partagent les ~519
    //          zones de santé → il faut cumuler les trois, sinon un tiers de la
    //          RDC manque. Valeurs composées « bu Aketi Zone de Santé » ;
    //          libellé court publié = « Aketi » (cf. antLabel). ──
    let antField = null, antennes = [], antLabel = {}, antMeta = {};
    const lbl = (a) => antLabel[a] || a;
    /* « bu Aketi Zone de Santé » → « Aketi » (même normalisation que le topojson
       des ZS du dashboard) ; « Bas Uele_Buta » → « Buta » côté antennes. */
    const shortLoc = (v, court) => {
      if (court) return court;
      let s = String(v || "").trim().replace(/\s*zones?\s+de\s+sant[eé]\s*$/i, "").trim();
      s = s.replace(/^[a-z]{2,3}\s+/, "").trim();
      return s || String(v || "").trim();
    };
    for (const fv of sheets.filter((s) => /^FILTER_VALUES/i.test(s.label) && s.urlName)) {
      const r = await fetchBin(exportUrl(fv.urlName, "csv"), 150000);
      if (!r) { log(`  ⚠ ${fv.label} : CSV indisponible.`); continue; }
      const rows = parseCsv(Buffer.from(r.b64, "base64").toString("utf8"));
      if (rows.length < 2) continue;
      const cols = rows[0];
      /* Le VRAI champ de filtre est _SELECTED_location_level (valeurs composées
         « Province_Antenne » / « bu Aketi Zone de Santé ») — la colonne
         « Antenne En » ne matche que « Buta » (constaté 25/07). On filtre par la
         valeur composée mais on affiche/publie le nom court. */
      const si = cols.findIndex((c) => /SELECTED_location_level/i.test(c));
      let ci = cols.findIndex((c) => /antenne/i.test(c));
      if (ci < 0 && si < 0) ci = 0;
      const vi = si >= 0 ? si : ci;
      if (vi < 0) continue;
      antField = cols[vi];
      const pi = cols.findIndex((c) => /province/i.test(c));
      for (const rr of rows.slice(1)) {
        const v = (rr[vi] || "").trim();
        if (!v || antennes.includes(v)) continue;
        antennes.push(v);
        /* Côté ZS la colonne « Antenne En » porte l'ANTENNE de rattachement, pas
           le nom court de la ZS → on ne l'utilise comme libellé que côté ANT. */
        const court = (!IS_ZS && ci >= 0) ? (rr[ci] || "").trim() : "";
        antLabel[v] = shortLoc(v, court);
        antMeta[v] = {
          province: pi >= 0 ? (rr[pi] || "").trim() : "",
          antenne: IS_ZS && ci >= 0 ? (rr[ci] || "").trim() : "",
        };
      }
      log(`  ✓ ${fv.label} : ${rows.length - 1} lignes (cumul ${antennes.length}).`);
    }
    antennes.sort((a, b) => lbl(a).localeCompare(lbl(b), "fr"));
    if (antennes.length) log(`✓ FILTER_VALUES : champ « ${antField} », ${antennes.length} ${IS_ZS ? "zones de santé" : "antennes"}${antennes.length <= 60 ? " : " + antennes.map(lbl).join(", ") : ""}`);
    if (!antennes.length) {
      log("⚠ Liste des antennes indisponible (CSV FILTER_VALUES vide) — export vue par défaut.");
    }

    // ── PRÉ-SCAN + VALIDATION en une passe : on teste le filtre sur la feuille
    //    principale pour CHAQUE antenne (pas juste 2 au hasard — les premières
    //    alphabétiques, Aru/Bandundu, sont souvent vides). Une antenne dont le
    //    CSV filtré est non vide = antenne active ; si les signatures diffèrent
    //    entre antennes, le filtre agit réellement. Le champ de filtre est aussi
    //    déterminé ici (variantes de casse/espaces testées sur la 1re passe). ──
    let filterOK = false;
    let activeAnt = antennes;
    if (antField && antennes.length) {
      /* Feuille test du filtre : il FAUT une feuille qui exporte du CSV — les
         KPI Manuel n'en ont jamais (repli du 25/07 → « aucune antenne avec
         données » à tort). Préférences connues, puis tout sauf KPI. */
      const PREF = IS_ZS
        ? ["Performance_Resume_HZ", "Supervision_HZ_P1", "CDF_HZ_P1", "Infirmier_HZ_P1", "Seances_HZ_P1"]
        : ["HZ_Scores_ANT", "Infirmier_ANT", "Taux_d_abandon_ANT", "CDF_Problemes_ANT", "Reunion_ANT"];
      const mainSheet = PREF.map((p) => sheets.find((s) => s.slug === p && s.urlName)).find(Boolean)
        || sheets.find((s) => s.urlName && !NO_ANT.test(s.label) && !/^KPI/i.test(s.label))
        || sheets.find((s) => s.urlName && !NO_ANT.test(s.label));
      if (mainSheet) {
        // Choisir la variante de nom de champ qui renvoie des données non vides
        let field = antField;
        const variants = [...new Set([antField, antField.replace(/\s+/g, "_"), antField.replace(/\s+/g, ""), "Antenne", "ANTENNE"])];
        const probe = antennes.slice(0, 12); // échantillon pour trouver le bon champ vite
        for (const fn of variants) {
          let hit = false;
          for (const ant of probe) {
            const r = await fetchBin(exportUrl(mainSheet.urlName, "csv",
              `${encodeURIComponent(fn)}=${encodeURIComponent(ant)}&:refresh=yes`), 150000);
            if (r && parseCsv(Buffer.from(r.b64, "base64").toString("utf8")).length > 1) { hit = true; break; }
          }
          if (hit) { field = fn; break; }
        }
        antField = field;
        /* ZS : 519 localisations → un pré-scan exhaustif coûterait ~30 min de
           requêtes avant même la phase data. On valide sur un ÉCHANTILLON (les
           signatures doivent différer) et on considère toutes les ZS actives ;
           celles qui n'ont pas de données seront simplement ignorées au fil de
           l'eau par la phase data (rows < 2). */
        if (IS_ZS && antennes.length > 80) {
          const smp = [antennes[0], antennes[Math.floor(antennes.length / 3)], antennes[Math.floor(antennes.length / 2)], antennes[antennes.length - 1]];
          const sig = [];
          for (const a of smp) {
            const r = await fetchBin(exportUrl(mainSheet.urlName, "csv",
              `${encodeURIComponent(field)}=${encodeURIComponent(a)}&:refresh=yes`), 150000);
            if (r) {
              const txt = Buffer.from(r.b64, "base64").toString("utf8");
              if (parseCsv(txt).length > 1) sig.push(md5(txt));
            }
          }
          filterOK = new Set(sig).size >= 2;
          log(`${filterOK ? "✓" : "⚠"} Filtre « ${field} » ${filterOK ? "validé" : "NON discriminant"} sur ${sig.length}/${smp.length} échantillons (${smp.map(lbl).join(", ")}) — ${antennes.length} zones de santé retenues.`);
        } else {
        log(`→ Pré-scan des ${antennes.length} antennes (champ « ${field} »)…`);
        const sigs = {};
        await runBatch(antennes, 3, async (ant) => {
          const r = await fetchBin(exportUrl(mainSheet.urlName, "csv",
            `${encodeURIComponent(field)}=${encodeURIComponent(ant)}&:refresh=yes`), 150000);
          if (r) {
            const txt = Buffer.from(r.b64, "base64").toString("utf8");
            if (parseCsv(txt).length > 1) sigs[ant] = md5(txt);
          }
        });
        const found = Object.keys(sigs);
        const distinct = new Set(Object.values(sigs));
        if (found.length >= 1 && (found.length === 1 || distinct.size >= 2 || found.length < antennes.length)) {
          activeAnt = antennes.filter((a) => found.includes(a));
          filterOK = true;
          log(`✓ Filtre actif — ${activeAnt.length} antennes avec données : ${activeAnt.map(lbl).join(", ")}`);
        } else if (found.length && distinct.size === 1) {
          log("⚠ Le filtre ne discrimine pas (données identiques partout) — export vue par défaut.");
        } else {
          log("⚠ Aucune antenne avec données — export vue par défaut.");
        }
        }
      }
    }

    // ── Feuilles susceptibles de porter des variantes par antenne ──
    const globalSheets = sheets.filter((s) => s.urlName && s.global);
    const antSheets = sheets.filter((s) => s.urlName && !NO_ANT.test(s.label) && !s.global);
    const antImages = {}; // slugFeuille → { antenne → chemin } (rempli APRÈS la phase data)

    // ── DONNÉES VIVANTES : CSV par antenne pour les feuilles de données ──
    //    (cartes/pages fixes exclues). Export lent unitairement → lots de 5 en
    //    parallèle, long timeout. Résultat : un JSON par feuille, toutes
    //    antennes confondues, colonne « Antenne » ajoutée → le dashboard rend
    //    de vraies tables filtrables, pas des captures.
    const dataFiles = {}; // slugFeuille → { file, rows }
    /* Feuilles globales (Ranking_ANT…) : un export unique, sans colonne Antenne
       — le classement est le même pour tout le monde. */
    for (const sh of globalSheets) {
      const r = await fetchBin(exportUrl(sh.urlName, "csv", ":refresh=yes"), 170000);
      if (!r) { log(`  ✗ ${sh.label} : pas de CSV`); continue; }
      const rows = parseCsv(Buffer.from(r.b64, "base64").toString("utf8"));
      if (rows.length < 2) { log(`  ✗ ${sh.label} : CSV vide`); continue; }
      const columns = rows[0];
      const records = rows.slice(1).map((rr) => { const o = {}; columns.forEach((c, i) => { o[c] = rr[i] ?? ""; }); return o; });
      const rel = `views/${sh.slug}.json`;
      writeFileSync(path.join(OUT, rel), JSON.stringify({ name: sh.label, urlName: sh.slug, columns, rows: records }));
      dataFiles[sh.slug] = { file: rel, rows: records.length };
      log(`  ✓ ${sh.label} (globale) : ${records.length} lignes`);
    }
    const dataSheets = antSheets.filter((s) => !FORCE_IMG.test(s.label)); // cartes incluses ; feuilles à CSV inexploitable exclues (image par antenne)
    if (!filterOK && dataSheets.length) {
      /* Filtre URL indisponible → CSV de la vue par défaut quand même, pour que
         les tables vivantes existent. */
      log(`→ Export data (vue par défaut) : ${dataSheets.length} feuilles…`);
      await runBatch(dataSheets, 3, async (sh) => {
        const r = await fetchBin(exportUrl(sh.urlName, "csv", ":refresh=yes"), 160000);
        if (!r) { log(`  ✗ ${sh.label} : pas de CSV`); return; }
        const rows = parseCsv(Buffer.from(r.b64, "base64").toString("utf8"));
        if (rows.length < 2) return;
        const columns = rows[0];
        const records = rows.slice(1).map((rr) => { const o = {}; columns.forEach((c, i) => { o[c] = rr[i] ?? ""; }); return o; });
        const rel = `views/${sh.slug}.json`;
        writeFileSync(path.join(OUT, rel), JSON.stringify({ name: sh.label, urlName: sh.slug, columns, rows: records }));
        dataFiles[sh.slug] = { file: rel, rows: records.length };
        log(`  ✓ ${sh.label} : ${records.length} lignes`);
      });
    }
    /* ── ZS : ~519 localisations. AVANT le 27/07 : boucle « une ZS, toutes ses
       feuilles » (~225 s/ZS, ~11 h le total) → couverture étalée sur ~7 nuits
       avec registre. DÉSORMAIS : BATCHING MULTI-VALEURS (validé le 27/07 —
       probe-multicsv.mjs/probe-multizs.mjs) : l'export .csv accepte plusieurs
       valeurs de filtre séparées par des virgules → paquets de ~100 ZS par
       requête, ~132 requêtes au total, couverture COMPLÈTE en un seul run.
       L'attribution de la colonne « Antenne » (= nom court de la ZS) se lit
       directement dans les lignes (« Zone de santé » = valeur composite) :
       aucune carte externe nécessaire. La fusion avec les données en ligne
       reste en place (par feuille) : une ZS absente d'un paquet garde ses
       lignes publiées. ── */
    const ZS_BULK = IS_ZS && filterOK && antennes.length > 80;
    const zsDone = [];           // ZS rafraîchies ce run (data + images)
    let zsCoverage = null;       // libellés de ZS présents dans les données publiées
    if (ZS_BULK && dataSheets.length) {
      const LEDGER = path.join(HERE, "zs_ledger.json");
      let ledger = {};
      try { ledger = JSON.parse(readFileSync(LEDGER, "utf8")); } catch (e) { }
      let cible = activeAnt.slice();
      const only = (process.env.MASHAKO_ZS_ONLY || "").split(",").map((s) => s.trim()).filter(Boolean);
      if (only.length) cible = activeAnt.filter((a) => only.some((q) =>
        lbl(a).toLowerCase() === q.toLowerCase() || a.toLowerCase().includes(q.toLowerCase())));
      const lim = Number(process.env.MASHAKO_ZS_LIMIT || 0);
      if (lim > 0) cible = cible.slice(0, lim);
      const PACK = Number(process.env.MASHAKO_ZS_PACK || 100);
      const packs = [];
      for (let i = 0; i < cible.length; i += PACK) packs.push(cible.slice(i, i + PACK));
      log(`→ Export data ZS : ${dataSheets.length} feuilles × ${cible.length} zones de santé (${packs.length} paquets de ≤${PACK}, multi-valeurs)…`);
      const acc = {};            // slugFeuille → { label, columns:[], rows:[] }
      const refreshedBySheet = {}; // slugFeuille → Set<ZS courtes rafraîchies>
      const globalDone = new Set(); // ZS (libellés courts) vues dans au moins une feuille
      /* Repli unitaire (une ZS à la fois) pour une feuille dont la réponse
         groupée ne porte pas de colonne « Zone de santé » — ne devrait jamais
         servir, mais garantit le format publié quoi qu'il arrive.
         ⚠ Parallélisé ×3 (27/07) : en séquence pure, 519 ZS ≈ 1,5-2 h PAR
         feuille « collapse » — inviable. ×3 reste sous le seuil de file
         d'attente du serveur (voir la note de runBatch). */
      const pullZsLegacy = async (sh) => {
        /* ── MOTEUR VizQL (02/09/2026) : cf. exportSheetLegacy côté Antenne.
           Ici c'est LE poste de coût de la synchro ZS (7 feuilles × 519 zones à
           ~33 s par URL ≈ 8-13 h) → ~13 s/zone à 6 sessions ≈ 20 min/feuille. ── */
        if (process.env.MASHAKO_SESSION !== "0") {
          try {
            const { exportParSessions } = await import("./vizql-export.mjs");
            const budget = Math.max(5, MAX_MINUTES - minutesActives());
            const r = await exportParSessions({
              ctx, wb: WORKBOOK, urlName: sh.urlName, slug: sh.slug, label: sh.label, month: CUR_MONTH, year: CUR_YEAR,
              zones: cible, antLabel, log, deadline: Date.now() + budget * 60000,
            });
            tick();
            const traitees = r.zonesOk + r.zonesVides;
            if (r.budgetEpuise || traitees >= cible.length * 0.9) {
              const a = acc[sh.slug] || (acc[sh.slug] = { label: sh.label, columns: [], rows: [] });
              for (const c of r.columns) if (c !== "Antenne" && a.columns.indexOf(c) < 0) a.columns.push(c);
              for (const o of r.records) {
                a.rows.push(o);
                (refreshedBySheet[sh.slug] = refreshedBySheet[sh.slug] || new Set()).add(o.Antenne);
                globalDone.add(o.Antenne);
              }
              log(`    ✓ ${sh.label} : ${r.records.length} lignes par sessions VizQL (${r.zonesOk}/${cible.length} ZS avec données, ${r.minutes.toFixed(1)} min)`);
              return r.records.length;
            }
            log(`    ⚠ ${sh.label} : moteur VizQL incomplet (${traitees}/${cible.length} ZS) — repli export par URL.`);
          } catch (e) { log(`    ⚠ ${sh.label} : moteur VizQL indisponible (${String(e.message || e).slice(0, 120)}) — repli export par URL.`); }
        }
        let nRows = 0, lotsVides = 0;
        for (let i = 0; i < cible.length; i += 60) {
          tick();
          if (minutesActives() > MAX_MINUTES) break;
          const lots = await runBatch(cible.slice(i, i + 60), 3, async (ant) => {
            const r = await fetchBin(exportUrl(sh.urlName, "csv",
              `${encodeURIComponent(antField)}=${encodeURIComponent(ant)}&:refresh=yes`), 150000);
            if (!r) return null;
            const rows = parseCsv(Buffer.from(r.b64, "base64").toString("utf8"));
            return rows.length > 1 ? { ant, rows } : null;
          });
          let lotRows = 0;
          for (const lot of lots) {
            if (!lot) continue;
            const a = acc[sh.slug] || (acc[sh.slug] = { label: sh.label, columns: [], rows: [] });
            const hdr = lot.rows[0];
            for (const c of hdr) if (a.columns.indexOf(c) < 0) a.columns.push(c);
            for (const rr of lot.rows.slice(1)) {
              const o = { Antenne: lbl(lot.ant) };
              hdr.forEach((c, ii) => { o[c] = rr[ii] ?? ""; });
              a.rows.push(o); nRows++; lotRows++;
            }
            (refreshedBySheet[sh.slug] = refreshedBySheet[sh.slug] || new Set()).add(lbl(lot.ant));
            globalDone.add(lbl(lot.ant));
          }
          /* WATCHDOG THROTTLING (28/07) : « Carte de Supervision_HZ » a passé
             des HEURES à recevoir des réponses vides à la chaîne (le serveur
             bride le compte) — 106 lignes en 6 h, pendant que les 21 autres
             feuilles attendaient. 2 paquets de 60 ZS consécutifs sans AUCUNE
             ligne = bridage (ou feuille vide pour la période) : on arrête
             cette feuille, les lignes déjà collectées sont conservées et
             fusionnées, la feuille sera reprise au prochain run. */
          if (!lotRows) {
            if (++lotsVides >= 2) {
              log(`    ⛔ ${sh.label} : 120 ZS consécutives sans aucune ligne — throttling présumé. Feuille interrompue à ${i}/${cible.length} ZS (${nRows} lignes conservées, reprise au prochain run).`);
              break;
            }
          } else lotsVides = 0;
          log(`    … ${sh.label} : ${Math.min(i + 60, cible.length)}/${cible.length} ZS (${nRows} lignes)`);
        }
        return nRows;
      };
      /* Liste blanche ZS issue de validate-zs-batch.mjs : les feuilles qui
         COLLAPSENT en groupé (verdict ok:false) partent directement en
         unitaire. Fichier absent ou feuille inconnue → groupé (défaut). */
      let zsVerdicts = null;
      try { zsVerdicts = JSON.parse(readFileSync(path.join(HERE, "zs_batch_verdicts.json"), "utf8")).verdicts; } catch (e) { }
      if (zsVerdicts) {
        const ko = Object.keys(zsVerdicts).filter((k) => zsVerdicts[k] && zsVerdicts[k].ok === false);
        if (ko.length) log(`→ Liste blanche ZS active : ${ko.length} feuille(s) en unitaire (${ko.join(" | ")}).`);
      }
      for (const sh of dataSheets) {
        tick();
        if (minutesActives() > MAX_MINUTES) { log(`⚠ Garde-fou ${MAX_MINUTES} min actives — arrêt phase data.`); break; }
        if (zsVerdicts && zsVerdicts[sh.label] && zsVerdicts[sh.label].ok === false) {
          log(`  ▶ ${sh.label} : groupé non fiable (validation) — export unitaire.`);
          const nU = await pullZsLegacy(sh);
          if (nU) log(`  ✓ ${sh.label} : ${nU} lignes (unitaire)`);
          else log(`  ✗ ${sh.label} : aucune donnée pour cette période`);
          continue;
        }
        const pull = async (pack) => {
          const qs = `${encodeURIComponent(antField)}=${pack.map(encodeURIComponent).join(",")}&:refresh=yes`;
          const r = await fetchBin(exportUrl(sh.urlName, "csv", qs), 150000);
          if (!r) return { pack, err: 1 };
          const rows = parseCsv(Buffer.from(r.b64, "base64").toString("utf8"));
          return rows.length > 1 ? { pack, rows } : { pack, empty: 1 };
        };
        errReset();
        let results = await runBatch(packs, 3, pull);
        /* Un paquet sans réponse (≠ vide) est un refus ponctuel : rejeu unitaire. */
        const failed = results.filter((x) => x.err).map((x) => x.pack);
        if (failed.length) {
          if (failed.length === results.length) await reviveFrame();
          const again = [];
          for (const p of failed) again.push(await pull(p));
          results = results.filter((x) => !x.err).concat(again);
          const rec = again.filter((x) => x.rows).length;
          if (rec) log(`  ↻ ${sh.label} : ${rec}/${failed.length} paquet(s) récupéré(s) au rattrapage`);
        }
        let nRows = 0, zi = -2; // -2 = pas encore évalué
        for (const res of results.filter((x) => x.rows)) {
          const hdr = res.rows[0];
          if (zi === -2) zi = hdr.findIndex((c) => /zone de sant/i.test(c));
          if (zi < 0) continue; // traité après coup par le repli
          const a = acc[sh.slug] || (acc[sh.slug] = { label: sh.label, columns: [], rows: [] });
          for (const c of hdr) if (a.columns.indexOf(c) < 0) a.columns.push(c);
          for (const rr of res.rows.slice(1)) {
            const o = {};
            hdr.forEach((c, i) => { o[c] = rr[i] ?? ""; });
            const court = shortLoc(String(rr[zi] || "").trim());
            o.Antenne = court;
            if (court) {
              (refreshedBySheet[sh.slug] = refreshedBySheet[sh.slug] || new Set()).add(court);
              globalDone.add(court);
            }
            a.rows.push(o); nRows++;
          }
        }
        if (zi < 0 && results.some((x) => x.rows)) {
          log(`  ⚠ ${sh.label} : pas de colonne « Zone de santé » dans l'export groupé — repli unitaire (lent).`);
          nRows = await pullZsLegacy(sh);
        }
        if (nRows) log(`  ✓ ${sh.label} : ${nRows} lignes (${results.filter((x) => x.rows).length}/${packs.length} paquets)`);
        else log(`  ✗ ${sh.label} : aucune donnée pour cette période`);
      }
      /* Registre : toutes les ZS vues ce run (sert au déverrouillage du
         backfill des mois passés — seuil 500/519). */
      for (const ant of cible) {
        if (globalDone.has(lbl(ant))) ledger[ant] = { at: new Date().toISOString(), sheets: Object.keys(acc).length, err: 0, empty: 0 };
      }
      writeFileSync(LEDGER, JSON.stringify(ledger, null, 1));
      zsDone.push(...cible.filter((a) => globalDone.has(lbl(a))));
      if (!zsDone.length) throw new Error("Aucune zone de santé exportée (throttle ?) — publication annulée.");
      /* FUSION avec ce qui est déjà publié : une ZS absente des paquets de
         CETTE feuille garde ses lignes en ligne (couverture cumulative, par
         feuille — plus fine qu'avant : l'ensemble était global). */
      const RAW = `https://raw.githubusercontent.com/MBOMBOmamu1993/snis-vaccination-api/${DATA_BRANCH}/${PFX}`;
      let kept = 0;
      for (const slugK of Object.keys(acc)) {
        const a = acc[slugK];
        const refreshed = refreshedBySheet[slugK] || new Set();
        try {
          const resp = await fetch(`${RAW}views/${slugK}.json?_=${Date.now()}`);
          if (resp.ok) {
            const old = await resp.json();
            const oldRows = (old.rows || []).filter((r) => !refreshed.has(String(r.Antenne || "").trim()));
            for (const c of (old.columns || [])) if (a.columns.indexOf(c) < 0) a.columns.push(c);
            a.rows = oldRows.concat(a.rows);
            kept += oldRows.length;
          }
        } catch (e) { /* première publication : rien à fusionner */ }
        const columns = ["Antenne", ...a.columns.filter((c) => c !== "Antenne")];
        const rel = `views/${slugK}.json`;
        writeFileSync(path.join(OUT, rel), JSON.stringify({ name: a.label, urlName: slugK, columns, rows: a.rows }));
        dataFiles[slugK] = { file: rel, rows: a.rows.length };
      }
      const coverage = new Set();
      for (const slugK of Object.keys(acc)) for (const r of acc[slugK].rows) coverage.add(String(r.Antenne || "").trim());
      zsCoverage = coverage;
      log(`✓ Data ZS : ${Object.keys(acc).length} feuilles, ${zsDone.length} ZS rafraîchies ce run, ${kept} lignes conservées → couverture ${coverage.size}/${antennes.length} zones de santé.`);
    }
    if (filterOK && !ZS_BULK && dataSheets.length) {
      /* ── BATCHING MULTI-VALEURS (validé le 27/07 — probe-multicsv.mjs) ─────
         L'export .csv accepte PLUSIEURS valeurs de filtre séparées par des
         virgules : UNE requête ramène les 51 antennes (~30 s) au lieu de 51
         requêtes (~7 min) → la phase data passe de ~2 h 20 à ~15 min, et le
         garde-fou n'est plus jamais atteint (cause n°1 des échecs de juillet).
         Attribution de la colonne « Antenne » par le contenu : colonne
         « Antenne En » quand elle existe, sinon carte ZS→antenne (cache
         zs_ant_map.json construit depuis le classeur ZS), sinon 1re colonne
         si c'est un libellé d'antenne. Repli automatique sur l'ancien export
         antenne par antenne si le résultat groupé est inexploitable. ── */
      const antSet = new Set(activeAnt.map((a) => lbl(a)));
      /* Carte ZS (nom court) → antenne (nom court) : colonne « Antenne En » des
         FILTER_VALUES du classeur ZS. Cache 7 j — les rattachements bougent peu. */
      const loadZsAntMap = async () => {
        const F = path.join(HERE, "zs_ant_map.json");
        try {
          const j = JSON.parse(readFileSync(F, "utf8"));
          if (Date.now() - Date.parse(j.at) < 7 * 864e5 && j.map && Object.keys(j.map).length > 400) return j.map;
        } catch (e) { }
        const map = {};
        try {
          for (const fv of ["FILTER_VALUES", "FILTER_VALUES_2", "FILTER_VALUES_3"]) {
            const r = await fetchBin(`${SERVER}/t/${SITE}/views/Mashako3_0RapportdelaZone/${fv}.csv?:refresh=yes`, 150000);
            if (!r) continue;
            const rows = parseCsv(Buffer.from(r.b64, "base64").toString("utf8"));
            if (rows.length < 2) continue;
            const cols = rows[0];
            const si = cols.findIndex((c) => /SELECTED_location_level/i.test(c));
            const ai2 = cols.findIndex((c) => /antenne/i.test(c));
            if (si < 0 || ai2 < 0) continue;
            for (const rr of rows.slice(1)) {
              const zs = shortLoc((rr[si] || "").trim());
              const an = (rr[ai2] || "").trim();
              if (zs && an) map[zs] = an;
            }
          }
          if (Object.keys(map).length > 400) { writeFileSync(F, JSON.stringify({ at: new Date().toISOString(), map })); log(`  ✓ Carte ZS→antenne rafraîchie (${Object.keys(map).length} ZS).`); }
          else throw new Error("carte incomplète");
        } catch (e) {
          log(`  ⚠ Carte ZS→antenne non rafraîchie (${e.message}) — repli sur le cache.`);
          try { return JSON.parse(readFileSync(F, "utf8")).map || {}; } catch (e2) { return {}; }
        }
        return map;
      };
      const zsToAnt = await loadZsAntMap();
      /* Ancien chemin (antenne par antenne) — conservé tel quel comme repli. */
      const exportSheetLegacy = async (sh) => {
        /* ── MOTEUR VizQL (02/09/2026, vizql-export.mjs) : une session par
           tranche d'antennes, categorical-filter + export « données résumé »
           ≈ 13 s/antenne au lieu de ~33 s par export .csv URL (session neuve à
           chaque requête). Même CSV. Sur la synchro du 01/09, ces 14 feuilles
           coûtaient 2 h 46 sur 3 h 53. Repli sur le chemin URL ci-dessous si le
           moteur échoue ou reste incomplet. MASHAKO_SESSION=0 pour le couper. ── */
        if (process.env.MASHAKO_SESSION !== "0") {
          try {
            const { exportParSessions } = await import("./vizql-export.mjs");
            const budget = Math.max(5, MAX_MINUTES - minutesActives());
            const r = await exportParSessions({
              ctx, wb: WORKBOOK, urlName: sh.urlName, slug: sh.slug, label: sh.label, month: CUR_MONTH, year: CUR_YEAR,
              zones: activeAnt, antLabel, log, deadline: Date.now() + budget * 60000,
            });
            tick();
            const traitees = r.zonesOk + r.zonesVides;
            if (r.budgetEpuise || traitees >= activeAnt.length * 0.9) {
              if (!r.records.length) { log(`  ✗ ${sh.label} : aucune donnée CSV (sessions VizQL : ${r.zonesVides} antennes vides)`); return false; }
              const rel = `views/${sh.slug}.json`;
              writeFileSync(path.join(OUT, rel), JSON.stringify({ name: sh.label, urlName: sh.slug, columns: r.columns, rows: r.records }));
              dataFiles[sh.slug] = { file: rel, rows: r.records.length };
              log(`  ✓ ${sh.label} : ${r.records.length} lignes (${r.zonesOk}/${activeAnt.length} antennes) [sessions VizQL, ${r.minutes.toFixed(1)} min]`);
              return true;
            }
            log(`  ⚠ ${sh.label} : moteur VizQL incomplet (${traitees}/${activeAnt.length} antennes) — repli export par URL.`);
          } catch (e) { log(`  ⚠ ${sh.label} : moteur VizQL indisponible (${String(e.message || e).slice(0, 120)}) — repli export par URL.`); }
        }
        const pullAnt = async (ant) => {
          const r = await fetchBin(exportUrl(sh.urlName, "csv",
            `${encodeURIComponent(antField)}=${encodeURIComponent(ant)}&:refresh=yes`), 150000);
          if (!r) return { ant, err: 1 };
          const rows = parseCsv(Buffer.from(r.b64, "base64").toString("utf8"));
          return rows.length > 1 ? { ant, rows } : { ant, empty: 1 };
        };
        errReset();
        let results = await runBatch(activeAnt, 3, pullAnt);
        /* RATTRAPAGE : une antenne SANS RÉPONSE (≠ vide) est un refus ponctuel de
           Tableau. Sans ce rejeu, une feuille entière disparaissait du dashboard —
           constaté le 25/07 à 15h10 : Infirmier_ANT et Carte_Infirmier_ANT ont
           renvoyé « aucune donnée » en 0,2 s alors qu'elles sortaient 519 lignes
           deux heures plus tôt, et la carte qui les lit s'est retrouvée vide. */
        const failedAnt = results.filter((x) => x.err).map((x) => x.ant);
        if (failedAnt.length) {
          /* 100 % d'échecs = ce n'est pas « Tableau refuse cette antenne », c'est
             la page qui est morte. On la recharge AVANT le rattrapage, sinon les
             51 rejeux séquentiels échouent aussi (jusqu'à 2 h perdues par feuille). */
          if (failedAnt.length === results.length) await reviveFrame();
          const again = [];
          for (const a of failedAnt) again.push(await pullAnt(a));
          results = results.filter((x) => !x.err).concat(again);
          const rec = again.filter((x) => x.rows).length;
          if (rec) log(`  ↻ ${sh.label} : ${rec}/${failedAnt.length} antenne(s) récupérée(s) au rattrapage`);
        }
        const ok = results.filter((x) => x.rows);
        if (!ok.length) {
          const nErr = results.filter((x) => x.err).length;
          log(`  ✗ ${sh.label} : aucune donnée CSV (${nErr} refus, ${results.length - nErr} vides)${nErr ? ` — causes : ${errSummary()}` : " — le serveur répond mais la feuille n'a pas de données pour cette période"}`);
          return false;
        }
        /* En-tête = union des colonnes de toutes les antennes (robuste si l'ordre
           ou le nombre varie d'une antenne à l'autre). */
        const colSet = [];
        for (const { rows } of ok) for (const c of rows[0]) if (colSet.indexOf(c) < 0) colSet.push(c);
        const columns = ["Antenne", ...colSet];
        const records = [];
        for (const { ant, rows } of ok) {
          const hdr = rows[0];
          for (const rr of rows.slice(1)) {
            const o = { Antenne: lbl(ant) };
            hdr.forEach((c, i) => { o[c] = rr[i] ?? ""; });
            records.push(o);
          }
        }
        const rel = `views/${sh.slug}.json`;
        writeFileSync(path.join(OUT, rel),
          JSON.stringify({ name: sh.label, urlName: sh.slug, columns, rows: records }));
        dataFiles[sh.slug] = { file: rel, rows: records.length };
        log(`  ✓ ${sh.label} : ${records.length} lignes (${ok.length}/${activeAnt.length} antennes) [mode unitaire]`);
        return true;
      };
      log(`→ Export data : ${dataSheets.length} feuilles × ${activeAnt.length} antennes (multi-valeurs là où validé, unitaire sinon)…`);
      /* ── LISTE BLANCHE multi-valeurs (validée le 27/07 sur run réel) ──────
         Ces feuilles conservent TOUTES leurs lignes de détail en groupé.
         Les autres COLLAPSENT silencieusement : classements/résumés (domaine
         du filtre), pivots « Noms de mesures » (Livraison_ANT_P1 : 30 lignes
         vides au lieu de 950 remplies), CDF_Problèmes (20 au lieu de 511) —
         constaté le 27/07. Doute → unitaire : la correction prime la vitesse. */
      const BATCH_OK = /^(HZ Scores_ANT|Supervision_Quality_ANT|Supervision_ANT_P1|R.+union_ANT|CDF_ANT|S.+ances_ANT|Taux d.abandon_ANT|Infirmier_ANT|Livraison_ANT_P2)$/i;
      for (const sh of dataSheets) {
        tick();
        if (minutesActives() > MAX_MINUTES) { log(`⚠ Garde-fou ${MAX_MINUTES} min actives — arrêt phase data.`); break; }
        if (!BATCH_OK.test(sh.label)) { await exportSheetLegacy(sh); continue; }
        const qs = `${encodeURIComponent(antField)}=${activeAnt.map(encodeURIComponent).join(",")}&:refresh=yes`;
        errReset();
        let r = await fetchBin(exportUrl(sh.urlName, "csv", qs), 150000);
        if (!r) { await reviveFrame(); r = await fetchBin(exportUrl(sh.urlName, "csv", qs), 150000); }
        if (!r) { log(`  ⚠ ${sh.label} : export groupé refusé — repli unitaire.`); await exportSheetLegacy(sh); continue; }
        const rows = parseCsv(Buffer.from(r.b64, "base64").toString("utf8"));
        if (rows.length < 2) { log(`  ✗ ${sh.label} : aucune donnée pour cette période`); continue; }
        const hdr = rows[0];
        const ai = hdr.findIndex((c) => /^Antenne( En)?$/i.test(String(c).trim()));
        const zi = hdr.findIndex((c) => /zone de sant/i.test(c));
        const columns = ["Antenne", ...hdr];
        const records = [];
        let sansAnt = 0;
        for (const rr of rows.slice(1)) {
          const o = {};
          hdr.forEach((c, i) => { o[c] = rr[i] ?? ""; });
          let ant = "";
          if (ai >= 0) ant = String(rr[ai] || "").trim();
          if (!ant && zi >= 0) ant = zsToAnt[String(rr[zi] || "").trim()] || "";
          if (!ant && antSet.has(String(rr[0] || "").trim())) ant = String(rr[0]).trim();
          if (!ant) sansAnt++;
          o.Antenne = ant;
          records.push(o);
        }
        if (sansAnt > records.length * 0.1) {
          log(`  ⚠ ${sh.label} : ${sansAnt}/${records.length} lignes sans rattachement antenne — repli unitaire.`);
          await exportSheetLegacy(sh);
          continue;
        }
        const rel = `views/${sh.slug}.json`;
        writeFileSync(path.join(OUT, rel),
          JSON.stringify({ name: sh.label, urlName: sh.slug, columns, rows: records }));
        dataFiles[sh.slug] = { file: rel, rows: records.length };
        log(`  ✓ ${sh.label} : ${records.length} lignes${sansAnt ? ` (⚠ ${sansAnt} sans antenne)` : ""}`);
      }
    }

    // ── Images par antenne — SEULEMENT pour les feuilles SANS tableau (cartes,
    //    visuels). Les feuilles de données sont déjà filtrables via leur table,
    //    inutile d'en exporter une image par antenne → charge réduite (durable
    //    en quotidien, évite le throttling). Les cartes suivent ainsi le filtre
    //    Antenne ; la période affichée sur ces images = période par défaut. ──
    /* ZS : une image par ZONE DE SANTÉ coûterait 519 × N exports (~5 h) → on ne
       les fait que pour la tranche de ZS rafraîchie ce run, et seulement si
       MASHAKO_ZS_IMG=1 (les feuilles sans CSV retombent sinon sur l'image par
       défaut du classeur, comme avant la 1re synchro). */
    const imgLocs = ZS_BULK ? (process.env.MASHAKO_ZS_IMG === "1" ? zsDone : []) : activeAnt;
    if (filterOK && imgLocs.length) {
      /* La page de garde est hors filtre pour les DONNÉES (NO_ANT) mais son
         visuel, lui, dépend bien de l'antenne : on l'ajoute aux visuels. */
      /* Seule la « Synthèse des constats » est affichée en image par antenne
         par le dashboard — KPI, Cover Page et Configuration sont rendus en
         HTML statique côté front (31/08/2026). Capturer le reste coûtait
         ~250 exports (~50 min) par run pour des fichiers jamais consultés. */
      const visualSheets = antSheets.filter((s) =>
        !dataFiles[s.slug] && /^Supervision_(ANT_P2|HZ_P3)$/i.test(s.label));
      const total = imgLocs.length * visualSheets.length;
      log(`→ Images par ${IS_ZS ? "zone de santé" : "antenne"} (visuels sans table) : ${imgLocs.length} × ${visualSheets.length} feuilles = ${total}…`);
      /* ── CAPTURE HAUTE DÉFINITION ──────────────────────────────────────────
         L'export PNG natif de Tableau est figé à 1366×768 (le paramètre :size
         est ignoré — vérifié le 26/07), or le dashboard affiche ces vues plus
         larges : le texte apparaît flou. On capture donc la vue rendue avec un
         facteur d'échelle 2 → ~3000×1900, texte net. Repli automatique sur
         l'export natif si la capture échoue. ── */
      async function captureHD(sh, ant, outAbs) {
        const p2 = await ctx.newPage();
        try {
          const cdp = await ctx.newCDPSession(p2);
          await cdp.send("Emulation.setDeviceMetricsOverride", { width: 1500, height: 950, deviceScaleFactor: 2, mobile: false });
          const u = `${SERVER}/t/${SITE}/views/${WORKBOOK}/${encodeURIComponent(sh.urlName)}` +
            `?:embed=y&:showVizHome=no&:toolbar=no&:tabs=no&${PERIOD_QS}` +
            `&${encodeURIComponent(antField)}=${encodeURIComponent(ant)}`;
          await p2.goto(u, { waitUntil: "domcontentloaded", timeout: 120000 });
          await p2.waitForTimeout(24000);
          for (let i = 0; i < 12; i++) {
            const busy = await p2.evaluate(() => !!document.querySelector("[class*='tb-loading'],[class*='LoadingIndicator'],[class*='tab-widget-loading']")).catch(() => false);
            if (!busy) break;
            await p2.waitForTimeout(2500);
          }
          const el = await p2.$(".tab-clientArea, [class*='clientArea'], #tabZoneWrapper");
          await (el || p2).screenshot({ path: outAbs });
          return true;
        } catch (e) { return false; }
        finally { await p2.close().catch(() => { }); }
      }
      /* Feuilles capturées en haute définition : celles dont le contenu est du
         TEXTE (illisible en 1366×768) — la page de garde en fait partie car elle
         affiche le nom de l'antenne et le mois. */
      const HD = /^(Supervision_ANT_P2|Cover ?Page)$/i;
      let done = 0;
      outer:
      for (const sh of visualSheets) {
        for (const ant of imgLocs) {
          tick();
          if (minutesActives() > MAX_MINUTES) {
            log(`⚠ Garde-fou ${MAX_MINUTES} min actives atteint — arrêt des variantes (${done}/${total}).`);
            break outer;
          }
          const rel = `views/${sh.slug}__${slug(lbl(ant))}.png`;
          const abs = path.join(OUT, rel);
          done++;
          if (HD.test(sh.label) && await captureHD(sh, ant, abs)) {
            (antImages[sh.slug] = antImages[sh.slug] || {})[lbl(ant)] = rel;
            continue;
          }
          const r = await fetchBin(exportUrl(sh.urlName, "png",
            `${encodeURIComponent(antField)}=${encodeURIComponent(ant)}&:refresh=yes`), 150000);
          if (r && r.ct.includes("image")) {
            writeFileSync(abs, Buffer.from(r.b64, "base64"));
            (antImages[sh.slug] = antImages[sh.slug] || {})[lbl(ant)] = rel;
          }
        }
        log(`  ✓ ${sh.label} (${done}/${total})${HD.test(sh.label) ? " — capture haute définition" : ""}`);
      }
    }

    // ── Écriture des images par défaut + meta ──
    const metaViews = [];
    for (const sh of sheets) {
      /* Feuille sans image mais AVEC données (rendu HTML pur) : elle doit
         figurer dans meta.json, sinon le dashboard ne la propose pas. */
      if (!sh.defaultPng) {
        const df0 = dataFiles[sh.slug];
        if (df0) metaViews.push({ name: sh.label, urlName: sh.slug, rows: df0.rows, file: df0.file, image: null, antImages: antImages[sh.slug] || null });
        continue;
      }
      const rel = `views/${sh.slug}.png`;
      writeFileSync(path.join(OUT, rel), sh.defaultPng);
      const df = dataFiles[sh.slug] || null;
      metaViews.push({
        name: sh.label, urlName: sh.slug,
        rows: df ? df.rows : 0, file: df ? df.file : null,
        image: rel, antImages: antImages[sh.slug] || null,
      });
    }
    if (!metaViews.length) throw new Error("Aucune feuille exportée.");

    /* ── GARANTIE ANTI-PERTE ────────────────────────────────────────────────
       Un refus ponctuel de Tableau ne doit JAMAIS faire disparaître une feuille
       du dashboard, ni la vider de ses données. Constaté le 25/07 : à 15h10
       Infirmier_ANT et Carte_Infirmier_ANT ont renvoyé « aucune donnée » en
       0,2 s (elles sortaient 519 lignes à 8h50) et Livraison_ANT_P1 a échoué à
       la résolution → les trois ont disparu de la publication.
       Ici on reprend de la publication précédente TOUTE feuille absente de ce
       run, ou présente mais vidée. Les fichiers eux-mêmes restent en ligne
       (publication par base_tree), il suffit de les re-référencer.
       ⚠ 28/07 : cette lecture a échoué SILENCIEUSEMENT (catch vide, réseau
       instable au réveil du PC) → les 15 feuilles non ré-exportées n'ont pas
       été restaurées, le garde anti-régression a compté « 3 feuilles » et
       annulé la publication — une nuit d'exports 519 ZS perdue (récupérée à
       la main via republish-zs.mjs). Désormais : 3 essais espacés, et si la
       lecture reste impossible alors qu'une publication existe déjà, on
       ANNULE la publication bruyamment (les exports locaux restent dans le
       dossier out/ ou out-zs/ — republish-zs.mjs permet de les publier plus
       tard) plutôt que de publier une version amputée ou de déclencher le
       garde à tort. ── */
    let prevMeta = null;
    for (let essai = 1; essai <= 3 && !prevMeta; essai++) {
      try {
        const r = await fetch(`https://raw.githubusercontent.com/MBOMBOmamu1993/snis-vaccination-api/${DATA_BRANCH}/${PFX}meta.json?_=${Date.now()}`);
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const j = await r.json();
        if (!j || !Array.isArray(j.views)) throw new Error("meta.json sans liste de vues");
        prevMeta = j;
      } catch (e) {
        log(`  ⟳ meta en ligne illisible (essai ${essai}/3 : ${String(e.message || e).slice(0, 100)})${essai < 3 ? ` — nouvel essai dans ${5 * essai} s…` : ""}`);
        if (essai < 3) await new Promise((res) => setTimeout(res, 5000 * essai));
      }
    }
    if (!prevMeta) {
      let dejaPublie = false;
      try { dejaPublie = (JSON.parse(readFileSync(BEST_COUNT_FILE, "utf8")).count || 0) > 0; } catch (e) { }
      if (dejaPublie) {
        log("⛔ Fusion anti-perte IMPOSSIBLE (meta en ligne illisible après 3 essais) — publication ANNULÉE pour ne rien écraser. Les exports locaux sont conservés : relancer `node republish-zs.mjs` quand le réseau revient.");
        notify("Synchro Mashako : publication annulee (meta en ligne illisible, reseau ?) — exports conserves localement, rien n'a ete ecrase.");
        return;
      }
      log("  ⚠ meta en ligne illisible — première publication présumée, on continue sans fusion.");
    } else {
      const byName = {};
      metaViews.forEach((v, i) => { byName[v.urlName] = i; });
      const kept = [];
      (prevMeta.views || []).forEach((pv, pi) => {
        const i = byName[pv.urlName];
        if (i == null) {
          const at = pv.stale_at || prevMeta.generated_at;
          metaViews.splice(Math.min(pi, metaViews.length), 0, Object.assign({}, pv, { stale_at: at }));
          metaViews.forEach((v, k) => { byName[v.urlName] = k; });
          kept.push(pv.name + " (absente)");
          return;
        }
        const nv = metaViews[i];
        if (!nv.file && pv.file) {
          nv.file = pv.file; nv.rows = pv.rows; nv.stale_at = pv.stale_at || prevMeta.generated_at;
          kept.push(pv.name + " (données)");
        }
        if (!nv.antImages && pv.antImages) nv.antImages = pv.antImages;
        if (!nv.image && pv.image) nv.image = pv.image;
      });
      if (kept.length) log(`⚠ Conservé de la publication précédente : ${kept.join(", ")}.`);
    }

    /* ── Détail ZS Dispo/Expiration, intégré à la synchro quotidienne (29/07) ──
       Avant : les *_ZS.json n'étaient régénérés qu'à la main et survivaient
       datés via la fusion. Désormais : régénérés à CHAQUE run ANT (chaîne
       crosstab sur les feuilles masquées _TABLE_…, session courante réutilisée)
       et publiés dans le même commit. Un échec n'annule JAMAIS la publication :
       les anciens fichiers restent référencés (fusion ci-dessus). */
    if (!IS_ZS) {
      try {
        const { exportAntZsDetail } = await import("./export-ant-zs-detail.mjs");
        const det = await exportAntZsDetail(page, { month: CUR_MONTH, year: CUR_YEAR, log });
        const posZs = {}; metaViews.forEach((v, i) => { posZs[v.urlName] = i; });
        for (const [urlName, d] of Object.entries(det)) {
          const entree = { name: d.label, urlName, rows: d.rows, file: d.file, image: null, antImages: null };
          if (posZs[urlName] == null) metaViews.push(entree);
          else metaViews[posZs[urlName]] = entree; // remplace l'entrée fusionnée (stale) par la version fraîche
        }
        log(`✓ Détail ZS quotidien : ${Object.entries(det).map(([k, v]) => `${k} (${v.rows} ZS, ${v.avec} avec données)`).join(", ")}`);
      } catch (e) {
        log(`⚠ Détail ZS quotidien en échec (${String(e.message || e).slice(0, 120)}) — la publication continue (anciens fichiers conservés via la fusion).`);
      }
    }
    const meta = {
      generated_at: new Date().toISOString(),
      server: SERVER.replace("https://", ""), site: SITE,
      workbook: { name: IS_ZS ? "Mashako 3.0 — Rapport de la Zone de Santé" : "Mashako 3.0 — Rapport de l'Antenne", contentUrl: WORKBOOK },
      main_view: MAIN_VIEW, original_url: UI_URL, sync_mode: "local-browser-v3",
      /* Période forcée sur les exports : le dashboard l'affiche dans le bandeau
         sans devoir aller la relire dans une feuille Carte (les feuilles ZS n'en
         ont pas). */
      period: { month: CUR_MONTH, year: CUR_YEAR },
      /* Côté ZS : on ne propose QUE les zones de santé effectivement présentes
         dans les données publiées (la couverture s'accumule run après run), en
         indiquant le total et le rattachement province/antenne pour le
         sélecteur groupé du dashboard. */
      antennes: filterOK ? (zsCoverage
        ? {
          field: antField, level: "zs",
          values: activeAnt.map(lbl).filter((l) => zsCoverage.has(l)),
          total: antennes.length,
          groups: activeAnt.reduce((o, a) => {
            const l = lbl(a);
            if (zsCoverage.has(l)) o[l] = { province: (antMeta[a] || {}).province || "", antenne: (antMeta[a] || {}).antenne || "" };
            return o;
          }, {}),
        }
        : { field: antField, values: activeAnt.map(lbl) }) : null,
      views: metaViews,
    };
    writeFileSync(path.join(OUT, "meta.json"), JSON.stringify(meta, null, 2));

    // ── Publication : branche mashako-data, UN commit sans parent, ref forcée ──
    /* GARDE ANTI-RÉGRESSION : ne JAMAIS écraser une bonne publication par un
       résultat maigre (throttling Tableau → 2-4 feuilles). On mémorise le
       meilleur nombre de feuilles atteint ; si ce run est nettement en dessous,
       on ne publie pas (le dashboard garde la version précédente, correcte). */
    const BEST_FILE = BEST_COUNT_FILE;
    let best = 0, bestData = 0;
    try { const bj = JSON.parse(readFileSync(BEST_FILE, "utf8")); best = bj.count || 0; bestData = bj.withData || 0; } catch (e) { }
    const withData = metaViews.filter((v) => v.file).length;
    const score = metaViews.length + withData; // feuilles + bonus données
    if (best > 0 && metaViews.length < Math.max(10, Math.floor(best * 0.6))) {
      log(`⛔ Résultat trop maigre (${metaViews.length} feuilles vs meilleur ${best}) — probable throttling Tableau. Publication ANNULÉE pour préserver la version en ligne.`);
      notify(`Synchro Mashako incomplete (${metaViews.length} feuilles) — Tableau bride probablement le compte. Version precedente conservee. Reessai a la prochaine synchro.`);
      return;
    }
    /* Même garde côté DONNÉES : ne pas remplacer une version riche en tables
       par un run où la phase CSV a été throttlée en cours de route. */
    if (bestData > 0 && withData < Math.max(5, Math.floor(bestData * 0.5))) {
      log(`⛔ Données trop maigres (${withData} feuilles avec CSV vs meilleur ${bestData}) — publication ANNULÉE pour préserver la version en ligne.`);
      notify(`Synchro Mashako : donnees CSV incompletes (${withData} feuilles) — version precedente conservee.`);
      return;
    }
    if (metaViews.length >= best) { writeFileSync(BEST_FILE, JSON.stringify({ count: metaViews.length, withData: withData, at: new Date().toISOString() })); }

    log("→ Publication (branche mashako-data)…");
    const blob = (buf) => {
      const p = path.join(OUT, "_payload.json");
      writeFileSync(p, JSON.stringify({ encoding: "base64", content: buf.toString("base64") }));
      return JSON.parse(gh([`${REPO}/git/blobs`, "-X", "POST"], p)).sha;
    };
    /* ── ANTI-ENOENT (crash du 26/07 21h16) ─────────────────────────────────
       La fusion anti-perte référence des fichiers de la publication
       PRÉCÉDENTE qui ne sont PAS sur le disque local (out/views est vidé à
       chaque run, et le garde-fou peut couper la phase images avant
       régénération). readFileSync plantait alors et TOUTE la publication
       avortait — 3 h d'exports perdues. Pour tout fichier référencé absent
       localement, on réutilise le blob DÉJÀ en ligne (même chemin dans
       l'arbre courant, qui sert aussi de base_tree). ── */
    let oldTreeSha = null; const oldSha = {};
    let brancheExiste = false;
    try {
      const oldRef = JSON.parse(gh([`${REPO}/git/refs/heads/${DATA_BRANCH}`]));
      brancheExiste = true;
      oldTreeSha = JSON.parse(gh([`${REPO}/git/commits/${oldRef.object.sha}`])).tree.sha;
      for (const it of JSON.parse(gh([`${REPO}/git/trees/${oldTreeSha}?recursive=1`])).tree || []) {
        if (it.type === "blob") oldSha[it.path] = it.sha;
      }
    } catch (e) { log(`  ⚠ lecture de l'arbre en ligne impossible : ${String(e.message || e).slice(0, 120)}`); }
    /* GARDE ABSOLUE : publier SANS base_tree sur une branche existante
       effacerait tout ce que ce run ne réécrit pas (préfixe zs/, archives
       periods/ antérieures, images non régénérées). Si la branche existe mais
       que son arbre est illisible, on annule plutôt que de risquer l'effacement. */
    if (brancheExiste && !oldTreeSha) {
      log("⛔ base_tree introuvable alors que la branche existe — publication ANNULÉE (protection zs/ et archives).");
      notify("Synchro Mashako : publication annulee (base_tree illisible) — rien n'a ete ecrase.");
      return;
    }
    let nbReused = 0; const nbMissing = [];
    const blobLocalOuEnLigne = (rel) => {
      try { return blob(readFileSync(path.join(OUT, rel))); } catch (e) { }
      if (oldSha[PFX + rel]) { nbReused++; return oldSha[PFX + rel]; }
      nbMissing.push(rel); return null;
    };
    const tree_ = [];
    for (const v of metaViews) {
      if (v.image) { const s = blobLocalOuEnLigne(v.image); if (s) tree_.push({ path: v.image, mode: "100644", type: "blob", sha: s }); else v.image = null; }
      if (v.file) { const s = blobLocalOuEnLigne(v.file); if (s) tree_.push({ path: v.file, mode: "100644", type: "blob", sha: s }); else v.file = null; }
      if (v.antImages) for (const ant of Object.keys(v.antImages)) {
        const s = blobLocalOuEnLigne(v.antImages[ant]);
        if (s) tree_.push({ path: v.antImages[ant], mode: "100644", type: "blob", sha: s });
        else delete v.antImages[ant];
      }
    }
    if (nbReused) log(`  ↻ ${nbReused} fichier(s) repris de la publication en ligne (non régénérés ce run).`);
    if (nbMissing.length) log(`  ⚠ ${nbMissing.length} fichier(s) introuvables (local + en ligne) — référence retirée : ${nbMissing.slice(0, 4).join(", ")}${nbMissing.length > 4 ? "…" : ""}`);
    /* meta.json APRÈS le nettoyage des références mortes. */
    tree_.unshift({ path: "meta.json", mode: "100644", type: "blob", sha: blob(Buffer.from(JSON.stringify(meta, null, 2))) });
    // ── ARCHIVES PAR PÉRIODE : le classeur est un rapport mensuel (_PARAM_month/
    //    _PARAM_year). Chaque publication (1) conserve les periods/* déjà en ligne,
    //    (2) duplique la publication courante sous periods/<AAAA-MM>/, (3) tient
    //    periods/index.json à jour → le front propose un filtre Mois/Année. ──
    const MOIS_NUM = { janvier: 1, fevrier: 2, mars: 3, avril: 4, mai: 5, juin: 6, juillet: 7, aout: 8, septembre: 9, octobre: 10, novembre: 11, decembre: 12 };
    function periodKeyFrom(mois, an) {
      const n = MOIS_NUM[String(mois || "").toLowerCase().normalize("NFD").replace(/[̀-ͯ]/g, "")];
      return (n && an) ? `${an}-${String(n).padStart(2, "0")}` : null;
    }
    let pKey = process.env.MASHAKO_PERIOD_KEY || periodKeyFrom(CUR_MONTH, CUR_YEAR) || null;
    if (!pKey) {
      for (const v of metaViews) {
        if (!v.file) continue;
        try {
          const dj = JSON.parse(readFileSync(path.join(OUT, v.file), "utf8"));
          const r0 = (dj.rows || [])[0];
          if (r0 && r0._PARAM_month) { pKey = periodKeyFrom(r0._PARAM_month, r0._PARAM_year); if (pKey) break; }
        } catch (e) { }
      }
    }
    /* base_tree = tout l'existant est PRÉSERVÉ (archives periods/, et l'autre
       classeur ANT/ZS) — seuls les chemins réécrits ici changent. Les clés
       d'archive déjà en ligne sont lues dans oldSha (plus de 2e fetch). */
    const periodKeys = [];
    const pref = `${PFX}periods/`;
    for (const p of Object.keys(oldSha)) {
      if (!p.startsWith(pref) || p === `${pref}index.json`) continue;
      const k = p.slice(pref.length).split("/")[0];
      if (k && !periodKeys.includes(k)) periodKeys.push(k);
    }
    if (pKey) {
      for (const t of tree_.slice()) tree_.push({ path: `periods/${pKey}/${t.path}`, mode: t.mode, type: "blob", sha: t.sha });
      if (!periodKeys.includes(pKey)) periodKeys.push(pKey);
      log(`→ Archive période ${pKey} (index : ${periodKeys.sort().join(", ")}).`);
    }
    if (periodKeys.length) {
      tree_.push({
        path: "periods/index.json", mode: "100644", type: "blob",
        sha: blob(Buffer.from(JSON.stringify({ periods: periodKeys.sort(), current: pKey || null, updated_at: new Date().toISOString() }, null, 2))),
      });
    }
    const treeOut = tree_.map((t) => ({ path: PFX + t.path, mode: t.mode, type: t.type, sha: t.sha }));
    /* GARDE VERCEL : la branche de données n'a ni package.json ni build — le
       push y déclenchait un déploiement preview qui échouait à chaque synchro
       (« No Next.js version detected »). vercel.json vit à la RACINE de la
       branche (hors PFX) ; base_tree le conserve, on le réécrit quand même
       pour couvrir le cas d'une branche recréée de zéro. */
    treeOut.push({
      path: "vercel.json", mode: "100644", type: "blob",
      sha: blob(Buffer.from(JSON.stringify({ $schema: "https://openapi.vercel.sh/vercel.json", git: { deploymentEnabled: { [DATA_BRANCH]: false } } }, null, 2) + "\n")),
    });
    const tp = path.join(OUT, "_tree.json");
    writeFileSync(tp, JSON.stringify(oldTreeSha ? { base_tree: oldTreeSha, tree: treeOut } : { tree: treeOut }));
    const tree = JSON.parse(gh([`${REPO}/git/trees`, "-X", "POST"], tp)).sha;
    const cp = path.join(OUT, "_commit.json");
    writeFileSync(cp, JSON.stringify({
      message: `auto: données Mashako 3.0 (${metaViews.length} feuilles${filterOK ? `, ${antennes.length} antennes` : ""}, ${new Date().toISOString().slice(0, 10)})`,
      tree, parents: [],
    }));
    const commit = JSON.parse(gh([`${REPO}/git/commits`, "-X", "POST"], cp)).sha;
    try {
      gh([`${REPO}/git/refs/heads/${DATA_BRANCH}`, "-X", "PATCH", "-f", `sha=${commit}`, "-F", "force=true"]);
    } catch (e) {
      const rp = path.join(OUT, "_ref.json");
      writeFileSync(rp, JSON.stringify({ ref: `refs/heads/${DATA_BRANCH}`, sha: commit }));
      gh([`${REPO}/git/refs`, "-X", "POST"], rp);
    }
    const nImgs = tree_.length - 1;
    log(`✓ Publié : ${commit.slice(0, 9)} — ${metaViews.length} feuilles, ${nImgs} images${filterOK ? `, filtre « ${antField} » (${antennes.length} antennes)` : ""}.`);
    log("— Synchro v3 terminée avec succès —");
  } catch (e) {
    log(`✖ ÉCHEC : ${e.message}`);
    notify(`La synchro Mashako a echoue : ${e.message}`);
    process.exitCode = 1;
  } finally {
    clearInterval(heartbeat);
    await ctx.close().catch(() => { });
    rmSync(LOCK, { force: true });
  }
}

main();
