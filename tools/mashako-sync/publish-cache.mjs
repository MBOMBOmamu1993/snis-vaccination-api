#!/usr/bin/env node
/* ─────────────────────────────────────────────────────────────────────────────
   publish-cache.mjs — publie sur la branche mashako-data ce qui est DÉJÀ dans
   out/views/, sans toucher à Tableau.

   Pourquoi : la synchro Antenne du 26/07 a récupéré 12 feuilles de données
   (dont « Performance Résumé_ANT », absente en ligne depuis le 25/07) puis a
   été TUÉE à 11h05 pendant la phase images — donc jamais publiée. Les fichiers
   sont intacts sur disque ; il n'y a aucune raison de redemander à Tableau
   (compte bridé) ce qu'on possède déjà.

   Principe, identique à sync.mjs : fusion sur la publication précédente
   (aucune feuille ne disparaît), un commit sans parent, base_tree pour
   préserver les archives periods/ et le préfixe zs/.
   ─────────────────────────────────────────────────────────────────────────── */
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync, readdirSync, existsSync } from "node:fs";
import path from "node:path";

const HERE = path.dirname(new URL(import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, "$1"));
const OUT = path.join(HERE, "out");
const VIEWS = path.join(OUT, "views");
const REPO = "repos/MBOMBOmamu1993/snis-vaccination-api";
const DATA_BRANCH = "mashako-data";
const BEST_FILE = path.join(HERE, "best_count.json");
const DRY = process.argv.includes("--dry");
const MONTH = process.env.MASHAKO_MONTH || "Juillet";
const YEAR = process.env.MASHAKO_YEAR || "2026";
const PKEY = process.env.MASHAKO_PERIOD_KEY || "2026-07";

const log = (m) => console.log(`[${new Date().toISOString()}] ${m}`);
/* ⚠ Une publication, c'est des centaines d'appels API : il suffit d'un hoquet
   réseau pour tout perdre. Vu deux fois — « error connecting to api.github.com »
   le 25/07 à 15h10, « TLS handshake timeout » le 26/07 à 22h20, chaque fois un
   run entier jeté. On réessaie donc, avec une pause croissante ; seules les
   erreurs de transport sont rejouables (un 4xx se reproduirait à l'identique). */
const RETRIABLE = /timeout|connection|connect|reset|EOF|handshake|temporarily|502|503|504|malformed|400/i;
const sleep = (ms) => { const t = Date.now() + ms; while (Date.now() < t); };
const gh = (args, inputFile) => {
  const a = ["api", ...args];
  if (inputFile) a.push("--input", inputFile);
  let last;
  for (let essai = 1; essai <= 5; essai++) {
    try {
      return execFileSync("gh", a, { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
    } catch (e) {
      last = e;
      const msg = String((e.stderr || "") + (e.message || ""));
      if (!RETRIABLE.test(msg) || essai === 5) throw e;
      const pause = 2000 * essai;
      console.log(`[${new Date().toISOString()}] ⟳ appel API en échec (${msg.trim().slice(0, 80)}) — nouvel essai ${essai + 1}/5 dans ${pause / 1000} s`);
      sleep(pause);
    }
  }
  throw last;
};

/* ── 1. publication précédente ── */
const prev = JSON.parse(
  execFileSync("curl", ["-s", `https://raw.githubusercontent.com/MBOMBOmamu1993/snis-vaccination-api/${DATA_BRANCH}/meta.json?_=${Date.now()}`], { encoding: "utf8" })
);
const views = (prev.views || []).map((v) => Object.assign({}, v));
log(`Publication en ligne : ${prev.generated_at} — ${views.length} feuilles.`);

/* ── 2. ce que le cache local contient ── */
const files = readdirSync(VIEWS);
const jsons = files.filter((f) => f.endsWith(".json"));
const antPngs = files.filter((f) => f.endsWith(".png") && f.includes("__"));
log(`Cache local : ${jsons.length} feuilles de données, ${antPngs.length} images par antenne.`);

/* Ordre du classeur (relevé sur le jumeau ZS) : Performance Résumé vient juste
   après Configuration, avant la Heatmap ; Ranking juste après la Heatmap. */
const AFTER = { "Performance Résumé_ANT": "Configuration_ANT", "Ranking_ANT": "Performance Heatmap_ANT" };

const idxOf = (name) => views.findIndex((v) => v.name === name);
const maj = [], nouv = [];

for (const f of jsons) {
  const j = JSON.parse(readFileSync(path.join(VIEWS, f), "utf8"));
  const rows = (j.rows || []).length;
  if (!rows) { log(`⏭ ${j.name} : fichier local vide, ignoré.`); continue; }
  const rel = `views/${f}`;
  let i = idxOf(j.name);
  if (i < 0) {
    const anchor = AFTER[j.name];
    const ai = anchor ? idxOf(anchor) : -1;
    const at = ai >= 0 ? ai + 1 : views.length;
    views.splice(at, 0, { name: j.name, urlName: j.urlName, rows, file: rel, image: null, antImages: null });
    nouv.push(`${j.name} (${rows} lignes)`);
  } else {
    const before = views[i].rows;
    views[i].rows = rows;
    views[i].file = rel;
    delete views[i].stale_at;
    maj.push(`${j.name} ${before}→${rows}`);
  }
}

/* Images par antenne présentes dans le cache (Supervision_ANT_P2 en HD ×2).
   Le front retombe proprement sur l'image par défaut pour les antennes non
   encore capturées, donc une couverture partielle est un gain net. */
const bySheet = {};
for (const f of antPngs) {
  const [sl, ant] = f.replace(/\.png$/, "").split("__");
  (bySheet[sl] = bySheet[sl] || {})[ant] = `views/${f}`;
}
for (const sl of Object.keys(bySheet)) {
  const i = views.findIndex((v) => v.urlName === sl);
  if (i < 0) { log(`⏭ images « ${sl} » : feuille inconnue en ligne, ignorées.`); continue; }
  views[i].antImages = Object.assign({}, views[i].antImages || {}, bySheet[sl]);
  maj.push(`${views[i].name} +${Object.keys(bySheet[sl]).length} images antenne`);
}

if (maj.length) log(`↻ Mises à jour : ${maj.join(", ")}.`);
if (nouv.length) log(`✚ Ajouts : ${nouv.join(", ")}.`);
if (!maj.length && !nouv.length) { log("Rien à publier."); process.exit(0); }

const meta = Object.assign({}, prev, {
  generated_at: new Date().toISOString(),
  sync_mode: "local-browser-v3 (publication depuis cache)",
  period: { month: MONTH, year: YEAR },
  views,
});

/* ── 3. gardes anti-régression (mêmes seuils que sync.mjs) ── */
let best = 0, bestData = 0;
try { const b = JSON.parse(readFileSync(BEST_FILE, "utf8")); best = b.count || 0; bestData = b.withData || 0; } catch (e) { }
const withData = views.filter((v) => v.file).length;
if (best > 0 && views.length < Math.max(10, Math.floor(best * 0.6))) { log(`⛔ ${views.length} feuilles vs meilleur ${best} — annulé.`); process.exit(1); }
if (bestData > 0 && withData < Math.max(5, Math.floor(bestData * 0.5))) { log(`⛔ ${withData} feuilles avec CSV vs meilleur ${bestData} — annulé.`); process.exit(1); }
log(`→ ${views.length} feuilles, dont ${withData} avec données (meilleur connu : ${best}/${bestData}).`);

if (DRY) { console.log(JSON.stringify(views.map((v) => ({ n: v.name, r: v.rows, f: !!v.file, ai: v.antImages ? Object.keys(v.antImages).length : 0 })), null, 1)); process.exit(0); }

/* ── 4. publication ── */
const blob = (buf) => {
  const p = path.join(OUT, "_payload.json");
  writeFileSync(p, JSON.stringify({ encoding: "base64", content: buf.toString("base64") }));
  return JSON.parse(gh([`${REPO}/git/blobs`, "-X", "POST"], p)).sha;
};

/* On n'envoie QUE les fichiers présents localement : tout le reste (images par
   défaut, feuilles non rafraîchies, archives) est conservé par base_tree. */
const tree_ = [{ path: "meta.json", mode: "100644", type: "blob", sha: blob(Buffer.from(JSON.stringify(meta, null, 2))) }];
let up = 0;
for (const f of [...jsons, ...antPngs]) {
  const rel = `views/${f}`;
  if (!views.some((v) => v.file === rel || (v.antImages && Object.values(v.antImages).includes(rel)))) continue;
  tree_.push({ path: rel, mode: "100644", type: "blob", sha: blob(readFileSync(path.join(VIEWS, f))) });
  up++;
}
log(`→ ${up} fichiers envoyés (blobs).`);

let oldTreeSha = null, periodKeys = [];
try {
  const oldRef = JSON.parse(gh([`${REPO}/git/refs/heads/${DATA_BRANCH}`]));
  oldTreeSha = JSON.parse(gh([`${REPO}/git/commits/${oldRef.object.sha}`])).tree.sha;
  const items = JSON.parse(gh([`${REPO}/git/trees/${oldTreeSha}?recursive=1`])).tree || [];
  for (const it of items) {
    if (it.type !== "blob" || !it.path.startsWith("periods/") || it.path === "periods/index.json") continue;
    const k = it.path.slice("periods/".length).split("/")[0];
    if (k && !periodKeys.includes(k)) periodKeys.push(k);
  }
} catch (e) { log(`⚠ arbre précédent illisible : ${e.message}`); }
if (!oldTreeSha) { log("⛔ base_tree introuvable — publication annulée (risque d'effacer l'existant)."); process.exit(1); }

/* Archive du mois courant : les mêmes blobs re-référencés sous periods/2026-07.
   Les feuilles non rafraîchies aujourd'hui y sont déjà (base_tree). */
for (const t of tree_.slice()) tree_.push({ path: `periods/${PKEY}/${t.path}`, mode: t.mode, type: "blob", sha: t.sha });
if (!periodKeys.includes(PKEY)) periodKeys.push(PKEY);
tree_.push({
  path: "periods/index.json", mode: "100644", type: "blob",
  sha: blob(Buffer.from(JSON.stringify({ periods: periodKeys.sort(), current: PKEY, updated_at: new Date().toISOString() }, null, 2))),
});
log(`→ Archive période ${PKEY} (index : ${periodKeys.sort().join(", ")}).`);

const tp = path.join(OUT, "_tree.json");
writeFileSync(tp, JSON.stringify({ base_tree: oldTreeSha, tree: tree_ }));
const tree = JSON.parse(gh([`${REPO}/git/trees`, "-X", "POST"], tp)).sha;
const cp = path.join(OUT, "_commit.json");
writeFileSync(cp, JSON.stringify({
  message: `auto: donnees Mashako 3.0 depuis cache (${views.length} feuilles, ${withData} avec CSV, ${new Date().toISOString().slice(0, 10)})`,
  tree, parents: [],
}));
const commit = JSON.parse(gh([`${REPO}/git/commits`, "-X", "POST"], cp)).sha;
gh([`${REPO}/git/refs/heads/${DATA_BRANCH}`, "-X", "PATCH", "-f", `sha=${commit}`, "-F", "force=true"]);
if (views.length >= best) writeFileSync(BEST_FILE, JSON.stringify({ count: views.length, withData, at: new Date().toISOString() }));
log(`✓ Publié : ${commit.slice(0, 9)} — ${views.length} feuilles, ${withData} avec données.`);
