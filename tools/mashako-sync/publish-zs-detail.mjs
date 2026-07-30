#!/usr/bin/env node
/** Publie les détails ZS (Dispo_vaccins_ZS / Vaccine_expiration_ZS) dans
 *  periods/<clé>/ de la branche mashako-data : fichiers + entrées dans le
 *  meta.json de la période. base_tree = rien d'autre n'est touché.
 *  Usage : node publish-zs-detail.mjs [2026-06]
 */
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(HERE, "out");
const REPO = "repos/MBOMBOmamu1993/snis-vaccination-api";
const DATA_BRANCH = "mashako-data";
const PKEY = process.argv[2] || "2026-06";
const RAW = "https://raw.githubusercontent.com/MBOMBOmamu1993/snis-vaccination-api/mashako-data";
const FICHIERS = ["Dispo_vaccins_ZS", "Vaccine_expiration_ZS"];
const log = (m) => console.log(`[${new Date().toISOString()}] ${m}`);
const RETRIABLE = /timeout|connection|connect|reset|EOF|handshake|temporarily|502|503|504|malformed|400/i;
const gh = (args, inputFile) => {
  const a = ["api", ...args];
  if (inputFile) a.push("--input", inputFile);
  let last;
  for (let essai = 1; essai <= 5; essai++) {
    try { return execFileSync("gh", a, { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 }); }
    catch (e) {
      last = e;
      const msg = String((e.stderr || "") + (e.message || ""));
      if (!RETRIABLE.test(msg) || essai === 5) throw e;
      const pause = 2000 * essai;
      log(`⟳ appel API en échec (${msg.trim().slice(0, 70)}) — nouvel essai ${essai + 1}/5 dans ${pause / 1000} s`);
      const fin = Date.now() + pause; while (Date.now() < fin) { }
    }
  }
  throw last;
};

/* 1. fichiers locaux */
const vues = [];
for (const f of FICHIERS) {
  const j = JSON.parse(readFileSync(path.join(OUT, "views", `${f}.json`), "utf8"));
  if (!(j.rows || []).length) { log(`⏭ ${f} : vide`); continue; }
  vues.push(j);
}
if (!vues.length) { log("Rien à publier."); process.exit(1); }

/* 2. meta de la période */
const meta = await (await fetch(`${RAW}/periods/${PKEY}/meta.json?_=${Date.now()}`)).json();
for (const j of vues) {
  const i = (meta.views || []).findIndex((v) => v.urlName === j.urlName || v.name === j.name);
  const entree = { name: j.name, urlName: j.urlName, rows: j.rows.length, file: `views/${j.urlName}.json`, image: null, antImages: null };
  if (i < 0) { meta.views.push(entree); log(`✚ ajout vue « ${j.name} » (${j.rows.length} lignes)`); }
  else { meta.views[i] = Object.assign({}, meta.views[i], entree); log(`↻ maj vue « ${j.name} » (${meta.views[i].rows} lignes)`); }
}
meta.generated_at = new Date().toISOString();

/* 3. publication */
const blob = (buf) => {
  const p = path.join(OUT, "_payload.json");
  writeFileSync(p, JSON.stringify({ encoding: "base64", content: buf.toString("base64") }));
  return JSON.parse(gh([`${REPO}/git/blobs`, "-X", "POST"], p)).sha;
};
const tree_ = [{ path: `periods/${PKEY}/meta.json`, mode: "100644", type: "blob", sha: blob(Buffer.from(JSON.stringify(meta, null, 2))) }];
for (const j of vues) {
  tree_.push({ path: `periods/${PKEY}/views/${j.urlName}.json`, mode: "100644", type: "blob", sha: blob(readFileSync(path.join(OUT, "views", `${j.urlName}.json`))) });
}
const oldRef = JSON.parse(gh([`${REPO}/git/refs/heads/${DATA_BRANCH}`]));
const oldTree = JSON.parse(gh([`${REPO}/git/commits/${oldRef.object.sha}`])).tree.sha;
writeFileSync(path.join(OUT, "_tree.json"), JSON.stringify({ base_tree: oldTree, tree: tree_ }));
const tree = JSON.parse(gh([`${REPO}/git/trees`, "-X", "POST"], path.join(OUT, "_tree.json"))).sha;
writeFileSync(path.join(OUT, "_commit.json"), JSON.stringify({
  message: `auto: détail ZS Dispo_vaccins + Vaccine_expiration (${PKEY}, ${vues.length} feuilles)`,
  tree, parents: [],
}));
const commit = JSON.parse(gh([`${REPO}/git/commits`, "-X", "POST"], path.join(OUT, "_commit.json"))).sha;
gh([`${REPO}/git/refs/heads/${DATA_BRANCH}`, "-X", "PATCH", "-f", `sha=${commit}`, "-F", "force=true"]);
log(`✓ Publié : ${commit.slice(0, 9)} — ${vues.length} feuilles de détail ZS sous periods/${PKEY}/.`);
