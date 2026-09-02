#!/usr/bin/env node
/** Publie l'outillage local dans tools/mashako-sync/ du dépôt.
 *
 *  Le workflow de secours (GitHub Actions) lit ses scripts là — sans cette
 *  republication, le cloud continue de tourner avec la version d'avant les
 *  correctifs locaux (constaté 29/07 : sync.mjs du cloud antérieur aux garde-fous
 *  veille/throttling/fusion). Un seul commit, base_tree : rien d'autre n'est
 *  touché.
 *
 *  Usage : node publier-outillage.mjs [fichier…]   (défaut : la liste ci-dessous)
 */
import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = "repos/MBOMBOmamu1993/snis-vaccination-api";
const DEST = "tools/mashako-sync/";
const log = (m) => console.log(m);

const DEFAUT = [
  "sync.mjs", "backfill-periods.mjs", "vizql-lib.mjs", "vizql-export.mjs", "zs_expected_columns.json", "ant_expected_columns.json", "catchup.mjs", "publish-cache.mjs", "xlsx-lite.mjs",
  "export-zs-as.mjs", "fusion-zs-as.mjs", "publish-zs-as.mjs", "extract-zs-filter-values.mjs",
  "urlnames.json", "urlnames-zs.json", "zs_ant_map.json", "zs_filter_values.json", "thumb-uris-zs.json",
  "package.json", "package-lock.json",
];
const fichiers = (process.argv.slice(2).length ? process.argv.slice(2) : DEFAUT).filter((f) => {
  if (existsSync(path.join(HERE, f))) return true;
  log(`⚠ ${f} introuvable — ignoré`); return false;
});

function gh(args, entree) {
  const a = ["api", ...args];
  if (entree) a.push("--input", entree);
  for (let essai = 1; essai <= 4; essai++) {
    try { return execFileSync("gh", a, { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 }); }
    catch (e) {
      if (essai === 4) throw e;
      const fin = Date.now() + 2000 * essai; while (Date.now() < fin) { }
    }
  }
}

const tmp = path.join(HERE, "out-zs", "_payload_tools.json");
const blob = (buf) => {
  writeFileSync(tmp, JSON.stringify({ encoding: "base64", content: buf.toString("base64") }));
  return JSON.parse(gh([`${REPO}/git/blobs`, "-X", "POST"], tmp)).sha;
};

const parent = JSON.parse(gh([`${REPO}/git/refs/heads/main`])).object.sha;
const base = JSON.parse(gh([`${REPO}/git/commits/${parent}`])).tree.sha;
const tree = fichiers.map((f) => {
  const buf = readFileSync(path.join(HERE, f));
  log(`  · ${f} (${(buf.length / 1024).toFixed(1)} Ko)`);
  return { path: DEST + f, mode: "100644", type: "blob", sha: blob(buf) };
});

const tp = path.join(HERE, "out-zs", "_tree_tools.json");
writeFileSync(tp, JSON.stringify({ base_tree: base, tree }));
const nouvelArbre = JSON.parse(gh([`${REPO}/git/trees`, "-X", "POST"], tp)).sha;
const cp = path.join(HERE, "out-zs", "_commit_tools.json");
writeFileSync(cp, JSON.stringify({
  message: `chore(mashako): outillage de synchro a jour cote cloud (${fichiers.length} fichiers)`,
  tree: nouvelArbre, parents: [parent],
}));
const commit = JSON.parse(gh([`${REPO}/git/commits`, "-X", "POST"], cp)).sha;
gh([`${REPO}/git/refs/heads/main`, "-X", "PATCH", "-f", `sha=${commit}`]);
log(`✓ ${fichiers.length} fichier(s) publiés dans ${DEST} — ${commit.slice(0, 9)}`);
