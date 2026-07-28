#!/usr/bin/env node
/** Publication du détail par aire de santé sur la branche mashako-data.
 *
 *  Pose uniquement les fichiers zs/views/<feuille>_AS.json, sur base_tree :
 *  tout le reste de la branche (classeur Antenne, feuilles ZS, archives
 *  periods/) est laissé intact. Le dashboard les charge par convention de nom,
 *  sans passer par meta.json — la publication est donc indépendante de la
 *  synchro et peut être rejouée à tout moment pendant que l'export tourne.
 *
 *  Usage : node publish-zs-as.mjs [--fusion]
 *          --fusion : recombine d'abord les tranches (fusion-zs-as.mjs).
 */
import { readFileSync, writeFileSync, readdirSync, existsSync } from "node:fs";
import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const VUES = path.join(HERE, "out-zs", "views");
const PFX = "zs/";
const REPO = "repos/MBOMBOmamu1993/snis-vaccination-api";
const BRANCHE = "mashako-data";
const log = (m) => console.log(`[${new Date().toISOString()}] ${m}`);

const RETRIABLE = /timeout|connection|connect|reset|EOF|handshake|temporarily|401|502|503|504|malformed|400/i;
function gh(args, entree) {
  const a = ["api", ...args];
  if (entree) a.push("--input", entree);
  let derniere;
  for (let essai = 1; essai <= 5; essai++) {
    try { return execFileSync("gh", a, { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 }); }
    catch (e) {
      derniere = e;
      const msg = String((e.stderr || "") + (e.message || ""));
      if (!RETRIABLE.test(msg) || essai === 5) throw e;
      log(`  ⟳ appel GitHub en échec (${msg.trim().slice(0, 70)}) — essai ${essai + 1}/5`);
      const fin = Date.now() + 2000 * essai; while (Date.now() < fin) { }
    }
  }
  throw derniere;
}

if (process.argv.includes("--fusion")) {
  execFileSync(process.execPath, [path.join(HERE, "fusion-zs-as.mjs")], { stdio: "inherit" });
}

const fichiers = readdirSync(VUES).filter((f) => /_AS\.json$/.test(f));
if (!fichiers.length) { log("✗ aucun fichier _AS.json à publier"); process.exit(1); }

let zones = new Set(), lignes = 0;
for (const f of fichiers) {
  try {
    const j = JSON.parse(readFileSync(path.join(VUES, f), "utf8"));
    (j.zones || []).forEach((z) => zones.add(z));
    lignes += (j.rows || []).length;
  } catch (e) { log(`⚠ ${f} illisible — ignoré`); }
}
log(`${fichiers.length} feuille(s), ${lignes} lignes, ${zones.size} zone(s) de santé couvertes`);

const ref = JSON.parse(gh([`${REPO}/git/refs/heads/${BRANCHE}`]));
const treeSha = JSON.parse(gh([`${REPO}/git/commits/${ref.object.sha}`])).tree.sha;

const tmp = path.join(HERE, "out-zs", "_payload_as.json");
const blob = (buf) => {
  writeFileSync(tmp, JSON.stringify({ encoding: "base64", content: buf.toString("base64") }));
  return JSON.parse(gh([`${REPO}/git/blobs`, "-X", "POST"], tmp)).sha;
};
const tree = fichiers.map((f) => ({
  path: `${PFX}views/${f}`, mode: "100644", type: "blob",
  sha: blob(readFileSync(path.join(VUES, f))),
}));

const tp = path.join(HERE, "out-zs", "_tree_as.json");
writeFileSync(tp, JSON.stringify({ base_tree: treeSha, tree }));
const nouvelArbre = JSON.parse(gh([`${REPO}/git/trees`, "-X", "POST"], tp)).sha;
const cp = path.join(HERE, "out-zs", "_commit_as.json");
writeFileSync(cp, JSON.stringify({
  message: `detail aire de sante : ${fichiers.length} feuilles, ${zones.size} zones, ${lignes} lignes (${new Date().toISOString().slice(0, 10)})`,
  tree: nouvelArbre, parents: [],
}));
const commit = JSON.parse(gh([`${REPO}/git/commits`, "-X", "POST"], cp)).sha;
gh([`${REPO}/git/refs/heads/${BRANCHE}`, "-X", "PATCH", "-f", `sha=${commit}`, "-F", "force=true"]);
log(`✓ Publié ${commit.slice(0, 9)} — ${fichiers.length} feuilles, ${zones.size} zones.`);
