#!/usr/bin/env node
/** Marque comme « à refaire » les feuilles d'une archive publiée dont la couverture
 *  en zones est trop faible (ex. août 2026 : 11 zones sur 519 pour la moitié des
 *  feuilles, synchro du 31/08 bridée). L'auto-complément de backfill-periods.mjs
 *  les ré-exporte alors par sessions VizQL.
 *
 *  Usage : node marquer-incomplet.mjs <zs|ant> <AAAA-MM> [seuilZones=400] [--dry]
 *  Ne touche qu'au meta.json de la période (base_tree) ; les anciens fichiers
 *  restent dans l'arbre mais ne sont plus référencés.
 */
import { execFileSync } from "node:child_process";
import { writeFileSync, mkdtempSync } from "node:fs";
import path from "node:path";
import os from "node:os";

const [canal, key, seuilArg, ...flags] = process.argv.slice(2);
if (!/^(zs|ant)$/.test(canal || "") || !/^\d{4}-\d{2}$/.test(key || "")) { console.log("Usage : node marquer-incomplet.mjs <zs|ant> <AAAA-MM> [seuilZones] [--dry]"); process.exit(2); }
const SEUIL = Number(seuilArg || 400);
const DRY = flags.includes("--dry");
const REPO = "repos/MBOMBOmamu1993/snis-vaccination-api";
const BRANCH = "mashako-data";
const PFX = canal === "zs" ? "zs/" : "";
const RAW = `https://raw.githubusercontent.com/MBOMBOmamu1993/snis-vaccination-api/${BRANCH}`;
const gh = (args, input) => execFileSync("gh", ["api", ...args, ...(input ? ["--input", input] : [])], { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
const tmp = mkdtempSync(path.join(os.tmpdir(), "marquer-"));

const meta = await (await fetch(`${RAW}/${PFX}periods/${key}/meta.json?_r=${Date.now()}`)).json();
const total = (meta.antennes && meta.antennes.values || []).length || 519;
const marques = [];
for (const v of meta.views) {
  if (!v.file) continue;
  let zones = 0, rows = 0;
  try {
    const j = await (await fetch(`${RAW}/${PFX}periods/${key}/${v.file}?_r=${Date.now()}`)).json();
    rows = j.rows.length; zones = new Set(j.rows.map((r) => r.Antenne)).size;
  } catch (e) { console.log(`  ? ${v.name} : fichier illisible (${e.message})`); continue; }
  const ok = zones >= SEUIL;
  console.log(`  ${ok ? "✓" : "✗"} ${v.name.padEnd(28)} ${String(rows).padStart(6)} lignes ${String(zones).padStart(4)}/${total} zones${ok ? "" : "  → à refaire"}`);
  if (!ok) { marques.push(v.name); v.file = null; v.rows = 0; v.deferred = true; v.marked_incomplete_at = new Date().toISOString(); v.previous_zones = zones; }
}
if (!marques.length) { console.log("Rien à marquer."); process.exit(0); }
console.log(`${marques.length} feuille(s) marquée(s) : ${marques.join(", ")}`);
if (DRY) { console.log("[dry] rien publié."); process.exit(0); }

meta.marked_incomplete = { at: new Date().toISOString(), seuil: SEUIL, views: marques };
const ref = JSON.parse(gh([`${REPO}/git/refs/heads/${BRANCH}`]));
const baseTree = JSON.parse(gh([`${REPO}/git/commits/${ref.object.sha}`])).tree.sha;
const payload = path.join(tmp, "blob.json");
writeFileSync(payload, JSON.stringify({ encoding: "base64", content: Buffer.from(JSON.stringify(meta, null, 2)).toString("base64") }));
const blob = JSON.parse(gh([`${REPO}/git/blobs`, "-X", "POST"], payload)).sha;
const treeP = path.join(tmp, "tree.json");
writeFileSync(treeP, JSON.stringify({ base_tree: baseTree, tree: [{ path: `${PFX}periods/${key}/meta.json`, mode: "100644", type: "blob", sha: blob }] }));
const tree = JSON.parse(gh([`${REPO}/git/trees`, "-X", "POST"], treeP)).sha;
const commitP = path.join(tmp, "commit.json");
writeFileSync(commitP, JSON.stringify({ message: `auto: ${PFX}${key} — ${marques.length} feuille(s) marquée(s) incomplètes (< ${SEUIL} zones), à recompléter`, tree, parents: [] }));
const commit = JSON.parse(gh([`${REPO}/git/commits`, "-X", "POST"], commitP)).sha;
gh([`${REPO}/git/refs/heads/${BRANCH}`, "-X", "PATCH", "-f", `sha=${commit}`, "-F", "force=true"]);
console.log(`✓ publié ${commit.slice(0, 9)}`);
