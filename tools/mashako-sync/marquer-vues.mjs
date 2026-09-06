#!/usr/bin/env node
/** Marque « à refaire » (file:null, deferred:true) des vues NOMMÉES de plusieurs
 *  archives ZS/ANT publiées, en UN SEUL commit (base_tree). Complète
 *  marquer-incomplet.mjs (qui juge au seuil de zones) pour les faux complets
 *  connus : feuille tronquée par l'export groupé, mauvaise feuille archivée…
 *
 *  Usage : node marquer-vues.mjs <zs|ant> "<AAAA-MM>[,<AAAA-MM>…]" "<vue>[,<vue>…]" [--dry]
 *  Plusieurs couples peuvent être passés en répétant période(s)+vues : 
 *    node marquer-vues.mjs zs 2026-05,2026-06 Supervision_HZ_P3 2026-06 CDF_HZ_P2
 */
import { execFileSync } from "node:child_process";
import { writeFileSync, mkdtempSync } from "node:fs";
import path from "node:path";
import os from "node:os";
const args = process.argv.slice(2);
const DRY = args.includes("--dry");
const [canal, ...rest] = args.filter((a) => a !== "--dry");
if (!/^(zs|ant)$/.test(canal || "") || rest.length < 2 || rest.length % 2) { console.log("Usage : node marquer-vues.mjs <zs|ant> <périodes> <vues> [<périodes> <vues>…] [--dry]"); process.exit(2); }
const REPO = "repos/MBOMBOmamu1993/snis-vaccination-api", BRANCH = "mashako-data";
const PFX = canal === "zs" ? "zs/" : "";
const RAW = `https://raw.githubusercontent.com/MBOMBOmamu1993/snis-vaccination-api/${BRANCH}`;
const gh = (a, input) => execFileSync("gh", ["api", ...a, ...(input ? ["--input", input] : [])], { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
const tmp = mkdtempSync(path.join(os.tmpdir(), "marquer-vues-"));
const cibles = {}; // key → Set(vues)
for (let i = 0; i < rest.length; i += 2) for (const k of rest[i].split(",")) for (const v of rest[i + 1].split(",")) (cibles[k] ||= new Set()).add(v.trim());
const ref = JSON.parse(gh([`${REPO}/git/refs/heads/${BRANCH}`]));
const baseTree = JSON.parse(gh([`${REPO}/git/commits/${ref.object.sha}`])).tree.sha;
const tree = []; const resume = [];
for (const [key, vues] of Object.entries(cibles)) {
  const meta = await (await fetch(`${RAW}/${PFX}periods/${key}/meta.json?_r=${Date.now()}`)).json();
  const marques = [];
  for (const v of meta.views) {
    if (!vues.has(v.name) && !vues.has(v.urlName)) continue;
    if (!v.file && v.deferred) { console.log(`  = ${key} ${v.name} : déjà différée`); continue; }
    marques.push(v.name);
    v.previous_rows = v.rows; v.file = null; v.rows = 0; v.deferred = true; v.marked_incomplete_at = new Date().toISOString(); delete v.partial; delete v.zones_traitees; delete v.empty;
  }
  const inconnues = [...vues].filter((n) => !meta.views.some((v) => v.name === n || v.urlName === n));
  if (inconnues.length) console.log(`  ? ${key} : vues inconnues ${inconnues.join(", ")}`);
  if (!marques.length) continue;
  meta.marked_views = [...(meta.marked_views || []), { at: new Date().toISOString(), views: marques }];
  console.log(`  ✗ ${key} : ${marques.join(", ")} → à refaire`);
  resume.push(`${key}:${marques.join("+")}`);
  if (DRY) continue;
  const p = path.join(tmp, `${key}.json`);
  writeFileSync(p, JSON.stringify({ encoding: "base64", content: Buffer.from(JSON.stringify(meta, null, 2)).toString("base64") }));
  tree.push({ path: `${PFX}periods/${key}/meta.json`, mode: "100644", type: "blob", sha: JSON.parse(gh([`${REPO}/git/blobs`, "-X", "POST"], p)).sha });
}
if (!tree.length) { console.log(DRY ? "[dry] rien publié." : "Rien à marquer."); process.exit(0); }
/* Relire la référence juste avant d'écrire : le cloud publie en parallèle. */
const ref2 = JSON.parse(gh([`${REPO}/git/refs/heads/${BRANCH}`]));
const base2 = ref2.object.sha === ref.object.sha ? baseTree : JSON.parse(gh([`${REPO}/git/commits/${ref2.object.sha}`])).tree.sha;
if (ref2.object.sha !== ref.object.sha) console.log(`  ⟳ la branche a bougé pendant la préparation (${ref.object.sha.slice(0, 9)} → ${ref2.object.sha.slice(0, 9)}) — base_tree rafraîchi`);
const tp = path.join(tmp, "tree.json"); writeFileSync(tp, JSON.stringify({ base_tree: base2, tree }));
const t = JSON.parse(gh([`${REPO}/git/trees`, "-X", "POST"], tp)).sha;
const cp = path.join(tmp, "commit.json"); writeFileSync(cp, JSON.stringify({ message: `auto: ${PFX}archives — vues marquées à refaire (${resume.join(" ; ")})`, tree: t, parents: [] }));
const c = JSON.parse(gh([`${REPO}/git/commits`, "-X", "POST"], cp)).sha;
gh([`${REPO}/git/refs/heads/${BRANCH}`, "-X", "PATCH", "-F", "force=true", "-f", `sha=${c}`]);
console.log(`✓ publié ${c.slice(0, 9)} — ${tree.length} meta.json`);
