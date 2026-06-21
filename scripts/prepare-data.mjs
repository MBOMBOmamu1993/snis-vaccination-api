#!/usr/bin/env node
/**
 * Copie les données pré-agrégées DHIS2 (docs/data*) vers public/data* afin
 * qu'elles soient servies en STATIQUE par Next/Vercel (décompression gzip côté
 * navigateur). docs/data* reste la SOURCE DE VÉRITÉ alimentée par le pipeline
 * backfill / update — on n'y touche pas. Ce script s'exécute avant dev/build.
 */
import { cp, mkdir, stat } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";

const ROOT = process.cwd();
const PUB = path.join(ROOT, "public", "data");

// On ne publie que le strict nécessaire au dashboard : les agrégats dashboard.
const SETS = [
  ["docs/data/dashboard", "public/data/dashboard"],
];

async function main() {
  await mkdir(path.dirname(PUB), { recursive: true });
  for (const [src, dst] of SETS) {
    const s = path.join(ROOT, src);
    const d = path.join(ROOT, dst);
    if (!existsSync(s)) { console.warn(`[prepare-data] source absente: ${src} (ignoré)`); continue; }
    await mkdir(path.dirname(d), { recursive: true });
    await cp(s, d, { recursive: true, force: true });
    const meta = path.join(d, "meta.json");
    const ok = existsSync(meta) ? "+ meta.json" : "";
    console.log(`[prepare-data] ${src} → ${dst} ${ok}`);
  }
  console.log("[prepare-data] terminé.");
}

main().catch((e) => { console.error("[prepare-data] échec:", e); process.exit(1); });
