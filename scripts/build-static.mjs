#!/usr/bin/env node
/**
 * Build statique pour Vercel : publie le dashboard re-skiné (docs/index.html)
 * et ses logos dans dist/. Les données pré-agrégées DHIS2 ne sont PAS dupliquées
 * — index.html les charge depuis GitHub Pages (cf. DATA_ORIGIN) afin de garder
 * le déploiement Vercel léger et la synchro quotidienne (backfill → Pages) intacte.
 */
import { cp, mkdir, rm } from "node:fs/promises";
import path from "node:path";

const ROOT = process.cwd();
const DIST = path.join(ROOT, "dist");

const FILES = [
  ["docs/index.html", "dist/index.html"],
  ["docs/oms-white.png", "dist/oms-white.png"],
  ["docs/pev-transparent.png", "dist/pev-transparent.png"],
  ["docs/canevas_revue_formative_pev.pptx", "dist/canevas_revue_formative_pev.pptx"],
  ["docs/.nojekyll", "dist/.nojekyll"],
];

await rm(DIST, { recursive: true, force: true });
await mkdir(DIST, { recursive: true });
for (const [src, dst] of FILES) {
  try {
    await cp(path.join(ROOT, src), path.join(ROOT, dst));
    console.log(`[build-static] ${src} → ${dst}`);
  } catch (e) {
    console.warn(`[build-static] ignoré ${src}: ${e.message}`);
  }
}
console.log("[build-static] terminé.");
