#!/usr/bin/env node
/**
 * Publie la session Tableau du PC dans le secret GitHub `TABLEAU_COOKIES`,
 * que la synchro cloud utilise quand le PC est éteint.
 *
 * À relancer uniquement après un `login.cmd` (nouvelle session Tableau).
 * Tant que la session vit, le cookie ne change pas : inutile de le republier.
 *
 * ⚠ Le profil Chrome doit être LIBRE (aucune synchro en cours), sinon Chrome
 * refuse de l'ouvrir deux fois. Le script le vérifie et s'arrête proprement.
 *
 * Usage :  node cloud/publier-cookies.mjs
 *          node cloud/publier-cookies.mjs --local   (écrit le fichier, sans envoyer)
 */
import { chromium } from "playwright";
import { execFileSync } from "node:child_process";
import { writeFileSync, statSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ICI = path.dirname(fileURLToPath(import.meta.url));
const RACINE = path.resolve(ICI, "..");
const PROFIL = path.join(RACINE, "browser-profile");
const SORTIE = path.join(RACINE, "cookies-tableau.json");
const DEPOT = process.env.MASHAKO_DEPOT || "MBOMBOmamu1993/snis-vaccination-api";
const LOCAL = process.argv.includes("--local");

/* Verrou partagé : une synchro en cours tient le profil Chrome. */
try {
  const st = statSync(path.join(RACINE, "out", ".sync.lock"));
  if (Date.now() - st.mtimeMs < 2 * 3600 * 1000) {
    console.log("⏭ Une synchro est en cours (verrou récent) — relance quand elle sera finie.");
    process.exit(0);
  }
} catch (e) { /* pas de verrou */ }

const ctx = await chromium.launchPersistentContext(PROFIL, {
  channel: "chrome", headless: true, ignoreDefaultArgs: ["--enable-automation"],
  args: ["--no-first-run", "--no-default-browser-check"],
});

try {
  const tous = await ctx.cookies();
  const cookies = tous.filter((c) => /tableau\.com$/.test((c.domain || "").replace(/^\./, "")));
  if (!cookies.some((c) => c.name === "workgroup_session_id")) {
    console.log("✖ Pas de session Tableau dans le profil. Lance d'abord login.cmd.");
    process.exitCode = 1;
  } else {
    const json = JSON.stringify(cookies, null, 2);
    writeFileSync(SORTIE, json);
    console.log(`→ ${cookies.length} cookies Tableau extraits (${cookies.map((c) => c.name).join(", ")}) → ${SORTIE}`);
    if (LOCAL) console.log("(mode --local : rien n'a été envoyé à GitHub)");
    else {
      execFileSync("gh", ["secret", "set", "TABLEAU_COOKIES", "--repo", DEPOT, "--body", json], { stdio: ["ignore", "inherit", "inherit"] });
      console.log("✅ Secret TABLEAU_COOKIES mis à jour — la synchro cloud repart avec cette session.");
    }
  }
} finally {
  await ctx.close();
}
