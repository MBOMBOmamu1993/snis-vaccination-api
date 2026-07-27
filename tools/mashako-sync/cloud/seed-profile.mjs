#!/usr/bin/env node
/**
 * Prépare un profil Chrome avec la session Tableau du PC — utilisé par la
 * synchro qui tourne sur GitHub Actions.
 *
 * Pourquoi ce détour : `sync.mjs` ouvre un profil Chrome persistant et fait ses
 * exports DEPUIS la page. On ne le modifie pas (consigne : ne toucher à rien de
 * ce qui marche) — on lui prépare donc simplement un profil déjà connecté.
 *
 * ⚠ PIÈGE RÉSOLU ICI : les cookies Tableau sont des cookies de SESSION
 * (`expires: -1`). Chrome ne les écrit pas sur le disque en se fermant — le
 * profil aurait donc été vide au lancement suivant. On leur donne une date
 * d'expiration explicite (30 jours) : côté serveur c'est rigoureusement le même
 * cookie, mais Chrome le conserve.
 *
 * Usage :
 *   TABLEAU_COOKIES='[{...}]' node cloud/seed-profile.mjs
 *   node cloud/seed-profile.mjs --fichier cookies-tableau.json
 *   node cloud/seed-profile.mjs --verifier      (contrôle sans rien écrire)
 */
import { chromium } from "playwright";
import { readFileSync, mkdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ICI = path.dirname(fileURLToPath(import.meta.url));
const RACINE = path.resolve(ICI, "..");
const PROFIL = path.join(RACINE, "browser-profile");
const SERVER = "https://eu-west-1a.online.tableau.com";
const VUE = `${SERVER}/#/site/axdata/views/Mashako3_0RapportdelAntenne/HZScores_ANT`;

const args = process.argv.slice(2);
const iFichier = args.indexOf("--fichier");
const VERIFIER = args.includes("--verifier");

function lireCookies() {
  if (iFichier >= 0) return JSON.parse(readFileSync(args[iFichier + 1], "utf8"));
  if (process.env.TABLEAU_COOKIES) return JSON.parse(process.env.TABLEAU_COOKIES);
  throw new Error("Aucun cookie : passe TABLEAU_COOKIES ou --fichier <chemin>.");
}

/* 30 jours : au-delà de la durée de vie réelle d'une session Tableau, ce qui
   est sans importance — c'est le serveur qui décide, pas le navigateur. */
const DANS_30_J = Math.floor(Date.now() / 1000) + 30 * 24 * 3600;

const cookies = lireCookies().map((c) => ({
  name: c.name,
  value: c.value,
  domain: c.domain || "eu-west-1a.online.tableau.com",
  path: c.path || "/",
  expires: !c.expires || c.expires < 0 ? DANS_30_J : c.expires,
  httpOnly: !!c.httpOnly,
  secure: c.secure !== false,
  sameSite: c.sameSite && ["Strict", "Lax", "None"].includes(c.sameSite) ? c.sameSite : "Lax",
}));

console.log(`→ ${cookies.length} cookies à injecter : ${cookies.map((c) => c.name).join(", ")}`);

mkdirSync(PROFIL, { recursive: true });
const ctx = await chromium.launchPersistentContext(PROFIL, {
  channel: "chrome", headless: true, ignoreDefaultArgs: ["--enable-automation"],
  viewport: { width: 1600, height: 950 },
  args: ["--no-first-run", "--no-default-browser-check", "--no-sandbox"],
});

try {
  await ctx.addCookies(cookies);

  /* Contrôle réel : on demande un export au serveur avec la session injectée.
     Un CSV = session valide ; du HTML = session morte (il faut relancer
     login.cmd sur le PC puis republier le secret). */
  const page = ctx.pages()[0] || await ctx.newPage();
  await page.goto(`${SERVER}/t/axdata/views/Mashako3_0RapportdelAntenne/CoverPage.png`,
    { waitUntil: "domcontentloaded", timeout: 180000 }).catch(() => { });

  const verdict = await page.evaluate(async (serveur) => {
    const url = `${serveur}/t/axdata/views/Mashako3_0RapportdelAntenne/PerformanceHeatmap_ANT.csv?_PARAM_month=Juillet&:refresh=yes`;
    try {
      const r = await fetch(url, { credentials: "include" });
      return { statut: r.status, type: r.headers.get("content-type") || "", taille: (await r.arrayBuffer()).byteLength };
    } catch (e) { return { erreur: String(e) }; }
  }, SERVER).catch((e) => ({ erreur: String(e) }));

  console.log(`→ Contrôle export : ${JSON.stringify(verdict)}`);
  const ok = verdict.statut === 200 && /csv/i.test(verdict.type || "");
  console.log(ok
    ? "✅ Session Tableau valide — le profil est prêt pour sync.mjs."
    : "✖ Session refusée. Sur le PC : login.cmd, puis republier le secret (cloud/publier-cookies.mjs).");
  if (!ok) process.exitCode = 1;
  if (VERIFIER) console.log("(mode --verifier : le profil est écrit quand même, c'est le lancement de Chrome qui le crée)");
} finally {
  /* La fermeture propre est ce qui écrit les cookies dans le profil. */
  await ctx.close();
}
