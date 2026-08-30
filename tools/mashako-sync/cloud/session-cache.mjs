#!/usr/bin/env node
/**
 * RECONDUCTION DE LA SESSION TABLEAU ENTRE DEUX RELAIS CLOUD.
 *
 * Le problème que ce script règle : le secret TABLEAU_COOKIES est figé au jour
 * où le PC l'a publié. La session qu'il contient vieillit, meurt (fin juillet
 * 2026 : un mois d'indicateurs Mashako gelés), et tout retombe sur un clic
 * humain. Or chaque passage du relais cloud UTILISE la session : le profil
 * Chrome du runner contient à la fin des cookies plus frais que le secret.
 * Il suffit de les reconduire d'un run à l'autre pour que la session ne meure
 * jamais d'inactivité — le PC peut rester éteint.
 *
 * Où : dans le cache GitHub Actions, CHIFFRÉS (AES-256-GCM, clé dans le secret
 * MASHAKO_SESSION_KEY). Le dépôt est PUBLIC : un cache en clair serait lisible
 * depuis une pull request de fork ; le chiffrement rend le cache inerte sans
 * le secret, que les forks n'ont pas.
 *
 * Usage (depuis tools/mashako-sync, MASHAKO_SESSION_KEY dans l'environnement) :
 *   node cloud/session-cache.mjs --sauver    profil Chrome → session-cloud.enc
 *   node cloud/session-cache.mjs --charger   session-cloud.enc → cookies-cache.json
 *
 * Codes de sortie : 0 OK ; 1 rien à sauver / cache absent, illisible ou sans
 * session (l'appelant se replie alors sur le secret TABLEAU_COOKIES).
 */
import { chromium } from "playwright";
import { createCipheriv, createDecipheriv, randomBytes, createHash } from "node:crypto";
import { readFileSync, writeFileSync, statSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ICI = path.dirname(fileURLToPath(import.meta.url));
const RACINE = path.resolve(ICI, "..");
const PROFIL = path.join(RACINE, "browser-profile");
const FICHIER = path.join(RACINE, "session-cloud.enc");
const SORTIE = path.join(RACINE, "cookies-cache.json");

const SAUVER = process.argv.includes("--sauver");
const CHARGER = process.argv.includes("--charger");

const brut = process.env.MASHAKO_SESSION_KEY || "";
if (!brut) {
  console.log("✖ MASHAKO_SESSION_KEY absent — reconduction de session impossible.");
  process.exit(1);
}
/* La clé peut être n'importe quelle chaîne robuste : on la condense en 32 octets. */
const CLE = createHash("sha256").update(brut).digest();

function chiffrer(texte) {
  const iv = randomBytes(12);
  const c = createCipheriv("aes-256-gcm", CLE, iv);
  const données = Buffer.concat([c.update(texte, "utf8"), c.final()]);
  return Buffer.concat([iv, c.getAuthTag(), données]).toString("base64");
}

function dechiffrer(b64) {
  const tout = Buffer.from(b64, "base64");
  const d = createDecipheriv("aes-256-gcm", CLE, tout.subarray(0, 12));
  d.setAuthTag(tout.subarray(12, 28));
  return Buffer.concat([d.update(tout.subarray(28)), d.final()]).toString("utf8");
}

if (SAUVER) {
  /* Mêmes filtres que cloud/publier-cookies.mjs : la session est dans les
     cookies du domaine tableau.com, et workgroup_session_id en est la preuve. */
  const ctx = await chromium.launchPersistentContext(PROFIL, {
    channel: "chrome", headless: true, ignoreDefaultArgs: ["--enable-automation"],
    args: ["--no-first-run", "--no-default-browser-check", "--no-sandbox"],
  });
  let cookies = [];
  try {
    cookies = (await ctx.cookies()).filter((c) => /tableau\.com$/.test((c.domain || "").replace(/^\./, "")));
  } finally {
    await ctx.close();
  }
  if (!cookies.some((c) => c.name === "workgroup_session_id")) {
    console.log("✖ Pas de session Tableau dans le profil — rien à reconduire.");
    process.exit(1);
  }
  writeFileSync(FICHIER, chiffrer(JSON.stringify(cookies)));
  console.log(`✅ Session reconduite : ${cookies.length} cookies chiffrés → ${path.basename(FICHIER)} (pour le prochain relais).`);
  process.exit(0);
}

if (CHARGER) {
  let texte;
  try {
    const st = statSync(FICHIER);
    texte = dechiffrer(readFileSync(FICHIER, "utf8"));
    const ageH = ((Date.now() - st.mtimeMs) / 3600000).toFixed(1);
    console.log(`→ Cache de session trouvé (âge ${ageH} h).`);
  } catch (e) {
    console.log(`✖ Cache de session absent ou illisible (${String(e.message).slice(0, 60)}) — repli sur le secret.`);
    process.exit(1);
  }
  let cookies;
  try { cookies = JSON.parse(texte); } catch (e) { cookies = null; }
  if (!Array.isArray(cookies) || !cookies.some((c) => c.name === "workgroup_session_id")) {
    console.log("✖ Cache déchiffré mais sans session Tableau — repli sur le secret.");
    process.exit(1);
  }
  writeFileSync(SORTIE, JSON.stringify(cookies, null, 2));
  console.log(`✅ ${cookies.length} cookies restaurés → ${path.basename(SORTIE)}.`);
  process.exit(0);
}

console.log("Usage : node cloud/session-cache.mjs --sauver | --charger");
process.exit(1);
