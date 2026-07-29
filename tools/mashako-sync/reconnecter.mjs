#!/usr/bin/env node
/** MAINTIEN DE LA SESSION TABLEAU — sonde, reconnexion silencieuse, republication.
 *
 *  Pourquoi c'est possible : quand la session Tableau tombe, la page ne demande
 *  PAS de mot de passe. Elle s'arrête sur le sélecteur de compte Google, parce
 *  que la session Google du profil, elle, est toujours vivante et bien plus
 *  durable. Il ne manque qu'un clic sur la vignette du compte — que ce script
 *  fait à la place de Felly, dans SON profil, sans aucun identifiant stocké.
 *
 *  Trois cas à la sortie :
 *    ✓ session valide            → rien à faire (et la visite la garde au chaud)
 *    ✓ reconnectée d'un clic     → cookies republiés dans le secret GitHub
 *    ⚠ Google demande plus       → mot de passe ou 2FA : là, seul Felly peut agir,
 *                                  on le dit clairement (alerte inchangée)
 *
 *  Le passage régulier sert AUSSI de maintien en vie : une session Tableau
 *  inactive expire, et la licence du compte est révoquée après un mois sans
 *  connexion. Une visite toutes les 30 min règle les deux.
 *
 *  Usage : node reconnecter.mjs [--sonde] [--visible]
 *          --sonde   : diagnostic seul, ne clique rien
 *          --visible : fenêtre Chrome affichée (dépannage)
 *  Sortie : 0 session utilisable, 1 intervention humaine requise.
 */
import { chromium } from "playwright";
import { execFileSync } from "node:child_process";
import { appendFileSync, statSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PROFILE = path.join(HERE, "browser-profile");
const JOURNAL = path.join(HERE, "reconnexion.log");
const SERVEUR = "https://eu-west-1a.online.tableau.com";
/* Feuille LÉGÈRE : la sonde ne doit pas dépendre du rendu d'un dashboard lourd
   (plusieurs minutes), seulement de la validité de la session. */
const CIBLE = `${SERVEUR}/#/site/axdata/views/Mashako3_0RapportdelAntenne/FILTER_VALUES_ANT`;
const PREUVE = `${SERVEUR}/t/axdata/views/Mashako3_0RapportdelAntenne/FILTER_VALUES_ANT.csv?:refresh=yes`;
const COMPTE = process.env.MASHAKO_COMPTE || "fellybokota@gmail.com";
const SONDE = process.argv.includes("--sonde");
const VISIBLE = process.argv.includes("--visible");

const log = (m) => {
  const l = `[${new Date().toISOString()}] ${m}`;
  console.log(l);
  try { appendFileSync(JOURNAL, l + "\n"); } catch (e) { }
};

/* Preuve de session : un export CSV répond en texte quand la session vit, et
   renvoie du HTML de connexion (ou une erreur) sinon. Beaucoup plus fiable et
   rapide que d'attendre le rendu d'un dashboard. */
async function sessionValide(page) {
  /* Une navigation encore en cours détruit le contexte d'exécution et ferait
     conclure à tort à une session morte : on laisse la page se poser, puis on
     réessaie. */
  for (let essai = 1; essai <= 3; essai++) {
    await page.waitForTimeout(essai === 1 ? 4000 : 6000);
    const ok = await page.evaluate(async (u) => {
      try {
        const r = await fetch(u, { credentials: "include", cache: "no-store" });
        if (!r.ok) return null;
        const t = await r.text();
        return t.length > 40 && !/<html|<!DOCTYPE/i.test(t.slice(0, 200));
      } catch (e) { return null; }
    }, PREUVE).catch(() => null);
    if (ok === true) return true;
    if (ok === false) return false; // réponse claire du serveur : session morte
  }
  return false;
}

/* État de la page, sans ambiguïté : c'est lui qui décide de la suite. */
async function etat(page) {
  return await page.evaluate(() => {
    const u = location.href;
    const txt = document.body ? document.body.innerText : "";
    const html = document.body ? document.body.innerHTML : "";
    const google = /accounts\.google\.|gds\.google\.|sso\.online\.tableau\.com/.test(u);
    return {
      url: u,
      tableau: /online\.tableau\.com/.test(u) && !google,
      chargement: /Ouverture du classeur|Processus en cours/i.test(txt),
      google,
      /* Le sélecteur de comptes n'exige qu'un clic ; un champ mot de passe ou
         une demande de validation exige Felly. */
      choixCompte: google && /Choisir un compte|Sélectionnez un compte|Choose an account/i.test(txt),
      motDePasse: google && (!!document.querySelector("input[type='password']") || /Saisissez votre mot de passe|Enter your password/i.test(txt)),
      deuxFacteurs: google && /valider|vérification en 2|2-Step|Confirmez|approuvez/i.test(txt) && !/Choisir un compte|Sélectionnez un compte/i.test(txt),
      aVignette: html.indexOf("data-identifier") >= 0,
    };
  }).catch(() => ({ url: "", tableau: false, chargement: true }));
}

async function attendre(page, predicat, plafondMs) {
  const fin = Date.now() + plafondMs;
  while (Date.now() < fin) {
    const e = await etat(page);
    if (predicat(e)) return e;
    await page.waitForTimeout(2500);
  }
  return await etat(page);
}

/* Une synchro en cours utilise DÉJÀ le profil : lui prendre Chrome la ferait
   échouer, et sa seule existence prouve que la session vit. On passe notre tour. */
try {
  const st = statSync(path.join(HERE, "out", ".sync.lock"));
  if (Date.now() - st.mtimeMs < 30 * 60000) {
    log("⏭ Synchro en cours (verrou frais) — la session est forcément valide, rien à faire.");
    process.exit(0);
  }
} catch (e) { /* pas de verrou : on continue */ }

let ctx = null, code = 1;
try {
  for (let essai = 1; essai <= 3 && !ctx; essai++) {
    try {
      ctx = await chromium.launchPersistentContext(PROFILE, {
        channel: "chrome", headless: !VISIBLE, ignoreDefaultArgs: ["--enable-automation"],
        viewport: { width: 1400, height: 900 }, args: ["--no-first-run", "--no-default-browser-check"],
      });
    } catch (e) {
      if (essai === 3) throw e;
      log(`⟳ Chrome n'a pas démarré (essai ${essai}/3) — le profil est peut-être utilisé ; nouvel essai dans 20 s`);
      await new Promise((r) => setTimeout(r, 20000));
    }
  }
  const page = ctx.pages()[0] || await ctx.newPage();
  await page.goto(CIBLE, { waitUntil: "domcontentloaded", timeout: 120000 }).catch(() => { });
  /* La chaîne Tableau → SSO → Google enchaîne plusieurs redirections : il faut
     attendre un état TERMINAL (vignette de compte, mot de passe, ou retour sur
     Tableau), sinon on conclut à tort sur une page de passage. */
  let e = await attendre(page, (s) => s.choixCompte || s.aVignette || s.motDePasse || s.deuxFacteurs ||
    (s.tableau && !/\/public\/idp\//.test(s.url)), 120000);
  if (e.tableau && !e.google && await sessionValide(page)) {
    log("✓ Session Tableau valide — rien à faire (la visite compte comme maintien en vie).");
    code = 0;
  }
  else if (e.motDePasse || e.deuxFacteurs) {
    log(`⚠ Google demande ${e.motDePasse ? "le mot de passe" : "une validation en deux étapes"} — seule une intervention humaine peut répondre. Sur le PC : login.cmd`);
  } else if (e.choixCompte || e.aVignette) {
    if (SONDE) { log("· Sélecteur de compte Google affiché — un clic suffirait (mode sonde : rien n'est cliqué)."); }
    else {
      log(`→ Session expirée, sélecteur de compte Google : sélection de ${COMPTE}…`);
      /* La vignette porte l'adresse en attribut ; repli sur le texte visible. */
      const cibles = [`div[data-identifier="${COMPTE}"]`, `[data-email="${COMPTE}"]`, `li:has-text("${COMPTE}")`, `text=${COMPTE}`];
      let clique = false;
      for (const sel of cibles) {
        const el = page.locator(sel).first();
        if (await el.count().catch(() => 0)) { await el.click({ timeout: 15000 }).catch(() => { }); clique = true; break; }
      }
      if (!clique) log("⚠ Vignette du compte introuvable — page Google inhabituelle.");
      else {
        e = await attendre(page, (s) => (s.tableau && !s.google) || s.motDePasse || s.deuxFacteurs, 180000);
        if (e.tableau && await sessionValide(page)) {
          log("✓ Reconnecté sans intervention — session Tableau rétablie.");
          try {
            execFileSync(process.execPath, [path.join(HERE, "cloud", "publier-cookies.mjs")], { stdio: "inherit" });
            log("✓ Secret TABLEAU_COOKIES republié — le relais cloud repart avec cette session.");
          } catch (err) {
            log(`⚠ Republication du secret impossible (${String(err.message).slice(0, 90)}) — à refaire : node cloud/publier-cookies.mjs`);
          }
          code = 0;
        } else if (e.motDePasse || e.deuxFacteurs) {
          log("⚠ Google réclame le mot de passe ou une validation : la session Google elle-même a expiré. Sur le PC : login.cmd");
        } else log(`⚠ État inattendu après le clic : ${e.url.slice(0, 110)}`);
      }
    }
  } else log(`⚠ Page inattendue : ${(e.url || "(vide)").slice(0, 130)}`);
} catch (e) {
  log(`✖ Échec : ${String(e.message).slice(0, 160)}`);
} finally {
  await ctx?.close().catch(() => { });
}
process.exit(code);
