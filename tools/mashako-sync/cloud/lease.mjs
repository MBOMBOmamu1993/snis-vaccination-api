#!/usr/bin/env node
/**
 * BAIL PARTAGÉ — coordination entre le PC de Felly et la VM de secours.
 *
 * Objectif : le PC reste PRIORITAIRE. La VM (Oracle Always Free) ne prend la
 * main que si personne ne travaille. Sans ce bail, les deux machines
 * pourraient exporter en même temps → Tableau bride le compte (constaté les
 * 24 et 25/07) et les deux runs échouent.
 *
 * Le bail vit dans une RÉFÉRENCE GIT HORS-BRANCHE, une par canal :
 *   refs/mashako/lease-ant   (rapport Antenne)
 *   refs/mashako/lease-zs    (rapport Zone de Santé)
 *
 * ⚠ Surtout PAS une branche : le dépôt est relié à Vercel, qui déclenche un
 * déploiement d'aperçu à chaque poussée sur refs/heads/*. Un bail renouvelé
 * toutes les 10 min ferait une centaine de builds par jour et saturerait le
 * projet. Les refs hors « refs/heads » sont ignorées par Vercel et n'encombrent
 * pas non plus la liste des branches.
 *
 * Chaque écriture est UN commit SANS PARENT, sur l'arbre VIDE, dont le message
 * porte le JSON du bail : 2 appels d'API pour écrire, 2 pour lire, et un
 * historique qui ne grossit jamais.
 *
 * Contenu de bail.json :
 *   { canal, etat: "actif"|"libre", titulaire: "pc:DESKTOP-X"|"vm:mashako-vm",
 *     pid, debut, battement, note, resultat }
 *
 * ⚠ RÈGLE ABSOLUE — FAIL-OPEN : si GitHub est injoignable, TOUTES les
 * fonctions renvoient null/false sans lever. Une panne réseau ne doit JAMAIS
 * empêcher une synchro de tourner ; au pire les deux machines travaillent en
 * même temps, ce qui reste moins grave qu'aucune synchro du tout.
 *
 * Usage en ligne de commande :
 *   node cloud/lease.mjs etat            → affiche les deux baux
 *   node cloud/lease.mjs poser zs        → pose un bail (test)
 *   node cloud/lease.mjs liberer zs      → libère
 */
import { execFileSync } from "node:child_process";
import { writeFileSync, unlinkSync } from "node:fs";
import { tmpdir, hostname } from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const REPO = process.env.MASHAKO_REPO || "repos/MBOMBOmamu1993/snis-vaccination-api";
/* pc = machine de Felly (défaut, aucun réglage à faire côté Windows)
   vm = machine de secours (le service systemd pose MASHAKO_ROLE=vm) */
const ROLE = process.env.MASHAKO_ROLE || "pc";
export const TITULAIRE = `${ROLE}:${hostname()}`;

/* Un bail plus vieux que ça est considéré PÉRIMÉ : c'est le délai au bout
   duquel la VM reprend un travail abandonné (PC éteint en pleine synchro).
   25 min = 2 battements manqués + marge réseau. */
export const FRAIS_MS = Number(process.env.MASHAKO_BAIL_FRAIS_MIN || 25) * 60_000;
const BATTEMENT_MS = Number(process.env.MASHAKO_BAIL_BATTEMENT_MIN || 10) * 60_000;

const RETRIABLE = /timeout|connection|connect|reset|EOF|handshake|temporarily|502|503|504/i;

function dormir(ms) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

function gh(args, corps) {
  const a = ["api", ...args];
  let tmp = null;
  if (corps) {
    tmp = path.join(tmpdir(), `bail-${process.pid}-${Date.now()}.json`);
    writeFileSync(tmp, JSON.stringify(corps));
    a.push("--input", tmp);
  }
  try {
    let derniere;
    for (let essai = 1; essai <= 3; essai++) {
      try {
        return execFileSync("gh", a, { encoding: "utf8", maxBuffer: 8 * 1024 * 1024, stdio: ["ignore", "pipe", "pipe"] });
      } catch (e) {
        derniere = e;
        if (!RETRIABLE.test(String(e.stderr || e.message || ""))) throw e;
        dormir(essai * 3000);
      }
    }
    throw derniere;
  } finally {
    if (tmp) { try { unlinkSync(tmp); } catch (_) { } }
  }
}

/** Nom de la référence, sans le préfixe « refs/ ». */
export function refNom(canal) { return `mashako/lease-${canal}`; }
/* Arbre vide de git : présent dans tout dépôt, évite de créer un blob. */
const ARBRE_VIDE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";

/** Lit le bail du canal. null = jamais posé, ou GitHub muet (fail-open). */
export function lire(canal) {
  try {
    const sha = gh([`${REPO}/git/ref/${refNom(canal)}`, "--jq", ".object.sha"]).trim();
    const msg = gh([`${REPO}/git/commits/${sha}`, "--jq", ".message"]);
    return JSON.parse(msg.slice(msg.indexOf("{")));
  } catch (e) {
    return null; /* 404 = jamais posé ; réseau = fail-open */
  }
}

function ecrire(canal, bail) {
  const commit = JSON.parse(gh([`${REPO}/git/commits`, "-X", "POST"],
    { message: `MASHAKO-BAIL ${JSON.stringify(bail)}`, tree: ARBRE_VIDE, parents: [] })).sha;
  try {
    gh([`${REPO}/git/refs/${refNom(canal)}`, "-X", "PATCH"], { sha: commit, force: true });
  } catch (e) {
    /* Première pose : la référence n'existe pas encore. */
    gh([`${REPO}/git/refs`, "-X", "POST"], { ref: `refs/${refNom(canal)}`, sha: commit });
  }
  return commit;
}

/**
 * Un AUTRE titulaire détient-il un bail encore frais ?
 * → l'objet bail (avec age_min) si oui, null sinon (y compris si c'est le mien
 *   ou s'il est périmé : dans les deux cas je peux travailler).
 */
export function bailAutre(canal) {
  const b = lire(canal);
  if (!b || b.etat !== "actif" || b.titulaire === TITULAIRE) return null;
  const age = Date.now() - Date.parse(b.battement || b.debut || 0);
  if (!(age >= 0 && age < FRAIS_MS)) return null; /* périmé → abandonné */
  return { ...b, age_min: Math.round(age / 60000) };
}

export function poser(canal, extra = {}) {
  const maintenant = new Date().toISOString();
  const bail = { canal, etat: "actif", titulaire: TITULAIRE, pid: process.pid, debut: maintenant, battement: maintenant, ...extra };
  try { ecrire(canal, bail); return bail; } catch (e) { return null; }
}

export function battre(canal, extra = {}) {
  try {
    const b = lire(canal);
    /* Quelqu'un d'autre a pris la main (mon bail avait péri) → je ne l'écrase pas. */
    if (b && b.etat === "actif" && b.titulaire !== TITULAIRE) return false;
    ecrire(canal, { ...(b || {}), canal, etat: "actif", titulaire: TITULAIRE, pid: process.pid, battement: new Date().toISOString(), ...extra });
    return true;
  } catch (e) { return false; }
}

export function liberer(canal, resultat = "fin") {
  try {
    ecrire(canal, { canal, etat: "libre", titulaire: TITULAIRE, fin: new Date().toISOString(), resultat });
    return true;
  } catch (e) { return false; }
}

/**
 * Pose le bail, le renouvelle tout seul, et le libère à la sortie du process
 * (y compris en cas de plantage : le hook « exit » suffit, execFileSync étant
 * synchrone). C'est la seule fonction dont sync.mjs a besoin.
 */
export function surveiller(canal, extra = {}) {
  const bail = poser(canal, extra);
  if (!bail) return null;
  const t = setInterval(() => battre(canal), BATTEMENT_MS);
  if (t.unref) t.unref();
  process.on("exit", () => { try { liberer(canal); } catch (_) { } });
  return bail;
}

/* ── Ligne de commande ─────────────────────────────────────────────────── */
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const [, , cmd, canal] = process.argv;
  const canaux = canal ? [canal] : ["ant", "zs"];
  if (cmd === "poser") console.log(JSON.stringify(poser(canal || "ant", { note: "test manuel" }), null, 2));
  else if (cmd === "liberer") console.log(liberer(canal || "ant", "manuel") ? "libéré" : "échec");
  else {
    console.log(`Titulaire local : ${TITULAIRE}\n`);
    for (const c of canaux) {
      const b = lire(c);
      if (!b) { console.log(`${c.toUpperCase().padEnd(4)} — aucun bail`); continue; }
      const age = Math.round((Date.now() - Date.parse(b.battement || b.debut || 0)) / 60000);
      const frais = b.etat === "actif" && age * 60000 < FRAIS_MS;
      console.log(`${c.toUpperCase().padEnd(4)} — ${b.etat.padEnd(5)} | ${b.titulaire} | dernier battement il y a ${age} min ${frais ? "✔ frais" : "✖ périmé"}${b.note ? " | " + b.note : ""}`);
    }
  }
}
