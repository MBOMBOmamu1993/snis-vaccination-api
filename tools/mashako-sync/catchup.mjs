#!/usr/bin/env node
/**
 * Rattrapage au démarrage de la machine.
 *
 * Les synchros Mashako sont planifiées (Antenne 7h, ZS 10h30, backfills le
 * soir). Si le PC est ÉTEINT ou en veille profonde à ces heures-là, Windows
 * saute purement et simplement l'occurrence — constaté le 26/07 : la synchro de
 * 7h n'a jamais tourné et les correctifs de la veille n'ont pas été publiés.
 *
 * Ce script, lancé à l'ouverture de session, regarde ce qui a RÉELLEMENT abouti
 * aujourd'hui (fichiers best_count*.json, écrits en fin de publication) et
 * relance séquentiellement ce qui manque. Il ne force rien : chaque synchro
 * garde ses propres garde-fous (verrou partagé, « déjà synchronisé
 * aujourd'hui », gardes anti-régression).
 *
 * Usage : node catchup.mjs   (déclenché par Démarrage\mashako-rattrapage.vbs)
 */
import { spawn } from "node:child_process";
import { readFileSync, appendFileSync, statSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const LOG = path.join(HERE, "catchup.log");
const LOCK = path.join(HERE, "out", ".sync.lock");
const NODE = process.execPath;

function log(m) {
  const line = `[${new Date().toISOString()}] ${m}`;
  console.log(line);
  try { appendFileSync(LOG, line + "\n"); } catch (e) { }
}
const today = () => new Date().toISOString().slice(0, 10);
function doneToday(file) {
  try {
    const j = JSON.parse(readFileSync(path.join(HERE, file), "utf8"));
    return String(j.at || "").slice(0, 10) === today();
  } catch (e) { return false; }
}
function lockBusy() {
  try { return Date.now() - statSync(LOCK).mtimeMs < 2 * 3600 * 1000; } catch (e) { return false; }
}
/* Attente que le verrou partagé se libère (une seule session Chrome à la fois) */
async function waitFree(maxMin) {
  const t0 = Date.now();
  while (lockBusy() && (Date.now() - t0) / 60000 < maxMin) {
    await new Promise((r) => setTimeout(r, 60000));
  }
  return !lockBusy();
}
function run(env, label) {
  return new Promise((res) => {
    log(`→ ${label} : démarrage`);
    const p = spawn(NODE, ["sync.mjs", "--background"], {
      cwd: HERE, env: { ...process.env, ...env, MASHAKO_HEADLESS: "1" },
      stdio: "ignore", detached: false,
    });
    p.on("exit", (c) => { log(`← ${label} : terminé (code ${c})`); res(c); });
    p.on("error", (e) => { log(`✖ ${label} : ${e.message}`); res(-1); });
  });
}

const main = async () => {
  log("— Rattrapage au démarrage —");
  const antOk = doneToday("best_count.json"), zsOk = doneToday("best_count_zs.json");
  log(`état du jour : Antenne ${antOk ? "déjà publiée" : "MANQUANTE"}, ZS ${zsOk ? "déjà publiée" : "MANQUANTE"}`);
  if (antOk && zsOk) { log("rien à rattraper."); return; }
  /* Laisser d'abord la main aux tâches planifiées si l'une vient de démarrer */
  if (lockBusy()) { log("une synchro tourne déjà — attente…"); await waitFree(240); }
  if (!antOk) { await run({}, "synchro Antenne"); await waitFree(10); }
  if (!zsOk) { await run({ MASHAKO_CFG: "zs" }, "synchro Zone de Santé"); }
  log("— Rattrapage terminé —");
};
main();
