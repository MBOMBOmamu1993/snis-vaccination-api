#!/usr/bin/env node
/** Backfill du DÉTAIL PAR AIRE DE SANTÉ des archives ZS, mois par mois, en 3 tranches
 *  parallèles (un profil Chrome par tranche), publication dans zs/periods/<AAAA-MM>/views/.
 *
 *  Usage : node backfill-as-chaine.mjs 2026-07 2026-08 2026-06 2026-05
 *  Env   : MASHAKO_AS_MOTEUR=session (défaut, export-zs-as-session.mjs × MASHAKO_SESSIONS=8) | tranches (export-zs-as.mjs × 3 profils)
 *  Env   : MASHAKO_AS_PASSES (déf. 3 passes max par mois), MASHAKO_AS_MINUTES (déf. 600 par passe),
 *          MASHAKO_AS_SHARDS (déf. 3).
 *  Journal : out-zs-as/chaine.log ; par mois : out-zs-as/<AAAA-MM>/worker<i>.log
 *  Reprenable : export-zs-as.mjs tient un journal daté par mois (zs_as_ledger_s<i>.json).
 */
import { spawn, execFileSync } from "node:child_process";
import { mkdirSync, appendFileSync, readFileSync, existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
const HERE = path.dirname(fileURLToPath(import.meta.url));
const MOIS = ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet", "Août", "Septembre", "Octobre", "Novembre", "Décembre"];
const PERIODES = process.argv.slice(2).filter((p) => /^\d{4}-\d{2}$/.test(p));
if (!PERIODES.length) { console.error("Usage : node backfill-as-chaine.mjs <AAAA-MM> [<AAAA-MM>…]"); process.exit(2); }
const MOTEUR = process.env.MASHAKO_AS_MOTEUR || "session"; // session (défaut) | tranches
const PASSES = Number(process.env.MASHAKO_AS_PASSES || 3), MINUTES = process.env.MASHAKO_AS_MINUTES || "600", N = Number(process.env.MASHAKO_AS_SHARDS || 3);
const RACINE = path.join(HERE, "out-zs-as"); mkdirSync(RACINE, { recursive: true });
const JOURNAL = path.join(RACINE, "chaine.log");
const log = (m) => { const l = `[${new Date().toISOString()}] ${m}`; console.log(l); appendFileSync(JOURNAL, l + "\n"); };
const attendre = (cmd, args, opts) => new Promise((res) => { const p = spawn(cmd, args, opts); p.on("exit", (c) => res(c)); p.on("error", () => res(-1)); });
const dormir = (ms) => new Promise((r) => setTimeout(r, ms));

for (const periode of PERIODES) {
  const [y, m] = periode.split("-").map(Number);
  const mois = MOIS[m - 1], an = String(y);
  const OUT = path.join(RACINE, periode); mkdirSync(path.join(OUT, "views"), { recursive: true });
  log(`══ ${periode} (${mois} ${an}) — moteur ${MOTEUR}${MOTEUR === "session" ? " × " + (process.env.MASHAKO_SESSIONS || 8) + " sessions" : " × " + N + " tranches"}, ${PASSES} passe(s) max, ${MINUTES} min/passe ══`);
  for (let passe = 1; passe <= PASSES; passe++) {
    log(`— ${periode} passe ${passe}/${PASSES} —`);
    const procs = [];
    if (MOTEUR === "session") {
      /* moteur VizQL (export-zs-as-session.mjs) : un seul processus, MASHAKO_SESSIONS sessions
         parallèles, ~7 exports/min mesurés à 6 sessions contre 4,4 pour 3 tranches classiques. */
      const logf = path.join(OUT, "session.log");
      const fd = (await import("node:fs")).openSync(logf, "a");
      procs.push(attendre(process.execPath, [path.join(HERE, "export-zs-as-session.mjs"), mois, an], {
        cwd: HERE, stdio: ["ignore", fd, fd],
        env: { ...process.env, MASHAKO_AS_OUT: OUT, MASHAKO_MINUTES: MINUTES, MASHAKO_SESSIONS: String(process.env.MASHAKO_SESSIONS || 8) },
      }));
    } else for (let i = 1; i <= N; i++) {
      const logf = path.join(OUT, `worker${i}.log`);
      const fd = (await import("node:fs")).openSync(logf, "a");
      procs.push(attendre(process.execPath, [path.join(HERE, "export-zs-as.mjs"), mois, an], {
        cwd: HERE, stdio: ["ignore", fd, fd],
        env: { ...process.env, MASHAKO_SHARD: `${i}/${N}`, MASHAKO_PROFILE: path.join(HERE, `browser-profile-as${i}`), MASHAKO_AS_OUT: OUT, MASHAKO_MINUTES: MINUTES },
      }));
      if (i < N) await dormir(30000);
    }
    const codes = await Promise.all(procs);
    log(`${periode} passe ${passe} : codes de sortie ${codes.join(", ")}`);
    /* Publication après CHAQUE passe (rien n'est perdu si la suivante échoue). */
    try {
      execFileSync(process.execPath, [path.join(HERE, "publish-zs-as.mjs"), "--fusion", "--periode", periode], { cwd: HERE, stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, MASHAKO_AS_OUT: OUT }, encoding: "utf8" })
        .split("\n").filter(Boolean).slice(-3).forEach((l) => log(`  ${l}`));
    } catch (e) { log(`⚠ publication ${periode} en échec : ${String(e.stderr || e.message).slice(0, 200)}`); }
    /* Terminé ? Le garde-fou de temps laisse « ⚠ Garde-fou » au journal ; sinon la passe est allée au bout. */
    let coupe = false;
    const journaux = MOTEUR === "session" ? ["session.log"] : Array.from({ length: N }, (_, i) => `worker${i + 1}.log`);
    for (const jf of journaux) {
      const t = existsSync(path.join(OUT, jf)) ? readFileSync(path.join(OUT, jf), "utf8") : "";
      const dernier = t.lastIndexOf("— Bilan —"); const seg = t.slice(Math.max(0, t.lastIndexOf("dashboard(s) ×", dernier)));
      if (/Garde-fou/.test(seg) || /session non capturée/.test(seg) && !/✓ /.test(seg)) coupe = true;
    }
    if (!coupe) { log(`✓ ${periode} : détail AS complet (toutes les tranches au bout).`); break; }
    if (passe < PASSES) { log(`${periode} : passe coupée par le budget ou la session — nouvelle passe dans 2 min.`); await dormir(120000); }
    else log(`⚠ ${periode} : ${PASSES} passes épuisées — relancer plus tard (journal de reprise conservé).`);
  }
}
log("— Chaîne terminée —");
