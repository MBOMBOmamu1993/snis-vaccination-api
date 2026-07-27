#!/usr/bin/env node
/**
 * ALERTE « session Tableau expirée » — le seul incident qui exige Felly.
 *
 * Quand la synchro de secours n'arrive plus à ouvrir Tableau avec la session du
 * PC, personne dans le cloud ne peut se reconnecter à la place de Felly. Ce
 * script ouvre alors une **issue GitHub assignée au propriétaire du dépôt** :
 * GitHub envoie un e-mail sur assignation, donc l'alerte arrive même si le PC
 * est éteint depuis des jours.
 *
 * L'issue est refermée automatiquement dès qu'une exécution retrouve une
 * session valide — il n'y en a jamais deux ouvertes en même temps.
 * Le veilleur du PC la relit au démarrage et affiche une fenêtre d'avertissement.
 *
 * Usage :
 *   node cloud/alerte.mjs --ouvrir ["détail technique"]
 *   node cloud/alerte.mjs --fermer
 *   node cloud/alerte.mjs --etat        (code 1 s'il y a une alerte en cours)
 */
import { execFileSync } from "node:child_process";
import { pathToFileURL } from "node:url";

const DEPOT = process.env.MASHAKO_DEPOT || "MBOMBOmamu1993/snis-vaccination-api";
const PROPRIETAIRE = DEPOT.split("/")[0];
const MARQUEUR = "[MASHAKO] Session Tableau expirée";

const args = process.argv.slice(2);
const OUVRIR = args.includes("--ouvrir");
const FERMER = args.includes("--fermer");

function gh(a, opts = {}) {
  return execFileSync("gh", a, { encoding: "utf8", maxBuffer: 8 * 1024 * 1024, stdio: ["ignore", "pipe", "pipe"], ...opts });
}

/** L'alerte en cours, s'il y en a une. */
export function alerteOuverte() {
  try {
    const issues = JSON.parse(gh(["issue", "list", "--repo", DEPOT, "--state", "open", "--limit", "50", "--json", "number,title,createdAt"]));
    return issues.find((i) => i.title.startsWith(MARQUEUR)) || null;
  } catch (e) { return null; }
}

const CORPS = (detail) => `La synchro de secours (GitHub Actions) n'arrive plus à ouvrir Tableau Cloud avec la session enregistrée.

**Tant que ce n'est pas réglé, seule ta machine peut synchroniser le Plan Mashako 3.0.**

### À faire sur le PC (2 minutes)

\`\`\`
cd C:\\Users\\felly\\mashako-sync
login.cmd                          → se reconnecter avec le compte Google,
                                     attendre l'affichage du classeur,
                                     puis FERMER la fenêtre Chrome
node cloud/publier-cookies.mjs     → republier la session dans le secret GitHub
\`\`\`

Puis vérifier que tout est reparti :

\`\`\`
gh workflow run mashako_test_session.yml
\`\`\`

Cette issue se refermera **toute seule** dès qu'une exécution retrouvera une session valide.

---
${detail ? "Détail technique : `" + detail + "`\n\n" : ""}Ouvert automatiquement par \`cloud/alerte.mjs\`.`;

/* Le bloc ci-dessous ne s'exécute qu'en ligne de commande : le veilleur du PC
   importe alerteOuverte() sans déclencher d'action. */
const EN_CLI = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;

if (!EN_CLI) { /* import seul */ }
else if (OUVRIR) {
  const dejaLa = alerteOuverte();
  const detail = args[args.indexOf("--ouvrir") + 1] && !args[args.indexOf("--ouvrir") + 1].startsWith("--")
    ? args[args.indexOf("--ouvrir") + 1] : "";
  if (dejaLa) {
    /* Pas de doublon : on commente l'existante, ce qui renotifie sans polluer. */
    try {
      gh(["issue", "comment", String(dejaLa.number), "--repo", DEPOT,
        "--body", `Toujours bloqué au ${new Date().toISOString()}.${detail ? " (" + detail + ")" : ""}`]);
    } catch (e) { }
    console.log(`⚠ Alerte déjà ouverte : issue #${dejaLa.number} (depuis ${dejaLa.createdAt}).`);
  } else {
    const out = gh(["issue", "create", "--repo", DEPOT,
      "--title", `${MARQUEUR} — reconnexion requise sur le PC`,
      "--body", CORPS(detail),
      "--assignee", PROPRIETAIRE]);
    console.log(`🔴 Alerte ouverte et assignée à ${PROPRIETAIRE} : ${out.trim()}`);
  }
} else if (FERMER) {
  const a = alerteOuverte();
  if (!a) console.log("✓ Aucune alerte en cours.");
  else {
    gh(["issue", "close", String(a.number), "--repo", DEPOT,
      "--comment", `Session Tableau de nouveau valide — synchro de secours opérationnelle (${new Date().toISOString()}).`]);
    console.log(`✅ Alerte #${a.number} refermée : la session fonctionne de nouveau.`);
  }
} else {
  const a = alerteOuverte();
  if (a) { console.log(`🔴 Alerte en cours : issue #${a.number} — ${a.title} (depuis ${a.createdAt})`); process.exitCode = 1; }
  else console.log("✓ Aucune alerte : la session Tableau est utilisable depuis le cloud.");
}
