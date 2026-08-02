import fs from 'node:fs';
import path from 'node:path';
import XLSX from 'xlsx';

const source = path.resolve(process.argv[2] || '.tmp-audit.xlsx');
const output = path.resolve(process.argv[3] || 'docs/data/ia/uid_catalogue_kasavubu_2026.json');
const wb = XLSX.readFile(source);

function rows(name) {
  const sheet = wb.Sheets[name];
  if (!sheet) throw new Error(`Feuille absente : ${name}`);
  return XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '', raw: false }).slice(4);
}

function clean(value) {
  return String(value ?? '').replace(/\s+/g, ' ').trim();
}

const indicateurs = rows('UID_Audit')
  .map((r) => ({
    programme: clean(r[0]),
    indicateur: clean(r[1]),
    uid: clean(r[2]),
    existe: clean(r[3]),
    type: clean(r[4]),
    nom_dhis2: clean(r[5]),
    nom_concordant: clean(r[6]),
    operandes_valides: clean(r[7]),
    observation: clean(r[8]),
  }))
  .filter((r) => r.programme && r.indicateur);

for (const indicateur of indicateurs) {
  if (indicateur.uid === 'N3HHnz0Waos') indicateur.aliases = 'réunions CODESA; réunions du CODESA; comptes rendus CODESA';
  if (indicateur.uid === 'zLIRMEWlQXy') indicateur.aliases = 'réunion de validation des données; réunions de validation; réunion ECZ; réunion hebdomadaire ECZ';
}

/* ECV est un dataElement du dépôt, pas un indicateur officiel du classeur.
   Sa formule métier a été confirmée par le propriétaire le 02/08/2026. */
indicateurs.push({
  programme: 'Vaccination',
  indicateur: "Proportion d'enfants complètement vaccinés (ECV)",
  uid: 'M2JQW0H44dI',
  existe: 'Oui',
  type: 'dataElement',
  nom_dhis2: 'ECV',
  nom_concordant: 'Oui',
  operandes_valides: 'Oui',
  observation: 'Formule confirmée : somme ECV sur la période / nourrissons survivants de la période × 100.',
});

const program_indicators = rows('Program_EVENT')
  .map((r) => ({
    uid: clean(r[0]),
    nom: clean(r[1]),
    programme: clean(r[2]),
    uid_programme: clean(r[3]),
    analytics_type: clean(r[4]),
    s1_2025: clean(r[5]),
    s1_2026: clean(r[6]),
    expression: clean(r[7]),
    filtre: clean(r[8]),
    endpoint: clean(r[9]),
  }))
  .filter((r) => r.uid && r.nom);

const a_configurer = rows('A_Configurer')
  .map((r) => ({
    programme: clean(r[0]),
    indicateur: clean(r[1]),
    description: clean(r[2]),
    numerateur_attendu: clean(r[3]),
    denominateur_attendu: clean(r[4]),
    score_meilleur_match: clean(r[5]),
    meilleur_candidat_dhis2: clean(r[6]),
  }))
  .filter((r) => r.programme && r.indicateur);

const programmes_event = [...new Map(program_indicators.map((r) => [r.uid_programme, {
  uid: r.uid_programme,
  nom: r.programme,
  endpoint: r.endpoint,
}])).values()].filter((r) => r.uid);

const catalogue = {
  version: '2026-08-02',
  source: 'Audit_indicateurs_DHIS2_Assistant_IA_Kasa-Vubu_2026.xlsx',
  regles: {
    priorite: 'Chercher ici avant une recherche DHIS2 libre, puis vérifier la métadonnée vivante.',
    valeur_partielle: 'Afficher la valeur en précisant les mois couverts ; ne pas remplacer par N/D.',
    ecv: 'Proportion ECV (%) = somme M2JQW0H44dI sur la période / nourrissons survivants de la période × 100.',
    reunion_validation: 'Utiliser zLIRMEWlQXy pour le pourcentage des réunions hebdomadaires tenues par l’ECZ ; vérifier les opérandes vivants si le canevas demande les nombres prévus/tenus.',
    codesa: 'Utiliser N3HHnz0Waos pour le pourcentage des réunions de CODESA réalisées avec comptes rendus.',
  },
  statistiques: {
    indicateurs_canevas: indicateurs.length,
    indicateurs_avec_uid: indicateurs.filter((r) => r.uid).length,
    program_indicators_event: program_indicators.length,
    programmes_event: programmes_event.length,
    a_configurer: a_configurer.length,
  },
  programmes_event,
  indicateurs,
  program_indicators,
  a_configurer,
};

fs.mkdirSync(path.dirname(output), { recursive: true });
fs.writeFileSync(output, `${JSON.stringify(catalogue, null, 2)}\n`, 'utf8');
console.log(`Catalogue écrit : ${output}`);
console.log(JSON.stringify(catalogue.statistiques));
