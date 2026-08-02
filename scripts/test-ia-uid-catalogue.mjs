import fs from 'node:fs/promises';
import assert from 'node:assert/strict';

const html = await fs.readFile(new URL('../docs/index.html', import.meta.url), 'utf8');
const catalogue = JSON.parse(await fs.readFile(new URL('../docs/data/ia/uid_catalogue_kasavubu_2026.json', import.meta.url), 'utf8'));

assert.equal(catalogue.statistiques.indicateurs_canevas, 482, '481 lignes audit + ECV confirmé');
assert.equal(catalogue.statistiques.indicateurs_avec_uid, 394, '393 UID audit + ECV confirmé');
assert.equal(catalogue.statistiques.program_indicators_event, 76);
assert.equal(catalogue.statistiques.programmes_event, 4);
assert.equal(catalogue.statistiques.a_configurer, 83);

function byUid(uid) {
  return catalogue.indicateurs.find((r) => r.uid === uid);
}

assert.match(byUid('M2JQW0H44dI').observation, /nourrissons survivants/);
assert.match(byUid('N3HHnz0Waos').indicateur, /CODESA/);
assert.match(byUid('zLIRMEWlQXy').aliases, /validation des données/);
for (const uid of ['QQXfuAm7cQL', 'BjD13mVG82e', 'pbirwrEK5xY', 'IvjxiT221Ms']) {
  assert.ok(catalogue.programmes_event.some((p) => p.uid === uid), `programme EVENT absent : ${uid}`);
}

assert.match(html, /name: 'rechercher_uid_canevas'/);
assert.match(html, /async function iaUidSearch\(input\)/);
assert.match(html, /uidSearch: iaUidSearch/);
assert.match(html, /Toute valeur présente, même partielle, doit être affichée/);
assert.match(html, /N3HHnz0Waos/);
assert.match(html, /zLIRMEWlQXy/);

console.log('OK — catalogue IA : tous programmes, 393 UID audit, 76 EVENT, ECV, CODESA et réunion de validation.');
