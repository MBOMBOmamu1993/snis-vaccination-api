import fs from 'node:fs';
import vm from 'node:vm';

const html = fs.readFileSync(new URL('../docs/index.html', import.meta.url), 'utf8');

function extract(start, end) {
  const a = html.indexOf(start);
  const b = html.indexOf(end, a);
  if (a < 0 || b < 0) throw new Error(`Bloc introuvable : ${start}`);
  return html.slice(a, b);
}

for (const token of [
  "name: 'verifier_correction_rvv_direct'",
  'rvvCorrectionDirect: iaRvvCorrectionDirect',
  "name === 'verifier_correction_rvv_direct'",
  'ctx.dhis2/load/loadUrl SANS await',
]) {
  if (!html.includes(token)) throw new Error(`Intégration RVV manquante : ${token}`);
}

const source = [
  extract('function iaRvvDates(input)', '/* PEV_DIRECT_BEGIN'),
  extract('function iaScoreQualiteDps(o)', 'function iaPevManualQuery(text)'),
].join('\n');

const calls = [];
const sandbox = {
  iaPevResolveOu: async () => ({ id: 'TuzDrpZ9zzl', name: 'Kasa-Vubu', level: 3 }),
  iaDhis2: async (endpoint, params, body) => {
    calls.push({ endpoint, params, body });
    return sandbox.response;
  },
  response: [],
  console,
};
vm.createContext(sandbox);
vm.runInContext(source, sandbox);

function equal(actual, expected, label) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a !== e) throw new Error(`${label}\nattendu: ${e}\nobtenu: ${a}`);
}

equal(sandbox.iaRvvDates({ periode: '202602' }), {
  debut: '2026-02-01', fin: '2026-02-28', periode: '202602',
}, 'Conversion mois');
equal(sandbox.iaRvvDates({ periode: '202402' }), {
  debut: '2024-02-01', fin: '2024-02-29', periode: '202402',
}, 'Conversion mois bissextile');
equal(sandbox.iaRvvDates({ periode: '2026S1' }), {
  debut: '2026-01-01', fin: '2026-06-30', periode: '2026S1',
}, 'Conversion semestre');

let result = await sandbox.iaRvvCorrectionDirect({ entite: 'Kasa-Vubu', niveau: 3, periode: '2026S1' });
equal({ n: result.rvv_persistantes, ok: result.rvv_corrigees, score: result.score_critere_5 },
  { n: 0, ok: true, score: 20 }, 'Absence de RVV');
equal(calls[0], {
  endpoint: 'dataAnalysis/validationRules', params: '',
  body: { startDate: '2026-01-01', endDate: '2026-06-30', ou: 'TuzDrpZ9zzl' },
}, 'Requête DHIS2 exacte');

sandbox.response = [
  { validationRuleId: 'r1', organisationUnitId: 'o1', periodId: '202606', leftsideValue: 2, rightsideValue: 1 },
  { validationRuleId: 'r1', organisationUnitId: 'o1', periodId: '202606', leftsideValue: 2, rightsideValue: 1 },
  { validationRuleId: 'r2', organisationUnitId: 'o2', periodId: '202605', leftsideValue: 5, rightsideValue: 3 },
];
result = await sandbox.iaRvvCorrectionDirect({ entite: 'Kasa-Vubu', niveau: 3, periode: '2026S1' });
equal({ n: result.rvv_persistantes, ok: result.rvv_corrigees, score: result.score_critere_5 },
  { n: 2, ok: false, score: 0 }, 'Présence de RVV et dédoublonnage');

const scoreOk = sandbox.iaScoreQualiteDps({
  completude: 96, promptitude: 95, datasets_atteints: 10, datasets_applicables: 10,
  rvv: 0, rapports_recus: 100, rvv_corrigees: true,
});
const scoreKo = sandbox.iaScoreQualiteDps({
  completude: 96, promptitude: 95, datasets_atteints: 10, datasets_applicables: 10,
  rvv: 1, rapports_recus: 100, rvv_corrigees: false,
});
if (scoreOk.total !== 100 || scoreOk.scores.rvv_corrigees !== 20) throw new Error('Score RVV corrigées invalide');
if (scoreKo.total !== 75 || scoreKo.scores.rvv_corrigees !== 0) throw new Error('Score RVV non corrigées invalide');

console.log('OK — critère 5 RVV direct : période exacte, présence/absence et barème 20/0 validés.');
