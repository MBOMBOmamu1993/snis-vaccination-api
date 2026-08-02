import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';

const html = fs.readFileSync(new URL('../docs/index.html', import.meta.url), 'utf8');
const match = html.match(/\/\* PEV_DIRECT_BEGIN[\s\S]*?\/\* PEV_DIRECT_END \*\//);
assert.ok(match, 'bloc PEV direct introuvable');

const population = { 2023: 100000, 2024: 110000, 2025: 120000 };
const totals = {
  VAR2: { 2023: 1000, 2024: 2000, 2025: 3000 },
  DTC1: { 2023: 3000, 2024: 3200, 2025: 3000 },
  DTC3: { 2023: 2500, 2024: 2700, 2025: 2500 },
  ECV: { 2025: 100 },
};
const analyticsCalls = [];

const sandbox = {
  console,
  Date,
  encodeURIComponent,
  decodeURIComponent,
  async iaDhis2(endpoint, params = '') {
    if (endpoint.startsWith('organisationUnits')) {
      return { organisationUnits: [{ id: 'TwSa8zUu09Q', name: 'kn Kinshasa Province', displayName: 'kn Kinshasa Province', level: 2, path: '/root/TwSa8zUu09Q' }] };
    }
    assert.equal(endpoint, 'analytics.json');
    const q = decodeURIComponent(params);
    const dx = q.match(/dimension=dx:([^&]+)/)?.[1]?.split(';') || [];
    const pe = q.match(/dimension=pe:([^&]+)/)?.[1]?.split(';') || [];
    analyticsCalls.push({ dx, pe });
    if (dx.includes('WLSKVyA8LoY')) {
      return { rows: pe.flatMap((p) => [
        ['WLSKVyA8LoY', p, 'AS1', String(population[p.slice(0, 4)] * 0.4)],
        ['WLSKVyA8LoY', p, 'AS2', String(population[p.slice(0, 4)] * 0.6)],
      ]) };
    }
    const rows = [];
    for (const p of pe) {
      const year = p.slice(0, 4);
      for (const [ag, cfg] of Object.entries(sandbox.IA_PEV_ANTIGENES)) {
        const requested = cfg.dx.filter((uid) => dx.includes(uid));
        if (requested.length) rows.push([requested[0], p, 'FOSA1', String(totals[ag]?.[year] || 0)]);
      }
      // L'API peut renvoyer ce COC VAR2 0-11 mois non demandé : il doit être ignoré.
      rows.push(['i5zmivDIHN8.dqydGQFHahb', p, 'FOSA_PARASITE', '9999']);
    }
    return { rows };
  },
};
vm.createContext(sandbox);
vm.runInContext(match[0], sandbox);

assert.equal(sandbox.IA_PEV_ANTIGENES.VAR2.field, 'VAR2_12_23');
assert.equal(sandbox.IA_PEV_ANTIGENES.VAR2.dx.length, 5);
assert.ok(sandbox.IA_PEV_ANTIGENES.VAR2.dx.every((uid) => uid.startsWith('i5zmivDIHN8.')), 'VAR2 doit utiliser uniquement les COC 12-23 mois');
assert.equal(sandbox.IA_PEV_ANTIGENES.ECV.den, 'ns');
assert.deepEqual(Array.from(sandbox.IA_PEV_ANTIGENES.ECV.dx), ['M2JQW0H44dI']);

const cv = await sandbox.iaPevDirect({
  entite: 'Kinshasa', niveau: 2, debut: '2023', fin: '2025',
  granularite: 'annuelle', antigenes: ['VAR2'], indicateurs: ['cv'],
});
assert.deepEqual(Array.from(cv.series, (r) => r.periode), ['2023', '2024', '2025']);
assert.equal(cv.series[2].population, 120000);
assert.equal(cv.series[2].doses.VAR2, 3000, 'les 9 999 doses fantômes ne doivent pas être ajoutées');
assert.ok(Math.abs(cv.series[2].cv_pct.VAR2 - (3000 / (120000 * 0.0349) * 100)) < 1e-9);
assert.ok(cv.lignes_fantomes_ignorees >= 3);
const popCalls = analyticsCalls.filter((c) => c.dx.includes('WLSKVyA8LoY'));
assert.equal(popCalls.length, 3, 'la population multiannuelle doit faire un appel isolé par année');
assert.ok(popCalls.every((c) => c.pe.length === 1), 'ne jamais grouper plusieurs années de population dans Analytics');
const var2Calls = analyticsCalls.filter((c) => c.dx.includes('i5zmivDIHN8.g6mIyKoGIh2'));
assert.equal(var2Calls.length, 3, 'les doses annuelles doivent aussi être interrogées année par année');
assert.ok(var2Calls.every((c) => c.pe.length === 1));

const enfants = await sandbox.iaPevDirect({
  entite: 'Kinshasa', niveau: 2, debut: '2025', fin: '2025',
  granularite: 'annuelle', indicateurs: ['zero_dose', 'sous_vaccines'],
});
assert.equal(enfants.series[0].cible_ns, 120000 * 0.0349);
assert.equal(enfants.series[0].zero_dose, 120000 * 0.0349 - 3000);
assert.equal(enfants.series[0].sous_vaccines, 120000 * 0.0349 - 2500);

const ecv = await sandbox.iaPevDirect({
  entite: 'Kinshasa', niveau: 2, debut: '202501', fin: '202506',
  granularite: 'total', antigenes: ['ECV'], indicateurs: ['cv'],
});
assert.equal(ecv.series[0].doses.ECV, 600);
assert.ok(Math.abs(ecv.series[0].cv_pct.ECV - (600 / (120000 * 0.0349 * 6 / 12) * 100)) < 1e-9);
assert.deepEqual(Array.from(ecv.uids_operandes.ECV), ['M2JQW0H44dI']);
assert.match(ecv.methode, /ECV=M2JQW0H44dI\/NS de la période/);

assert.match(html, /calculer_pev_direct exclusivement/);
assert.match(html, /un UID dataElement NU est INTERDIT/);
assert.match(html, /Proportion ECV \(%\) = somme ECV sur TOUTE la période sélectionnée ÷ nourrissons survivants de la même période × 100/);
assert.doesNotMatch(html, /dx:UID sans \.COC = total toutes catégories \(suffisant/);

console.log('OK — calcul PEV direct : COC exacts, ECV/NS sur période sélectionnée, population AS, CV VAR2, ZD/SV et filtre anti-lignes-fantômes.');
