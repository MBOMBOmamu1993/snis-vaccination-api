#!/usr/bin/env node
/**
 * Test de régression de l'Assistant IA DHIS2 — Kimi K3.1 Pro, API Kimi officielle,
 * route /kimi/v1/chat/completions du Worker de production (aucune route Ollama).
 *
 * Le code du dashboard est EXTRAIT de index.html et exécuté tel quel : choix du
 * modèle (iaProvKey/iaProv), construction de la requête et relances (iaCallAPI),
 * conversion de l'historique (iaMsgsToOpenAI), lecture du flux SSE
 * (iaReadOpenAIStream), prompt système, outils, calculs PEV directs
 * (calculer_pev_direct), DHIS2 via le proxy, normalisation des figures.
 *
 * Usage :
 *   PEV_ACCESS_CODE=PEV-XXXX-XXXX node scripts/test_ia_kimi.mjs ["question"]
 *   (IA_HTML=chemin/vers/index.html pour tester une copie modifiée)
 *   question : défaut = les 2 questions de référence (CV VAR1 pays, graphique Luiza)
 *
 * Chaque tour consomme 1 requête du code d'accès. Code temporaire (à supprimer après) :
 *   cd cloudflare-worker
 *   npx wrangler kv key put --namespace-id fb104235615740b9a9d117355f864751 \
 *     "code:PEV-TEST-XXXX" '{"total":80,"remaining":80}' --remote
 *
 * À lancer avant tout changement du modèle, du prompt système, des outils ou du Worker.
 */
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
const FILE = process.env.IA_HTML || path.join(ROOT, 'docs', 'index.html');
const CUSTOM_Q = process.argv[2];
const CODE = process.env.PEV_ACCESS_CODE;
const DATA_ORIGIN = 'https://mbombomamu1993.github.io/snis-vaccination-api/';
const MAX_TURNS = 30;
if (!CODE) { console.error('PEV_ACCESS_CODE manquant (voir en-tête du fichier).'); process.exit(2); }

const html = fs.readFileSync(FILE, 'utf8').replace(/\r\n/g, '\n');
const between = (a, b) => {
  const i = html.indexOf(a); if (i < 0) throw new Error('début introuvable : ' + a);
  const j = html.indexOf(b, i); if (j < 0) throw new Error('fin introuvable : ' + b);
  return html.slice(i, j + b.length);
};
const fn = (sig) => { /* fonction indentée à 12 espaces, jusqu'à sa accolade fermante */
  const i = html.indexOf('\n            ' + sig); if (i < 0) throw new Error('fonction introuvable : ' + sig);
  const line = html.slice(i + 1, html.indexOf('\n', i + 1));
  if (/\}\s*$/.test(line) && (line.match(/\{/g) || []).length === (line.match(/\}/g) || []).length) return line;
  const j = html.indexOf('\n            }\n', i + 1); return html.slice(i + 1, j + 14);
};

const src = [
  between('            var IA_CLAUDE_KEYHINT', "                return cfg('key_kimi');\n            }"),
  fn('function iaSystem() {'),
  between('            var IA_TOOLS = [', '\n            ];'),
  fn('function iaToolName(tc)'), fn('function iaUnwrapArrays(v)'), fn('function iaParseToolArgs(s)'),
  fn('function iaMsgAppelCoupe(name, n)'), fn('function iaToolArgs(tc)'), fn('function iaMissingAwait(code)'),
  fn('function iaPevManualQuery(text)'), fn('function iaScoreQualiteDps(o)'),
  fn('function iaParseMaybe(v)'), fn('function iaFixFigure(fig)'), fn('function iaFigureFromTable(colonnes, lignes)'),
  fn('function iaDeref(v)'), fn('function iaDerefFigure(fig)'), fn('function iaDerefVega(spec)'),
  fn('function iaNormalizeResult(inp)'),
  between('            async function iaLoadPath(p) {', '            /* PEV_DIRECT_END */'),
  fn('function iaMsgsToOpenAI(messages)'), fn('function iaSanitizeHistory(messages)'),
  fn('async function iaCallAPI(messages, onText, onThink, provKey, _rs)'),
  fn('async function iaReadOpenAIStream(resp, onText, onThink)'),
].join('\n');

const cfgStore = new Map();
const cfg = (k, v) => { if (v === undefined) return cfgStore.get(k) || ''; cfgStore.set(k, String(v)); };
let ADMIN = false;
async function fetchGzip(p) {
  const u = DATA_ORIGIN + String(p).replace(/^\.\//, '').replace(/^\//, '');
  const r = await fetch(u); if (!r.ok) throw new Error('HTTP ' + r.status + ' sur ' + u);
  const buf = Buffer.from(await r.arrayBuffer());
  return JSON.parse((u.endsWith('.gz') ? zlib.gunzipSync(buf) : buf).toString('utf8'));
}
const names = ['iaSystem', 'IA_TOOLS', 'iaProvKey', 'iaProv', 'iaUserKey', 'IA_CLIENT_MODELS', 'IA_KIMI_MODELS',
  'IA_DEFAULT_PROVIDER', 'IA_CLIENT_DEFAULT_PROVIDER', 'iaCallAPI', 'iaToolName', 'iaToolArgs', 'iaNormalizeResult',
  'iaUnwrapArrays', 'iaMissingAwait', 'iaScoreQualiteDps', 'iaPevManualQuery', 'iaDhis2', 'iaUidSearch', 'iaPevDirect',
  'iaProgramIndicatorDirect', 'iaRvvCorrectionDirect', 'iaLoadPath', 'iaLoadUrl', 'iaParseMaybe'];
const IA = { reg: {} };
const factory = new Function('S', 'AG', 'IA_TOOL_RESULT_MAX', 'IA_PROXY_URL', 'IA_DHIS2_CODE', 'cfg', 'iaIsAdmin',
  'fetchGzip', 'mL', 'IA_TOPO_ZS', 'IA', '"use strict";\n' + src + '\nreturn {' + names.join(',') + '};');
const D = factory({ data: [], flt: [], vals: {}, filters: {} }, [], 8000, 'https://pev-ia-proxy.pev-rdc.workers.dev',
  'PEV-DHIS-7QK4XZ', cfg, () => ADMIN, fetchGzip, (x) => x, 'https://example.invalid/rdc_zs.topojson', IA);

/* ── 1. Choix du modèle : toujours l'API Kimi K3.1 Pro, jamais Ollama ── */
function route(provider, admin) {
  ADMIN = admin; cfgStore.clear(); if (provider !== null) cfg('provider', provider);
  const pk = D.iaProvKey(); const p = D.iaProv(pk); return { pk, p, saved: cfg('provider') };
}
const K31 = 'kimi:kimi-k3.1-pro';
for (const [prov, admin] of [[null, false], ['', false], ['kimi:kimi-k3', false], ['kimi-k3', false], ['minimax-m3', false],
  ['claude-opus5', false], [null, true], ['kimi:kimi-k3', true], ['kimi-k3', true], ['minimax-m3', true],
  ['deepseek-v4-pro', true], ['qwen3.5:397b', true]]) {
  const r = route(prov, admin);
  assert.equal(r.pk, K31, `réglage ${prov} (${admin ? 'admin' : 'client'}) → ${r.pk}`);
  assert.equal(r.p.path, '/kimi/v1/chat/completions'); assert.equal(r.p.model, 'kimi-k3.1-pro');
  assert.equal(r.p.kind, 'openai'); assert.equal(r.p.reasoning_effort, 'high');
  assert.equal(r.saved, K31, 'réglage enregistré');
}
for (const keep of ['claude-opus5', 'claude', 'gpt:gpt-5.6-sol', 'kimi:kimi-k2.6', 'kimi:kimi-k2.7-code']) {
  const r = route(keep, true); assert.equal(r.pk, keep, 'choix admin conservé : ' + keep);
  assert.notEqual(r.p.path, '/api/chat');
}
assert.equal(D.iaProv('nimporte-quoi').path, '/kimi/v1/chat/completions');
assert.deepEqual(D.IA_CLIENT_MODELS.map((m) => m.v), [K31]);
assert.ok(!/ollama/i.test(JSON.stringify(D.IA_CLIENT_MODELS) + JSON.stringify(D.IA_KIMI_MODELS)));
console.log('✓ Routage : client et admin → Kimi K3.1 Pro (/kimi/v1/chat/completions) ; anciens réglages Ollama/K3 migrés ; aucune route /api/chat.');

/* ── 2. Analyses réelles : boucle agentique du dashboard ── */
ADMIN = false; cfgStore.clear(); cfg('mode', 'code'); cfg('code', CODE);
const REFUS_JS = 'REFUSÉ : calcul PEV manuel détecté. Utilise calculer_pev_direct avec les antigènes, l’entité et la période demandés ; les UID nus/Analytics libres produisent des CV incohérentes avec le dashboard.';
const REFUS_AN = 'REFUSÉ : opérande PEV dans une requête Analytics libre. Utilise calculer_pev_direct pour CV, zéro-dose et sous-vaccinés.';
const cut = (s) => (s.length > 8000 ? s.slice(0, 8000) + '\n…[TRONQUÉ]' : s);
let captured = [], pevOut = [];
async function runTool(name, input) {
  if (input && input._appel_coupe) throw new Error('APPEL COUPÉ (' + name + ')');
  if (name === 'executer_js') {
    if (D.iaPevManualQuery(input.code)) throw new Error(REFUS_JS);
    const ctx = { S: { data: [] }, data: [], flt: [], AG: [], load: D.iaLoadPath, loadUrl: D.iaLoadUrl, dhis2: D.iaDhis2,
      uidSearch: D.iaUidSearch, pevDirect: D.iaPevDirect, programIndicatorDirect: D.iaProgramIndicatorDirect,
      rvvCorrectionDirect: D.iaRvvCorrectionDirect, mL: (x) => x, scoreQualiteDps: D.iaScoreQualiteDps,
      geo: async () => { throw new Error('ctx.geo indisponible dans ce test'); },
      afficher: (spec) => { captured.push(D.iaParseMaybe(spec) || spec); return true; },
      keep: (v) => { const k = '@ref:' + (Object.keys(IA.reg).length + 1); IA.reg[k] = v; return k; }, topoZS: 'topo' };
    const f = new Function('ctx', '"use strict";return (async function(){' + input.code + '\n})();');
    const out = await f(ctx);
    let s = out === undefined ? 'undefined (le code doit se terminer par return)' : JSON.stringify(out);
    s = cut(s); const miss = D.iaMissingAwait(String(input.code || ''));
    if (miss.length) s = '⚠ BUG PROBABLE : ctx.' + miss[0] + '() appelé SANS await.\n\n' + s;
    return s;
  }
  if (name === 'requete_dhis2') {
    if (/analytics/i.test(String(input.endpoint || '')) && D.iaPevManualQuery(input.params)) throw new Error(REFUS_AN);
    const o = await D.iaDhis2(input.endpoint, input.params, input.corps);
    return cut(typeof o === 'string' ? o : JSON.stringify(o));
  }
  if (name === 'rechercher_uid_canevas') return JSON.stringify(await D.iaUidSearch(input));
  if (name === 'calculer_pev_direct') { const o = await D.iaPevDirect(input); pevOut.push(o); return JSON.stringify(o); }
  if (name === 'calculer_indicateur_programme_direct') return JSON.stringify(await D.iaProgramIndicatorDirect(input));
  if (name === 'verifier_correction_rvv_direct') return JSON.stringify(await D.iaRvvCorrectionDirect(input));
  if (name === 'afficher_resultat') { captured.push(input); return 'OK : résultat affiché à l\'utilisateur avec boutons de téléchargement.'; }
  if (name === 'generer_rapport' || name === 'generer_excel' || name === 'generer_image') { captured.push({ _livrable: name, ...input }); return 'OK : ' + name + ' généré et téléchargé chez l\'utilisateur.'; }
  return 'Outil ' + name + ' non utilisé dans ce test.';
}

function graphOk() {
  return captured.filter((a) => !a._livrable).map((a) => D.iaNormalizeResult(D.iaUnwrapArrays(a)))
    .some((n) => (n.type === 'graphique' || n.type === 'carte') && Array.isArray(n.figure?.data) && n.figure.data.length &&
      n.figure.data.every((t) => (Array.isArray(t.x) && Array.isArray(t.y) && t.y.length >= 2) || t.values || t.z));
}

async function run(question) {
  captured = []; pevOut = [];
  const msgs = [{ role: 'user', content: question }];
  const t0 = Date.now(); const calls = [];
  for (let turn = 1; turn <= MAX_TURNS; turn++) {
    const resp = await D.iaCallAPI(msgs, null, null, D.iaProvKey());
    const am = { role: 'assistant', content: resp.content || '' };
    if (resp.thinking) am.thinking = resp.thinking;
    if (resp.tool_calls.length) am.tool_calls = resp.tool_calls;
    msgs.push(am);
    console.log(`  tour ${turn} (${Math.round((Date.now() - t0) / 1000)} s) raisonnement=${(resp.thinking || '').length} car. texte=${(resp.content || '').length} car. outils=${resp.tool_calls.length} fin=${resp.done_reason}`);
    if (!resp.tool_calls.length) return { done: true, turns: turn, text: resp.content || '', calls, secs: Math.round((Date.now() - t0) / 1000) };
    for (const tc of resp.tool_calls) {
      const name = D.iaToolName(tc); const args = D.iaToolArgs(tc); calls.push(name);
      let out; try { out = await runTool(name, args); } catch (e) { out = 'Erreur : ' + (e?.message || e); }
      console.log(`    → ${name} ${tc.id ? '[' + tc.id + ']' : ''} | ${String(out).slice(0, 150).replace(/\n/g, ' ')}`);
      msgs.push({ role: 'tool', tool_name: name, tool_call_id: tc.id || null, content: String(out) });
    }
  }
  return { done: false, turns: MAX_TURNS, calls };
}

const cases = CUSTOM_Q ? [{ q: CUSTOM_Q, graph: /graphique|courbe|évolution/i.test(CUSTOM_Q) }] : [
  { q: 'Donne-moi la CV VAR1 du pays pour 2025', graph: false },
  { q: "Donne-moi l'évolution annuelle de la CV VAR1 de 2020 à 2025 pour l'antenne PEV de Luiza, présentée sur un graphique", graph: true },
];
let fail = 0;
for (const c of cases) {
  console.log('\n▶ ' + c.q);
  let r;
  try { r = await run(c.q); } catch (e) { console.log('  ✗ ERREUR : ' + (e?.message || e)); fail++; continue; }
  const pevOk = pevOut.length > 0 && pevOut.some((o) => o && !o.erreur && !o.error);
  const ok = r.done && r.text.trim().length > 0 && (!c.graph || graphOk());
  console.log(`  outils : ${r.calls.join(' → ') || '(aucun)'}`);
  console.log(`  calculer_pev_direct : ${pevOut.length} appel(s)${pevOk ? '' : ' — aucun résultat exploitable'}`);
  if (c.graph) console.log(`  graphique valide : ${graphOk()}`);
  console.log('  réponse finale :\n    ' + (r.text || '').slice(0, 900).replace(/\n/g, '\n    '));
  console.log(ok ? `  ✓ PASS (${r.turns} tours, ${r.secs} s)` : '  ✗ FAIL');
  if (!ok) fail++;
}
process.exit(fail ? 1 : 0);
