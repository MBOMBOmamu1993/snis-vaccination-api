#!/usr/bin/env node
/**
 * Test de régression de l'assistant IA — rejoue la boucle agentique du dashboard
 * contre le Worker déployé, avec n'importe quel modèle Ollama Cloud, et valide
 * automatiquement les résultats (figures Plotly valides, pas de boucle infinie).
 *
 * Le prompt système, les outils et les fonctions de normalisation sont EXTRAITS
 * de docs/index.html à l'exécution : le test reste synchronisé avec le dashboard
 * sans duplication.
 *
 * Usage :
 *   PEV_ACCESS_CODE=PEV-XXXX-XXXX node scripts/test_ia_ollama.mjs [modele] ["question"]
 *
 *   modele   : défaut minimax-m3 (ex. glm-5.2, deepseek-v4-pro, qwen3.5:397b)
 *   question : défaut = les 2 questions de référence (CV VAR1 pays, graphique Luiza)
 *
 * Code d'accès temporaire (à supprimer après) :
 *   cd cloudflare-worker
 *   npx wrangler kv key put --namespace-id fb104235615740b9a9d117355f864751 \
 *     "code:PEV-TEST-XXXX" '{"total":80,"remaining":80}' --remote
 *
 * Sortie : détail tour par tour + verdict PASS/FAIL par question (exit 1 si échec).
 * À lancer avant tout changement du prompt système, des outils ou du Worker.
 */
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import { fileURLToPath } from 'node:url';

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
const PROXY = 'https://pev-ia-proxy.pev-rdc.workers.dev';
const DATA_ORIGIN = 'https://mbombomamu1993.github.io/snis-vaccination-api/';
const CODE = process.env.PEV_ACCESS_CODE;
const DHIS2_CODE = 'PEV-DHIS-7QK4XZ';
const MAX_TURNS = 30;

if (!CODE) { console.error('PEV_ACCESS_CODE manquant (voir en-tête du fichier).'); process.exit(2); }

/* ── Extraction du vrai code du dashboard ── */
const html = fs.readFileSync(path.join(ROOT, 'docs', 'index.html'), 'utf8');
const grab = (re) => { const m = html.match(re); if (!m) throw new Error('bloc introuvable: ' + re); return m[1] || m[0]; };
const sandbox = {};
function loadFn(re) { (0, eval)(grab(re).replace(/^( *)function (\w+)/, '$1sandbox.$2 = function')); }
globalThis.sandbox = sandbox;
globalThis.S = { data: [], vals: {}, filters: {} }; globalThis.AG = []; globalThis.IA_TOOL_RESULT_MAX = 8000;
(0, eval)(grab(/function iaSystem\(\) \{[\s\S]*?\n {12}\}/).replace('function iaSystem()', 'sandbox.iaSystem = function ()'));
(0, eval)(grab(/var IA_TOOLS = \[[\s\S]*?\n {12}\];/).replace('var IA_TOOLS', 'sandbox.IA_TOOLS'));
loadFn(/( *function iaParseMaybe\(v\) \{[\s\S]*?\n {12}\})/);
loadFn(/( *function iaFixFigure\(fig\) \{[\s\S]*?\n {12}\})/);
loadFn(/( *function iaFigureFromTable\(colonnes, lignes\) \{[\s\S]*?\n {12}\})/);
loadFn(/( *function iaNormalizeResult\(inp\) \{[\s\S]*?\n {12}\})/);
loadFn(/( *function iaUnwrapArrays\(v\) \{[\s\S]*?\n {12}\})/);
loadFn(/( *function iaMissingAwait\(code\) \{[\s\S]*?\n {12}\})/);
loadFn(/( *function iaScoreQualiteDps\(o\) \{[\s\S]*?\n {12}\})/);
const { iaSystem, IA_TOOLS, iaParseMaybe, iaFixFigure, iaFigureFromTable, iaNormalizeResult, iaUnwrapArrays, iaMissingAwait, iaScoreQualiteDps } = sandbox;
// résout les dépendances croisées des fonctions extraites
globalThis.iaParseMaybe = iaParseMaybe; globalThis.iaFixFigure = iaFixFigure;
globalThis.iaFigureFromTable = iaFigureFromTable; globalThis.iaUnwrapArrays = iaUnwrapArrays;

/* ── Outils (mêmes comportements que le dashboard) ── */
async function dhis2(endpoint, params) {
  let ep = String(endpoint || '').replace(/^\/?api\/?/, '').replace(/^\/+/, '');
  let inline = ''; const qm = ep.indexOf('?');
  if (qm >= 0) { inline = ep.slice(qm + 1); ep = ep.slice(0, qm); }
  let qs;
  if (params && typeof params === 'object') qs = Object.keys(params).map(k => Array.isArray(params[k]) ? params[k].map(x => k + '=' + x).join('&') : k + '=' + params[k]).join('&');
  else qs = String(params || '').replace(/^\?/, '');
  qs = [inline, qs].filter(Boolean).join('&');
  const r = await fetch(PROXY + '/dhis2/api/' + ep + (qs ? '?' + qs : ''), { headers: { 'x-access-code': DHIS2_CODE } });
  const t = await r.text();
  if (!r.ok) throw new Error('DHIS2 HTTP ' + r.status + ' : ' + t.slice(0, 300));
  try { return JSON.parse(t); } catch { return t; }
}
async function loadLocal(p) {
  p = String(p || '').replace(/^\.\//, '');
  const r = await fetch(DATA_ORIGIN + p);
  if (!r.ok) throw new Error('HTTP ' + r.status + ' sur ' + p);
  const buf = Buffer.from(await r.arrayBuffer());
  return JSON.parse(p.endsWith('.gz') ? zlib.gunzipSync(buf).toString('utf8') : buf.toString('utf8'));
}
const captured = [];
async function runTool(name, input) {
  if (name === 'requete_dhis2') {
    const out = await dhis2(input.endpoint, input.params);
    let t = typeof out === 'string' ? out : JSON.stringify(out);
    return t.length > 8000 ? t.slice(0, 8000) + '\n…[TRONQUÉ]' : t;
  }
  if (name === 'executer_js') {
    const ctx = { data: [], flt: [], AG: [], dhis2, load: loadLocal, loadUrl: async () => { throw new Error('indisponible'); }, mL: x => x, scoreQualiteDps: iaScoreQualiteDps };
    const fn = new Function('ctx', '"use strict";return (async function(){' + input.code + '\n})();');
    const out = await fn(ctx);
    let s = out === undefined ? 'undefined (le code doit se terminer par return)' : JSON.stringify(out);
    if (s.length > 8000) s = s.slice(0, 8000) + '\n…[TRONQUÉ]';
    const miss = iaMissingAwait(String(input.code || ''));
    if (miss.length) s = '⚠ BUG PROBABLE : ctx.' + miss[0] + '() appelé SANS await — réécris avec await.\n\n' + s;
    return s;
  }
  if (name === 'afficher_resultat') { captured.push(input); return 'OK : résultat affiché.'; }
  if (name === 'generer_rapport') { captured.push({ _rapport: true, ...input }); return 'OK : rapport généré et téléchargé.'; }
  throw new Error('Outil inconnu : ' + name);
}

/* ── Boucle agentique ── */
async function run(model, question) {
  captured.length = 0;
  const msgs = [{ role: 'user', content: question }];
  const t0 = Date.now();
  for (let turn = 1; turn <= MAX_TURNS; turn++) {
    const r = await fetch(PROXY + '/api/chat', {
      method: 'POST', headers: { 'content-type': 'application/json', 'x-access-code': CODE },
      body: JSON.stringify({ model, stream: false, think: 'high', messages: [{ role: 'system', content: iaSystem() }, ...msgs], tools: IA_TOOLS }),
    });
    if (!r.ok) { console.log(`  HTTP ${r.status}: ${(await r.text()).slice(0, 250)}`); return { done: false, turns: turn }; }
    const d = await r.json();
    const m = d.message || {};
    const tcs = m.tool_calls || [];
    console.log(`  tour ${turn} (${Math.round((Date.now() - t0) / 1000)}s) texte=${String(m.content || '').length}c outils=${tcs.length}`);
    const am = { role: 'assistant', content: m.content || '' };
    if (m.thinking) am.thinking = m.thinking;
    if (tcs.length) am.tool_calls = tcs;
    msgs.push(am);
    if (!tcs.length) return { done: true, turns: turn, text: m.content || '' };
    for (const tc of tcs) {
      const name = tc.function?.name || '';
      let args = tc.function?.arguments;
      if (typeof args === 'string') { try { args = JSON.parse(args); } catch { args = {}; } }
      let out;
      try { out = await runTool(name, args || {}); } catch (e) { out = 'Erreur : ' + (e?.message || e); }
      console.log(`    -> ${name} | ${String(out).slice(0, 90).replace(/\n/g, ' ')}`);
      msgs.push({ role: 'tool', tool_name: name, content: String(out) });
    }
  }
  return { done: false, turns: MAX_TURNS };
}

function validateGraph() {
  const graphs = captured.filter(a => !a._rapport).map(a => iaNormalizeResult(iaUnwrapArrays(a)))
    .filter(n => n.type === 'graphique' || n.type === 'carte');
  return graphs.some(n => Array.isArray(n.figure?.data) && n.figure.data.length &&
    n.figure.data.every(t => (Array.isArray(t.x) && Array.isArray(t.y) && t.y.length >= 2) || t.values || t.z));
}

const model = process.argv[2] || 'minimax-m3';
const custom = process.argv[3];
const cases = custom
  ? [{ q: custom, graph: /graphique|courbe|évolution/i.test(custom) }]
  : [
    { q: 'Donne-moi la CV VAR1 du pays pour 2025', graph: false },
    { q: "Donne-moi l'évolution annuelle de la CV VAR1 de 2020 à 2025 pour l'antenne PEV de Luiza, présentée sur un graphique", graph: true },
  ];

let fail = 0;
for (const c of cases) {
  console.log(`\n=== ${model} — « ${c.q} » ===`);
  const res = await run(model, c.q);
  const graphOk = c.graph ? validateGraph() : true;
  const pass = res.done && graphOk && res.turns < MAX_TURNS;
  console.log(`  => ${pass ? 'PASS' : 'FAIL'} (terminé=${res.done}, tours=${res.turns}, graphique valide=${graphOk}, affichages=${captured.length})`);
  if (!pass) fail++;
}
process.exit(fail ? 1 : 0);
