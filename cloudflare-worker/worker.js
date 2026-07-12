/**
 * ═══════════════════════════════════════════════════════════════════════
 *  PROXY IA — Dashboard PEV de routine (snis-vaccination-api)
 * ═══════════════════════════════════════════════════════════════════════
 *  Rôles :
 *   1. /v1/messages        → proxy vers l'API Anthropic (clé cachée ici),
 *                            réservé aux codes d'accès valides (quota).
 *   2. /dhis2/api/*        → proxy GET lecture seule vers le DHIS2 (SNIS RDC),
 *                            identifiants cachés ici, codes d'accès requis.
 *   3. /acheter            → page de paiement en ligne (CinetPay, mobile money)
 *      /webhook/cinetpay   → notification de paiement → création auto du code
 *      /retour             → page où l'acheteur récupère son code
 *   4. /admin/codes        → création/consultation de codes (token admin)
 *   5. /verifier?code=     → solde restant d'un code
 *
 *  Secrets à configurer (wrangler secret put NOM ou dashboard Cloudflare) :
 *   ANTHROPIC_API_KEY   — clé API console.anthropic.com
 *   DHIS2_BASE_URL      — ex. https://snisrdc.com (sans /api)
 *   DHIS2_USERNAME      — compte DHIS2 (lecture seule recommandé)
 *   DHIS2_PASSWORD
 *   ADMIN_TOKEN         — long mot de passe pour /admin/*
 *   CINETPAY_APIKEY     — (après création compte marchand CinetPay)
 *   CINETPAY_SITE_ID
 *
 *  Binding KV requis : CODES
 *  Variable d'env  : ALLOWED_ORIGIN = https://mbombomamu1993.github.io
 * ═══════════════════════════════════════════════════════════════════════
 */

/* ── Offres de vente (à ajuster librement) ──
   requests = nombre de requêtes IA incluses. Une analyse complète consomme
   en moyenne 3 à 6 requêtes (l'IA lit les données puis répond). */
const OFFERS = [
  { id: 'S', label: 'Découverte — ±20 analyses', requests: 100, amount: 15000, currency: 'CDF' },
  { id: 'M', label: 'Standard — ±60 analyses', requests: 300, amount: 35000, currency: 'CDF' },
  { id: 'L', label: 'Pro — ±150 analyses', requests: 750, amount: 70000, currency: 'CDF' },
];

const ALLOWED_MODELS = ['claude-opus-4-8', 'claude-sonnet-5', 'claude-haiku-4-5'];
const MAX_TOKENS_CAP = 16000;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const origin = request.headers.get('Origin') || '';
    const cors = corsHeaders(env, origin);

    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: cors });

    try {
      if (url.pathname === '/v1/messages' && request.method === 'POST') return await proxyAnthropic(request, env, cors);
      if (url.pathname.startsWith('/dhis2/api/')) return await proxyDhis2(request, env, cors, url);
      if (url.pathname === '/verifier') return await checkCode(env, url, cors);
      if (url.pathname.startsWith('/admin/codes')) return await adminCodes(request, env, url, cors);
      if (url.pathname === '/acheter' && request.method === 'POST') {
        if (!env.CINETPAY_APIKEY) return buyPage(env, url);
        const form = await request.formData();
        const offer = OFFERS.find(o => o.id === form.get('offer')) || OFFERS[0];
        const payUrl = await cinetpayInit(env, offer, url.origin);
        return Response.redirect(payUrl, 303);
      }
      if (url.pathname === '/acheter') return buyPage(env, url);
      if (url.pathname === '/webhook/cinetpay' && request.method === 'POST') return await cinetpayWebhook(request, env);
      if (url.pathname === '/retour') return await returnPage(env, url);
      return json({ error: 'Introuvable' }, 404, cors);
    } catch (e) {
      return json({ error: String(e && e.message || e) }, 500, cors);
    }
  },
};

/* ═══ CORS ═══ */
function corsHeaders(env, origin) {
  const allowed = (env.ALLOWED_ORIGIN || '*').split(',').map(s => s.trim());
  const ok = allowed.includes('*') || allowed.includes(origin);
  return {
    'Access-Control-Allow-Origin': ok ? (origin || '*') : allowed[0],
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
    'Access-Control-Allow-Headers': 'content-type,anthropic-version,x-access-code',
    'Access-Control-Max-Age': '86400',
  };
}
function json(obj, status = 200, extra = {}) {
  return new Response(JSON.stringify(obj), { status, headers: { 'content-type': 'application/json; charset=utf-8', ...extra } });
}

/* ═══ Codes d'accès (KV) ═══
   KV clé  : code:<CODE>   valeur : {"total":N,"remaining":N,"created":iso,"tx":"..."}
   KV clé  : tx:<TRANSACTION_ID> valeur : {"code":"...","status":"paid"} */
function genCode() {
  const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  const part = n => Array.from(crypto.getRandomValues(new Uint8Array(n))).map(b => A[b % A.length]).join('');
  return `PEV-${part(4)}-${part(4)}`;
}
async function getCode(env, code) {
  if (!code) return null;
  const v = await env.CODES.get('code:' + code.trim().toUpperCase());
  return v ? JSON.parse(v) : null;
}
async function putCode(env, code, data) {
  await env.CODES.put('code:' + code.trim().toUpperCase(), JSON.stringify(data));
}
async function requireCode(request, env) {
  const code = (request.headers.get('x-access-code') || '').trim().toUpperCase();
  const rec = await getCode(env, code);
  if (!rec) throw Object.assign(new Error("Code d'accès invalide. Achetez un code sur la page « Obtenir un code »."), { status: 401 });
  if (rec.remaining <= 0) throw Object.assign(new Error('Code épuisé (' + rec.total + ' requêtes consommées). Achetez un nouveau code.'), { status: 402 });
  return { code, rec };
}

/* ═══ 1. Proxy Anthropic ═══ */
async function proxyAnthropic(request, env, cors) {
  let auth;
  try { auth = await requireCode(request, env); }
  catch (e) { return json({ error: { type: 'auth', message: e.message } }, e.status || 401, cors); }
  /* Les codes marqués dhis2_only n'ouvrent PAS l'accès Anthropic (crédits) */
  if (auth.rec.dhis2_only) return json({ error: { type: 'auth', message: 'Ce code ne donne accès qu\'au DHIS2. Achetez un code d\'accès pour l\'assistant IA.' } }, 403, cors);

  const body = await request.json();
  if (!ALLOWED_MODELS.includes(body.model)) body.model = ALLOWED_MODELS[0];
  if (!body.max_tokens || body.max_tokens > MAX_TOKENS_CAP) body.max_tokens = MAX_TOKENS_CAP;

  const upstream = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'anthropic-version': request.headers.get('anthropic-version') || '2023-06-01',
      'x-api-key': env.ANTHROPIC_API_KEY,
    },
    body: JSON.stringify(body),
  });

  /* Décompte : 1 requête réussie = 1 unité (les erreurs ne comptent pas). */
  if (upstream.ok) {
    auth.rec.remaining -= 1;
    await putCode(env, auth.code, auth.rec);
  }
  return new Response(upstream.body, {
    status: upstream.status,
    headers: { 'content-type': upstream.headers.get('content-type') || 'application/json', 'x-quota-restant': String(auth.rec.remaining), ...cors },
  });
}

/* ═══ 2. Proxy DHIS2 (GET, lecture seule) ═══ */
async function proxyDhis2(request, env, cors, url) {
  if (request.method !== 'GET') return json({ error: 'GET uniquement (lecture seule)' }, 405, cors);
  try { await requireCode(request, env); }
  catch (e) { return json({ error: e.message }, e.status || 401, cors); }
  if (!env.DHIS2_BASE_URL) return json({ error: 'DHIS2 non configuré sur le proxy' }, 503, cors);

  const path = url.pathname.replace(/^\/dhis2\/api\//, '');
  /* Endpoints en écriture ou sensibles interdits */
  if (/^(users|me\/|system\/|apps|dataStore.*\bPOST)/i.test(path)) return json({ error: 'Endpoint non autorisé' }, 403, cors);

  const target = env.DHIS2_BASE_URL.replace(/\/+$/, '') + '/api/' + path + url.search;
  const upstream = await fetch(target, {
    headers: {
      Authorization: 'Basic ' + btoa(env.DHIS2_USERNAME + ':' + env.DHIS2_PASSWORD),
      Accept: 'application/json',
    },
  });
  const text = await upstream.text();
  return new Response(text, { status: upstream.status, headers: { 'content-type': 'application/json; charset=utf-8', ...cors } });
}

/* ═══ 3. Vérification de solde ═══ */
async function checkCode(env, url, cors) {
  const rec = await getCode(env, url.searchParams.get('code') || '');
  if (!rec) return json({ valide: false }, 200, cors);
  return json({ valide: true, total: rec.total, restant: rec.remaining }, 200, cors);
}

/* ═══ 4. Admin ═══ */
async function adminCodes(request, env, url, cors) {
  const tok = (request.headers.get('Authorization') || '').replace(/^Bearer\s+/i, '');
  if (!env.ADMIN_TOKEN || tok !== env.ADMIN_TOKEN) return json({ error: 'Non autorisé' }, 401, cors);

  if (request.method === 'POST') {
    const b = await request.json().catch(() => ({}));
    const code = genCode();
    await putCode(env, code, { total: b.requests || 100, remaining: b.requests || 100, created: new Date().toISOString(), tx: b.note || 'admin' });
    return json({ code, requests: b.requests || 100 }, 200, cors);
  }
  /* GET /admin/codes/PEV-XXXX-XXXX */
  const m = url.pathname.match(/^\/admin\/codes\/(.+)$/);
  if (m) {
    const rec = await getCode(env, decodeURIComponent(m[1]));
    return json(rec || { error: 'inconnu' }, rec ? 200 : 404, cors);
  }
  /* GET /admin/codes → liste (max 1000) */
  const list = await env.CODES.list({ prefix: 'code:', limit: 1000 });
  const out = [];
  for (const k of list.keys) {
    const v = await env.CODES.get(k.name);
    out.push({ code: k.name.slice(5), ...(v ? JSON.parse(v) : {}) });
  }
  return json(out, 200, cors);
}

/* ═══ 5. Paiement CinetPay ═══ */
function htmlPage(title, body) {
  return new Response(`<!doctype html><html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>${title}</title>
<style>body{font-family:system-ui,Segoe UI,Roboto,sans-serif;background:#f4f6fb;margin:0;display:flex;min-height:100vh;align-items:center;justify-content:center}
.card{background:#fff;border-radius:16px;box-shadow:0 8px 30px rgba(26,35,126,.12);padding:32px;max-width:460px;width:92%}
h1{color:#1a237e;font-size:20px;margin:0 0 6px}p{color:#555;font-size:14px}
.offer{display:flex;justify-content:space-between;align-items:center;border:1px solid #dfe2ee;border-radius:12px;padding:14px 16px;margin:10px 0;cursor:pointer}
.offer:hover{border-color:#1a237e;background:#f0f2fa}.offer b{color:#1a237e}
.price{font-weight:800;color:#d97757}
.code{font-size:26px;font-weight:800;color:#1a237e;letter-spacing:2px;text-align:center;background:#f0f2fa;border-radius:12px;padding:18px;margin:16px 0}
.err{background:#fdecea;color:#a12622;border-radius:10px;padding:12px;font-size:13px}
button{background:#1a237e;color:#fff;border:none;border-radius:10px;padding:12px 18px;font-size:14px;font-weight:700;cursor:pointer;width:100%}
small{color:#999}</style></head><body><div class="card">${body}</div></body></html>`,
    { headers: { 'content-type': 'text/html; charset=utf-8' } });
}

function buyPage(env, url) {
  if (!env.CINETPAY_APIKEY || !env.CINETPAY_SITE_ID) {
    return htmlPage('Paiement bientôt disponible',
      `<h1>💳 Paiement en ligne — bientôt disponible</h1>
       <p>Le paiement automatique par mobile money (M-Pesa, Orange Money, Airtel Money) est en cours d'activation.
       Revenez bientôt, ou utilisez votre propre clé API Anthropic dans ⚙ Accès en attendant.</p>`);
  }
  const offers = OFFERS.map(o =>
    `<form method="POST" action="/acheter" style="margin:0"><input type="hidden" name="offer" value="${o.id}">
     <button type="submit" class="offer" style="background:#fff;color:#333;text-align:left">
       <span><b>${o.label}</b><br><small>${o.requests} requêtes IA</small></span>
       <span class="price">${o.amount.toLocaleString('fr-FR')} ${o.currency}</span></button></form>`).join('');
  return htmlPage("Obtenir un code d'accès",
    `<h1>🎫 Obtenir un code d'accès</h1>
     <p>Choisissez une offre — paiement par <b>mobile money</b> (M-Pesa, Orange Money, Airtel Money) ou carte.
     Votre code s'affiche immédiatement après le paiement.</p>${offers}
     <small>Assistant IA du Dashboard PEV de routine — RDC.</small>`);
}

async function cinetpayInit(env, offer, origin) {
  const txId = 'PEV' + Date.now() + Math.floor(Math.random() * 1e6);
  const resp = await fetch('https://api-checkout.cinetpay.com/v2/payment', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      apikey: env.CINETPAY_APIKEY,
      site_id: env.CINETPAY_SITE_ID,
      transaction_id: txId,
      amount: offer.amount,
      currency: offer.currency,
      description: 'Code IA Dashboard PEV — ' + offer.label,
      notify_url: origin + '/webhook/cinetpay',
      return_url: origin + '/retour?tx=' + txId,
      channels: 'ALL',
    }),
  });
  const data = await resp.json();
  if (!data || !data.data || !data.data.payment_url) throw new Error('Échec init CinetPay : ' + JSON.stringify(data).slice(0, 300));
  await putTx(env, txId, { status: 'pending', offer: offer.id, created: new Date().toISOString() });
  return data.data.payment_url;
}
async function putTx(env, tx, data) { await env.CODES.put('tx:' + tx, JSON.stringify(data)); }
async function getTx(env, tx) { const v = await env.CODES.get('tx:' + tx); return v ? JSON.parse(v) : null; }

async function cinetpayCheck(env, txId) {
  const r = await fetch('https://api-checkout.cinetpay.com/v2/payment/check', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ apikey: env.CINETPAY_APIKEY, site_id: env.CINETPAY_SITE_ID, transaction_id: txId }),
  });
  const d = await r.json();
  return d && d.data && (d.data.status === 'ACCEPTED' || d.code === '00');
}

/* Délivrance (idempotente) d'un code après paiement confirmé */
async function deliverCode(env, txId) {
  const tx = await getTx(env, txId);
  if (!tx) return null;
  if (tx.code) return tx.code; // déjà délivré
  const ok = await cinetpayCheck(env, txId);
  if (!ok) return null;
  const offer = OFFERS.find(o => o.id === tx.offer) || OFFERS[0];
  const code = genCode();
  await putCode(env, code, { total: offer.requests, remaining: offer.requests, created: new Date().toISOString(), tx: txId });
  tx.status = 'paid'; tx.code = code;
  await putTx(env, txId, tx);
  return code;
}

async function cinetpayWebhook(request, env) {
  const ct = request.headers.get('content-type') || '';
  let txId = '';
  if (ct.includes('json')) { const b = await request.json().catch(() => ({})); txId = b.cpm_trans_id || b.transaction_id || ''; }
  else { const f = await request.formData().catch(() => null); if (f) txId = f.get('cpm_trans_id') || f.get('transaction_id') || ''; }
  if (txId) await deliverCode(env, String(txId));
  return new Response('OK');
}

async function returnPage(env, url) {
  const txId = url.searchParams.get('tx') || '';
  /* POST /acheter → redirection CinetPay */
  if (!txId) return htmlPage('Erreur', `<div class="err">Transaction manquante.</div>`);
  const code = await deliverCode(env, txId);
  if (code) {
    return htmlPage('Votre code d\'accès',
      `<h1>✅ Paiement confirmé</h1><p>Voici votre code d'accès — copiez-le dans l'onglet
       « Génération des analyses » → ⚙ Accès → Code d'accès :</p>
       <div class="code">${code}</div>
       <p><b>Conservez-le précieusement</b> : il ne sera plus affiché. Vous pouvez vérifier votre solde
       à tout moment dans l'application.</p>`);
  }
  return htmlPage('Paiement en attente',
    `<h1>⏳ Paiement en cours de confirmation</h1>
     <p>Votre paiement n'est pas encore confirmé. Attendez quelques secondes puis
     <a href="/retour?tx=${encodeURIComponent(txId)}">actualisez cette page</a>.
     Si le problème persiste, le paiement a peut-être échoué.</p>`);
}
