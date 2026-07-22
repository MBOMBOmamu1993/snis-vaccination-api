/**
 * ═══════════════════════════════════════════════════════════════════════
 *  PROXY IA — Dashboard PEV de routine (snis-vaccination-api)
 * ═══════════════════════════════════════════════════════════════════════
 *  Rôles :
 *   1. DEUX fournisseurs d'IA, au choix du client (bascule en cas d'indisponibilité) :
 *        /api/chat        → Ollama Cloud (MiniMax M3)   — clé OLLAMA_API_KEY
 *        /v1/messages     → Anthropic (Claude)          — clé ANTHROPIC_API_KEY
 *      Dans les deux cas : soit un code d'accès valide (quota décompté, clé du
 *      service utilisée), soit la PROPRE clé de l'appelant via x-ollama-key /
 *      x-anthropic-key (relais pur, aucun quota consommé).
 *      Le relais Ollama est indispensable : ollama.com ne renvoie aucun en-tête
 *      CORS, un appel direct depuis le navigateur est donc impossible.
 *   2. /dhis2/api/*        → proxy GET lecture seule vers le DHIS2 (SNIS RDC),
 *                            identifiants cachés ici, codes d'accès requis.
 *   3. Vente de codes. Trois modes (PAYMENT_PROVIDER pour forcer) :
 *        • Commande en ligne (défaut, sans config) : /commander → le client
 *          laisse nom + email, suit sa commande en direct (barre de progression),
 *          dépose au numéro affiché après votre approbation, envoie sa capture ;
 *          vous livrez depuis /admin → le code s'affiche aussitôt sur sa page
 *          (+ copie par e-mail si BREVO_API_KEY est posé).
 *        • CinetPay (automatique) : actif si CINETPAY_APIKEY + SITE_ID posés.
 *        • WhatsApp (manuel)    : boutons wa.me (PAYMENT_PROVIDER=whatsapp).
 *      /webhook/cinetpay   → notification CinetPay → création auto du code
 *      /retour             → page où l'acheteur récupère son code (CinetPay)
 *   4. /admin              → console protégée : commandes en attente (approbation,
 *                            capture, livraison) + création manuelle de codes.
 *                            /admin/codes → API JSON.
 *   5. /verifier?code=     → solde restant d'un code
 *   6. /essai              → code d'essai gratuit : 7 jours / 50 requêtes,
 *                            1 par appareil (empreinte IP+UA), puis paiement
 *
 *  Secrets à configurer (wrangler secret put NOM ou dashboard Cloudflare) :
 *   OLLAMA_API_KEY      — clé API ollama.com (Settings → API keys)
 *   ANTHROPIC_API_KEY   — clé API console.anthropic.com
 *   KIMI_API_KEY        — clé API platform.kimi.ai (Moonshot) : assistant de
 *                         lecture des captures + fournisseur Kimi K3 du dashboard
 *                         (les trois sont facultatives : un fournisseur sans clé
 *                          reste utilisable par les clients ayant leur propre clé)
 *   DHIS2_BASE_URL      — ex. https://snisrdc.com (sans /api)
 *   DHIS2_USERNAME      — compte DHIS2 (lecture seule recommandé)
 *   DHIS2_PASSWORD
 *   ADMIN_TOKEN         — long mot de passe pour /admin/*
 *   MPESA_INFOS         — instructions de dépôt affichées au client après
 *                         approbation (ex. « M-Pesa : 0824 000 000 — nom : … »)
 *   BREVO_API_KEY       — (optionnel) clé API Brevo (300 e-mails/jour gratuits)
 *   MAIL_FROM           — (optionnel) expéditeur vérifié Brevo (ex. Gmail dédié)
 *   MAIL_TO_ADMIN       — (optionnel) votre email : rapport quotidien des ventes
 *   TELEGRAM_BOT_TOKEN  — (optionnel) alertes Telegram (@BotFather)
 *   TELEGRAM_CHAT_ID    — (optionnel) via @userinfobot
 *   CALLMEBOT_PHONE     — (optionnel) alertes sur VOTRE WhatsApp (callmebot.com,
 *   CALLMEBOT_APIKEY      format international SANS « + », ex. 243812345678)
 *   WHATSAPP_NUMBER     — (mode whatsapp uniquement) numéro dédié, format
 *                         international SANS « + » (ex. 243812345678)
 *   CINETPAY_APIKEY     — (après création compte marchand CinetPay)
 *   CINETPAY_SITE_ID
 *
 *  Binding KV requis : CODES
 *  Variable d'env  : ALLOWED_ORIGIN = https://mbombomamu1993.github.io
 * ═══════════════════════════════════════════════════════════════════════
 */

/* ── Offres de vente (à ajuster librement) ──
   requests = nombre de requêtes IA incluses. Une analyse complète consomme
   en moyenne 3 à 6 requêtes (l'IA lit les données puis répond).
   Tarif : 0,10 $/requête. Le modèle tourne sur l'abonnement Ollama Cloud
   (forfait mensuel, pas de facturation à la requête) : la marge dépend donc
   du volume mensuel, pas d'un coût unitaire à l'appel. */
const REQUESTS_PER_USD = 10;
const CUSTOM_MIN_USD = 1;
const CUSTOM_MAX_USD = 500;
const OFFERS = [
  { id: 'S', label: 'Découverte', analyses: '±12 analyses', requests: 50, amount: 5, currency: 'USD' },
  { id: 'M', label: 'Standard', analyses: '±25 analyses', requests: 100, amount: 10, currency: 'USD' },
  { id: 'L', label: 'Pro', analyses: '±50 analyses', requests: 200, amount: 20, currency: 'USD' },
  { id: 'XL', label: 'Expert', analyses: '±75 analyses', requests: 300, amount: 30, currency: 'USD' },
];

/* Modèles Ollama Cloud : la liste vit chez Ollama (route /api/tags relayée au
   dashboard) — on ne fige donc PAS d'allowlist ici, seulement une validation de
   forme + un défaut. Le tag « :cloud » (ou suffixe -cloud) est obligatoire —
   sans lui, Ollama cherche un modèle local et renvoie « model not found ». */
const DEFAULT_OLLAMA_MODEL = 'minimax-m3:cloud';
const ALLOWED_ANTHROPIC_MODELS = ['claude-opus-4-8', 'claude-fable-5', 'claude-sonnet-5', 'claude-haiku-4-5'];
const OLLAMA_CHAT_URL = 'https://ollama.com/api/chat';
const OLLAMA_TAGS_URL = 'https://ollama.com/api/tags';
const ANTHROPIC_URL = 'https://api.anthropic.com/v1/messages';
/* Moonshot : plateforme internationale api.moonshot.ai (platform.kimi.ai).
   Si la clé vient de la plateforme chinoise : poser KIMI_API_BASE =
   https://api.moonshot.cn dans les vars. */
const KIMI_DEFAULT_BASE = 'https://api.moonshot.ai';
const DEFAULT_KIMI_MODEL = 'kimi-k3';
const NUM_PREDICT_CAP = 16000; /* plafond de tokens générés (équiv. max_tokens) */
const MAX_TOKENS_CAP = 16000;

/* ── Essai gratuit : TRIAL_DAYS jours (ou TRIAL_REQUESTS requêtes, la 1re
   échéance compte) PAR APPAREIL. Empreinte = hash(IP + User-Agent) ;
   un nouvel appui sur « Essai gratuit » rend le même code. ── */
const TRIAL_DAYS = 7;
const TRIAL_REQUESTS = 50;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const origin = request.headers.get('Origin') || '';
    const cors = corsHeaders(env, origin);

    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: cors });

    try {
      if (url.pathname === '/api/chat' && request.method === 'POST') return await proxyOllama(request, env, cors);
      if (url.pathname === '/api/tags' && request.method === 'GET') return await ollamaTags(request, env, cors);
      if (url.pathname === '/kimi/v1/chat/completions' && request.method === 'POST') return await proxyKimi(request, env, cors);
      if (url.pathname === '/v1/messages' && request.method === 'POST') return await proxyAnthropic(request, env, cors);
      if (url.pathname.startsWith('/dhis2/api/')) return await proxyDhis2(request, env, cors, url);
      if (url.pathname === '/verifier') return await checkCode(env, url, cors);
      if (url.pathname === '/essai') return await trialGrant(request, env, cors);
      if (url.pathname === '/commander') return await orderPage(request, env, url);
      if (url.pathname === '/suivre') return trackPage(env, url);
      if (url.pathname === '/api/commande') return await apiOrder(request, env, url, cors);
      if (url.pathname === '/commande/preuve' && request.method === 'POST') return await uploadProof(request, env);
      if (url.pathname === '/admin/preuve') return await adminProofImage(env, url);
      if (url.pathname === '/admin/rapport') return await adminReport(env, url);
      if (url.pathname.startsWith('/admin/codes')) return await adminCodes(request, env, url, cors);
      if (url.pathname === '/acheter' && request.method === 'POST') {
        if (payProvider(env) !== 'cinetpay') return buyPage(env, url);
        const form = await request.formData();
        let offer;
        if (form.get('offer') === 'C') {
          const usd = Math.floor(Number(form.get('montant')));
          if (!(usd >= CUSTOM_MIN_USD && usd <= CUSTOM_MAX_USD)) {
            return htmlPage('Montant invalide',
              `<h1>Montant invalide</h1><div class="err">Entrez un montant entier entre ${CUSTOM_MIN_USD} $ et ${CUSTOM_MAX_USD} $.</div>
               <p style="margin-top:14px"><a href="/acheter">← Retour aux offres</a></p>`);
          }
          offer = { id: 'C', label: `Personnalisé — ${usd} $`, requests: usd * REQUESTS_PER_USD, amount: usd, currency: 'USD' };
        } else {
          offer = OFFERS.find(o => o.id === form.get('offer')) || OFFERS[0];
        }
        const payUrl = await cinetpayInit(env, offer, url.origin);
        return Response.redirect(payUrl, 303);
      }
      if (url.pathname === '/acheter') return buyPage(env, url);
      if (url.pathname === '/admin') return await adminPage(request, env);
      if (url.pathname === '/webhook/cinetpay' && request.method === 'POST') return await cinetpayWebhook(request, env);
      if (url.pathname === '/retour') return await returnPage(env, url);
      return json({ error: 'Introuvable' }, 404, cors);
    } catch (e) {
      return json({ error: String(e && e.message || e) }, 500, cors);
    }
  },
  /* Crons ([triggers] dans wrangler.toml) :
     • toutes les 5 min : relance des alertes téléphone en échec (file notifq:*)
     • 17 h UTC (18 h Kinshasa) : rapport des ventes du jour + passe de relance. */
  async scheduled(event, env, ctx) {
    const p = (async () => {
      await retryNotifQueue(env);
      if (event && event.cron && event.cron.indexOf('*/5') === 0) return; /* passe courte */
      await dailyReport(env);
    })();
    if (ctx && typeof ctx.waitUntil === 'function') ctx.waitUntil(p);
    return p;
  },
};

/* ═══ CORS ═══ */
function corsHeaders(env, origin) {
  const allowed = (env.ALLOWED_ORIGIN || '*').split(',').map(s => s.trim());
  const ok = allowed.includes('*') || allowed.includes(origin);
  return {
    'Access-Control-Allow-Origin': ok ? (origin || '*') : allowed[0],
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
    'Access-Control-Allow-Headers': 'content-type,anthropic-version,x-access-code,x-ollama-key,x-anthropic-key,x-kimi-key',
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
  if (rec.expires && Date.now() > Date.parse(rec.expires)) {
    throw Object.assign(new Error(rec.trial
      ? "Votre semaine d'essai gratuit est terminée. Achetez un code (⚙ Accès → « Obtenir un code ») pour continuer à utiliser l'assistant."
      : 'Code expiré. Achetez un nouveau code.'), { status: 402 });
  }
  if (rec.remaining <= 0) throw Object.assign(new Error('Code épuisé (' + rec.total + ' requêtes consommées). Achetez un nouveau code.'), { status: 402 });
  return { code, rec };
}

/* Empreinte d'appareil : hash SHA-256(IP + User-Agent) — anti-abus essai
   gratuit et anti-spam commandes. */
async function fpHash(request) {
  const ip = request.headers.get('cf-connecting-ip') || 'x';
  const ua = request.headers.get('user-agent') || '';
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(ip + '|' + ua));
  return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, '0')).join('').slice(0, 32);
}

/* ═══ Essai gratuit : 7 jours (ou TRIAL_REQUESTS requêtes) par appareil ═══
   Idempotent : un nouvel appel depuis le même appareil rend le même code
   (pratique si l'utilisateur l'a perdu), sans jamais créer de second essai.
   Le code d'essai fonctionne avec tous les fournisseurs IA et le proxy DHIS2. */
async function trialGrant(request, env, cors) {
  if (request.method !== 'GET' && request.method !== 'POST') return json({ error: 'GET/POST uniquement' }, 405, cors);
  const fp = await fpHash(request);
  const prev = await env.CODES.get('trial:' + fp);
  if (prev) {
    const t = JSON.parse(prev);
    const rec = await getCode(env, t.code);
    return json({ code: t.code, essai: true, deja: true, restant: rec ? rec.remaining : 0, expire: t.expires }, 200, cors);
  }
  const code = genCode();
  const expires = new Date(Date.now() + TRIAL_DAYS * 864e5).toISOString();
  await putCode(env, code, { total: TRIAL_REQUESTS, remaining: TRIAL_REQUESTS, created: new Date().toISOString(), tx: 'essai', trial: true, expires });
  await env.CODES.put('trial:' + fp, JSON.stringify({ code, expires }));
  return json({ code, essai: true, restant: TRIAL_REQUESTS, expire: expires }, 200, cors);
}

/* ═══ 1. Proxys IA (Ollama Cloud + Anthropic) ═══
   Deux modes d'authentification, communs aux deux fournisseurs :
     • clé personnelle de l'appelant (x-ollama-key / x-anthropic-key) → relais
       pur, aucun quota consommé (il paie son propre usage) ;
     • x-access-code : code d'accès acheté → la clé du service est utilisée et
       1 requête est décomptée du solde.
   Dans les deux cas le corps et le flux de réponse sont transmis tels quels. */
async function resolveIaAuth(request, env, ownKeyHeader, serviceKeyName) {
  const ownKey = (request.headers.get(ownKeyHeader) || '').trim();
  if (ownKey) return { key: ownKey, auth: null };

  const auth = await requireCode(request, env); /* lève une erreur si invalide */
  /* Les codes marqués dhis2_only n'ouvrent PAS l'accès à l'IA (quota) */
  if (auth.rec.dhis2_only) throw Object.assign(new Error('Ce code ne donne accès qu\'au DHIS2. Achetez un code d\'accès pour l\'assistant IA.'), { status: 403 });
  if (!env[serviceKeyName]) throw Object.assign(new Error('Ce fournisseur n\'est pas configuré sur le proxy (' + serviceKeyName + ' absent). Choisissez l\'autre fournisseur dans ⚙ Accès, ou utilisez votre propre clé.'), { status: 503 });
  return { key: env[serviceKeyName], auth: auth };
}

/* Décompte : 1 requête réussie = 1 unité (les erreurs ne comptent pas).
   Le mode « clé personnelle » ne touche jamais au quota. */
async function finishIaResponse(env, upstream, resolved, cors, defaultCt) {
  const headers = { 'content-type': upstream.headers.get('content-type') || defaultCt, ...cors };
  if (resolved.auth) {
    if (upstream.ok) {
      resolved.auth.rec.remaining -= 1;
      await putCode(env, resolved.auth.code, resolved.auth.rec);
    }
    headers['x-quota-restant'] = String(resolved.auth.rec.remaining);
  }
  return new Response(upstream.body, { status: upstream.status, headers });
}

async function proxyOllama(request, env, cors) {
  let resolved;
  try { resolved = await resolveIaAuth(request, env, 'x-ollama-key', 'OLLAMA_API_KEY'); }
  catch (e) { return json({ error: { type: 'auth', message: e.message } }, e.status || 401, cors); }

  let body;
  try { body = await request.json(); }
  catch (e) { return json({ error: { type: 'invalid_request', message: 'Corps JSON invalide' } }, 400, cors); }

  /* N'importe quel modèle Cloud du compte : validation de forme seulement.
     Un modèle inconnu produit un « model not found » propre côté Ollama. */
  if (typeof body.model !== 'string' || !/^[\w.\/:-]{1,80}$/.test(body.model)) body.model = DEFAULT_OLLAMA_MODEL;
  body.options = body.options || {};
  if (!body.options.num_predict || body.options.num_predict > NUM_PREDICT_CAP) body.options.num_predict = NUM_PREDICT_CAP;

  const upstream = await fetch(OLLAMA_CHAT_URL, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      accept: 'application/x-ndjson',
      authorization: 'Bearer ' + resolved.key,
    },
    body: JSON.stringify(body),
  });
  return await finishIaResponse(env, upstream, resolved, cors, 'application/x-ndjson');
}

/* Liste des modèles Ollama Cloud du compte — permet au dashboard d'afficher
   automatiquement les modèles ajoutés/mis à jour chez Ollama, sans redéployer.
   Métadonnées non sensibles : tout code d'accès valide suffit (y compris les
   codes dhis2_only), aucune requête n'est décomptée ; une clé personnelle
   x-ollama-key liste les modèles de SON compte. */
async function ollamaTags(request, env, cors) {
  const ownKey = (request.headers.get('x-ollama-key') || '').trim();
  let key = ownKey;
  if (!key) {
    try { await requireCode(request, env); }
    catch (e) { return json({ error: { type: 'auth', message: e.message } }, e.status || 401, cors); }
    if (!env.OLLAMA_API_KEY) return json({ error: { type: 'config', message: 'OLLAMA_API_KEY absent du proxy.' } }, 503, cors);
    key = env.OLLAMA_API_KEY;
  }
  const upstream = await fetch(OLLAMA_TAGS_URL, { headers: { authorization: 'Bearer ' + key, accept: 'application/json' } });
  const text = await upstream.text();
  return new Response(text, {
    status: upstream.status,
    headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'public, max-age=600', ...cors },
  });
}

/* Proxy Kimi (Moonshot AI) — API compatible OpenAI (SSE). Même logique que les
   autres fournisseurs : code d'accès (quota) OU clé personnelle x-kimi-key. */
async function proxyKimi(request, env, cors) {
  let resolved;
  try { resolved = await resolveIaAuth(request, env, 'x-kimi-key', 'KIMI_API_KEY'); }
  catch (e) { return json({ error: { type: 'auth', message: e.message } }, e.status || 401, cors); }

  let body;
  try { body = await request.json(); }
  catch (e) { return json({ error: { type: 'invalid_request', message: 'Corps JSON invalide' } }, 400, cors); }

  if (typeof body.model !== 'string' || !/^[\w.\/:-]{1,80}$/.test(body.model)) body.model = DEFAULT_KIMI_MODEL;
  if (body.max_tokens && body.max_tokens > MAX_TOKENS_CAP) body.max_tokens = MAX_TOKENS_CAP;

  const upstream = await fetch((env.KIMI_API_BASE || KIMI_DEFAULT_BASE).replace(/\/+$/, '') + '/v1/chat/completions', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      accept: 'text/event-stream',
      authorization: 'Bearer ' + resolved.key,
    },
    body: JSON.stringify(body),
  });
  return await finishIaResponse(env, upstream, resolved, cors, 'text/event-stream');
}

async function proxyAnthropic(request, env, cors) {
  let resolved;
  try { resolved = await resolveIaAuth(request, env, 'x-anthropic-key', 'ANTHROPIC_API_KEY'); }
  catch (e) { return json({ error: { type: 'auth', message: e.message } }, e.status || 401, cors); }

  let body;
  try { body = await request.json(); }
  catch (e) { return json({ error: { type: 'invalid_request', message: 'Corps JSON invalide' } }, 400, cors); }

  if (!ALLOWED_ANTHROPIC_MODELS.includes(body.model)) body.model = ALLOWED_ANTHROPIC_MODELS[0];
  if (!body.max_tokens || body.max_tokens > MAX_TOKENS_CAP) body.max_tokens = MAX_TOKENS_CAP;

  const upstream = await fetch(ANTHROPIC_URL, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'anthropic-version': request.headers.get('anthropic-version') || '2023-06-01',
      'x-api-key': resolved.key,
    },
    body: JSON.stringify(body),
  });
  return await finishIaResponse(env, upstream, resolved, cors, 'application/json');
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
  const expire = !!rec.expires && Date.now() > Date.parse(rec.expires);
  return json({ valide: true, total: rec.total, restant: rec.remaining, essai: !!rec.trial, expire: rec.expires || null, expire_passé: expire }, 200, cors);
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

/* Mini-console web protégée : file des commandes (approbation → capture →
   livraison) + création manuelle de codes. GET → login ; POST → actions.
   Le token est re-signé à chaque action (champ caché), rien n'est stocké. */
async function adminPage(request, env) {
  if (request.method !== 'POST') return adminLogin();
  const f = await request.formData().catch(() => null);
  const tok = f ? String(f.get('token') || '') : '';
  if (!env.ADMIN_TOKEN || tok !== env.ADMIN_TOKEN) return adminLogin('Token invalide.');
  const action = String(f.get('action') || 'dashboard');
  let flash = '';

  if (action === 'creer') {
    const requests = Math.max(1, Math.min(100000, Math.floor(Number(f.get('requests')) || 0)));
    const note = String(f.get('note') || '').slice(0, 120);
    const code = genCode();
    await putCode(env, code, { total: requests, remaining: requests, created: new Date().toISOString(), tx: note || 'manuel' });
    flash = `<div class="okbox">✅ Code créé : <b style="font-size:20px;letter-spacing:1px">${code}</b><br>${requests} requêtes${note ? ' — ' + esc(note) : ''}</div>`;
  }
  if (action === 'approuver') {
    const o = await getOrder(env, String(f.get('id') || ''));
    if (o && o.status === 'pending') {
      o.status = 'approved'; o.approvedAt = new Date().toISOString();
      await putOrder(env, o);
      flash = `<div class="okbox">✅ Commande ${o.id} approuvée — le client voit maintenant où déposer.</div>`;
      await sendMail(env, o.email, o.nom, '✅ Commande validée — instructions de paiement (' + o.label + ')',
        `<p>Bonjour ${esc(o.nom)},</p><p>Votre commande <b>${esc(o.label)}</b> (${o.requests} requêtes IA) est <b>validée</b>.</p>
         <p><b>Déposez ${o.amount} $</b> (mobile money) ici :</p>
         <p style="background:#f0f2fa;border-radius:10px;padding:12px;font-weight:700">${esc(env.MPESA_INFOS || '')}</p>
         <p>Puis envoyez la capture sur votre page de suivi — votre code s'y affichera aussitôt après vérification.</p>`);
    }
  }
  if (action === 'livrer') {
    const o = await getOrder(env, String(f.get('id') || ''));
    if (o && o.status === 'proof') {
      /* Requêtes ajustées au montant RÉELLEMENT payé (lu par l'IA sur la
         capture) : ex. client a payé 15 $ pour une offre 10 $ → 150 requêtes. */
      const finalReq = o.aiAmount >= 1 ? Math.round(o.aiAmount * REQUESTS_PER_USD) : o.requests;
      const finalAmount = o.aiAmount >= 1 ? o.aiAmount : o.amount;
      const code = genCode();
      await putCode(env, code, { total: finalReq, remaining: finalReq, created: new Date().toISOString(), tx: o.id + ' — ' + o.nom });
      o.status = 'delivered'; o.code = code; o.deliveredAt = new Date().toISOString();
      await putOrder(env, o);
      /* Registre PERMANENT des ventes (sans expiration) pour le rapport */
      await env.CODES.put('sale:' + o.id, JSON.stringify({ deliveredAt: o.deliveredAt, nom: o.nom, email: o.email, label: o.label, requests: finalReq, amount: finalAmount, code }));
      flash = `<div class="okbox">🎫 Code <b style="font-size:20px;letter-spacing:1px">${code}</b> (${finalReq} requêtes) livré à ${esc(o.nom)} — il s'affiche <b>déjà</b> sur sa page${env.MAIL_FROM ? ' et part par e-mail' : ''}.</div>`;
      await sendMail(env, o.email, o.nom, "🎫 Votre code d'accès — Assistant IA Dashboard PEV",
        `<p>Bonjour ${esc(o.nom)},</p><p>Paiement confirmé — merci ! Voici votre code d'accès (<b>${finalReq} requêtes IA</b>) :</p>
         <p style="font-size:24px;font-weight:800;letter-spacing:2px;background:#f0f2fa;border-radius:12px;padding:16px;text-align:center">${code}</p>
         <p>Collez-le dans l'onglet « Génération des analyses » → ⚙ Accès → Code d'accès.<br>Conservez-le précieusement.</p>`);
    }
  }
  return adminDashboard(env, tok, flash);
}

function adminLogin(err) {
  return htmlPage('Console admin',
    `<h1>🔐 Console admin</h1>${err ? `<div class="err">${esc(err)}</div>` : ''}
     <form method="POST" action="/admin">
       <input type="password" name="token" required placeholder="Token admin" style="width:100%;box-sizing:border-box;margin:8px 0;padding:11px;border:1px solid #dfe2ee;border-radius:10px;font-size:14px">
       <button type="submit">Entrer</button>
     </form>`);
}

async function adminDashboard(env, tok, flash) {
  const list = await env.CODES.list({ prefix: 'ord:', limit: 200 });
  const orders = [];
  for (const k of list.keys) {
    const v = await env.CODES.get(k.name);
    if (v) { const o = JSON.parse(v); if (o.status !== 'delivered') orders.push(o); }
  }
  orders.sort((a, b) => String(b.created || '').localeCompare(String(a.created || '')));
  const badge = { pending: '🕐 À APPROUVER', approved: '💵 Attente capture', proof: '🧾 PREUVE REÇUE — à vérifier' };
  const cards = orders.length ? orders.map(o => {
    let actions = '';
    if (o.status === 'pending') {
      actions = `<form method="POST" action="/admin" style="margin:8px 0 0"><input type="hidden" name="token" value="${esc(tok)}"><input type="hidden" name="action" value="approuver"><input type="hidden" name="id" value="${o.id}"><button type="submit" style="background:#2e7d32">✅ Approuver</button></form>`;
    } else if (o.status === 'approved') {
      actions = `<small>⏳ En attente de la capture du client…</small>`;
    } else if (o.status === 'proof') {
      const finalReq = o.aiAmount >= 1 ? Math.round(o.aiAmount * REQUESTS_PER_USD) : o.requests;
      actions = `<div style="display:flex;gap:8px;margin-top:8px"><a href="/admin/preuve?id=${o.id}&token=${encodeURIComponent(tok)}" target="_blank" rel="noopener" style="flex:1;text-align:center;background:#5c6bc0;color:#fff;border-radius:10px;padding:12px;font-weight:700;text-decoration:none;font-size:14px">🖼️ Capture</a><form method="POST" action="/admin" style="flex:1;margin:0"><input type="hidden" name="token" value="${esc(tok)}"><input type="hidden" name="action" value="livrer"><input type="hidden" name="id" value="${o.id}"><button type="submit" style="background:#2e7d32">🎫 Livrer (${finalReq} req.)</button></form></div>`;
    }
    const aiBox = o.aiNote ? `<div style="background:#eef6ff;border-left:3px solid #0b5e8e;border-radius:8px;padding:8px 10px;margin-top:8px;font-size:12.5px;color:#123">🤖 ${esc(o.aiNote)}</div>` : '';
    return `<div class="offer" style="cursor:default;display:block">
      <div style="display:flex;justify-content:space-between;align-items:center"><b>${esc(o.nom)}</b><span class="price">${o.amount} $</span></div>
      <small>${esc(o.email)} · ${esc(o.label)} · <b>${o.requests} requêtes</b><br>${new Date(o.created).toLocaleString('fr-FR')} · ${o.id}</small>
      <div style="margin-top:6px;font-size:12px;font-weight:700;color:#7a4a08">${badge[o.status] || o.status}</div>
      ${aiBox}${actions}</div>`;
  }).join('') : '<p style="text-align:center;color:#888">Aucune commande en attente. 👍</p>';
  const warnMpsa = env.MPESA_INFOS ? '' : `<div class="err">⚠️ Secret <b>MPESA_INFOS</b> non configuré : vos clients ne verront pas où déposer. Voir JOUR-J-COMMANDES.md.</div>`;
  const inp = 'width:100%;box-sizing:border-box;margin:8px 0;padding:11px;border:1px solid #dfe2ee;border-radius:10px;font-size:14px';
  return htmlPage('Console admin',
    `<h1>🔐 Console admin</h1>
     <style>.okbox{background:#e8f5e9;color:#1b5e20;border-radius:10px;padding:12px;font-size:13.5px;margin:10px 0}</style>
     ${flash}${warnMpsa}
     <div style="display:flex;justify-content:space-between;align-items:center;margin:16px 0 4px">
       <h2 style="font-size:16px;color:#1a237e;margin:0">📦 Commandes en cours (${orders.length})</h2>
       <a href="/admin/rapport?token=${encodeURIComponent(tok)}" style="font-size:12.5px;font-weight:700">📊 Rapport des ventes →</a>
     </div>
     ${cards}
     <h2 style="font-size:16px;color:#1a237e;margin:18px 0 4px">➕ Créer un code manuellement</h2>
     <form method="POST" action="/admin">
       <input type="hidden" name="token" value="${esc(tok)}"><input type="hidden" name="action" value="creer">
       <input type="number" name="requests" required min="1" max="100000" placeholder="Nombre de requêtes (ex. 100)" style="${inp}">
       <input type="text" name="note" placeholder="Note (client, offre, paiement…)" style="${inp}">
       <button type="submit">Générer le code</button>
     </form>
     <small>Repères : 5 $ = 50 requêtes · 10 $ = 100 · 20 $ = 200 · 30 $ = 300.</small>`);
}

/* Image de preuve (réservée à l'admin, via token dans l'URL signée à la volée) */
async function adminProofImage(env, url) {
  const tok = url.searchParams.get('token') || '';
  if (!env.ADMIN_TOKEN || tok !== env.ADMIN_TOKEN) return new Response('Non autorisé', { status: 401 });
  const v = await env.CODES.get('proof:' + (url.searchParams.get('id') || ''));
  if (!v) return new Response('Introuvable', { status: 404 });
  const p = JSON.parse(v);
  const bin = Uint8Array.from(atob(p.b64), c => c.charCodeAt(0));
  return new Response(bin, { headers: { 'content-type': p.ct || 'image/png', 'cache-control': 'private, max-age=60' } });
}

/* Rapport des ventes : registre permanent (sale:*) — tableau + totaux */
async function adminReport(env, url) {
  const tok = url.searchParams.get('token') || '';
  if (!env.ADMIN_TOKEN || tok !== env.ADMIN_TOKEN) return adminLogin('Token manquant ou invalide.');
  const list = await env.CODES.list({ prefix: 'sale:', limit: 1000 });
  const rows = [];
  for (const k of list.keys) {
    const v = await env.CODES.get(k.name);
    if (v) rows.push(JSON.parse(v));
  }
  rows.sort((a, b) => String(b.deliveredAt || '').localeCompare(String(a.deliveredAt || '')));
  const now = Date.now(), day = 864e5;
  const sum = arr => arr.reduce((s, o) => s + (o.amount || 0), 0);
  const t24 = sum(rows.filter(o => now - Date.parse(o.deliveredAt || 0) < day));
  const t7 = sum(rows.filter(o => now - Date.parse(o.deliveredAt || 0) < 7 * day));
  const t30 = sum(rows.filter(o => now - Date.parse(o.deliveredAt || 0) < 30 * day));
  const trs = rows.map(o => `<tr style="border-bottom:1px solid #eef0f7">
    <td style="padding:8px 6px">${new Date(o.deliveredAt).toLocaleString('fr-FR')}</td>
    <td style="padding:8px 6px">${esc(o.nom)}<br><small>${esc(o.email)}</small></td>
    <td style="padding:8px 6px">${esc(o.label)}</td>
    <td style="padding:8px 6px;text-align:right">${o.requests}</td>
    <td style="padding:8px 6px;text-align:right"><b>${o.amount} $</b></td>
    <td style="padding:8px 6px"><small>${o.code || ''}</small></td></tr>`).join('');
  return htmlPage('Rapport des ventes',
    `<style>.card{max-width:780px}table{width:100%;border-collapse:collapse;font-size:12.5px}</style>
     <h1>📊 Rapport des ventes</h1>
     <div style="display:flex;gap:8px;flex-wrap:wrap;margin:12px 0">
       ${[['Dernières 24 h', t24], ['7 jours', t7], ['30 jours', t30], ['Total (registre)', sum(rows)]].map(([l, v]) =>
         `<div style="flex:1;min-width:110px;background:#f0f2fa;border-radius:12px;padding:12px;text-align:center"><div style="font-size:20px;font-weight:800;color:#1a237e">${v} $</div><small>${l}</small></div>`).join('')}
     </div>
     <p><b>${rows.length}</b> vente(s) livrée(s).</p>
     ${rows.length ? `<div style="overflow-x:auto"><table>
       <tr style="text-align:left;color:#1a237e;border-bottom:2px solid #dfe2ee"><th style="padding:8px 6px">Livré le</th><th style="padding:8px 6px">Client</th><th style="padding:8px 6px">Offre</th><th style="padding:8px 6px">Req.</th><th style="padding:8px 6px">Montant</th><th style="padding:8px 6px">Code</th></tr>
       ${trs}</table></div>` : '<p style="text-align:center;color:#888">Aucune vente livrée pour le moment.</p>'}
     <p style="margin-top:14px"><a href="/admin">← Console admin</a></p>`);
}

/* Rapport quotidien automatique (cron Cloudflare, 18 h Kinshasa) :
   récapitulatif des ventes du jour → e-mail et/ou notification téléphone. */
async function dailyReport(env) {
  const list = await env.CODES.list({ prefix: 'sale:', limit: 1000 });
  const today = new Date(Date.now() + 3600e3).toISOString().slice(0, 10); /* jour Kinshasa (UTC+1) */
  const sales = [];
  for (const k of list.keys) {
    const v = await env.CODES.get(k.name);
    if (!v) continue;
    const o = JSON.parse(v);
    if (new Date(Date.parse(o.deliveredAt || 0) + 3600e3).toISOString().slice(0, 10) === today) sales.push(o);
  }
  if (!sales.length) return; /* rien à signaler : silence */
  const total = sales.reduce((s, o) => s + (o.amount || 0), 0);
  const lines = sales.map(o => `• ${o.nom} — ${o.label} — ${o.amount} $`).join('\n');
  await notifyAdmin(env, `📊 Rapport PEV du ${today} : ${sales.length} vente(s), total ${total} $\n${lines}`);
  if (env.MAIL_TO_ADMIN) {
    const trs = sales.map(o => `<tr><td style="padding:6px">${esc(o.nom)}</td><td style="padding:6px">${esc(o.label)}</td><td style="padding:6px">${o.requests}</td><td style="padding:6px"><b>${o.amount} $</b></td><td style="padding:6px"><small>${o.code || ''}</small></td></tr>`).join('');
    await sendMail(env, env.MAIL_TO_ADMIN, '', `📊 Rapport des ventes du ${today} — ${total} $`,
      `<h3>Ventes du jour : ${sales.length} — total ${total} $</h3>
       <table style="border-collapse:collapse;font-size:13px"><tr style="color:#1a237e"><th>Client</th><th>Offre</th><th>Req.</th><th>Montant</th><th>Code</th></tr>${trs}</table>
       <p><a href="https://pev-ia-proxy.pev-rdc.workers.dev/admin">Console admin</a></p>`);
  }
}

/* ═══ 5. Vente de codes : commande en ligne / WhatsApp / CinetPay ═══ */
/* Mode actif : PAYMENT_PROVIDER (env) sinon auto —
   CinetPay si ses clés sont posées, à défaut la commande en ligne intégrée
   (validation manuelle, sans aucune configuration requise). */
function payProvider(env) {
  const forced = (env.PAYMENT_PROVIDER || '').trim().toLowerCase();
  if (forced === 'cinetpay' || forced === 'whatsapp' || forced === 'commande') return forced;
  if (env.CINETPAY_APIKEY && env.CINETPAY_SITE_ID) return 'cinetpay';
  return 'commande';
}
const esc = s => String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
function htmlPage(title, body) {
  return new Response(`<!doctype html><html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>${title}</title>
<style>
:root{--navy:#1a237e;--accent:#3949ab}
*{box-sizing:border-box}
body{font-family:system-ui,'Segoe UI',Roboto,sans-serif;background:linear-gradient(160deg,#edf0f8,#e2e7f4);margin:0;display:flex;min-height:100vh;align-items:center;justify-content:center;padding:24px 0}
.card{background:#fff;border-radius:18px;box-shadow:0 12px 44px rgba(26,35,126,.15);max-width:480px;width:94%;overflow:hidden}
.brand{background:linear-gradient(90deg,var(--navy),var(--accent));color:#fff;padding:13px 22px;font-size:13px;font-weight:700;letter-spacing:.4px;display:flex;align-items:center;gap:8px}
.brand small{font-weight:400;opacity:.75;margin-left:auto}
.inner{padding:24px 26px 26px}
h1{color:var(--navy);font-size:20px;margin:0 0 8px}
p{color:#555;font-size:14px;line-height:1.55}
.offer{display:flex;justify-content:space-between;align-items:center;border:1.5px solid #dfe2ee;border-radius:12px;padding:14px 16px;margin:10px 0;cursor:pointer;transition:all .15s}
.offer:hover{border-color:var(--navy);background:#f4f6fd;transform:translateY(-1px);box-shadow:0 4px 14px rgba(26,35,126,.09)}
.offer b{color:var(--navy)}
.price{font-weight:800;color:#d97757;font-size:17px}
.code{font-size:26px;font-weight:800;color:var(--navy);letter-spacing:2px;text-align:center;background:#f0f2fa;border-radius:12px;padding:18px;margin:16px 0;word-break:break-all}
.err{background:#fdecea;color:#a12622;border-radius:10px;padding:12px;font-size:13px;margin:8px 0}
button{background:var(--navy);color:#fff;border:none;border-radius:10px;padding:12px 18px;font-size:14px;font-weight:700;cursor:pointer;width:100%;transition:filter .15s}
button:hover{filter:brightness(1.18)}
small{color:#999}
label{font-size:13px;color:#333}
a{color:var(--navy)}
</style></head><body>
<div class="card"><div class="brand">🩺 Assistant IA — Dashboard PEV<small>DHIS2 · RDC</small></div><div class="inner">${body}</div></div></body></html>`,
    { headers: { 'content-type': 'text/html; charset=utf-8' } });
}

function buyPage(env, url) {
  const provider = payProvider(env);
  if (provider === 'whatsapp') return whatsappBuyPage(env);
  if (provider === 'commande') return orderOffersPage(env);
  if (provider !== 'cinetpay') {
    return htmlPage('Paiement bientôt disponible',
      `<h1>💳 Codes d'accès — bientôt disponibles</h1>
       <p>La vente de codes d'accès (paiement mobile money : M-Pesa, Orange Money, Airtel Money) est en cours d'activation.
       Revenez bientôt, ou utilisez votre propre clé API (Ollama Cloud ou Anthropic) dans ⚙ Accès en attendant.</p>`);
  }
  const offers = OFFERS.map(o =>
    `<form method="POST" action="/acheter" style="margin:0"><input type="hidden" name="offer" value="${o.id}">
     <button type="submit" class="offer" style="background:#fff;color:#333;text-align:left">
       <span><b>${o.label}</b> — <b style="color:#1a237e">${o.analyses}</b><br><small>${o.requests} requêtes IA</small></span>
       <span class="price">${o.amount.toLocaleString('fr-FR')} $</span></button></form>`).join('');
  const custom =
    `<form method="POST" action="/acheter" style="margin:0"><input type="hidden" name="offer" value="C">
     <div class="offer" style="cursor:default;display:block">
       <b>Montant libre</b> — <b style="color:#1a237e">≈ 2 à 3 analyses par dollar</b><br><small>${REQUESTS_PER_USD} requêtes IA par dollar — payez le montant de votre choix</small>
       <div style="display:flex;gap:8px;align-items:center;margin-top:10px">
         <input type="number" name="montant" min="${CUSTOM_MIN_USD}" max="${CUSTOM_MAX_USD}" step="1" required placeholder="ex. 7"
           style="width:110px;padding:10px;border:1px solid #dfe2ee;border-radius:10px;font-size:15px"> <b>$</b>
         <button type="submit" style="width:auto;padding:10px 18px">Payer</button>
       </div>
     </div></form>`;
  return htmlPage("Obtenir un code d'accès",
    `<h1>🎫 Obtenir un code d'accès</h1>
     <p>Choisissez une offre ou entrez le montant de votre choix — paiement par <b>mobile money</b>
     (M-Pesa, Orange Money, Airtel Money) ou <b>carte bancaire</b>.
     Votre code s'affiche immédiatement après le paiement.</p>${offers}${custom}
      <small>Assistant IA du Dashboard PEV de routine — RDC.</small>`);
}

/* ── Mode WhatsApp : chaque offre ouvre une conversation (numéro dédié) avec un
      message pré-rempli. Paiement mobile money manuel → le vendeur crée le code
      via /admin et l'envoie dans la conversation. ── */
function waLink(num, text) {
  return 'https://wa.me/' + num + '?text=' + encodeURIComponent(text);
}
const WA_BTN = 'display:inline-block;margin-top:6px;background:#25d366;color:#fff;font-weight:700;font-size:12px;border-radius:8px;padding:7px 12px;text-decoration:none;border:none;cursor:pointer';
function whatsappBuyPage(env) {
  const num = String(env.WHATSAPP_NUMBER || '').replace(/[^0-9]/g, '');
  const offers = OFFERS.map(o =>
    `<div class="offer" style="cursor:default">
      <span><b>${o.label}</b> — <b style="color:#1a237e">${o.analyses}</b><br><small>${o.requests} requêtes IA</small></span>
      <span style="text-align:right;white-space:nowrap"><span class="price">${o.amount.toLocaleString('fr-FR')} $</span><br>
      <a href="${waLink(num, `Bonjour, je souhaite acheter l’offre ${o.label} (${o.analyses} — ${o.amount} $) pour l’assistant IA du Dashboard PEV.`)}" target="_blank" rel="noopener" style="${WA_BTN}">💬 Commander</a></span></div>`).join('');
  const custom =
    `<div class="offer" style="cursor:default;display:block">
      <b>Montant libre</b> — <b style="color:#1a237e">≈ 2 à 3 analyses par dollar</b><br><small>${REQUESTS_PER_USD} requêtes IA par dollar — choisissez le montant de votre choix</small>
      <div style="display:flex;gap:8px;align-items:center;margin-top:10px">
        <input type="number" id="mt" min="${CUSTOM_MIN_USD}" max="${CUSTOM_MAX_USD}" step="1" placeholder="ex. 7"
          style="width:110px;padding:10px;border:1px solid #dfe2ee;border-radius:10px;font-size:15px"> <b>$</b>
        <button onclick="var v=Math.floor(Number(document.getElementById('mt').value));if(!(v>=${CUSTOM_MIN_USD}&&v<=${CUSTOM_MAX_USD})){alert('Montant entre ${CUSTOM_MIN_USD} et ${CUSTOM_MAX_USD} $');return false;}window.open('https://wa.me/${num}?text='+encodeURIComponent('Bonjour, je souhaite acheter un code personnalisé de '+v+' $ ('+(v*${REQUESTS_PER_USD})+' requêtes IA) pour l’assistant IA du Dashboard PEV.'),'_blank');return false;"
          style="${WA_BTN}margin-top:0;padding:10px 18px;font-size:14px">💬 Commander</button>
      </div>
    </div>`;
  return htmlPage("Obtenir un code d'accès",
    `<h1>🎫 Obtenir un code d'accès</h1>
     <p>Choisissez une offre : <b>WhatsApp s'ouvre avec un message prêt à envoyer</b>.
     Vous payez par <b>mobile money</b> (M-Pesa, Orange Money, Airtel Money) et votre
     code vous est envoyé dans la conversation dès réception du paiement.</p>
     ${offers}${custom}
     <div style="background:#f0f2fa;border-radius:10px;padding:12px;margin:14px 0;font-size:12.5px;color:#444;line-height:1.5">
       <b>Comment ça marche ?</b><br>
       1. Cliquez « 💬 Commander » → envoyez le message WhatsApp pré-rempli.<br>
       2. Le numéro mobile money à créditer vous est confirmé dans la conversation.<br>
       3. Payez, puis envoyez la capture du paiement.<br>
       4. Vous recevez immédiatement votre code — à coller dans l'onglet
       « Génération des analyses » → ⚙ Accès → Code d'accès.</div>
     <small>Assistant IA du Dashboard PEV de routine — RDC.</small>`);
}

/* ═══ 5b. Commande en ligne intégrée (validation manuelle, sans WhatsApp) ═══
   Flux : le client commande (nom + email + offre, tous OBLIGATOIRES) → il suit
   sa commande en direct (barre de progression) → vous APPROUVEZ dans /admin →
   il voit le numéro de dépôt (MPESA_INFOS) et envoie sa capture → vous LIVREZ
   dans /admin → le code s'affiche aussitôt sur sa page (+ e-mail si configuré).
   KV : ord:<ID> {…,status} (30 j) · proof:<ID> {ct,b64} (30 j) */

const ORDER_TTL = 30 * 86400;
const MAX_PENDING_PER_DEVICE = 3;
function genOrderId() {
  const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  return 'CMD-' + Array.from(crypto.getRandomValues(new Uint8Array(6))).map(b => A[b % A.length]).join('');
}
const randKey = () => Array.from(crypto.getRandomValues(new Uint8Array(12))).map(b => b.toString(16).padStart(2, '0')).join('');
async function getOrder(env, id) { if (!/^CMD-[A-Z0-9]{6}$/.test(id || '')) return null; const v = await env.CODES.get('ord:' + id); return v ? JSON.parse(v) : null; }
async function putOrder(env, o) { await env.CODES.put('ord:' + o.id, JSON.stringify(o), { expirationTtl: ORDER_TTL }); }

/* Page /acheter (mode commande) : les offres mènent au formulaire /commander */
function orderOffersPage(env) {
  const offers = OFFERS.map(o =>
    `<a href="/commander?offre=${o.id}" class="offer" style="text-decoration:none;color:#333">
      <span><b>${o.label}</b> — <b style="color:#1a237e">${o.analyses}</b><br><small>${o.requests} requêtes IA</small></span>
      <span class="price">${o.amount.toLocaleString('fr-FR')} $</span></a>`).join('');
  const custom =
    `<div class="offer" style="cursor:default;display:block">
      <b>Montant libre</b> — <b style="color:#1a237e">≈ 2 à 3 analyses par dollar</b><br><small>${REQUESTS_PER_USD} requêtes IA par dollar — choisissez le montant de votre choix</small>
      <div style="display:flex;gap:8px;align-items:center;margin-top:10px">
        <input type="number" id="mt" min="${CUSTOM_MIN_USD}" max="${CUSTOM_MAX_USD}" step="1" placeholder="ex. 7"
          style="width:110px;padding:10px;border:1px solid #dfe2ee;border-radius:10px;font-size:15px"> <b>$</b>
        <button onclick="var v=Math.floor(Number(document.getElementById('mt').value));if(!(v>=${CUSTOM_MIN_USD}&&v<=${CUSTOM_MAX_USD})){alert('Montant entre ${CUSTOM_MIN_USD} et ${CUSTOM_MAX_USD} $');return false;}location.href='/commander?offre=C&montant='+v;return false;"
          style="width:auto;padding:10px 18px">Commander</button>
      </div>
    </div>`;
  return htmlPage("Obtenir un code d'accès",
    `<h1>🎫 Obtenir un code d'accès</h1>
     <p>Choisissez une offre : vous laisserez votre <b>nom</b> et votre <b>e-mail</b>, puis vous
     paierez par <b>mobile money</b> (M-Pesa, Orange Money, Airtel Money) au numéro indiqué après
     validation. <b>Votre code s'affichera instantanément</b> sur votre page de suivi
     (et vous sera aussi envoyé par e-mail).</p>
     ${offers}${custom}
     <small>Assistant IA du Dashboard PEV de routine — RDC.</small>`);
}

/* Formulaire de commande (GET) + création (POST) — champs tous obligatoires */
async function orderPage(request, env, url) {
  const inp = 'width:100%;box-sizing:border-box;margin:6px 0 12px;padding:11px;border:1px solid #dfe2ee;border-radius:10px;font-size:14px';
  if (request.method === 'GET') {
    const pre = (url.searchParams.get('offre') || 'M').toUpperCase();
    const preM = url.searchParams.get('montant') || '';
    const opts = OFFERS.map(o =>
      `<option value="${o.id}"${o.id === pre ? ' selected' : ''}>${o.label} — ${o.analyses} — ${o.amount} $</option>`).join('') +
      `<option value="C"${pre === 'C' ? ' selected' : ''}>Montant libre (10 requêtes / $)</option>`;
    return htmlPage('Commander un code',
      `<h1>📝 Votre commande</h1>
       <form method="POST" action="/commander">
         <label><b>Offre *</b></label>
         <select name="offre" id="of" required style="${inp};background:#fff" onchange="document.getElementById('mtwrap').style.display=this.value==='C'?'block':'none'">${opts}</select>
         <div id="mtwrap" style="display:${pre === 'C' ? 'block' : 'none'}">
           <label><b>Montant (USD) *</b> <small>— entre ${CUSTOM_MIN_USD} et ${CUSTOM_MAX_USD} $</small></label>
           <input type="number" name="montant" id="mt" min="${CUSTOM_MIN_USD}" max="${CUSTOM_MAX_USD}" step="1" value="${preM}" placeholder="ex. 15" style="${inp}">
         </div>
         <label><b>Nom complet *</b></label>
         <input type="text" name="nom" required minlength="2" maxlength="80" placeholder="ex. Dr Jean Ilunga" style="${inp}">
         <label><b>Votre e-mail *</b> <small>— le code vous y sera envoyé</small></label>
         <input type="email" name="email" required maxlength="120" placeholder="ex. jean@gmail.com" style="${inp}">
         <button type="submit">✅ Envoyer ma commande</button>
       </form>
       <small>* champs obligatoires. Après envoi, une page de suivi s'ouvre : gardez-la ouverte,
       elle se met à jour toute seule à chaque étape.</small>
       <script>var s=document.getElementById('of'),m=document.getElementById('mt');function t(){var c=s.value==='C';document.getElementById('mtwrap').style.display=c?'block':'none';m.required=c;}s.onchange=t;t();<\/script>`);
  }
  if (request.method !== 'POST') return json({ error: 'GET/POST uniquement' }, 405);
  const form = await request.formData().catch(() => null);
  if (!form) return htmlPage('Erreur', `<div class="err">Formulaire illisible.</div>`);
  const nom = String(form.get('nom') || '').trim().slice(0, 80);
  const email = String(form.get('email') || '').trim().slice(0, 120);
  if (nom.length < 2 || !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) {
    return htmlPage('Champs invalides',
      `<h1>Vérifiez vos informations</h1><div class="err">Nom complet (min. 2 caractères) et e-mail valide sont <b>obligatoires</b>.</div>
       <p style="margin-top:14px"><a href="javascript:history.back()">← Corriger</a></p>`);
  }
  let offer;
  if (String(form.get('offre')).toUpperCase() === 'C') {
    const usd = Math.floor(Number(form.get('montant')));
    if (!(usd >= CUSTOM_MIN_USD && usd <= CUSTOM_MAX_USD)) {
      return htmlPage('Montant invalide',
        `<h1>Montant invalide</h1><div class="err">Entrez un montant entier entre ${CUSTOM_MIN_USD} $ et ${CUSTOM_MAX_USD} $.</div>
         <p style="margin-top:14px"><a href="javascript:history.back()">← Corriger</a></p>`);
    }
    offer = { id: 'C', label: `Personnalisé — ${usd} $`, requests: usd * REQUESTS_PER_USD, amount: usd, currency: 'USD' };
  } else {
    offer = OFFERS.find(o => o.id === String(form.get('offre')).toUpperCase()) || OFFERS[0];
  }
  /* Anti-spam : max MAX_PENDING_PER_DEVICE commandes non livrées par appareil */
  const fp = await fpHash(request);
  const list = await env.CODES.list({ prefix: 'ord:', limit: 200 });
  let pending = 0;
  for (const k of list.keys) {
    const v = await env.CODES.get(k.name);
    if (!v) continue;
    const o = JSON.parse(v);
    if (o.fp === fp && o.status !== 'delivered') pending++;
  }
  if (pending >= MAX_PENDING_PER_DEVICE) {
    return htmlPage('Trop de commandes en cours',
      `<h1>⏳ Patience…</h1><div class="err">Vous avez déjà ${pending} commandes en cours de traitement.
       Attendez leur livraison avant d'en passer une nouvelle.</div>`);
  }
  const o = {
    id: genOrderId(), key: randKey(), fp, nom, email,
    offer: offer.id, label: offer.label, requests: offer.requests, amount: offer.amount,
    status: 'pending', created: new Date().toISOString(),
  };
  /* Approbation INSTANTANÉE automatique (AUTO_APPROVE ≠ '0') : le client voit
     aussitôt où déposer — aucun code n'est délivré à cette étape, le vrai
     contrôle reste la livraison après vérification du paiement par le vendeur. */
  if ((env.AUTO_APPROVE || '1') !== '0') { o.status = 'approved'; o.approvedAt = o.created; }
  await putOrder(env, o);
  /* Alerte téléphone : résumé client + offre + lien console. ⚠️ Format testé
     contre le pare-feu CallMeBot : la référence entre parenthèses sur sa propre
     ligne y provoque un 403 — garder « Réf CMD-… » en ligne, sans parenthèses. */
  await notifyAdmin(env,
    `🛒 Nouvelle commande PEV — Réf ${o.id}\n👤 ${nom} (${email})\n🎫 ${o.label} — ${o.amount} $\n💵 En attente de sa capture. Console : https://pev-ia-proxy.pev-rdc.workers.dev/admin`);
  return Response.redirect(url.origin + '/suivre?id=' + o.id + '&key=' + o.key, 303);
}

/* Page de suivi client : interroge /api/commande toutes les 4 s et affiche
   l'étape en cours (validation → dépôt + preuve → code). */
function trackPage(env, url) {
  const id = url.searchParams.get('id') || '';
  const key = url.searchParams.get('key') || '';
  if (!/^CMD-[A-Z0-9]{6}$/.test(id) || !/^[0-9a-f]{24}$/.test(key)) return htmlPage('Erreur', `<div class="err">Lien de commande invalide.</div>`);
  return htmlPage('Suivi de commande',
    `<h1>📦 Suivi de votre commande</h1>
     <div class="barwrap" id="bar"><div class="bar"></div></div>
     <div id="zone"><p>Chargement…</p></div>
     <style>
       .barwrap{background:#e8ebf5;border-radius:8px;height:10px;overflow:hidden;margin:10px 0 16px}
       .bar{height:100%;width:40%;border-radius:8px;background:linear-gradient(90deg,#1a237e,#5c6bc0);animation:slide 1.2s infinite}
       @keyframes slide{0%{margin-left:-40%}100%{margin-left:100%}}
       .paybox{background:#f0f2fa;border:2px dashed #1a237e;border-radius:12px;padding:14px;font-size:16px;font-weight:700;color:#1a237e;text-align:center;margin:10px 0}
       .code{font-size:24px;font-weight:800;color:#1a237e;letter-spacing:2px;text-align:center;background:#f0f2fa;border-radius:12px;padding:16px;margin:12px 0;word-break:break-all}
       input[type=file]{width:100%;box-sizing:border-box;margin:8px 0;padding:10px;border:1px solid #dfe2ee;border-radius:10px;background:#fff}
       button{margin-top:8px}
     </style>
     <script>
       var ID=${JSON.stringify(id)}, KEY=${JSON.stringify(key)}, last='';
       try{localStorage.setItem('pev_cmd', JSON.stringify({id:ID,key:KEY}));}catch(e){}
       var zone=document.getElementById('zone'), bar=document.getElementById('bar');
       function render(j){
         if(!j||j.error){bar.style.display='none';zone.innerHTML='<div class="err">Commande introuvable ou lien expiré.</div>';return;}
         if(j.status===last)return; last=j.status;
         if(j.status==='pending'){
           bar.style.display='block';
           zone.innerHTML='<p><b>1. Commande envoyée ✓</b> ('+ j.label +')</p><p>⏳ <b>En attente de validation</b> — le vendeur a été <b>notifié instantanément</b> sur son téléphone et valide en général en quelques minutes. <b>Ne fermez pas cette page</b>, elle avance toute seule.</p>';
         }else if(j.status==='approved'){
           bar.style.display='none';
           zone.innerHTML='<p><b>1. Commande validée ✓</b></p><p><b>2. Déposez '+j.amount+' $</b> (mobile money) au numéro suivant :</p><div class="paybox">'+j.payment+'</div>'
             +'<p><b>3. Envoyez la capture du paiement</b> comme preuve :</p>'
             +'<form method="POST" action="/commande/preuve" enctype="multipart/form-data">'
             +'<input type="hidden" name="id" value="'+ID+'"><input type="hidden" name="key" value="'+KEY+'">'
             +'<input type="file" name="preuve" accept="image/*" required>'
             +'<button type="submit">📤 Soumettre la preuve</button></form>';
         }else if(j.status==='proof'){
           bar.style.display='block';
           zone.innerHTML='<p><b>Capture reçue ✓</b></p><p>⏳ <b>Vérification du paiement en cours</b> — votre code d\\'accès va apparaître ici automatiquement dès confirmation.</p>';
         }else if(j.status==='delivered'){
           bar.style.display='none';
           try{localStorage.removeItem('pev_cmd');}catch(e){}
           zone.innerHTML='<p><b>✅ Paiement confirmé — merci !</b></p><p>Voici votre code d\\'accès :</p><div class="code">'+j.code+'</div>'
             +'<button onclick="navigator.clipboard.writeText(\\''+j.code+'\\');this.textContent=\\'✓ Copié !\\'">📋 Copier le code</button>'
             +'<p style="margin-top:12px">Collez-le dans l\\'onglet « Génération des analyses » → ⚙ Accès → Code d\\'accès.'
              +(j.mail?' <b>Le même code vous a aussi été envoyé par e-mail</b> : vérifiez votre boîte de réception <b>et le dossier Spam / Courrier indésirable</b> — il reste de toute façon affiché ici sur votre écran.':'')+' Conservez-le précieusement : il ne sera plus affiché ailleurs.</p>';
         }
       }
       async function poll(){try{var r=await fetch('/api/commande?id='+ID+'&key='+KEY);render(await r.json());}catch(e){} if(last!=='delivered')setTimeout(poll,4000);}
       poll();
     <\/script>`);
}

/* État d'une commande (JSON) — protégé par la clé secrète de la commande */
async function apiOrder(request, env, url, cors) {
  const o = await getOrder(env, url.searchParams.get('id') || '');
  if (!o || o.key !== (url.searchParams.get('key') || '')) return json({ error: 'introuvable' }, 404, cors);
  const out = { status: o.status, label: o.label, amount: o.amount, requests: o.requests, mail: !!env.MAIL_FROM };
  if (o.status !== 'pending') out.payment = esc(env.MPESA_INFOS || 'Le vendeur vous indiquera le numéro de dépôt ici dans un instant — patientez ou contactez-le.');
  if (o.status === 'delivered') out.code = o.code;
  return json(out, 200, cors);
}

/* Réception de la capture de paiement (image ≤ 4 Mo) */
async function uploadProof(request, env) {
  const f = await request.formData().catch(() => null);
  if (!f) return htmlPage('Erreur', `<div class="err">Fichier illisible.</div>`);
  const id = String(f.get('id') || ''), key = String(f.get('key') || '');
  const o = await getOrder(env, id);
  if (!o || o.key !== key) return htmlPage('Erreur', `<div class="err">Commande introuvable.</div>`);
  if (o.status !== 'approved') return Response.redirect(new URL(request.url).origin + '/suivre?id=' + id + '&key=' + key, 303);
  const file = f.get('preuve');
  if (!file || typeof file.arrayBuffer !== 'function' || !file.size || file.size > 4 * 1024 * 1024 || !String(file.type || '').startsWith('image/')) {
    return htmlPage('Image invalide',
      `<h1>Image invalide</h1><div class="err">Envoyez une <b>image</b> (capture d'écran JPG/PNG) de moins de 4 Mo.</div>
       <p style="margin-top:14px"><a href="/suivre?id=${id}&key=${key}">← Réessayer</a></p>`);
  }
  const buf = new Uint8Array(await file.arrayBuffer());
  let bin = '';
  for (let i = 0; i < buf.length; i += 8192) bin += String.fromCharCode.apply(null, buf.subarray(i, i + 8192));
  const b64 = btoa(bin);
  await env.CODES.put('proof:' + id, JSON.stringify({ ct: file.type, b64 }), { expirationTtl: ORDER_TTL });
  o.status = 'proof'; o.proofAt = new Date().toISOString();
  /* L'assistant IA lit la capture et en extrait le récapitulatif (aide à la
     vérification — le vendeur garde le dernier clic). */
  const note = await aiProofSummary(env, b64, file.type);
  if (note) { o.aiNote = note; const amt = aiAmountFromNote(note); if (amt) o.aiAmount = amt; }
  await putOrder(env, o);
  await notifyAdmin(env,
    `🧾 Preuve reçue — Réf ${o.id}\n👤 ${o.nom} — ${o.amount} $\n👉 Vérifiez puis livrez le code : https://pev-ia-proxy.pev-rdc.workers.dev/admin`);
  return Response.redirect(new URL(request.url).origin + '/suivre?id=' + id + '&key=' + key, 303);
}

/* ═══ E-mail transactionnel ═══
   Deux prestataires, dans l'ordre :
   1. EMAILJS (emailjs.com, 200 e-mails/mois gratuits) — les e-mails partent
      DEPUIS le vrai compte Gmail du vendeur (connexion OAuth gérée par EmailJS)
      → authentifiés, donc livrés même chez Gmail. Secrets : EMAILJS_SERVICE_ID,
      EMAILJS_TEMPLATE_ID, EMAILJS_PUBLIC_KEY (+ EMAILJS_PRIVATE_KEY si le compte
      exige la clé privée pour les appels serveur). Modèle attendu : variables
      {{to_email}}, {{to_name}}, {{subject}} et {{{message_html}}} (HTML brut).
   2. BREVO (secours — 300/jour) si BREVO_API_KEY + MAIL_FROM sont posés.
      ⚠️ Un expéditeur @gmail.com via Brevo est jeté silencieusement par Gmail. */
async function sendMail(env, toEmail, toName, subject, html) {
  if (!toEmail) return false;
  if (env.EMAILJS_SERVICE_ID && env.EMAILJS_TEMPLATE_ID && env.EMAILJS_PUBLIC_KEY) {
    try {
      const body = {
        service_id: env.EMAILJS_SERVICE_ID, template_id: env.EMAILJS_TEMPLATE_ID,
        user_id: env.EMAILJS_PUBLIC_KEY,
        template_params: { to_email: toEmail, to_name: toName || toEmail, subject, message_html: html },
      };
      if (env.EMAILJS_PRIVATE_KEY) body.accessToken = env.EMAILJS_PRIVATE_KEY;
      const r = await fetch('https://api.emailjs.com/api/v1.0/email/send', {
        method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body),
      });
      const ok = r.status === 200;
      await env.CODES.put('notif:last-mail', JSON.stringify({ at: new Date().toISOString(), ch: 'emailjs', status: r.status, ok, to: toEmail, body: (await r.text()).slice(0, 200) }), { expirationTtl: 86400 });
      if (ok) return true; /* sinon : on bascule sur Brevo */
    } catch (e) {
      await env.CODES.put('notif:last-mail', JSON.stringify({ at: new Date().toISOString(), ch: 'emailjs', status: 0, ok: false, to: toEmail, body: String(e && e.message || e) }), { expirationTtl: 86400 });
    }
  }
  if (!env.BREVO_API_KEY || !env.MAIL_FROM) return false;
  try {
    const r = await fetch('https://api.brevo.com/v3/smtp/email', {
      method: 'POST',
      headers: { 'api-key': env.BREVO_API_KEY, 'content-type': 'application/json' },
      body: JSON.stringify({
        sender: { email: env.MAIL_FROM, name: 'Assistant PEV DHIS2' },
        /* Brevo rejette un « name » vide (400 missing_parameter) : ne l'envoyer
           que s'il est renseigné. */
        to: [Object.assign({ email: toEmail }, toName ? { name: toName } : {})],
        subject, htmlContent: html,
      }),
    });
    await env.CODES.put('notif:last-mail', JSON.stringify({ at: new Date().toISOString(), ch: 'brevo', status: r.status, ok: r.ok, to: toEmail, body: (await r.text()).slice(0, 200) }), { expirationTtl: 86400 });
    return r.ok;
  } catch (e) {
    await env.CODES.put('notif:last-mail', JSON.stringify({ at: new Date().toISOString(), ch: 'brevo', status: 0, ok: false, to: toEmail, body: String(e && e.message || e) }), { expirationTtl: 86400 });
    return false;
  }
}

/* ═══ Assistant IA de gestion des paiements ═══
   Lit la capture mobile money et résume : montant, opérateur, référence, date.
   Prestataire : AI_HELPER_PROVIDER = kimi (défaut, KIMI_API_KEY) | claude | ollama.
   Modèle      : AI_HELPER_MODEL (défauts : kimi-k3 · claude-opus-4-8
                 · minimax-m3:cloud).
   ⚠️ Ne VALIDE jamais un paiement : aide à la lecture seulement — le vendeur
   garde le clic « Livrer » (une capture peut toujours être falsifiée). */
async function aiProofSummary(env, b64, ct) {
  const provider = (env.AI_HELPER_PROVIDER || 'kimi').toLowerCase();
  const prompt = 'Cette image est une capture de paiement mobile money (M-Pesa, Orange Money ou Airtel Money, RDC). '
    + 'Lis-la et réponds en UNE ligne, format exact : Montant : <montant+devise> · Opérateur : <opérateur> · Réf : <référence> · Date : <date/heure>. '
    + 'Si une info est illisible écris « ? ». Si ce n\'est PAS une capture de paiement, réponds exactement « PAS_UN_PAIEMENT ».';
  const clean = t => {
    if (!t) return null;
    t = String(t).trim().slice(0, 250);
    return t.includes('PAS_UN_PAIEMENT') ? '⚠️ Cette image ne semble pas être une capture de paiement' : t;
  };
  try {
    if (provider === 'claude' && env.ANTHROPIC_API_KEY) {
      const r = await fetch(ANTHROPIC_URL, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'anthropic-version': '2023-06-01', 'x-api-key': env.ANTHROPIC_API_KEY },
        body: JSON.stringify({
          model: env.AI_HELPER_MODEL || 'claude-opus-4-8', max_tokens: 150,
          messages: [{ role: 'user', content: [
            { type: 'image', source: { type: 'base64', media_type: ct || 'image/png', data: b64 } },
            { type: 'text', text: prompt }] }],
        }),
      });
      const d = await r.json();
      return clean(d && d.content && d.content[0] && d.content[0].text);
    }
    if (provider === 'kimi' && env.KIMI_API_KEY) {
      const r = await fetch((env.KIMI_API_BASE || KIMI_DEFAULT_BASE).replace(/\/+$/, '') + '/v1/chat/completions', {
        method: 'POST',
        headers: { 'content-type': 'application/json', authorization: 'Bearer ' + env.KIMI_API_KEY },
        body: JSON.stringify({
          model: env.AI_HELPER_MODEL || 'kimi-k3', max_tokens: 150, reasoning_effort: 'low',
          messages: [{ role: 'user', content: [
            { type: 'image_url', image_url: { url: 'data:' + (ct || 'image/png') + ';base64,' + b64 } },
            { type: 'text', text: prompt }] }],
        }),
      });
      const d = await r.json();
      return clean(d && d.choices && d.choices[0] && d.choices[0].message && d.choices[0].message.content);
    }
    /* défaut : Ollama Cloud — MiniMax M3 (vision), modifiable via AI_HELPER_MODEL */
    if (env.OLLAMA_API_KEY) {
      const r = await fetch(OLLAMA_CHAT_URL, {
        method: 'POST',
        headers: { 'content-type': 'application/json', accept: 'application/json', authorization: 'Bearer ' + env.OLLAMA_API_KEY },
        body: JSON.stringify({
          model: env.AI_HELPER_MODEL || 'minimax-m3:cloud', stream: false,
          options: { num_predict: 150 },
          messages: [{ role: 'user', content: prompt, images: [b64] }],
        }),
      });
      const d = await r.json();
      return clean(d && d.message && d.message.content);
    }
  } catch (e) { return null; }
  return null;
}

/* Montant USD lu par l'IA (ex. « Montant : 10 USD » → 10) — null si doute/CDF */
function aiAmountFromNote(note) {
  if (!note || note.startsWith('⚠️')) return null;
  const m = note.match(/Montant\s*:\s*(?:[^\d]{0,6})(\d+(?:[.,]\d+)?)\s*(USD|\$)/i);
  return m ? Number(m[1].replace(',', '.')) : null;
}

/* ═══ Alerte téléphone du vendeur à chaque étape (nouvelle commande, preuve) ═══
   Deux canaux GRATUITS, activables indépendamment par secrets :
   • Telegram  : TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID (via @BotFather)
   • WhatsApp  : CALLMEBOT_PHONE + CALLMEBOT_APIKEY (via callmebot.com, message
                 envoyé sur VOTRE propre WhatsApp — aucun client ne le voit)
   ⚠️ CallMeBot GRATUIT est soumis à quota : HTTP 200 = message réellement remis ;
   tout autre code (210…) = souvent perdu silencieusement. Tout échec est mis
   en FILE (KV notifq:<id>, 2 h) et RELANCÉ par le cron des 5 min (max 3 essais).
   Chaque tentative est journalisée dans KV (notif:last, 24 h). */

/* Un appel CallMeBot isolé (numéro assaini : un « + » ou espace dans le secret
   provoquait un 403 silencieux). ok=true UNIQUEMENT sur 200. */
function callmebotOnce(env, text) {
  const phone = String(env.CALLMEBOT_PHONE).replace(/[^0-9]/g, '');
  return fetch('https://api.callmebot.com/whatsapp.php?phone=' + encodeURIComponent(phone)
    + '&text=' + encodeURIComponent(text) + '&apikey=' + encodeURIComponent(env.CALLMEBOT_APIKEY))
    .then(async r => ({ ch: 'callmebot', status: r.status, ok: r.status === 200, body: (await r.text()).slice(0, 150) }))
    .catch(e => ({ ch: 'callmebot', status: 0, ok: false, body: String(e && e.message || e) }));
}

async function notifyAdmin(env, text) {
  const jobs = [];
  const log = { at: new Date().toISOString(), results: [] };
  if (env.TELEGRAM_BOT_TOKEN && env.TELEGRAM_CHAT_ID) {
    jobs.push(fetch('https://api.telegram.org/bot' + env.TELEGRAM_BOT_TOKEN + '/sendMessage', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ chat_id: env.TELEGRAM_CHAT_ID, text, disable_web_page_preview: true }),
    }).then(async r => ({ ch: 'telegram', status: r.status, ok: r.ok, body: (await r.text()).slice(0, 150) }))
      .catch(e => ({ ch: 'telegram', status: 0, ok: false, body: String(e && e.message || e) })));
  }
  if (env.CALLMEBOT_PHONE && env.CALLMEBOT_APIKEY) {
    jobs.push(callmebotOnce(env, text).then(async res => {
      if (res.status !== 200) {
        /* Mise en file pour le cron des 5 min (3 relances max sur ~15 min) */
        const id = Array.from(crypto.getRandomValues(new Uint8Array(6))).map(b => b.toString(16).padStart(2, '0')).join('');
        await env.CODES.put('notifq:' + id, JSON.stringify({ text, tries: 0, created: new Date().toISOString() }), { expirationTtl: 7200 });
        res.queued = true;
      }
      return res;
    }));
  }
  if (jobs.length) {
    log.results = await Promise.all(jobs);
    await env.CODES.put('notif:last', JSON.stringify(log), { expirationTtl: 86400 });
  }
}

/* Cron des 5 min : relance les alertes CallMeBot en échec (file notifq:*).
   Succès (200) ou 3 essais → sortie de file ; sinon nouvel essai au prochain cron. */
async function retryNotifQueue(env) {
  if (!env.CALLMEBOT_PHONE || !env.CALLMEBOT_APIKEY) return;
  const list = await env.CODES.list({ prefix: 'notifq:', limit: 20 });
  for (const k of list.keys) {
    const v = await env.CODES.get(k.name);
    if (!v) { await env.CODES.delete(k.name); continue; }
    const q = JSON.parse(v);
    const res = await callmebotOnce(env, q.text);
    await env.CODES.put('notif:last', JSON.stringify({ at: new Date().toISOString(), results: [Object.assign({ retry: (q.tries || 0) + 1 }, res)] }), { expirationTtl: 86400 });
    if (res.status === 200 || (q.tries || 0) >= 2) await env.CODES.delete(k.name);
    else await env.CODES.put(k.name, JSON.stringify({ text: q.text, tries: (q.tries || 0) + 1, created: q.created }), { expirationTtl: 7200 });
  }
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
  await putTx(env, txId, { status: 'pending', offer: offer.id, requests: offer.requests, created: new Date().toISOString() });
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
  const requests = tx.requests || (OFFERS.find(o => o.id === tx.offer) || OFFERS[0]).requests;
  const code = genCode();
  await putCode(env, code, { total: requests, remaining: requests, created: new Date().toISOString(), tx: txId });
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
