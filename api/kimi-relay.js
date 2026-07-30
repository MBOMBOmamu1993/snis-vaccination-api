// api/kimi-relay.js — Relais muet vers l'endpoint d'abonnement Kimi Code.
//
// Pourquoi ce relais : le WAF Cloudflare d'api.kimi.com bloque les sous-requêtes
// émises depuis un Cloudflare Worker (page « Attention Required! », IP datacenter
// + empreinte TLS). Les IP AWS de Vercel ne sont pas bloquées. Le worker
// Cloudflare pev-ia-proxy garde TOUTE la logique métier (codes d'accès, quotas,
// gating des modèles payants) ; ce relais ne fait que transiter la requête et la
// réponse (streaming SSE inclus) — il ne stocke rien, la clé x-api-key appartient
// à l'appelant et n'est pas conservée.
//
// Cible UNIQUE et en dur : https://api.kimi.com/coding/v1/messages (POST).

const TARGET = 'https://api.kimi.com/coding/v1/messages';

module.exports.config = { maxDuration: 60 };

module.exports = async function handler(req, res) {
  if (req.method !== 'POST') {
    res.status(405).json({ error: 'POST uniquement' });
    return;
  }
  try {
    const chunks = [];
    for await (const c of req) chunks.push(c);
    const upstream = await fetch(TARGET, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'anthropic-version': req.headers['anthropic-version'] || '2023-06-01',
        'x-api-key': req.headers['x-api-key'] || '',
      },
      body: Buffer.concat(chunks),
    });
    res.status(upstream.status);
    const ct = upstream.headers.get('content-type');
    if (ct) res.setHeader('content-type', ct);
    const reader = upstream.body.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      res.write(Buffer.from(value));
    }
    res.end();
  } catch (e) {
    if (!res.headersSent) res.status(502);
    res.end(JSON.stringify({ error: { message: 'Relais Kimi injoignable : ' + ((e && e.message) || e) } }));
  }
};
