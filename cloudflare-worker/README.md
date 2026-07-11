# Proxy IA — Dashboard PEV de routine

Ce dossier contient le **proxy Cloudflare Worker** qui alimente l'onglet
**« Génération des analyses »** du dashboard (`docs/index.html`).

Il remplit 3 fonctions :

1. **Proxy API Claude (Anthropic)** — votre clé API reste secrète côté serveur ;
   les utilisateurs consomment avec un **code d'accès** à quota.
2. **Proxy DHIS2 (lecture seule)** — l'IA peut interroger l'API Web du SNIS RDC
   (n'importe quel élément de données/indicateur) sans jamais exposer vos
   identifiants DHIS2.
3. **Vente automatique de codes** — page `/acheter` avec paiement **CinetPay**
   (M-Pesa, Orange Money, Airtel Money, carte). Le code s'affiche automatiquement
   après paiement : personne n'a besoin de vous contacter.

---

## Déploiement pas à pas (~20 minutes)

### Étape 0 — Prérequis (comptes à créer)

| Compte | Où | Pour quoi |
|---|---|---|
| Anthropic Console | https://console.anthropic.com | Clé API + crédits (min 5–10 $) |
| Cloudflare (gratuit) | https://dash.cloudflare.com | Héberger ce proxy |
| CinetPay (marchand) | https://cinetpay.com | Paiement mobile money (KYC : pièce d'identité + infos bancaires). *Peut se faire plus tard — tout le reste fonctionne sans.* |

### Étape 1 — Installer wrangler et se connecter

```bash
npm install -g wrangler
cd cloudflare-worker
wrangler login          # ouvre le navigateur
```

### Étape 2 — Créer le stockage des codes (KV)

```bash
npx wrangler kv namespace create CODES
```

Copiez l'`id` affiché dans `wrangler.toml` (remplacez `REMPLACEZ_PAR_L_ID_GENERE`).

### Étape 3 — Configurer les secrets

```bash
npx wrangler secret put ANTHROPIC_API_KEY     # sk-ant-... (console.anthropic.com)
npx wrangler secret put DHIS2_BASE_URL        # ex. https://snisrdc.com
npx wrangler secret put DHIS2_USERNAME        # compte DHIS2 (lecture seule conseillé)
npx wrangler secret put DHIS2_PASSWORD
npx wrangler secret put ADMIN_TOKEN           # inventez un long mot de passe (30+ caractères)
# Plus tard, quand le compte CinetPay est validé :
npx wrangler secret put CINETPAY_APIKEY
npx wrangler secret put CINETPAY_SITE_ID
```

### Étape 4 — Déployer

```bash
npx wrangler deploy
```

L'URL affichée ressemble à `https://pev-ia-proxy.<votre-compte>.workers.dev`.

### Étape 5 — Brancher le dashboard

Dans `docs/index.html`, cherchez la ligne :

```js
var IA_PROXY_URL = '';
```

et mettez-y l'URL du Worker :

```js
var IA_PROXY_URL = 'https://pev-ia-proxy.<votre-compte>.workers.dev';
```

Poussez sur GitHub → l'onglet IA propose alors le mode « 🎫 Code d'accès »
et le bouton « 💳 Obtenir un code ».

---

## Créer des codes manuellement (avant/à côté du paiement en ligne)

```bash
curl -X POST https://pev-ia-proxy.<compte>.workers.dev/admin/codes \
  -H "Authorization: Bearer VOTRE_ADMIN_TOKEN" \
  -H "content-type: application/json" \
  -d '{"requests": 300, "note": "Dr Kabongo - paiement du 12/07"}'
# → {"code":"PEV-A7K2-9MQ4","requests":300}
```

Consulter un code : `GET /admin/codes/PEV-A7K2-9MQ4` (même en-tête Authorization).
Lister tous les codes : `GET /admin/codes`.
Solde public d'un code : `GET /verifier?code=PEV-A7K2-9MQ4` (sans token).

## Tarifs

Les offres (nombre de requêtes, prix CDF) se modifient en tête de `worker.js`
(constante `OFFERS`). Repère : une analyse complète ≈ 3 à 6 requêtes IA ;
une requête Opus 4.8 vous coûte ≈ 0,01–0,05 $. Fixez vos prix avec de la marge.

## Sécurité

- Les clés (Anthropic, DHIS2, CinetPay) ne quittent **jamais** le Worker.
- Le proxy DHIS2 n'accepte que des **GET** (lecture seule) et bloque les
  endpoints sensibles (users, system…). Utilisez quand même un compte DHIS2
  **en lecture seule** dédié.
- Chaque requête IA décrémente le quota du code ; un code épuisé est refusé.
- CORS restreint à `ALLOWED_ORIGIN` (GitHub Pages).
