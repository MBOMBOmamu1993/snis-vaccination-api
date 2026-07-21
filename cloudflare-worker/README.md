# Proxy IA — Dashboard PEV de routine

Ce dossier contient le **proxy Cloudflare Worker** qui alimente l'onglet
**« Génération des analyses »** du dashboard (`docs/index.html`).

Il remplit 3 fonctions :

1. **Proxy API Claude (Anthropic)** — votre clé API reste secrète côté serveur ;
   les utilisateurs consomment avec un **code d'accès** à quota.
2. **Proxy DHIS2 (lecture seule)** — l'IA peut interroger l'API Web du SNIS RDC
   (n'importe quel élément de données/indicateur) sans jamais exposer vos
   identifiants DHIS2.
3. **Vente de codes d'accès** — page `/acheter` avec 2 modes :
   - **WhatsApp (manuel, actif par défaut)** : boutons « 💬 Commander » par offre
     → le client paie par mobile money sur votre numéro dédié, vous générez son
     code en 10 s depuis votre téléphone via la mini-console **`/admin`**.
     Voir `JOUR-J-WHATSAPP.md`.
   - **CinetPay (automatique, après obtention du RCCM)** : paiement en ligne
     (M-Pesa, Orange Money, Airtel Money, carte) → le code s'affiche tout seul
     après paiement. Bascule automatique dès que les clés CinetPay sont posées.
     Voir `JOUR-J-CINETPAY.md`.
4. **Essai gratuit 7 jours** — endpoint `/essai` : 50 requêtes IA valables 7 jours,
   1 par appareil (empreinte IP + navigateur). À expiration, l'utilisateur est
   renvoyé vers l'achat d'un code.

---

## Déploiement pas à pas (~20 minutes)

### Étape 0 — Prérequis (comptes à créer)

| Compte | Où | Pour quoi |
|---|---|---|
| Anthropic Console | https://console.anthropic.com | Clé API + crédits (min 5–10 $) |
| Cloudflare (gratuit) | https://dash.cloudflare.com | Héberger ce proxy |
| Numéro WhatsApp dédié | SIM séparée (quelques $) | Recevoir les commandes des clients. *Une SIM dédiée préserve votre numéro personnel.* |
| CinetPay (marchand) | https://cinetpay.com | Paiement automatique (KYC : pièce d'identité + RCCM). *Plus tard — tout fonctionne sans.* |

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
npx wrangler secret put WHATSAPP_NUMBER       # numéro dédié, format international SANS « + » (ex. 243812345678)
# Plus tard, quand le compte CinetPay est validé (RCCM obtenu) :
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

## Créer des codes manuellement (vente WhatsApp, partenaires, tests)

**Depuis votre téléphone (recommandé)** : ouvrez `https://pev-ia-proxy.<compte>.workers.dev/admin`
(mettez-la en favori), entrez le token admin + le nombre de requêtes → le code
`PEV-XXXX-XXXX` s'affiche, prêt à copier dans WhatsApp. Mettez une note
(client, offre, paiement reçu) pour le suivi.

**Depuis un terminal** :

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

Les offres se modifient en tête de `worker.js` (constante `OFFERS`) :
**5 $ / 10 $ / 20 $ / 30 $** (≈ 12 / 25 / 50 / 75 analyses), plus un **montant
libre** saisi par le client (`REQUESTS_PER_USD` requêtes par dollar, bornes
`CUSTOM_MIN_USD`–`CUSTOM_MAX_USD`).
Tarif : 0,10 $/requête ≈ 3× le coût API (vous encaissez le triple de votre
dépense Anthropic). Repère : une analyse complète ≈ 3 à 6 requêtes IA ;
une requête Opus 4.8 coûte ≈ 0,01–0,05 $.

## Sécurité

- Les clés (Anthropic, DHIS2, CinetPay) ne quittent **jamais** le Worker.
- Le proxy DHIS2 n'accepte que des **GET** (lecture seule) et bloque les
  endpoints sensibles (users, system…). Utilisez quand même un compte DHIS2
  **en lecture seule** dédié.
- Chaque requête IA décrémente le quota du code ; un code épuisé est refusé.
- CORS restreint à `ALLOWED_ORIGIN` (GitHub Pages).
