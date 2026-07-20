# Jour J — Activer le paiement automatique (après obtention du RCCM)

> **En attendant le RCCM, le mode WhatsApp est actif** (voir `JOUR-J-WHATSAPP.md`) :
> vous vendez déjà vos codes manuellement, sans frais. Dès que les 2 secrets
> CinetPay ci-dessous sont posés, `/acheter` bascule **automatiquement** en
> paiement en ligne — rien à recoder, et les codes WhatsApp déjà vendus restent
> valides.

Tout le reste est déjà déployé et testé (13/07/2026). Il ne reste que ces étapes.

## 0. Obtenir le RCCM (préalable, quelques jours)

1. **GUCE — Guichet Unique de Création d'Entreprise** (guichetunique.cd,
   agences à Kinshasa et en provinces).
2. Choisissez la forme **« établissement / personne physique »** : pas besoin
   de créer une société (SARL) — l'établissement donne un RCCM à votre nom,
   suffisant pour le KYC CinetPay, à coût modique.
3. Pièces typiques : pièce d'identité, photo, adresse, activité déclarée
   (ex. « services informatiques »). Repartez avec le **numéro RCCM** et
   l'attestation.

## 1. Compte marchand CinetPay (1 à 3 jours de validation)

1. https://cinetpay.com → Créer un compte **marchand**, pays **RD Congo**.
2. KYC : votre pièce d'identité + **numéro RCCM** (obtenu au GUCE, guichetunique.cd).
3. **Paramètres → Compte de reversement** : enregistrer le numéro mobile money
   (M-Pesa / Orange Money / Airtel Money) ou le compte bancaire qui recevra
   l'argent des utilisateurs. C'est CE compte qui reçoit les paiements.

## 2. Récupérer les 2 identifiants (5 min)

- Espace marchand → **Intégration → API Key** → copier.
- Créer un **service/site** : nom « Assistant IA Dashboard PEV »,
  URL `https://mbombomamu1993.github.io` → noter le **Site ID** (un nombre).
- Ne rien configurer d'autre chez CinetPay : le Worker envoie déjà
  `notify_url` et `return_url` à chaque transaction.

## 3. Poser les secrets (5 min)

```bash
cd C:\Users\felly\snis-vaccination-api\cloudflare-worker
npx wrangler secret put CINETPAY_APIKEY    # coller l'API Key
npx wrangler secret put CINETPAY_SITE_ID   # coller le Site ID
```

⚠️ Piège connu sur ce PC : les `secret put` interactifs lancés depuis une
session Claude (préfixe `!`) créent des secrets VIDES. Méthode fiable :
créer un fichier `secrets.json` :

```json
{ "CINETPAY_APIKEY": "votre_api_key", "CINETPAY_SITE_ID": "votre_site_id" }
```

puis `npx wrangler secret bulk secrets.json` et supprimer `secrets.json`.
Vérifier avec `npx wrangler secret list` (les 2 noms doivent apparaître).

Aucun redéploiement nécessaire : la page /acheter bascule automatiquement
de « bientôt disponible » aux offres.

## 4. Test réel (10 min)

1. Ouvrir https://pev-ia-proxy.pev-rdc.workers.dev/acheter → les 4 offres
   (5 $ / 10 $ / 20 $ / 30 $) + le champ « Montant libre » s'affichent.
2. Acheter l'offre Découverte (5 $) avec votre propre mobile money.
3. Après paiement → la page /retour affiche un code `PEV-XXXX-XXXX`.
4. Dashboard → onglet IA → ⚙ Accès → « 🎫 Code d'accès » → coller le code → poser une question.
5. Vérifier la transaction dans l'espace CinetPay, puis le reversement
   (commission CinetPay ~3,5 % déduite).

## Rappels

- Prix / nombre de requêtes : constante `OFFERS` en tête de `worker.js`,
  puis `npx wrangler deploy`.
- Vente manuelle possible à tout moment (sans CinetPay) :
  `POST /admin/codes` avec le header `Authorization: Bearer <ADMIN_TOKEN>`
  (voir README.md).
- Suivi des codes vendus : `GET /admin/codes` (même header).
- Le code intégré `PEV-DHIS-7QK4XZ` est réservé au DHIS2 : il est refusé
  sur la route IA (testé) — vos crédits Anthropic sont protégés.
