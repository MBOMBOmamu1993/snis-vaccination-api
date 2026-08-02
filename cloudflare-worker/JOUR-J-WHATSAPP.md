# Jour J — Vendre des codes via WhatsApp (actif MAINTENANT, sans RCCM)

Mode de vente manuel : le client clique « 💬 Commander » sur `/acheter`, paie par
mobile money sur votre numéro dédié, et vous lui envoyez son code généré en 10 s
via la console `/admin`. **0 $ de frais, 0 inscription, fonctionne aujourd'hui.**

Bascule future : dès que `CINETPAY_APIKEY` + `CINETPAY_SITE_ID` seront posés
(voir `JOUR-J-CINETPAY.md`), `/acheter` passera automatiquement en paiement
en ligne — **rien à recoder**.

## 1. Numéro dédié (confidentialité)

1. Achetez une **SIM prépayée séparée** (quelques $) — c'est elle que verront
   vos clients, jamais votre numéro personnel.
2. Installez **WhatsApp Business** (gratuit) dessus : profil avec un nom
   commercial (ex. « Support Dashboard PEV »), logo, réponses rapides.
3. Rechargez cette SIM en mobile money : c'est le numéro que les clients
   créditeront (M-Pesa / Orange Money / Airtel Money).

ℹ️ Transparence : le nom enregistré sur la SIM apparaît lors de la confirmation
mobile money du client — inévitable pour tout paiement légal, et rassurant
pour l'acheteur. Votre identité personnelle, elle, reste hors de la page web.

## 2. Poser le secret (5 min)

Format : international **sans « + »** ni espaces — ex. `243812345678`.

⚠️ Piège connu sur ce PC : les `secret put` interactifs lancés depuis une
session d'assistant créent des secrets VIDES. Méthode fiable — créer un
fichier `secrets.json` dans ce dossier :

```json
{ "WHATSAPP_NUMBER": "243812345678" }
```

```bash
cd C:\Users\felly\snis-vaccination-api\cloudflare-worker
npx wrangler secret bulk secrets.json
```

puis **supprimer `secrets.json`**. Vérifier avec `npx wrangler secret list`.
Aucun redéploiement nécessaire : `/acheter` bascule automatiquement en mode
WhatsApp (avant le secret : page « bientôt disponible »).

## 3. Test (5 min)

1. Ouvrir https://pev-ia-proxy.pev-rdc.workers.dev/acheter
   → les 5 offres (20 / 30 / 40 / 50 / 100 $) + « Montant libre » (10 $ minimum) s'affichent avec
   les boutons verts « 💬 Commander ».
2. Cliquer un bouton → WhatsApp s'ouvre sur votre numéro dédié avec le message
   pré-rempli (« Bonjour, je souhaite acheter l'offre Standard (±75 analyses — 30 $)… »).
3. Console admin : ouvrir https://pev-ia-proxy.pev-rdc.workers.dev/admin sur
   votre téléphone → **mettre en favori**. Token admin + 100 requêtes + note
   « test » → un code `PEV-XXXX-XXXX` s'affiche.
4. Coller ce code dans le dashboard (⚙ Accès → « 🎫 Code d'accès ») et poser
   une question → ça répond. Revendre ensuite ce code test à personne. 🙂

## 4. Déroulé d'une vente (répéter pour chaque client)

1. Le client envoie le message pré-rempli → vous répondez avec le **numéro
   mobile money à créditer** (gagnez du temps : enregistrez une « réponse
   rapide » dans WhatsApp Business).
2. Il paie et envoie la **capture** du paiement.
3. Vous vérifiez la réception, puis ouvrez `/admin` (favori) :
   token + requêtes (20 $=200 · 30 $=300 · 40 $=400 · 50 $=500 · 100 $=1 000) + note
   (nom du client, montant) → **Générer**.
4. Vous collez le code dans la conversation. Terminé — le client suit son
   solde lui-même dans l'application (⚙ Accès → vérifier).

## 5. Suivi

- Tous les codes vendus : `GET /admin/codes` (header
  `Authorization: Bearer <ADMIN_TOKEN>`) — les notes permettent de rapprocher
  paiements et codes.
- Solde d'un code : `GET /verifier?code=PEV-XXXX-XXXX` (public, sans token).

## Rappels

- Prix / offres : constante `OFFERS` en tête de `worker.js`, puis
  `npx wrangler deploy`.
- Le mode WhatsApp ne coûte rien : pas de commission (vs ~3,5 % chez CinetPay).
- Quand le RCCM sera obtenu : suivre `JOUR-J-CINETPAY.md` (2 secrets) —
  la bascule est instantanée et les codes WhatsApp déjà vendus restent valides.
