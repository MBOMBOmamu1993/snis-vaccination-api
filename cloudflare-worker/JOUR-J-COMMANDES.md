# Jour J — Vente en ligne intégrée (commande → preuve → code instantané)

Système de vente **sans WhatsApp ni RCCM**, entièrement dans la plateforme :

**Client** : `/acheter` → choisit l'offre → `/commander` (nom + email + offre,
champs **obligatoires**) → approbation **automatique instantanée** → voit le
**numéro de dépôt** (`MPESA_INFOS`) → envoie sa **capture** → barre de
progression → **le code s'affiche tout seul** dès livraison (+ copie e-mail).

**Vous** : alerté **sur votre téléphone** à chaque étape → `/admin` liste les
commandes (nom, email, offre, montant) → l'**assistant IA lit la capture**
(récap montant/référence/opérateur) → vous cliquez **🎫 Livrer** → le code
part. `/admin/rapport` = tableau des ventes + totaux. **Rapport quotidien
automatique à 18 h** (cron Cloudflare).

⚠️ L'IA aide à LIRE les captures, elle ne valide jamais un paiement : une
capture peut être falsifiée — vérifiez toujours la réception sur votre compte
avant de livrer.

## 1. Obligatoire : votre numéro de dépôt (2 min)

Dans `secrets.json` (ce dossier), remplacez la ligne `MPESA_INFOS` par ex. :

```json
"MPESA_INFOS": "M-Pesa : 0824 123 456 (nom : J. Ilunga)"
```

C'est ce texte que le client voit après commande. Vous pouvez y mettre plusieurs
numéros : `"M-Pesa : 0824… · Orange Money : 0899… (nom : J. Ilunga)"`.

## 2. Alertes sur votre téléphone (optionnel mais recommandé)

### Option A — sur VOTRE WhatsApp (CallMeBot, gratuit, 5 min)

1. Ajoutez le contact **+34 644 51 95 23** dans WhatsApp.
2. Envoyez-lui le message : `I allow callmebot to send me messages`
3. Vous recevez votre **apikey** personnelle en réponse.
4. Dans `secrets.json` : `"CALLMEBOT_PHONE": "243XXXXXXXXX"` (votre numéro,
   sans +) et `"CALLMEBOT_APIKEY": "votre_apikey"`.

### Option B — Telegram (le plus fiable, gratuit, 5 min)

1. Dans Telegram, ouvrez **@BotFather** → `/newbot` → nom `PEV Ventes Bot`
   → copiez le **token** → `"TELEGRAM_BOT_TOKEN"`.
2. Envoyez n'importe quel message à votre bot, puis ouvrez **@userinfobot**
   → copiez votre **chat id** → `"TELEGRAM_CHAT_ID"`.

(Les deux options peuvent être actives en même temps.)

## 3. E-mails automatiques (optionnel)

Le client reçoit son code par e-mail EN PLUS de l'affichage instantané, et vous
recevez une notif à chaque commande + le rapport quotidien :

1. Créez un Gmail dédié (ex. `analysepevdhis2@gmail.com`) — c'est l'expéditeur
   « no-reply » vu par les clients.
2. Compte **Brevo** gratuit (brevo.com, 300 e-mails/jour) → **SMTP & API →
   API Keys** → copiez la clé → `"BREVO_API_KEY"`.
3. Brevo → **Expéditeurs** → ajoutez ce Gmail et validez le lien reçu.
4. `"MAIL_FROM": "analysepevdhis2@gmail.com"` et
   `"MAIL_TO_ADMIN": "votre.email.perso@gmail.com"`.

## 4. Poser les secrets

```bash
cd C:\Users\felly\snis-vaccination-api\cloudflare-worker
npx wrangler secret bulk secrets.json
```

puis **supprimez `secrets.json`**. Aucun redéploiement nécessaire.

## 5. Test de bout en bout (10 min)

1. `/acheter` → offre Essentiel (20 $) → remplissez nom/email → le dépôt s'affiche
   avec VOTRE numéro.
2. Envoyez une capture quelconque → vous êtes alerté sur votre téléphone.
3. `/admin` → la commande apparaît avec le récap IA 🤖 → **🎫 Livrer** →
   le code s'affiche sur la page client (et part par e-mail si configuré).
4. `/admin/rapport` → la vente apparaît avec les totaux.

## Réglages avancés (variables/secrets optionnels)

| Nom | Rôle | Défaut |
|---|---|---|
| `AUTO_APPROVE` | `'0'` = approbation manuelle des commandes | auto |
| `AI_HELPER_PROVIDER` | `ollama` · `claude` · `kimi` | `kimi` |
| `AI_HELPER_MODEL` | modèle vision (défauts : `minimax-m3:cloud` · `claude-opus-4-8` · `kimi-k3`) | selon provider |
| `KIMI_API_KEY` | requis si provider `kimi` (platform.kimi.ai) | — |
| `KIMI_API_BASE` | `https://api.moonshot.cn` si clé de la plateforme chinoise | `.ai` |
| `PAYMENT_PROVIDER` | `commande` · `whatsapp` · `cinetpay` (force le mode) | auto |
| `OFFERS` (code) | offres et prix | 20/30/40/50/100 $ + montant libre dès 10 $ |

⚠️ Pour Ollama, le modèle doit être **vision** — `minimax-m3:cloud` par défaut.
`kimi-k3` est le modèle vedette actuel de Moonshot (vision native) ; épinglez une
autre version via `AI_HELPER_MODEL` si besoin. Si le récap 🤖 n'apparaît pas sur
une capture, essayez `AI_HELPER_PROVIDER` = `claude` (Opus 4.8, lecture la plus
fiable, ~0,002 $/capture).
