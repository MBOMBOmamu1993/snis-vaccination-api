# État actuel du projet

Projet : snis-vaccination-api (Dashboard PEV + worker Cloudflare pev-ia-proxy)

Contexte permanent :
- rename_map = source de vérité
- ne pas modifier monthly pour corriger l'affichage
- harmoniser aggregate_dashboard.py et index.html avec les champs réels

État actuel (21/07/2026) :
- Vente de codes IA : système « commande en ligne » COMPLET et déployé
  (client : /acheter → /commander (nom+email obligatoires) → approbation auto →
  dépôt M-Pesa (MPESA_INFOS) → upload capture → admin livre → code affiché + e-mail).
- Admin /admin : file de commandes + OCR IA des captures (AI_HELPER_PROVIDER=kimi
  par défaut, modèle kimi-k3, clé KIMI_API_KEY posée le 20/07) + /admin/rapport
  (ventes) + rapport quotidien cron 17h UTC (18h Kinshasa).
- Dashboard docs/index.html : VUE CLIENT épurée (21/07) — le client ne voit QUE
  le code d'accès + essai gratuit + « Obtenir un code », et 6 modèles fixes
  (MiniMax M3, GLM 5.2, DeepSeek V4 Pro, Kimi K2.7, Kimi K3, Claude Opus 4.8 ;
  mention « adapté aux tâches les plus complexes et exigeantes » sur K3 et
  Opus 4.8). VUE ADMIN complète (onglet « Ma clé API » + liste dynamique
  Ollama/Kimi/Claude) : ouvrir le dashboard avec ?admin=1 une fois (mémorisé
  dans le navigateur), ?admin=0 pour repasser en vue client. Essai gratuit =
  7 JOURS / 50 requêtes (TRIAL_DAYS=7 depuis le 21/07 — avant : 31 j).
  3 fournisseurs IA au choix — Ollama Cloud
  (forfait, modèles dynamiques via /api/tags), Kimi (usage, kind 'openai',
  options préfixées « kimi: » : kimi-k3, kimi-k2.7-code, kimi-k2.6),
  Anthropic Claude Opus 4.8 (usage). Clés perso : pev_ia_key_ollama / key_kimi / key_claude.
- Worker Kimi : base https://api.moonshot.ai (surcharge KIMI_API_BASE pour .cn),
  route /kimi/v1/chat/completions, DEFAULT_KIMI_MODEL=kimi-k3.
- E-mails : sendMail accepte 2 prestataires dans l'ordre — 1) EMAILJS (ACTIF
  depuis le 21/07, priorité, e-mails partant du VRAI Gmail du vendeur via OAuth
  → livrés chez Gmail ; secrets EMAILJS_SERVICE_ID=service_pxomvps /
  TEMPLATE_ID=template_1ph1lhu / PUBLIC_KEY + PRIVATE_KEY posés — ⚠️ le compte
  est en « strict mode » : la PRIVATE_KEY (accessToken) est OBLIGATOIRE pour
  l'API serveur, sans elle → 403 « API access in strict mode » ; modèle
  {{to_email}}/{{to_name}}/{{subject}}/{{{message_html}}}) ; 2) BREVO en secours
  (accepté 201 mais Gmail jette silencieusement l'expéditeur @gmail.com relayé —
  inutilisable seul). Réservés au RAPPORT QUOTIDIEN (MAIL_TO_ADMIN) + e-mails
  CLIENTS (validation, livraison du code = preuve écrite).
  Vente test e2e validée le 21/07 : CMD-MXMLRV → code PEV-J6NJ-2VLS (10 req.)
  livré, e-mail client via EmailJS 200 OK (trace notif:last-mail).
  ⚠️ Gmail range parfois l'e-mail du code en SPAM (vérifié en direct) — la page
  de suivi affiche donc désormais la consigne « vérifiez boîte de réception ET
  Spam, le code reste affiché à l'écran » (déployé le 21/07). Expéditeur réel =
  anlysepevdhis@gmail.com (Gmail connecté), Bcc = fellybokota@gmail.com
  (archive-preuve). Historique des envois consultable : GET api.emailjs.com
  /api/v1.1/history?user_id=<PUBLIC>&accessToken=<PRIVATE>.
  ⚠️ Diagnostic KV : `wrangler kv key get/list` renvoie vide sur ce namespace
  (supports_url_encoding) — utiliser l'API REST Cloudflare directe
  (/storage/kv/namespaces/<id>/values/<clé>) pour lire les clés notif:*/ord:*.
- WhatsApp CallMeBot : 2 pièges documentés (20/07) —
  1) 403 = réf « (CMD-xxx) » entre parenthèses sur sa propre ligne (WAF) →
     format « Réf CMD-xxx » en ligne, validé en direct.
  2) 210 = QUOTA gratuit épuisé → message accepté mais perdu silencieusement
     (ok=true UNIQUEMENT sur 200). Parade : file KV notifq:* + cron */5 * * * *
     qui relance jusqu'à 3 fois (~15 min) — testé en direct (relance partie à
     l'heure du cron). En volume normal (2 alertes/commande) le quota suffit ;
     une rafale de tests l'épuise pour la journée. Secrets reposés
     (CALLMEBOT_PHONE=243813662142, CALLMEBOT_APIKEY).
- Secrets Cloudflare posés : OLLAMA/ANTHROPIC/KIMI API keys, DHIS2_*, ADMIN_TOKEN,
  MPESA_INFOS, BREVO_API_KEY, MAIL_FROM, MAIL_TO_ADMIN, CALLMEBOT_*, WHATSAPP_NUMBER.
- CinetPay : prêt mais inactif (attente RCCM) ; PAYMENT_PROVIDER force un mode sinon auto.
- Telegram : ACTIF depuis le 20/07 soir (bot @pev_ventes_bot, TELEGRAM_BOT_TOKEN
  + TELEGRAM_CHAT_ID posés) — test e2e ok:true instantané. Canal fiable et
  illimité ; c'est le canal de référence, CallMeBot sert de secours.
- Diagnostic notifs : clés KV notif:last et notif:last-mail (TTL 24 h) = journal
  des derniers envois (à consulter en cas de doute) ; file de relance notifq:*.
- ⚠️ GIT LOCAL CASSÉ (clone partiel blob:none, réseau instable) : git status /
  commit / write-tree CLASSIQUES PENDENT indéfiniment (lazy-fetch massif des
  blobs « racily clean »). Procédure qui marche (21/07) :
  $env:GIT_NO_LAZY_FETCH="1" ; git -c gc.auto=0 add <fichiers> ;
  git write-tree --missing-ok ; git commit-tree <tree> -p HEAD -m "..." ;
  git update-ref refs/heads/main <sha> ; git push origin main.
  Supprimer .git/index.lock + .git/gc.pid si une tentative précédente a été tuée.

État 22/07/2026 — IA TOUS PROGRAMMES DHIS2 (déployé) :
- Le prompt système iaSystem() de docs/index.html (commun à TOUS les modèles :
  Ollama Cloud, Kimi, Claude) couvre désormais TOUS les programmes du SNIS, pas
  seulement le PEV. Section « Tous les programmes du SNIS — repères vérifiés »
  ajoutée d'après exploration réelle de l'instance : 42 datasets (A primaires /
  B secondaires+PEV / C SIGL / D hôpital / E sang / F BCZ / J tertiaire +
  PNLP/PNLT/PNLS/PNSR/NUT/PNSM/PNSOV/PNRBC/PNSBD/PNEL/PNIRA/PROSANI/IDSR hebdo /
  campagnes), ~9 482 dataElements, ~1 570 indicateurs (préfixes 1a…2n + IDSR +
  cartes de score), org 1 RDC/26 prov/519 ZS/10 405 AS/25 672 FOSA.
- Règles clés enseignées aux modèles : indicateur officiel dans analytics
  (UN par appel, lourd — repli dataElements du numerator/denominator si 5xx) ;
  découverte par indicators.json?filter=name:ilike: ; UID dataElements IDENTIQUES
  entre anciennes éditions (≤2024) et « -Edition 2025 » (≥2025) → analytics sur
  l'élément couvre toutes les années ; complétude = <datasetUID>.REPORTING_RATE,
  globaux L3KahLq4YFo/bnyWMbmL5IR ; coefficients population WLSKVyA8LoY (0,04 /
  0,0349 / 0,036 / 0,149 / 0,113 / 0,057) ; IDSR = périodes hebdo (2026W12,
  LAST_12_WEEKS) ; programIndicators = canevas DV ABANDONNÉS sans données →
  ne pas utiliser.