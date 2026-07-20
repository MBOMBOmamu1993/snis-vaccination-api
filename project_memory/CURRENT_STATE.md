# État actuel du projet

Projet : snis-vaccination-api (Dashboard PEV + worker Cloudflare pev-ia-proxy)

Contexte permanent :
- rename_map = source de vérité
- ne pas modifier monthly pour corriger l'affichage
- harmoniser aggregate_dashboard.py et index.html avec les champs réels

État actuel (20/07/2026) :
- Vente de codes IA : système « commande en ligne » COMPLET et déployé
  (client : /acheter → /commander (nom+email obligatoires) → approbation auto →
  dépôt M-Pesa (MPESA_INFOS) → upload capture → admin livre → code affiché + e-mail).
- Admin /admin : file de commandes + OCR IA des captures (AI_HELPER_PROVIDER=kimi
  par défaut, modèle kimi-k3, clé KIMI_API_KEY posée le 20/07) + /admin/rapport
  (ventes) + rapport quotidien cron 17h UTC (18h Kinshasa).
- Dashboard docs/index.html : 3 fournisseurs IA au choix — Ollama Cloud
  (forfait, modèles dynamiques via /api/tags), Kimi (usage, kind 'openai',
  options préfixées « kimi: » : kimi-k3, kimi-k2.7-code, kimi-k2.6),
  Anthropic Claude Opus 4.8 (usage). Clés perso : pev_ia_key_ollama / key_kimi / key_claude.
- Worker Kimi : base https://api.moonshot.ai (surcharge KIMI_API_BASE pour .cn),
  route /kimi/v1/chat/completions, DEFAULT_KIMI_MODEL=kimi-k3.
- E-mails Brevo : envoi OK (201). Réservés au RAPPORT QUOTIDIEN (MAIL_TO_ADMIN)
  + e-mails CLIENTS (validation commande, livraison du code). Plus d'e-mail
  admin à chaque commande (demande utilisateur 20/07 soir).
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
