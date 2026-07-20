# Prochaine étape

1. Test bout-en-bout réel par l'utilisateur : /acheter → commande → alerte
   WhatsApp (résumé + lien) → capture → /admin (récap IA 🤖 + 🖼️ Capture) →
   Livrer → code sur la page client + e-mail client.
2. Quand le RCCM sera obtenu : créer le compte CinetPay, poser CINETPAY_APIKEY +
   CINETPAY_SITE_ID → bascule automatique en paiement 100 % automatique.
3. Vérifier un vrai rapport quotidien cron à 18 h Kinshasa (ventes sale:*
   permanents → WhatsApp court + e-mail détaillé).
4. Optionnel mais RECOMMANDÉ : alertes Telegram (TELEGRAM_BOT_TOKEN/CHAT_ID,
   5 min via @BotFather + @userinfobot) en secours illimité de CallMeBot
   (quota gratuit journalier capricieux — le worker relance via notifq:* +
   cron 5 min, mais Telegram n'a aucune limite).
5. Dépôt local : fichiers de données docs/data* non synchronisés localement
   (clone partiel, réseau instable) — faire `git checkout -- docs/` quand la
   connexion est stable (télécharge ~2000 blobs, reprise possible).
