# Prochaine étape

1. Test bout-en-bout réel par l'utilisateur : /acheter → commande → alerte
   WhatsApp (résumé + lien) → capture → /admin (récap IA 🤖 + 🖼️ Capture) →
   Livrer → code sur la page client + e-mail client.
2. Quand le RCCM sera obtenu : créer le compte CinetPay, poser CINETPAY_APIKEY +
   CINETPAY_SITE_ID → bascule automatique en paiement 100 % automatique.
3. Vérifier un vrai rapport quotidien cron à 18 h Kinshasa (ventes sale:*
   permanents → WhatsApp court + e-mail détaillé).
4. Vérifier demain que WhatsApp (CallMeBot) est revenu après reset du quota
   (une commande test suffit) — Telegram reste le canal principal, actif.
5. Dépôt local : fichiers de données docs/data* non synchronisés localement
   (clone partiel, réseau instable) — faire `git checkout -- docs/` quand la
   connexion est stable (télécharge ~2000 blobs, reprise possible).
