# Prochaine étape

1. Exporter le détail par zone de santé de Dispo_vaccins_ANT et
   Vaccine_expiration_ANT_P1 via la chaîne crosstab Excel (1 session filtrée
   51 antennes par tableau de bord, feuille _TABLE_…), publier les JSON et les
   brancher dans le dashboard (tableau vivant éclaté par ZS — demande Felly
   27/07). Pour les dates d'expiration exactes : creuser la feuille
   Vaccine_expiration_HZ du classeur ZS ou le téléchargement « données
   complètes ».
2. Lancer la synchro ZS (code batché — couverture complète des 519 ZS en un
   run) après validation groupé-vs-unitaire par feuille (validate-zs-batch.mjs).
3. Backfills : ANT 2026-04→2025-07 (1 mois/soir à 20:00, ~10 soirs) puis ZS
   (seuil 500/519 débloqué par la synchro ZS complète, 23:30).
4. Quand le RCCM sera obtenu : créer le compte CinetPay, poser CINETPAY_APIKEY +
   CINETPAY_SITE_ID → bascule automatique en paiement 100 % automatique.
5. Dépôt local : fichiers de données docs/data* non synchronisés localement
   (clone partiel, réseau instable) — faire `git checkout -- docs/` quand la
   connexion est stable (télécharge ~2000 blobs, reprise possible).
