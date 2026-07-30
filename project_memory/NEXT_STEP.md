# Prochaine étape

1. Dates « Expiration la plus proche » (conformité Vaccine_expiration — demande
   Felly 27/07) : lancer `node probe-underlying-dates.mjs` sur browser-profile
   dès qu'une synchro libère le profil Chrome (fenêtre entre deux runs — la
   sonde est prête, piste tabdoc/get-underlying-data). Si dates présentes :
   étendre export-ant-zs-detail.mjs (colonnes _date_expiry_*), régénérer
   Vaccine_expiration_ZS.json (juin+juillet), republier — le rendu dashboard
   (paires date/%, sous-ligne colorée) est DÉJÀ en place (29/07).
2. Backfills : ANT 2026-04→2025-07 (1 mois/soir à 20:00 — fix du 29/07 en
   place : heartbeat verrou + lancement Chrome protégé, le crash de lancement
   est résolu) puis ZS (23:30, débloqué depuis le 28/07 — ledger 519/519).
3. Vérifier à la synchro ANT du 30/07 que le détail ZS quotidien tourne
   (sync.log → « ✓ Détail ZS quotidien : Dispo_vaccins_ZS… ») et que les
   *_ZS.json sont frais dans le commit publié.
4. Quand le RCCM sera obtenu : créer le compte CinetPay, poser CINETPAY_APIKEY
   + CINETPAY_SITE_ID → bascule automatique en paiement 100 % automatique.
5. Copie Documents\snis-vaccination-api : retard 705+ commits, fichiers non
   commités — laissée telle quelle (décision Felly 29/07 : pas d'écrasement).
   Le repo principal est synchronisé et git y est redevenu sain.
