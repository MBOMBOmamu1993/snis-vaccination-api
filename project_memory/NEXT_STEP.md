# Prochaine étape

1. Synchro ZS complète (519 ZS) : attendre la fin de validate-zs-batch.mjs
   (reprise incrémentale — verdicts dans zs_batch_verdicts.json), puis lancer
   `set MASHAKO_CFG=zs && node sync.mjs --background` (liste blanche lue dans
   les verdicts ; feuilles collapse → repli unitaire parallélisé ×3). Vérifier
   la publication et la couverture (registre zs_ledger.json → débloque le
   backfill ZS de 23:30, seuil 500/519).
2. Dates « Expiration la plus proche » (conformité Vaccine_expiration avec
   l'original — demande Felly 27/07) : retenter l'extraction quand le canal
   commandes est stable (410 en rafale ~9h) : POST tabdoc/get-underlying-data
   (includeAllColumns=true, visualIdPresModel) depuis le contexte page — voir
   CURRENT_STATE §D. Puis régénérer Vaccine_expiration_ZS.json (juin+juillet)
   avec les colonnes date+%, republier, config dashboard (paires date/% par
   antigène, texte coloré).
3. Backfills : ANT 2026-04→2025-07 (1 mois/soir à 20:00, ~10 soirs — 2026-05
   déjà fait) puis ZS (23:30, débloqué par l'étape 1).
4. Industrialiser le détail ZS : intégrer export-ant-zs-detail.mjs à la
   synchro quotidienne (sinon les *_ZS.json ne se rafraîchissent pas — ils
   survivent aux publications grâce à la fusion, mais datent).
5. Quand le RCCM sera obtenu : créer le compte CinetPay, poser CINETPAY_APIKEY
   + CINETPAY_SITE_ID → bascule automatique en paiement 100 % automatique.
6. Dépôt local : fichiers de données docs/data* non synchronisés localement
   (clone partiel, réseau instable) — faire `git checkout -- docs/` quand la
   connexion est stable (télécharge ~2000 blobs, reprise possible).
