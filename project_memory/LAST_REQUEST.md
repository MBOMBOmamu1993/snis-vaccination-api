# Dernière requête

Date : 31/07/2026
Dernière demande : Page « Plan Mashako 3.0 — Rapport Zone de Santé » : les
  feuilles ne ressemblaient pas aux visuels du classeur Tableau original —
  double tableau (synthèse ZS + carte séparée « Détail par aire de santé »),
  Séances/Taux d'abandon sans lignes AS (404), en-têtes bruts, booléens
  « Faux »/« 0 », pagination fantôme. Refonte à l'identique des captures
  originales : UN visuel par feuille = ligne/bloc de synthèse ZS en tête +
  situation des aires de santé.
Fichiers concernés : docs/index.html (nouveau moteur mkZsRender + MK_ZS_CFG +
  MK_ZS_BUILD, mapping MK_AS_FILE dans mkAsLoad, hook dans mkLoadView),
  tools/mashako-sync/test-mkas-harness.mjs (réécrit pour les builders),
  tools/mashako-sync/out-zs/views/*.json (fixtures réels téléchargés),
  project_memory/*
Statut : TERMINÉ ET DÉPLOYÉ (29b7c518e sur main, 31/07) — 17 feuilles reprises
  (Supervision P1/P2/P3, Séances P1/P2, Taux d'abandon P1/P2, Livraison P1/P2,
  CDF P1/P2/NF, Vaccine expiration P1/P2, Vaccine dispo P1/P2, Infirmier P1/P2,
  Carte de Supervision en KPI) ; syntaxe JS OK ; harnais vert (builders +
  moteur, données réelles zone Aba) : ligne ZS en tête partout, plus de carte
  « Détail par aire de santé » séparée, ✓/✗ au lieu de Faux/Oui, compteur et
  exports sur la table fusionnée. Push main → GitHub Pages success + Vercel
  rebuild, nouveau code vérifié en ligne. Fixtures out-zs exclues (.gitignore).
  Voir REQUEST_HISTORY du 31/07/2026.
