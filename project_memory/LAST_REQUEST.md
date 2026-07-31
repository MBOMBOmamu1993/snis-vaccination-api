# Dernière requête

Date : 31/07/2026 (2)
Dernière demande : Rendre la carte de supervision visible (elle ne l'était
  pas) en s'aidant du geojson des aires de santé de la RDC partagé
  (Downloads\RDC_aires_de_sante.geojson, > 50 Mo) pour extraire les polygones/
  points par ZS et présenter la carte comme l'original ; et réafficher la
  feuille Vaccine_dispo_HZ_P1, absente du dashboard alors qu'elle existe dans
  le classeur original.
Fichiers concernés : docs/index.html (filtre mkRender, mkZsGeoLoad,
  mkZsCartePoints, mkZsCarteDraw, layout mkZsRenderCarte),
  docs/data/mashako/geo_as_points.json (nouveau, 276 Ko),
  tools/mashako-sync/extract-as-geo.mjs (nouveau),
  tools/mashako-sync/test-mkas-harness.mjs (contrôles carte + dispo),
  project_memory/*
Statut : TERMINÉ — onglet Vaccine_dispo_HZ_P1 réaffiché (rendu 100 %
  crosstab) ; carte de supervision = contour ZS + croix colorées par Centre de
  Santé (vert qualité / jaune 2 critères / orange 1 / rouge 0 / gris non
  supervisée) avec étiquettes et légende ; 517 ZS × 9 573 points extraits
  (Aketi = 19 AS comme l'original) ; harnais vert, syntaxe 5/5 blocs OK.
  Voir REQUEST_HISTORY du 31/07/2026 (2).
