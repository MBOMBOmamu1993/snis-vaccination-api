# Historique des requêtes

## Format
Date :
Demande :
Fichiers concernés :
Action faite :
Résultat :

---

Date : 30/07/2026
Demande : « Tu vérifies tout ceci aussi » — rapport du 29/07 (session Claude) :
  détail AS (26 zones collectées non publiées, 11 fichiers en ligne), 2 actions
  prévues (corriger les erreurs frontend mkAsAppend + relais cloud du détail
  AS), état synchro ZS (pannes, verrou orphelin, rattrapage).
Fichiers concernés : docs/index.html, tools/mashako-sync/cloud/arbitre.mjs,
  export-zs-as.mjs, .github/workflows/mashako_cloud.yml,
  tools/mashako-sync/test-mkas-harness.mjs (nouveau), project_memory/*
Action faite : (1) Vérifications : 11 fichiers _AS.json en ligne confirmé ;
  workers AS relancés depuis (75 zones) ; verrou orphelin résorbé ; synchro ZS
  en cours via rattrapage. (2) Republication du détail AS en plein run
  (39b51f20c — 18 feuilles, 75 zones, 6 473 lignes). (3) Audit mkAsAppend sur
  les vrais JSON : 6 erreurs trouvées — libellés Supervision décalés (ordre
  réel b2..b6), pivots booléens rendus « 1 » au lieu du nom du critère, fuite
  technique SUPERVISION (« · 0 »), Livraison sans en-têtes antigènes et sans
  les _perc par antigène (junkés trop tôt, couleur _COLOR_new non capturée),
  colonne Centre de Santé vide, globales Approv/conditions répétées sur chaque
  bloc. (4) Corrections docs/index.html + harnais test-mkas-harness.mjs qui
  évalue les vraies fonctions sur les vrais JSON (contrôles : ordre libellés,
  pivots par nom, % par antigène 102%/1567%, pas de « · 0 », Centre seulement
  si rempli) — tout passe ; 5/5 blocs vm.Script ; push e3c7daf77. (5) Relais
  cloud AS : faisabilité confirmée (Actions activées, TABLEAU_COOKIES frais,
  workflow de secours éprouvé) ; canal « as » dans l'arbitre (preuve via
  Supervision_HZ_P1_AS.json du jour, grace 14h45, export reprenable 270 min +
  publication fusion même partielle) ; bail « as » dans export-zs-as.mjs
  (fail-open) ; cache Actions porte zs_as_ledger*.json + out-zs/views ;
  dry-run 3 canaux OK ; push f55486517.
Résultat : détail AS en ligne (75 zones) et rendu conforme à l'original ;
  la 5e chaîne a son filet cloud (PC prioritaire, reprise sur journal) ;
  mémoire à jour.

---

Date : 29/07/2026
Demande : Lire la mémoire, vérifier les actions du jour + l'état de
  synchronisation et backfill, procéder aux deux modifications prévues en
  mémoire (dates « Expiration la plus proche » + industrialiser le détail ZS
  dans la synchro quotidienne), et synchroniser le repo local avec le distant
  (« en retard ou vice versa », sans écrasement).
Fichiers concernés : mashako-sync/sync.mjs, backfill-periods.mjs,
  export-ant-zs-detail.mjs, probe-underlying-dates.mjs (nouveau),
  tools/mashako-sync/* (repo), docs/index.html, project_memory/*
Action faite : (1) État des lieux : synchro ANT du jour en cours (publiée
  ae25ba583, 33 feuilles/922 images) ; ledger ZS 519/519 (28/07) → backfill ZS
  débloqué ; backfills ANT (20h) et ZS (23h30) plantés au lancement Chrome
  depuis 2 jours. (2) Diagnostic git : retard simple de 112 commits, ZÉRO
  divergence (artefact shallow) ; modifs locales = bruit CRLF/périmé (aucune
  perte). Synchro complète : checkout docs/ en 2 passes (les lazy-fetches
  arrivent en petits paquets promisor malgré les erreurs affichées) + ff-merge
  → HEAD == origin/main == d0eeeacf4 ; 7 Go de tmp_pack nettoyés (disque 95 %
  → 93 %) ; git classique redevenu sain. Copie Documents non touchée (décision
  Felly). (3) Fix backfill (cause racine) : verrou écrit une fois pour un run
  de 10 h mais considéré périmé à 2 h → concurrents lançaient Chrome sur le
  profil occupé (crash exitCode 21) ; heartbeat du verrou 15 min dans sync.mjs
  + lancement protégé (3 essais, abandon propre) dans backfill-periods.mjs.
  (4) Détail ZS industrialisé : export-ant-zs-detail.mjs refactoré (fonction
  exportée réutilisant la session) + hook sync.mjs après fusion (mode ANT,
  non bloquant) → *_ZS.json régénérés et publiés à chaque synchro quotidienne.
  (5) Dates expiration : sonde probe-underlying-dates.mjs écrite
  (tabdoc/get-underlying-data) ; auth impossible à chaud (profil dates sans
  session Google, cookies récoltés périmés, DB Cookies verrouillée par Chrome)
  → sonde à lancer dès qu'un profil se libère ; patch rendu docs/index.html
  appliqué (config d: '_date_expiry_*' + sous-ligne date dans colornum,
  rétrocompatible, 5/5 blocs valides). (6) Outillage poussé vers
  tools/mashako-sync/ (2 modifiés + 4 nouveaux) — le veilleur cloud ne propage
  pas le code (surveillance seule).
Résultat : repo synchronisé et réparé ; crashs backfill résolus (effet dès le
  prochain créneau) ; détail ZS quotidien en place (effet au run du 30/07) ;
  rendu dates prêt — extraction des dates en attente d'une fenêtre de profil.

---

Date : 27/07/2026 (2) — reprise après interruption OpenCode (abonnement)
Demande : Finaliser la session : « enlever les images et remplacer avec
  tableau vivant, conforme à l'original » (Dispo_vaccins_ANT +
  Vaccine_expiration_ANT_P1) + « les modifs ne sont pas en ligne sur mon
  dashboard » + finir la synchro ZS.
Fichiers concernés : mashako-sync/export-ant-zs-detail.mjs,
  publish-zs-detail.mjs, validate-zs-batch.mjs, sync.mjs, probe-*.mjs,
  docs/index.html, project_memory/*
Action faite : (1) Diagnostic « pas en ligne » : les données ÉTAIENT publiées
  mais sur 2 boutons séparés en fin de liste ; les feuilles d'origine restaient
  images/vide. Le dashboard de Felly = snis-vaccination-dhis2.vercel.app
  (Vercel auto-déploie main). (2) Export détail ZS juillet (517 ZS dispo, 367
  expiration) + publication racine (1cebcf9f7). (3) Patch mkRender : greffe du
  file …_ZS sur Dispo_vaccins_ANT (si sans données — juillet) et
  Vaccine_expiration_ANT_P1 (toujours, CSV dégénéré = 1re feuille du dashboard
  _PAGE_TITLE) ; doublons masqués ; juin conserve son propre fichier + détail
  ZS en complément (fe792aeaa, testé sur les 2 meta.json, déployé Vérifié).
  (4) Chasse aux dates « Expiration la plus proche » : absentes du crosstab
  Excel (qui ne porte que % + couleur) et du pres-model bootstrap (fenêtres
  paginées) ; CSV direct des feuilles _TABLE = 404 (masquées) ; page 2026.2 =
  shell JS (tableauscraper.loads inutilisable) ; piste validée =
  tabdoc/get-summary-data/get-underlying-data (api.py) — NON aboutie : canal
  commandes en rafale de 410 (session migre de nœud). Leçons consignées :
  SID avec suffixe « -0:0 » obligatoire (x-session-id n'en a pas), RAZ des
  jetons avant navigation, adoption du global-session-header des réponses 410.
  (5) Validation ZS : gel >60 min (fetch .csv sans garde-temps sur
  Vaccine_dispo_HZ_P2) → AbortController 90 s + reprise incrémentale
  (zs_batch_verdicts.json) ; verdicts partiels : Résumé/Heatmap/CarteSup/
  Dispo_HZ_P1 COLLAPSE (multi-valeurs = 1re ZS seule), Supervision_P1/P2 OK.
  (6) sync.mjs : liste blanche ZS lue dans les verdicts + pullZsLegacy
  parallélisé ×3 (≈30 min/feuille collapse au lieu de 1,5-2 h).
Résultat : dashboard conforme (tables vivantes sur les feuilles d'origine,
  images remplacées) ; dates d'expiration en attente (piste tabdoc à retenter) ;
  validation ZS relancée ; synchro ZS prête à lancer avec liste blanche.

---

Date : 27/07/2026
Demande : Prendre la relève sur la synchro Mashako qui échoue depuis 2 jours
  (synchro qui tourne avec beaucoup d'échecs, backfills en attente) + finir la
  chaîne d'export Excel « tableau croisé » (sonde en échec à la dernière étape)
  + Dispo_vaccins_ANT en tableau vivant éclaté par zone de santé (pas des
  images) + Vaccine_expiration_ANT_P1 vide dans le dashboard alors que le
  Tableau original a des données + consigner en mémoire que certaines vues ne
  donnent leurs vraies données Excel qu'avec un filtre (pas « All »).
Fichiers concernés : mashako-sync/sync.mjs, backfill-periods.mjs,
  publish-cache.mjs, probe-crosstab-http.mjs, probe-multizs.mjs,
  probe-multicsv.mjs, probe-param.mjs, test-batch.mjs, repair-sheets.mjs,
  project_memory/*
Action faite : (1) Diagnostic complet : 7 tentatives/0 succès le 26/07 — crash
  ENOENT à la publication (fusion anti-perte référençant des PNG absents du
  disque), garde-fou 180 min trop court, morts silencieuses (veille PC + runs
  tués par lancement non détaché), verrous backfill/synchro, gh 400 transitoire.
  (2) Correctifs sync.mjs : réutilisation des SHA de blobs en ligne (anti-ENOENT)
  + garde absolue base_tree (protection zs/ et archives) ; garde-fou 300 min ;
  handlers de crash journalisés ; retry ×5 sur appels API GitHub ; BATCHING
  MULTI-VALEURS (_SELECTED_location_level=A,B,C — 1 requête par feuille ANT,
  paquets de ~100 ZS) avec LISTE BLANCHE validée par run réel (les feuilles
  pivots/classements/cartes COLLAPSENT en groupé → export unitaire conservé).
  (3) Chaîne crosstab Excel réparée et validée : 410 au téléchargement (en-têtes
  de routage manquants) + liste de feuilles fausse (hash SPA non rechargé →
  passer par about:blank ; Global-Session-Header lu dans les en-têtes de
  RÉPONSE /vizql/) ; sessions « jeunes » (~10-40 s, sans rendu canvas) ;
  feuilles masquées _TABLE_… = détail ZS/AS (Dispo : 514 ZS × 16 antigènes ;
  Expiry : % + couleur × 8 antigènes) inaccessibles en CSV.
  (4) Publication ANT Juillet complète (a1aab66cf : 30 feuilles, 21 avec
  données — Livraison_P1 950, CDF_Problèmes 511, Ranking 51 réparés en
  unitaire) ; backfill restructuré (1 mois/run + groupé).
Résultat : synchro ANT réparée et publiée ; méthode Excel crosstab validée de
  bout en bout ; en cours : détail ZS Dispo/Expiration + synchro ZS + backfills.

Date : 20/07/2026 (journée)
Demande : Vente de codes IA intégrée sans WhatsApp (commande → approbation →
  dépôt M-Pesa → capture → livraison instantanée) + alertes téléphone + e-mails +
  OCR IA des captures + rapport des ventes + cron quotidien + Kimi K3 dans le
  dashboard ; puis finalisation avec clé Kimi payée.
Fichiers concernés : cloudflare-worker/worker.js, cloudflare-worker/wrangler.toml,
  cloudflare-worker/JOUR-J-COMMANDES.md, docs/index.html
Action faite : Système de commande complet (KV ord:/proof:/sale:), console /admin
  enrichie, /admin/rapport, cron 17h UTC, notifications Telegram/CallMeBot/Brevo,
  proxy Kimi /kimi/v1/chat/completions ; dashboard : fournisseur Kimi (kind openai,
  kimi-k3/k2.7-code/k2.6) à côté d'Ollama et Claude ; fix e-mail Brevo (name vide) ;
  fix CallMeBot (numéro assaini) ; KIMI_API_KEY posée et testée (kimi-k3 répond) ;
  commandes de test purgées du KV.
Résultat : tout fonctionne SAUF WhatsApp CallMeBot (403 — apikey à régénérer par
  l'utilisateur). E-mail Brevo OK (201). Kimi K3 OK via proxy. Tests worker 19/19.

---

Date : 20/07/2026 (soir)
Demande : Pourquoi les notifications de commande n'arrivent pas + finaliser KIMI
  + push/merge en production. (Session précédente bloquée par maintenance.lock
  git + fetch partiel qui plafonnait sur réseau instable.)
Fichiers concernés : cloudflare-worker/worker.js, project_memory/*
Action faite : Diagnostic via KV notif:last (--remote !) : CallMeBot 403, Brevo
  201. Bissection CallMeBot → cause racine : le WAF rejette la réf « (CMD-xxx) »
  entre parenthèses sur sa propre ligne (403) — pas l'apikey. Nouveau format
  testé en direct (210 ok) : résumé client + offre + montant + lien /admin, Réf
  en ligne. E-mails admin par commande supprimés (demande utilisateur : e-mail =
  rapport quotidien uniquement) ; e-mails clients conservés. Secrets CallMeBot
  reposés. E2E validé : commande → WhatsApp ok:true ; capture PNG → OCR Kimi K3
  exact (montant/opérateur/réf/date) ; image stockée octet pour octet dans KV.
  Worker redéployé (cron 0 17 * * *). Kimi K3 testé en stream via proxy (SSE
  reasoning_content OK). Scripts inline index.html : 4/4 valides. Push : clone
  partiel + réseau instable → commit construit par-dessus origin/main sans
  télécharger les 2000 blobs de données (read-tree + add fichiers modifiés +
  commit-tree + update-ref + push fast-forward).
Résultat : notifications WhatsApp FONCTIONNELLES, Kimi K3 opérationnel (chat +
  vision OCR captures), production à jour (worker + GitHub Pages).

---

Date : 22/07/2026
Demande : Doter TOUS les modèles IA de l'onglet « Génération des analyses et
  rapports » (Kimi K3 déjà bon, l'étendre aux autres) de la capacité d'analyser
  et rapporter les données et indicateurs de TOUS les programmes DHIS2 (pas
  seulement le PEV), pour toutes les années et toutes les entités demandées.
  Ne pas utiliser les canevas DV/DVD (abandonnés, sans données) — se concentrer
  sur services primaires/secondaires, dataElements, indicateurs et program
  indicators.
Fichiers concernés : docs/index.html (prompt système iaSystem + accueil/chips IA),
  project_memory/*
Action faite : Exploration complète du DHIS2 national via le proxy
  (pev-ia-proxy/dhis2/api) : 42 datasets (A primaires, B secondaires+PEV, C SIGL,
  D hôpital, E banque de sang, F BCZ, J tertiaire + PNLP/PNLT/PNLS/PNSR/NUT/PNSM/
  PNSOV/PNRBC/PNSBD/PNEL/PNIRA/PROSANI/IDSR hebdo/campagnes), 9 482 dataElements,
  1 570 indicateurs (66+ groupes préfixés 1a…2n + IDSR/cartes de score/campagnes),
  76 programIndicators (uniquement canevas DV à événements = abandonnés, sans
  données). Conventions de calcul vérifiées en direct : formules #{DE(.COC)} et
  R{dataset.ACTUAL/EXPECTED_REPORTS}, types (Percentage ×100, Per 100 000, Number),
  dénominateurs population WLSKVyA8LoY × coefficients (0,04 / 0,0349 / 0,036 /
  0,149 / 0,113 / 0,057), indicateur official interrogeable dans analytics
  (1 par appel — lourd, repli sur dataElements si 5xx), REPORTING_RATE par
  dataset + indicateurs globaux L3KahLq4YFo (complétude) / bnyWMbmL5IR
  (promptitude), éditions 2025 avec UID d'éléments identiques (analytics couvre
  toutes les années sans couture), org : 1 RDC / 26 provinces / 519 ZS / 10 405
  AS / 25 672 FOSA, surveillance IDSR hebdo (pe:2026W12, LAST_12_WEEKS OK).
  Prompt système commun à TOUS les modèles (Ollama/Kimi/Claude) réécrit :
  portée tous programmes + section « Tous les programmes du SNIS — repères
  vérifiés » + formules généralisées (taux d'attaque, létalité…) ; accueil et
  exemples (chips) multi-programmes. Syntaxe JS validée (node --check, 13 blocs).
  Commit construit par-dessus origin/main (a6b133933, 14 commits d'avance :
  canevas + admin secret) via read-tree + hash-object + commit-tree + update-ref.
Résultat : tous les modèles IA peuvent analyser/rapporter tous les programmes
  DHIS2 (PEV inclus) sur n'importe quelle année/entité ; production à jour.

---

Date : 22/07/2026 (2)
Demande : Fiabiliser le function-calling de l'assistant IA : ids natifs des
  tool_calls conservés + reasoning_content renvoyé à Kimi K3 + appariement
  explicite par tid ; filet de sécurité iaSanitizeHistory + retry auto sur
  erreur 400 tool_call ; compétences cartographie : helper ctx.geo (GeoJSON
  DHIS2) + recette cartes choroplèthes + score qualité des données dans le
  prompt ; format 'carte' (HTML interactif) dans generer_rapport. (Session
  précédente plantée au milieu — reprise complète et finalisation.)
Fichiers concernés : docs/index.html (iaReadOpenAIStream, iaReadAnthropicStream,
  iaMsgsToOpenAI, iaMsgsToAnthropic, iaSanitizeHistory, iaCallAPI, iaAgentLoop,
  iaGeo, iaRunTool, iaMissingAwait, iaSystem, IA_TOOLS/generer_rapport,
  iaRepCarte, iaGenReport), project_memory/*
Action faite : ids natifs (Kimi call_*, Anthropic toolu_*) propagés du flux SSE
  à l'historique interne puis aux conversions ; messages tool dotés de
  tool_call_id (appariement explicite, repli positionnel pour les historiques
  sans tid) ; reasoning_content renvoyé à Kimi (sinon 400 « must be passed
  back ») ; iaSanitizeHistory (copie : tool_calls incomplets dégradés en texte,
  réponses partielles/orphelines repliées en note user, vérification par
  ensembles de tid) + retry unique dans iaCallAPI sur 400 tool_call ; ctx.geo
  (organisationUnits.geojson, parent LEVEL-1/UID) + recette carte Plotly
  choropleth (featureidkey properties.id) + recette score qualité (0,5
  complétude + 0,3 promptitude + 0,2 cohérence) dans iaSystem ; generer_rapport
  format 'carte' → iaRepCarte : page HTML autonome (Plotly 2.35.0 CDN, figures
  navigables non rasterisées, GeoJSON embarqué, garde/tableaux/signature).
  Tests : 4/4 blocs script valides (vm.Script), 26 scénarios (conversions,
  SSE fragmenté 2 appels parallèles, sanitize, détection 400) + génération
  carte HTML (JS injecté valide) — tous OK. Push : API GitHub directe
  (blob → tree → commit → ref) car git local cassé (clone partiel).
Résultat : function-calling fiabilisé pour Kimi K3/Claude/Ollama, cartes
  interactives DHIS2 disponibles (chat + fichier autonome), production à jour.
---

Date : 31/07/2026
Demande : « Rapport ZS » (Plan MASHAKO 3.0) : les visuels ne correspondent pas
  au Mashako original — « je n'ai jamais demandé deux tableaux (ZS + détails
  aire de santé) », une seule ligne de synthèse ZS au-dessus, la situation des
  aires de santé comme dans l'original (11 captures Tableau fournies vs 15
  captures du dashboard).
Fichiers concernés : docs/index.html, tools/mashako-sync/test-mkas-harness.mjs,
  tools/mashako-sync/out-zs/views/ (fixtures), project_memory/*
Action faite : (1) Exploration en essaim : code (IIFE Mashako, mkDrawView +
  mkAsAppend = le double tableau dénoncé), données (branche mashako-data : les
  17 _AS.json existent ; HZ absent pour Livraison/CDF/Infirmier → agrégation
  client ; Vaccine_dispo_HZ_P1.json principal = 404 → rendu 100 % crosstab),
  captures originales (spec par feuille). (2) Refonte docs/index.html : mapping
  MK_AS_FILE (Seances_→Sances_, Taux_d_abandon_→Tauxdabandon_, bug 404),
  moteur mkZsRender (une carte : bandeau + synthèse ZS en tête + table AS,
  recherche/compteur/pagination sur les lignes AS, exports Excel/PPTX sur le
  visuel fusionné, fallback archives sans _AS), MK_ZS_CFG + builders par
  feuille (libellés exacts de l'original ; booléens → ✓/✗ ; pastilles %
  (n/d) ; alertes expiration multi-lignes ; dispo semaines 0-1 rouge/2+ vert ;
  Carte de Supervision = 2 KPI + note critères). mkAsAppend conservé uniquement
  pour les feuilles ZS non reprises. Page ANT intacte. (3) Harnais réécrit :
  évalue les vraies fonctions sur les vrais JSON (zone Aba) — 30+ contrôles,
  dont moteur complet avec DOM simulé (plus de carte « Détail par aire de
  santé », compteur AS, _mkXls fusionné).
Résultat : tous les contrôles passent ; syntaxe des 5 blocs script OK.
  Commit 29b7c518e poussé sur main (31/07) → GitHub Pages success ET Vercel
  rebuild, nouveau code vérifié en ligne sur les deux. Fixtures out-zs
  exclues du versioning (.gitignore, regénérables depuis mashako-data).
  Note : workflow « synchro de secours (cloud) » en échec préexistant.

---

Date : 31/07/2026 (2)
Demande : « La carte de supervision n'est pas visible » — utiliser le geojson
  des aires de santé de la RDC (partagé dans Downloads, > 50 Mo) pour extraire
  les polygones par ZS et présenter la carte comme l'original ; et « afficher
  la feuille Vaccine_dispo_HZ_P1 » absente du dashboard alors qu'elle existe
  dans le classeur original.
Fichiers concernés : docs/index.html, docs/data/mashako/geo_as_points.json,
  tools/mashako-sync/extract-as-geo.mjs, tools/mashako-sync/test-mkas-harness.mjs,
  project_memory/*
Action faite : (1) Onglet Vaccine_dispo_HZ_P1 : mkRender filtrait les vues sans
  file ni image (vue trop grosse pour Tableau → file:null) ; filtre assoupli
  pour les feuilles de MK_ZS_CFG → onglet visible, rendu 100 % crosstab.
  (2) Carte : geojson RDC_aires_de_sante.geojson (9 573 MultiPolygones,
  id = UID DHIS2) joint à docs/data_as/ou_map_as.json par UID (Org3 = ZS) —
  les 31 préfixes 2 lettres n'étant pas uniques — puis centroïdes par ZS au
  format noms courts Mashako dans docs/data/mashako/geo_as_points.json
  (276 Ko, 517 ZS, Aketi = 19 AS comme l'original). Rendu Vega-Lite
  (mkEmbedMap) : contour ZS (topojson MK_TOPO_URL) + croix « + » colorées
  (vert qualité b6 ; 2 critères jaune ; 1 orange ; 0 rouge ; non supervisée
  gris) + étiquettes « … Centre de Santé » + légende, KPI à gauche.
Résultat : harnais vert (contrôles carte + dispo ajoutés), syntaxe 5/5 blocs
  OK. Déployé sur main (Pages + Vercel automatiques).
---

Date : 01/08/2026 (Assistant IA — canevas PPTX)
Demande : corriger les skills de l’Assistant IA DHIS2 après une génération non conforme (mise en forme altérée, score qualité absent, graphiques/diapositives ajoutés), en conservant strictement les diapositives hors DHIS2 comme PTF, puis prétester Kimi avant validation.
Action faite : audit XML des PPTX original et `_MAJ` (49 vs 54 diapositives ; D35 sans tableau ; PTF paginé ; slides/graphiques ajoutés), ajout d’un mode strict transactionnel à `modifier_presentation`, création d’un tableau DrawingML éditable sur la forme ciblée, verrouillage des modifications hors périmètre, contrôles finaux, barème DPS `ctx.scoreQualiteDps`, consignes système non ambiguës et garde-fou de durée.
Résultat : prétest moteur PASS (49 diapositives, seule D35 diffère, D6 identique, tableau score visible et sans débordement) ; prétest réel Kimi K3 PASS (`requete_dhis2` → `requete_dhis2` → `modifier_presentation`), sans opération interdite. Changements locaux, non déployés.
