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

État 31/07/2026 (2) — CARTE DE SUPERVISION VISIBLE + ONGLET DISPO P1 RÉAFFICHÉ :
- Onglet Vaccine_dispo_HZ_P1 : absent de la barre de feuilles car filtré par
  mkRender (file:null + image:null dans zs/meta.json — vue trop grosse pour
  Tableau). Filtre assoupli : toute feuille de MK_ZS_CFG est conservée ; le
  rendu se fait 100 % crosstab _AS (déjà validé).
- Carte (feuille Carte de Supervision_HZ) : contour ZS + croix « + » par
  Centre de Santé avec étiquette, comme l'original. Source geo = le geojson
  partagé par Felly (C:\Users\felly\Downloads\RDC_aires_de_sante.geojson,
  59,8 Mo, 9 573 MultiPolygones, id=UID DHIS2). Jointure par UID via
  docs/data_as/ou_map_as.json (Org3 = ZS) — le préfixe 2 lettres n'est PAS
  unique (31 préfixes). Script tools/mashako-sync/extract-as-geo.mjs
  (centroïde du plus grand anneau, noms courts Mashako, 4 décimales) →
  docs/data/mashako/geo_as_points.json (276 Ko, 517 ZS, 9 573 points ;
  Aketi = 19 AS comme l'original).
- Rendu : mkZsCartePoints (vert = qualité b6 ; sinon critères b3/b4/b5 :
  2 jaune, 1 orange, 0 rouge ; AS sans ligne → gris) + mkZsCarteDraw
  (Vega-Lite via mkEmbedMap : geoshape contour ZS du topojson MK_TOPO_URL +
  point shape cross coloré + text étiquettes « … Centre de Santé » + légende
  discrète) ; layout KPI à gauche / carte à droite ; repli note si pas de
  coordonnées. Harnais : contrôles mkZsCartePoints + couverture dispo OK.

État 31/07/2026 — RAPPORT ZS REFAIT À L'IDENTIQUE DE L'ORIGINAL (29b7c518e, EN PRODUCTION) :
- Demande Felly : les feuilles du Rapport ZS (Plan Mashako 3.0) ne ressemblaient
  pas au classeur Tableau original (double tableau « synthèse ZS » + carte
  séparée « Détail par aire de santé », jamais demandé). Spec = 11 captures
  du classeur original (ZS Aketi, juin 2026).
- docs/index.html : nouveau moteur mkZsRender — UNE carte par feuille =
  bandeau bleu (titre/sous-titre/chips de l'original) + ligne/bloc de synthèse
  ZS EN TÊTE + tableau des aires de santé. Ligne ZS épinglée dans la même
  table quand les colonnes coïncident (Livraison, Expiration, CDF_NF,
  Infirmier), mini-bloc avec son propre en-tête juste au-dessus sinon
  (Supervision, CDF_P1, Dispo) — comme Tableau. MK_ZS_CFG (17 feuilles :
  Supervision P1/P2/P3, Séances P1/P2, Taux d'abandon P1/P2, Livraison P1/P2,
  CDF P1/P2/NF, Vaccine expiration P1/P2, Vaccine dispo P1/P2, Infirmier
  P1/P2, Carte de Supervision) + MK_ZS_BUILD par kind. Feuilles « ZS seule »
  (Supervision_P2, CDF_P2, Vaccine_dispo_P2) = bloc synthèse uniquement.
- Détails fidélité : booléens → ✓ vert / ✗ rouge (fini « Faux »/« Oui »/« 0 ») ;
  pastilles % avec fraction (n/d) ; alertes expiration multi-lignes (« Alerte
  expiration dans N jours (M doses) ») ; dispo en semaines 0-1 rouge / 2+ vert ;
  titres exacts (« Suivi de la réalisation de la supervision des aires de
  santé », etc.) ; P2 sans « — suite ».
- Données : ligne ZS depuis _ROLE:'HZ' du crosstab _AS, sinon agrégée côté
  client (% d'aires conformes — Livraison/CDF/Infirmier n'ont pas de HZ) ;
  Vaccine_dispo_HZ_P1 rendu 100 % crosstab (sa vue principale = 404) ;
  fix 404 Séances/Taux d'abandon via MK_AS_FILE (Seances_→Sances_,
  Taux_d_abandon_→Tauxdabandon_) dans mkAsLoad.
- Préservé : page ANT, filtres en cascade, périodes/archives (note discrète
  si pas de _AS archivé), onglets pilotés par meta, exports Excel/PPTX (sur le
  visuel fusionné via box._mkXls), lightbox. mkAsAppend ne tourne plus que
  pour les feuilles ZS non reprises.
- Validation : 5/5 blocs script vm.Script OK ; harnais test-mkas-harness.mjs
  RÉÉCRIT (évalue MK_ZS_BUILD/mkZsRender sur les vrais JSON, zone Aba —
  30+ contrôles dont moteur avec DOM simulé : plus de carte « Détail par aire
  de santé », compteur AS, export fusionné) — tout vert. Fixtures
  tools/mashako-sync/out-zs/ NON versionnées (.gitignore ; regénérables depuis
  la branche mashako-data, commande dans l'en-tête du harnais).
- Déploiement : push main → Pages success + Vercel rebuild, nouveau code
  vérifié en ligne sur les deux (grep mkZsRender).
- ⚠️ À SURVEILLER (préexistant, sans rapport) : le workflow planifié
  « Mashako 3.0 — synchro de secours (cloud) » échoue (runs 12h16/13h26).

État 30/07/2026 — DÉTAIL AS : REPUBLICATION + FRONTEND CORRIGÉ + RELAIS CLOUD :

A) Vérification du rapport du 29/07 (session Claude) :
- Confirmé : 11 fichiers _AS.json seulement en ligne (la fusion de 26 zones
  n'avait jamais été republiée après la coupure réseau du 29/07 07:06) ;
  workers AS relancés depuis (75 zones couvertes au 30/07 02:00) ; verrou
  orphelin résorbé ; synchro ZS partie via rattrapage.
- Republication faite en plein run (prévue pour) : 39b51f20c — 18 feuilles,
  75 zones (vs 11), 6 473 lignes.

B) Erreurs frontend mkAsAppend (demande du 29/07, « on corrigera demain ») —
   TOUTES corrigées dans docs/index.html (e3c7daf77) :
1. Libellés Supervision décalés : MK_AS_LABELS réécrit dans l'ordre RÉEL des
   blocs (b2=taux, b3=localisation, b4=cohérence, b5=durée, b6=qualité).
2. Pivots booléens affichés « 1 » : une colonne dont toutes les valeurs ∈
   {'1',''} est un pivot → on affiche SON NOM (Oui/Non, Faux/Vrai, Qualité…).
3. Fuite SUPERVISION (« · 0 » partout) → filtrée dans mkAsJunk.
4. Livraison : en-têtes antigènes dans l'ordre réel (b2..b14) + globales
   (Approv/conditions) dédupliquées (1re occurrence seulement) + _perc_…
   conservés AVANT le junk → % par antigène affichés (ratio >1 → 102%, 1567%…).
5. Couleurs Livraison : suffixe _COLOR_new (et pas seulement _COLOR) capturé.
6. Colonne « Centre de Santé » affichée seulement si ≥1 ligne a une valeur.
- Validations : 5/5 blocs vm.Script + harnais test-mkas-harness.mjs (dans
  tools/mashako-sync/) qui évalue les VRAIES fonctions du fichier sur les
  vrais JSON (Supervision/Livraison/Infirmier × Aba/Aketi) — tous contrôles OK.

C) Relais cloud du détail AS (5e chaîne, la seule sans filet) — f55486517 :
- Faisabilité confirmée : Actions activées, secret TABLEAU_COOKIES frais
  (reconnecter.mjs republie), workflow mashako_cloud.yml déjà éprouvé
  (arbitre + lease + seed-profile + alertes).
- Canal « as » dans cloud/arbitre.mjs : preuve de publication via
  zs/views/Supervision_HZ_P1_AS.json (generated_at du jour, toute heure) ;
  grace 14h00+45 min (le PC a la priorité après sa passe ZS) ; lancer =
  export-zs-as.mjs (MASHAKO_MINUTES=270, reprenable) + publish-zs-as.mjs
  --fusion (même partiel — chaque zone publiée est gagnée).
- Bail « as » dans export-zs-as.mjs (surveiller + bailAutre, fail-open) :
  PC et cloud ne tournent JAMAIS en même temps.
- Cache Actions : zs_as_ledger*.json + out-zs/views (journal de reprise porté
  entre exécutions — proposition du 29/07).
- Dry-run validé sur les 3 canaux : ANT attente 07h00, ZS attente 10h30,
  AS « déjà publié aujourd'hui (75 zones) ».

État 29/07/2026 — GIT LOCAL RÉPARÉ + FIX BACKFILL + DÉTAIL ZS INDUSTRIALISÉ :

A) Synchro git (repo principal C:\Users\felly\snis-vaccination-api) :
- Le repo local était simplement EN RETARD de 112 commits (zéro divergence —
  prouvé après `fetch --deepen=400`, HEAD 4e742c260 = ancêtre de origin/main).
  Les « 242 commits d'avance » affichés = artefact du clone shallow.
- Toutes les « modifications » locales étaient du BRUIT : worker.js et
  docs/index.html identiques au distant modulo CRLF ; scripts/aggregate_*.py =
  versions PÉRIMÉES (le distant a le RECO) ; docs/data*.gz = régénérations
  locales (gzip non déterministe). AUCUNE perte à la synchro.
- Le lazy-fetch massif pend, MAIS il télécharge en fait en PETITS paquets
  promisor (~22/min) pendant que checkout affiche ses erreurs — procédure qui
  marche : laisser la 1re passe de `git checkout -- docs/` finir (rc=255 avec
  erreurs, mais tous les blobs arrivent), puis RELANCER la même commande
  (2e passe locale, ~12 s). ff-merge ensuite trivial.
- Résultat : HEAD == origin/main == d0eeeacf4, arbre propre, git classique
  redevenu sain (blobs locaux → plus de pendaison). 7 Go de tmp_pack résiduels
  supprimés (disque était à 95 %).
- La copie Documents\snis-vaccination-api (retard 705+) n'est PAS touchée
  (décision Felly : pas d'écrasement).

B) Fix backfill (cause racine des crashs Chrome 27-29/07) :
- sync.mjs écrivait son verrou UNE fois au démarrage ; or le backfill (et toute
  synchro concurrente) le considère périmé après 2 h → pendant un run de ~10 h,
  le backfill de 20h/23h30 lançait Chrome sur le profil occupé → crash
  launchPersistentContext (exitCode 21) non intercepté.
- Correctif : heartbeat du verrou toutes les 15 min dans sync.mjs (netoyé dans
  finally) + lancement Chrome protégé dans backfill-periods.mjs (3 essais ×
  30 s, puis abandon PROPRE code 3, verrou libéré). Backfills restants : ANT
  2026-04→2025-07 (1 mois/soir 20h), ZS débloqué (ledger 519/519 le 28/07).

C) Détail ZS industrialisé (NEXT_STEP #4 du 27/07 — FAIT) :
- export-ant-zs-detail.mjs refactoré : cœur exporté
  `exportAntZsDetail(page, {month, year, log})` (réutilise la session Playwright
  fournie) + entrée CLI autonome conservée.
- sync.mjs : hook en mode ANT après la fusion anti-perte, avant meta.json —
  régénère out/views/Dispo_vaccins_ZS.json + Vaccine_expiration_ZS.json à
  CHAQUE synchro quotidienne et les publie dans le même commit (remplace
  l'entrée fusionnée stale ; échec non bloquant). Effet à la synchro du 30/07.
- Outillage poussé vers tools/mashako-sync/ du repo (le veilleur cloud ne
  propage PAS le code — surveillance seule) : sync.mjs, backfill-periods.mjs
  modifiés + export-ant-zs-detail.mjs, probe-underlying-dates.mjs,
  validate-zs-batch.mjs, publish-zs-detail.mjs ajoutés.

D) Dates « Expiration la plus proche » (demande 27/07 — EN COURS) :
- Sonde probe-underlying-dates.mjs écrite (tabdoc/get-underlying-data,
  includeAllColumns, visualIdPresModel — piste §D du 27/07).
- Blocage auth : browser-profile-dates sans session (Google SSO vit dans le
  profil principal seul) ; cookies-tableau.json (récolte 00:48) déjà périmé ;
  DB Cookies du profil principal non copiable à chaud (verrou Chrome,
  robocopy /B sans privilège). → Sonde à lancer sur browser-profile dès qu'une
  synchro libère le profil (fenêtre entre deux runs).
- Patch rendu docs/index.html DÉJÀ EN PLACE (rétrocompatible) : config
  Expiration → `d: '_date_expiry_<AG>'` sur les 8 antigènes ; cellHtml colornum
  affiche la date en sous-ligne 10 px sous le % si présente. Sans données date
  → rendu identique. 5/5 blocs script valides (vm.Script).

État 27/07/2026 — SYNCHRO MASHAKO RÉPARÉE + EXPORT EXCEL CROSSTAB (déployé) :

A) Synchro Mashako (snis-vaccination-api, onglet Plan Mashako 3.0) :
- La synchro tourne en LOCAL sur le PC (C:\Users\felly\mashako-sync) — le
  workflow GitHub est en veille (PAT désactivés par l'admin axdata le 24/07).
  5 tâches planifiées : ANT 07:00 (+rattrapages), ZS 10:30, backfills ANT 20:00
  et ZS 23:30, rattrapage au démarrage (catchup.mjs).
- PUBLICATION ANT Juillet COMPLÈTE (a1aab66cf) : 30 feuilles, 21 avec données
  vivantes, 286 images par antenne. Référence corrigée : Livraison_P1 950
  lignes, CDF_Problèmes 511, Ranking 51.
- Causes d'échec d'avant, toutes corrigées dans sync.mjs :
  1. Crash ENOENT à la publication (fusion anti-perte référençait des PNG
     absents du disque local) → réutilisation des SHA des blobs déjà en ligne
     + GARDE ABSOLUE : publication annulée si base_tree illisible (protection
     zs/ et archives periods/).
  2. Garde-fou 180 min toujours atteint → 300 min + phase data ~3× plus
     rapide grâce au batching.
  3. Morts silencieuses (4-5/jour) → handlers unhandledRejection/
     uncaughtException qui journalisent avant de mourir.
  4. Appels API GitHub qui hoquettent (« malformed request » 400 transitoire,
     vérifié rejouable à la main) → retry ×5 avec pause croissante dans
     sync.mjs et publish-cache.mjs.
- BATCHING MULTI-VALEURS (découverte du 27/07) : l'export .csv accepte
  PLUSIEURS valeurs de filtre séparées par des virgules
  (_SELECTED_location_level=A,B,C) → UNE requête par feuille pour les 51
  antennes (~30 s au lieu de ~7 min) ; paquets de ~100 ZS côté ZS (couverture
  complète en 1 run au lieu de ~7 nuits). Attribution de la colonne Antenne
  par le contenu (colonne « Antenne En », sinon carte ZS→antenne
  zs_ant_map.json construite depuis les FILTER_VALUES du classeur ZS).
  ⚠ LISTE BLANCHE : certaines feuilles COLLAPSENT en groupé (livraisons
  pivots, classements, cartes — ex. Livraison_P1 : 30 lignes vides au lieu de
  950) → export unitaire conservé pour elles (BATCH_OK dans sync.mjs).
- Backfill : 1 mois par exécution (fini les verrous de 27 h) + groupé
  multi-valeurs → ~20 min/mois. ANT : 10 mois à rattraper (2026-04→2025-07),
  ~10 soirs à 20:00. ZS : déblocage automatique du seuil 500/519 dès la 1re
  synchro ZS complète.

B) Export Excel « tableau croisé » Tableau (chaîne validée, probes
   probe-crosstab-http.mjs / probe-multizs.mjs / probe-multicsv.mjs) :
   ① POST …/commands/tabsrv/export-crosstab-server-dialog → liste les
     feuilles réelles + sheetdocId (découverte des feuilles masquées _TABLE_…)
   ② POST …/commands/tabsrv/export-crosstab-to-excel-server (sheetdocId) →
     clé de fichier temporaire
   ③ GET …/tempfile/sessions/{sid}?key=…&keepfile=yes&attachment=yes → .xlsx
   Clés techniques :
   - global-session-header = nœud VizQL (sinon 410 Gone) : lu dans les
     EN-TÊTES DE RÉPONSE /vizql/ (pas besoin de capturer les requêtes).
   - Session « jeune » suffisante (~10-40 s) : pas besoin d'attendre le rendu
     canvas (~4 min) pour lancer les commandes.
   - x-tsi-active-tab / la vue de la session détermine la liste de feuilles :
     réinitialiser la capture à chaque navigation (le hash ne recharge pas la
     page — passer par about:blank).
   - Filtres multi-valeurs acceptés (virgules) sur commands ET sur .csv.
   - ⚠ DEMANDE FELLY 27/07, À RETENIR : certaines vues ne livrent leurs
     VRAIES données Excel QUE lorsqu'on les exporte AVEC UN FILTRE appliqué
     (mono ou multi-valeurs) — SANS filtre (« All »), le tableau de bord
     retombe sur sa localisation par défaut (ex. Aketi seule) au lieu de
     tout donner. Toujours exporter avec un filtre explicite.
   - Le détail par ZS/Aire de Santé vit dans les feuilles masquées _TABLE_…
     (ex. _TABLE_vaccine_av_ANT : 514 ZS × 16 antigènes — semaines de stock +
     dispo « Vrai » ; _TABLE_vaccine_expiry_ANT_P1 : % alerte expiration +
     couleur par ZS × 8 antigènes) alors que le CSV ne donne que la synthèse
     antenne. Le CSV .csv ne suffit PAS pour le détail ZS/AS → crosstab Excel.

État 27/07/2026 (2) — DÉTAIL ZS EN LIGNE + CONFORMITÉ DASHBOARD (déployé) :

C) Feuilles « vivantes » Dispo/Expiration (demandes Felly 27/07) :
- export-ant-zs-detail.mjs : exporte le détail ZS de Dispo_vaccins_ANT et
  Vaccine_expiration_ANT_P1 via la chaîne crosstab (1 session filtrée 51
  antennes par dashboard) → out/views/Dispo_vaccins_ZS.json (517 ZS × 14
  antigènes : semaines de stock + flag Vrai/Faux) et Vaccine_expiration_ZS.json
  (juin 435 ZS / juil. 367 ZS × 8 antigènes : % alerte + couleur green/red).
  Carte ZS→antenne : zs_ant_map.json (517 ZS).
- Publié : juin (periods/2026-06, 7a72202c4 via publish-zs-detail.mjs) ET
  juillet (racine + periods/2026-07, 1cebcf9f7 via publish-cache.mjs — il
  publie tout out/views/, donc y déposer les *_ZS.json suffit).
- ⚠ CONSTATS CLÉS : (1) Dispo_vaccins_ANT n'a AUCUN CSV pour juillet (Tableau
  répond « aucune donnée pour cette période », 51/51 vides — la période n'est
  pas encore consolidée) → seules les images + le détail ZS existent ;
  (2) le CSV de Vaccine_expiration_ANT_P1 est TOUJOURS dégénéré (2 colonnes)
  car l'export .csv d'un DASHBOARD ne ramène que sa 1re feuille
  (_PAGE_TITLE_location_ANT) — d'où la « table vide ».
- Branchement dashboard (docs/index.html, fe792aeaa, mkRender) : si
  Dispo_vaccins_ANT n'a pas de file → on greffe le file …_ZS ; idem
  Vaccine_expiration_ANT_P1 (toujours). L'entrée …_ZS en doublon est masquée.
  À juin, Dispo garde SON fichier (686 lignes) et le détail ZS reste un vrai
  complément. ⚠ Le dashboard de Felly = snis-vaccination-dhis2.vercel.app
  (Vercel auto-déploie main ; GitHub Pages = miroir). En cas de « pas en
  ligne » → penser Ctrl+F5 (cache navigateur).
- Rendu : flag = valeur colorée par la colonne …_avail (Vrai→vert/Faux→rouge) ;
  % expiration = mkBarCell coloré par …_COLOR (green/red littéraux Tableau).

D) Leçons techniques du 27/07 (corrections des notes B) :
- ⚠ SID VizQL : l'en-tête x-session-id est SANS suffixe, l'URL /sessions/ le
  porte AVEC (« XXXX-0:0 ») — les commandes exigent le suffixe. Ne JAMAIS
  laisser l'en-tête écraser le SID capté d'URL (sinon 410 aléatoires).
- ⚠ Le dialogue crosstab peut répondre 200 VIDE ou 410 en rafale quand la
  session migre de nœud (surtout serveur chargé ~9h) : RAZ SID/GSH/XSRF avant
  chaque navigation de vue, bootstrap par la feuille légère FILTER_VALUES_ANT,
  attendre le canvas, puis REJOUER le dialogue en adoptant le
  global-session-header des réponses 410 (session migrée).
- DATES « Expiration la plus proche » : présentes à l'écran mais ABSENTES du
  crosstab Excel (qui ne porte que % + couleur) et du pres-model bootstrap
  (fenêtres paginées). Piste validée : tableauscraper api.py → commandes
  tabdoc/get-summary-data et tabdoc/get-underlying-data (POST
  …/sessions/{sid}/commands/tabdoc/get-underlying-data, champs maxRows,
  includeAllColumns=true, visualIdPresModel={worksheet, dashboard}). Pas
  tabsrv ! Page Tableau 2026.2 = shell JS (tsConfigContainer vide) →
  tableauscraper.loads() inutilisable tel quel ; piloter les commandes depuis
  le contexte page Playwright. Venv Python : mashako-sync/.venv
  (tableauscraper installé). À RETENTER quand le canal commandes est stable.
- Validation ZS (validate-zs-batch.mjs) : fetch .csv SANS garde-temps = gel
  total (>60 min sur Vaccine_dispo_HZ_P2) → AbortController 90 s + course
  Node 120 s + reprise incrémentale (verdicts écrits par feuille, skip des
  déjà verdictées). Verdicts partiels : COLLAPSE = Résumé, Heatmap, Carte
  Supervision, Dispo_HZ_P1 (le multi-valeurs ne ramène que la 1re ZS) ;
  OK = Supervision_P1/P2.
- sync.mjs : liste blanche ZS lue dans zs_batch_verdicts.json (ok:false →
  repli unitaire) + pullZsLegacy PARALLÉLISÉ ×3 (519 ZS ≈ 30 min/feuille au
  lieu de 1,5-2 h ; ×3 = seuil file d'attente serveur, voir note runBatch).

État 22/07/2026 (2) — FIABILITÉ TOOL_CALLS + CARTOGRAPHIE (déployé) :

- Function-calling durci (docs/index.html, tous fournisseurs) : les identifiants
  NATIFS des tool_calls (Kimi call_*, Anthropic toolu_*) sont conservés de bout
  en bout (flux SSE → historique interne → conversions) ; les réponses tool
  portent tool_call_id et l'appariement est EXPLICITE par tid (repli positionnel
  pour les anciens historiques sans tid). Kimi K3 : le reasoning_content d'un
  message assistant est RENVOYÉ tel quel au tour suivant (iaMsgsToOpenAI) —
  sinon 400 « reasoning_content must be passed back ».
- Filet de sécurité iaSanitizeHistory(messages) : répare une COPIE de
  l'historique (tool_calls sans toutes leurs réponses → dégradés en texte,
  réponses partielles/orphelines repliées en note user ; appariement vérifié
  par ensembles de tid quand ils existent). Retry AUTOMATIQUE unique dans
  iaCallAPI sur erreur 400 mentionnant tool_calls/tool_use/tool_result/
  tool_call_id/reasoning_content. IA.msgs n'est jamais modifié.
- Cartographie : helper ctx.geo(niveau, parent) dans executer_js → GeoJSON
  DHIS2 (organisationUnits.geojson ; 2=provinces via parent 'LEVEL-1', 3=ZS
  d'une province, 4=AS ; features properties.id=UID, properties.na=nom) ;
  prompt système enrichi : RECETTE CARTE choroplèthe Plotly (geojson +
  locations UID + featureidkey 'properties.id', échelle provinces national /
  ZS d'UNE province) + recette SCORE QUALITÉ des données (0,5×complétude
  REPORTING_RATE + 0,3×promptitude REPORTING_RATE_ON_TIME + 0,2×cohérence
  interne, sur 100). iaMissingAwait couvre aussi ctx.geo.
- generer_rapport : NOUVEAU format 'carte' (enum + iaRepCarte) = page HTML
  INTERACTIVE autonome téléchargée (.html) — figures Plotly navigables
  (zoom/survol, jamais rasterisées), GeoJSON et données embarqués, Plotly
  2.35.0 via CDN, page de garde/tableaux/signature identiques aux rapports.
- Tests : 4/4 blocs script valides (vm.Script) + 26 scénarios de conversion
  OpenAI/Anthropic, flux SSE fragmenté multi-appels, sanitize (copie intacte,
  tid mismatch, historique sain inchangé) et détection 400 — tous OK.
État actuel (31/07/2026) :
- Assistant IA : 3 NOUVEAUX outils livrables ajoutés (docs/index.html + worker) —
  1) generer_excel : classeur .xlsx natif multi-feuilles via ExcelJS 4.4.0 (CDN,
  chargé à la demande par iaEnsureExcelJS) — cellules colorées {t,bg} réelles,
  en-têtes stylés bleu PEV figés, largeurs auto, vrais nombres Excel ;
  2) generer_image : visuel vectoriel (logo, affiche, infographie, schéma) écrit
  en SVG par le modèle — rendu via <img> (jamais de DOM injecté), téléchargeable
  SVG + PNG 3× (rasterisation canvas, iaSvgToPng) — PAS de photo-réalisme ;
  3) envoyer_email : nouvelle route worker POST /envoyer (clientMail) — code
  d'accès requis (requireCode), 1 unité décomptée à l'envoi réussi, corps HTML
  ≤ 40 000 car. enveloppé dans un bandeau « Assistant IA — Dashboard PEV »
  (anti-usurpation), aucune pièce jointe (limites EmailJS/Brevo) — l'essentiel
  va dans le corps ; réponse {ok, quota}. Réutilise sendMail (EmailJS→Brevo).
  Prompt système : point « 6. EXCEL, VISUELS & E-MAIL » ajouté ; textes d'aide,
  hero et chips mis à jour. Vérifs : node --check worker OK, 5 blocs inline OK.
  ⚠ DÉPLOIEMENT requis des DEUX côtés : push docs/ (Pages/Vercel) + wrangler
  deploy du worker (sinon envoyer_email renvoie 404 « Introuvable »).

État actuel (31/07/2026, soir) :
- FIDÉLITÉ DES CALCULS (correctif anti-hallucination, suite à un cas réel :
  cartographie enfants ZD par province 2025 inventée au lieu d'appliquer la
  recette du dashboard). Prompt système renforcé à 3 endroits :
  1) nouvelle section « FIDÉLITÉ DES CALCULS — RÈGLE ABSOLUE » : ordre strict =
     recette du dashboard pour le PEV (reproduction à l'identique) →
     configuration DHIS2 (indicateurs officiels numerator/denominator, canevas)
     pour tout programme → sinon le dire franchement ; JAMAIS de définition
     issue des connaissances générales ; DIVERGENCE repo vs DHIS2 en ligne →
     DHIS2 EN LIGNE = RÉFÉRENCE FINALE (donner la valeur DHIS2 + signaler l'écart) ;
  2) recettes EXACTES ZD/SV ajoutées aux Conventions PEV : ZD_mois =
     max(0, NS_mensuelle − DTC1_mois), SV_mois = max(0, NS_mensuelle − DTC3_mois),
     NS_mensuelle = Pop_par_AS × 0,0349/12 — CIBLES AJUSTÉES PRIORITAIRES
     (cibles_ajustees/<année>.json : SV_ajust/12 par ZS, niveau ZS max, cf.
     _cvZDAjust) ; Pop_par_AS disponible en local (data_as/dashboard/by_as/*) ;
     interdits : calcul annuel d'un bloc + définition « aucun antigène » ;
     exemple guidé complet « cartographie ZD par province 2025 » (cibles
     ajustées + by_zs + somme par province + choroplèthe) ;
  3) Méthode point 1 : « n'invente JAMAIS un chiffre NI une définition ».
  Vérifs : 5 blocs inline OK + build-static OK. Dashboard-only (worker inchangé).

État actuel (31/07/2026, nuit — CORRECTIF ZD/SV) :
- BUG CUMUL MULTI-PÉRIODES corrigé : Pop_par_AS est une population ANNUELLE ;
  l'ancien clamping MENSUEL (Σ max(0, Pop×3,49%/12 − doses_mois)) gonflait ZD/SV
  dès plusieurs mois sélectionnés (un mois de rattrapage ne compensait pas).
  Nouvelle formule PÉRIODE par entité (_cvZD/_cvESV, _cxZD/_cxESV, _cvZDAjust) :
  max(0, Pop_annuelle × 3,49 % × n_mois/12 − doses_période), sommée par AS (ou ZS
  pour l'ajustée). Test numérique : rattrapage 6×0 + 6×70 → ancien 209,4 gonflé,
  nouveau 0 (juste) ; cas régulier et trimestre inchangés.
- RÈGLE MÉTIER actée : « ZD » TOUT COURT = ZD ADMIN (cible = population DHIS2) ;
  ZD AJUSTÉE (cibles_ajustees) uniquement si le client la nomme. Prompt iaSystem
  réécrit en conséquence (recette ZD/SV + exemple guidé admin via analytics
  WLSKVyA8LoY LEVEL-4 + uNdFg1eymsa ; FIDÉLITÉ 1 : « agrégation par entité sur
  la période », plus de « clamping mensuel » — l'entrée précédente est caduque
  sur ce point).
