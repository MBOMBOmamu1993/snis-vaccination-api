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