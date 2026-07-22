# Historique des requêtes

## Format
Date :
Demande :
Fichiers concernés :
Action faite :
Résultat :

---

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