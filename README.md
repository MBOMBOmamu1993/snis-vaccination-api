# Dashboard PEV de routine — RDC

Tableau de bord de la **vaccination de routine** (Programme Élargi de Vaccination · OMS — RDC).
Données issues **exclusivement du DHIS2**, présentées par **province** et par **période**.

L'interface reprend le design du *Tableau de bord Tshopo / Tshuapa* (thème navy/OMS,
écran de chargement, page de bienvenue, navigation par onglets, visuels exportables).

## Architecture

- **Application Next.js (App Router)** — interface du dashboard (`app/`, `components/`, `lib/`).
- **Pipeline DHIS2 inchangé** — l'extraction `backfill` / `update` et l'agrégation
  (scripts Python, workflows GitHub) restent la **source de vérité** dans `docs/data*`.
  On n'y touche pas.
- **Service des données** — `scripts/prepare-data.mjs` copie `docs/data/dashboard`
  vers `public/data/dashboard` au build. Les fichiers `.json.gz` sont **décompressés
  dans le navigateur** (`DecompressionStream`), comme l'ancien dashboard : aucune
  charge serveur, des milliers d'enregistrements sans planter le serveur.
- **Ancien dashboard statique** (`docs/index.html`) conservé tel quel (GitHub Pages).

## Onglets

1. **Contrôle qualité des données** — complétude & promptitude, évolution mensuelle.
2. **Données de vaccination** — doses par antigène, taux d'abandon, courbe de suivi.
3. **Logistique** — stratégie fixe / avancée-mobile par antigène.
4. **Télécharger canevas revue formative** — génération automatique du canevas
   officiel **PPTX éditable**, diapos 10–12 renseignées avec les données DHIS2 de
   la période filtrée (page de garde, processus, complétude/promptitude/doses,
   taux d'abandon par zone de santé).

Chaque graphique et chaque tableau dispose d'un menu d'export (PNG, JPEG, PDF, SVG,
CSV, XLS, tableau de données).

## Développement

```bash
npm install
npm run dev      # http://localhost:3000  (prepare-data s'exécute automatiquement)
npm run build    # build de production
```

## Déploiement Vercel

Le déploiement Vercel publie le **dashboard statique `docs/index.html`** (re-skiné
design Tshuapa/Shiny PEV), **pas** l'application Next.js. Configuration dans
`vercel.json` :

1. **Framework Preset = Other** (`"framework": null`) — surtout *ne pas* laisser
   « Next.js » dans les *Project Settings* Vercel, sinon Vercel construit l'app
   `app/` au lieu de `docs/index.html` et les modifications de `index.html`
   n'apparaissent jamais en production.
2. La `buildCommand` exécute `node scripts/build-static.mjs`, qui copie
   `docs/index.html` + logos dans `dist/` (`outputDirectory`). L'`installCommand`
   est neutralisée (`echo skip`) : le script n'utilise que des modules Node natifs.
3. Les données pré-agrégées ne sont **pas** dupliquées dans `dist/` : `index.html`
   les charge depuis GitHub Pages (`DATA_ORIGIN`), ce qui garde le déploiement léger.
4. `index.html` est servi avec `Cache-Control: must-revalidate` (cf. `headers`)
   pour qu'une nouvelle version soit toujours reprise sans cache CDN/navigateur.
5. Aucune variable d'environnement requise pour l'affichage.

> L'application **Next.js** (`app/`, `components/`, `lib/`) reste dans le dépôt pour
> le développement local (`npm run dev`) mais n'est pas ce que Vercel déploie.

> Les données affichées proviennent du DHIS2 et sont sujettes à validation /
> modifications rétroactives.
