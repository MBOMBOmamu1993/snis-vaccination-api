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

1. Importer le dépôt dans Vercel (framework détecté : **Next.js**).
2. La `buildCommand` (`vercel.json`) exécute `node scripts/prepare-data.mjs && next build`
   pour publier les données fraîches issues du pipeline backfill.
3. Aucune variable d'environnement requise pour l'affichage (données statiques pré-agrégées).

> Les données affichées proviennent du DHIS2 et sont sujettes à validation /
> modifications rétroactives.
