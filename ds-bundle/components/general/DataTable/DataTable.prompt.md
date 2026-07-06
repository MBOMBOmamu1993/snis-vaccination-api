DataTable from snis-vaccination-dhis2. Use via `window.SnisPev.DataTable` (bundle loaded from the root `_ds_bundle.js`).

Tableau générique au style « dtable » du dashboard (en-tête bleu marine,
 1re colonne entité navy, façon Shiny PEV RDC).
 - `exportFilename` affiche automatiquement les boutons CSV / Excel.
 - `heat` colore conditionnellement les cellules selon les seuils FOURNIS PAR
   LA PAGE (aucun seuil n'est inventé ici ; cf. logique des tons KPI).

## Examples

### CouvertureParProvince

```jsx
() => <DataTable columns={columns} rows={rows} />;

/** Coloration conditionnelle heatmap (seuils fournis par la page). */
```

### AvecHeatmap

```jsx
() => <DataTable columns={columns} rows={rows} heat={heat} />;

/** `exportFilename` ajoute automatiquement les boutons CSV / Excel. */
```

### AvecExport

```jsx
() => (
  <DataTable columns={columns} rows={rows} exportFilename="couverture_par_province" heat={heat} />
);

/** Sans données : bascule sur EmptyState. */
```

### SansDonnees

```jsx
() => <DataTable columns={columns} rows={[]} />
```
