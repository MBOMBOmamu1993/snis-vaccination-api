EmptyState from snis-vaccination-dhis2. Use via `window.SnisPev.EmptyState` (bundle loaded from the root `_ds_bundle.js`).

## Examples

### ParDefaut

```jsx
() => <EmptyState />;

/** Titre et message personnalisés. */
```

### Personnalise

```jsx
() => (
  <EmptyState
    title="Aucune province sélectionnée"
    message="Choisissez au moins une province et une période dans la barre de filtres pour afficher les données DHIS2."
  />
);

/** Dans une carte (usage type : visuel sans données pour le filtre courant). */
```

### DansUneCarte

```jsx
() => (
  <Card>
    <EmptyState message="Aucune donnée disponible pour la période sélectionnée." />
  </Card>
)
```
