LineTrend from snis-vaccination-dhis2. Use via `window.SnisPev.LineTrend` (bundle loaded from the root `_ds_bundle.js`).

## Examples

### SerieUnique

```jsx
() => (
  <LineTrend
    months={months}
    exportTitle="Complétude mensuelle"
    series={[{ name: "Complétude", data: [88, 90, 91, 93, 92, 94], color: "#0093d5" }]}
  />
);

/** Plusieurs séries : légende automatique, courbes lissées. */
```

### MultiSeries

```jsx
() => (
  <LineTrend
    height={260}
    months={months}
    exportTitle="Suivi des indicateurs qualité"
    series={[
      { name: "Complétude", data: [88, 90, 91, 93, 92, 94], color: "#0093d5" },
      { name: "Promptitude", data: [64, 69, 71, 75, 74, 79], color: "#f59e0b" },
      { name: "Couverture Penta3", data: [78, 80, 83, 85, 84, 87], color: "#1f9d57" },
    ]}
  />
);

/** Valeurs manquantes (`null`) : la courbe les relie (connectNulls). */
```

### AvecTrous

```jsx
() => (
  <LineTrend
    months={months}
    exportTitle="Promptitude Tshuapa"
    series={[{ name: "Promptitude", data: [58, null, 62, null, 66, 61], color: "#7c3aed" }]}
  />
)
```
