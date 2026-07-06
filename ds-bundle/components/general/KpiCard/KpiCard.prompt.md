KpiCard from snis-vaccination-dhis2. Use via `window.SnisPev.KpiCard` (bundle loaded from the root `_ds_bundle.js`).

## Examples

### IndicateursQualite

```jsx
() => (
  <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(200px, 1fr))", gap: 12 }}>
    <KpiCard label="Complétude" value="94,2 %" tone="good" icon="clipboard" sub="1 842 / 1 955 rapports attendus" />
    <KpiCard label="Promptitude" value="78,6 %" tone="warn" icon="time" sub="Rapports transmis avant le 5 du mois" />
    <KpiCard label="Abandon Penta1 → Penta3" value="12,4 %" tone="bad" icon="alert" sub="Seuil OMS : < 10 %" />
    <KpiCard label="Enfants vaccinés Penta3" value="128 450" tone="navy" icon="syringe" pct={87.3} />
  </div>
);

/** Les 8 tons disponibles (dégradés pleins, texte blanc). */
```

### Tons

```jsx
() => (
  <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(150px, 1fr))", gap: 10 }}>
    <KpiCard label="Neutral" value="1 955" tone="neutral" icon="doc" sub="Aires de santé" />
    <KpiCard label="Navy" value="26" tone="navy" icon="map" sub="Provinces DPS" />
    <KpiCard label="Good" value="91 %" tone="good" icon="check" sub="Objectif atteint" />
    <KpiCard label="Warn" value="74 %" tone="warn" icon="alert" sub="Sous le seuil" />
    <KpiCard label="Bad" value="52 %" tone="bad" icon="down" sub="Action requise" />
    <KpiCard label="Brand" value="87 %" tone="brand" icon="shield" sub="Couverture VAR" />
    <KpiCard label="Violet" value="63 410" tone="violet" icon="child" sub="Doses BCG" />
    <KpiCard label="Teal" value="4 812" tone="teal" icon="truck" sub="Stratégie avancée" />
  </div>
);

/** Sous-titre `% réalisation` (pct) — valeur fournie, puis indisponible. */
```

### AvecRealisation

```jsx
() => (
  <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(200px, 1fr))", gap: 12 }}>
    <KpiCard label="Doses Penta3 administrées" value="128 450" tone="brand" icon="syringe" pct={87.3} />
    <KpiCard label="Doses VAR administrées" value="96 204" tone="violet" icon="shield" pct={null} />
  </div>
)
```
