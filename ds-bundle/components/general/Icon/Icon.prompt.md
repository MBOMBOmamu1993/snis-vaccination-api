Icon from snis-vaccination-dhis2. Use via `window.SnisPev.Icon` (bundle loaded from the root `_ds_bundle.js`).

## Examples

### Navigation

```jsx
() => (
  <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>{NAVIGATION.map(cell)}</div>
);

/** Icônes du domaine santé / vaccination / logistique. */
```

### SanteEtLogistique

```jsx
() => (
  <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>{SANTE.map(cell)}</div>
);

/** Icônes données & indicateurs. */
```

### DonneesEtIndicateurs

```jsx
() => (
  <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>{DONNEES.map(cell)}</div>
);

/** La couleur et la taille viennent du contexte (currentColor + className). */
```

### TaillesEtCouleurs

```jsx
() => (
  <div style={{ display: "flex", alignItems: "center", gap: 14 }}>
    <Icon name="syringe" className="w-4 h-4 text-surface-700" />
    <Icon name="syringe" className="w-6 h-6 text-oms-500" />
    <Icon name="syringe" className="w-8 h-8 text-navy-700" />
    <Icon name="syringe" className="w-10 h-10 text-good-500" />
    <span className="section-bar" style={{ display: "inline-flex", marginBottom: 0 }}>
      <Icon name="syringe" /> Dans un bandeau
    </span>
  </div>
)
```
