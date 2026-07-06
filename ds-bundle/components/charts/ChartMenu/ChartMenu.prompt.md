ChartMenu from snis-vaccination-dhis2. Use via `window.SnisPev.ChartMenu` (bundle loaded from the root `_ds_bundle.js`).

## Examples

### BoutonDansUnChart

```jsx
() => (
  <div className="relative card" style={{ height: 120 }}>
    <ChartMenu getInstance={() => null} getContainer={() => null} option={option} title="Doses par antigène" />
    <div className="text-[11.5px] text-surface-700">
      Zone du graphique — le menu ≡ (en haut à droite) propose : plein écran, impression,
      PNG, JPEG, PDF, SVG, CSV, XLS et tableau de données.
    </div>
  </div>
)
```
