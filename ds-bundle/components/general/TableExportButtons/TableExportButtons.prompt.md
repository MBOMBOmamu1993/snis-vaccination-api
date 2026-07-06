TableExportButtons from snis-vaccination-dhis2. Use via `window.SnisPev.TableExportButtons` (bundle loaded from the root `_ds_bundle.js`).

Les deux boutons d'export. Sans `data`, le tableau exporté est le premier
<table> de la carte (.card) ou de la <section> la plus proche du bouton.

## Examples

### VarianteCard

```jsx
() => (
  <Card className="max-w-md">
    <div className="card-header">
      <div className="card-title">Doses par antigène</div>
      <TableExportButtons filename="doses_par_antigene" data={data} />
    </div>
    <div className="text-[11.5px] text-surface-700">Le tableau exporté provient de `data` (ou du DOM le plus proche).</div>
  </Card>
);

/** Variante « bar » : bordure et texte blancs, pour le bandeau navy. */
```

### VarianteBar

```jsx
() => (
  <SectionBar
    icon="table"
    right={<TableExportButtons variant="bar" filename="triangulation_doses" data={data} />}
  >
    Triangulation des doses
  </SectionBar>
)
```
