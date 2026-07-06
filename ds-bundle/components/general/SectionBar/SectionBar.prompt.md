SectionBar from snis-vaccination-dhis2. Use via `window.SnisPev.SectionBar` (bundle loaded from the root `_ds_bundle.js`).

Bandeau de section bleu marine OMS (cf. maquette). `right` : contenu aligné
 à droite (boutons d'export des tableaux, cf. specs feedback TL 01).

## Examples

### BandeauSimple

```jsx
() => (
  <SectionBar icon="clipboard">Contrôle qualité des données</SectionBar>
);

/** Avec actions alignées à droite (boutons d'export, variante « bar »). */
```

### AvecExport

```jsx
() => (
  <SectionBar
    icon="syringe"
    right={
      <TableExportButtons
        variant="bar"
        filename="doses_par_antigene"
        data={{ columns: ["Antigène", "Doses"], rows: [["BCG", 12040], ["Penta3", 9860]] }}
      />
    }
  >
    Données de vaccination — doses par antigène
  </SectionBar>
);

/** Plusieurs sections successives structurant une page. */
```

### SectionsDePage

```jsx
() => (
  <div>
    <SectionBar icon="clipboard">Complétude &amp; promptitude</SectionBar>
    <div className="mb-4 text-[12px] text-surface-700">Contenu de la section…</div>
    <SectionBar icon="analyse">Évolution mensuelle</SectionBar>
    <div className="mb-4 text-[12px] text-surface-700">Contenu de la section…</div>
    <SectionBar icon="truck">Logistique — stratégie fixe / avancée</SectionBar>
    <div className="text-[12px] text-surface-700">Contenu de la section…</div>
  </div>
)
```
