Pager from snis-vaccination-dhis2. Use via `window.SnisPev.Pager` (bundle loaded from the root `_ds_bundle.js`).

Barre de navigation entre les pages d'un tableau paginé.

## Examples

### PremierePage

```jsx
() => (
  <Pager page={1} pageCount={12} setPage={noop} start={0} end={30} total={347} />
);

/** Page courante au milieu : fenêtre glissante + ellipses des deux côtés. */
```

### PageIntermediaire

```jsx
() => (
  <Pager page={6} pageCount={12} setPage={noop} start={150} end={180} total={347} />
);

/** Dernière page (bouton « Suivant » désactivé, tranche partielle). */
```

### DernierePage

```jsx
() => (
  <Pager page={12} pageCount={12} setPage={noop} start={330} end={347} total={347} />
)
```
