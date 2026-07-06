EChart from snis-vaccination-dhis2. Use via `window.SnisPev.EChart` (bundle loaded from the root `_ds_bundle.js`).

## Examples

### Histogramme

```jsx
() => (
  <Card>
    <CardHeader title="Doses administrées par antigène" subtitle="Juin 2026 · Toutes provinces" icon="syringe" iconTone="blue" />
    <EChart
      height={260}
      exportTitle="Doses par antigène"
      option={{
        grid: { left: 8, right: 8, top: 24, bottom: 4, containLabel: true },
        tooltip: { trigger: "axis" },
        xAxis: { type: "category", data: ["BCG", "Penta1", "Penta2", "Penta3", "VPO3", "VAR1", "PCV13"] },
        yAxis: { type: "value" },
        series: [{ type: "bar", data: [118204, 132880, 130215, 128450, 121772, 96204, 125661] }],
      }}
    />
  </Card>
);

/** Barres empilées multi-séries — la palette PEV s'applique automatiquement. */
```

### BarresEmpilees

```jsx
() => (
  <EChart
    height={240}
    exportTitle="Doses par stratégie"
    option={{
      grid: { left: 8, right: 8, top: 30, bottom: 4, containLabel: true },
      tooltip: { trigger: "axis" },
      legend: { top: 0 },
      xAxis: { type: "category", data: ["Kinshasa", "Kongo Central", "Tshopo", "Tshuapa", "Nord-Kivu"] },
      yAxis: { type: "value" },
      series: [
        { name: "Stratégie fixe", type: "bar", stack: "s", data: [42100, 28400, 15800, 9600, 24800] },
        { name: "Stratégie avancée", type: "bar", stack: "s", data: [8200, 9100, 7400, 6100, 8900] },
      ],
    }}
  />
);

/** `menu={false}` masque le menu d'export (jauges, sparklines). */
```

### JaugeSansMenu

```jsx
() => (
  <EChart
    height={200}
    menu={false}
    option={{
      series: [{
        type: "gauge",
        min: 0, max: 100,
        progress: { show: true, width: 10 },
        axisLine: { lineStyle: { width: 10 } },
        axisTick: { show: false }, splitLine: { show: false }, axisLabel: { show: false },
        pointer: { show: false },
        detail: { formatter: "{value} %", fontSize: 22, offsetCenter: [0, 0], color: PEV_PALETTE[0] },
        data: [{ value: 87.3, name: "Couverture Penta3" }],
      }],
    }}
  />
)
```
