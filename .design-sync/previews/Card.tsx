import { Card, CardHeader, KpiCard } from "snis-vaccination-dhis2";

/** Carte de base : fond blanc, coins arrondis, ombre douce, élévation au survol. */
export const CarteSimple = () => (
  <Card>
    <div className="card-title">Complétude des rapports DHIS2</div>
    <div className="card-subtitle mt-0.5">1 842 rapports reçus sur 1 955 attendus au 30 juin 2026.</div>
    <div className="kpi-value mt-3">94,2 %</div>
    <div className="kpi-sub mt-1">+2,1 points vs mai 2026</div>
  </Card>
);

/** Composition type d'un visuel du dashboard : en-tête + contenu. */
export const CarteDeVisuel = () => (
  <Card>
    <CardHeader
      title="Taux d'abandon Penta1 → Penta3"
      subtitle="Par province · Janv.–Juin 2026"
      icon="down"
      iconTone="red"
    />
    <div className="grid grid-cols-2 gap-3">
      <KpiCard label="Abandon moyen" value="12,4 %" tone="bad" icon="alert" sub="Seuil OMS : < 10 %" />
      <KpiCard label="Provinces conformes" value="14 / 26" tone="navy" icon="map" sub="Abandon < 10 %" />
    </div>
  </Card>
);

/** `className` étend la carte (ici : largeur bornée + padding réduit). */
export const AvecClassName = () => (
  <Card className="max-w-sm card-pad">
    <div className="card-title">Note méthodologique</div>
    <div className="text-[11.5px] text-surface-700 mt-1">
      Les taux sont calculés sur les doses rapportées dans DHIS2 (stratégies fixe et avancée confondues).
    </div>
  </Card>
);
