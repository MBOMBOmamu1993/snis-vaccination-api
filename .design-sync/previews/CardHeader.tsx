import { Card, CardHeader, TableExportButtons } from "snis-vaccination-dhis2";

/** En-tête de carte type : titre + sous-titre + badge icône dégradé. */
export const EnTeteDeVisuel = () => (
  <Card>
    <CardHeader
      title="Évolution mensuelle de la complétude"
      subtitle="Rapports DHIS2 attendus vs reçus · Janv.–Juin 2026"
      icon="analyse"
      iconTone="navy"
    />
    <div className="text-[12px] text-surface-700">Zone de contenu du visuel (graphique ou tableau).</div>
  </Card>
);

/** Avec contenu aligné à droite (boutons d'export du tableau). */
export const AvecActions = () => (
  <Card>
    <CardHeader
      title="Doses administrées par antigène"
      subtitle="Stratégie fixe + avancée · Province de la Tshopo"
      icon="syringe"
      iconTone="blue"
      right={<TableExportButtons filename="doses_par_antigene" data={{ columns: ["Antigène", "Doses"], rows: [["BCG", 12040], ["Penta3", 9860]] }} />}
    />
    <div className="text-[12px] text-surface-700">Zone de contenu du visuel.</div>
  </Card>
);

/** Les 7 tons d'icône disponibles. */
export const TonsDIcone = () => (
  <div style={{ display: "grid", gap: 8 }}>
    <Card><CardHeader title="Navy" subtitle="Complétude et promptitude" icon="clipboard" iconTone="navy" /></Card>
    <Card><CardHeader title="Teal" subtitle="Logistique et chaîne du froid" icon="fridge" iconTone="teal" /></Card>
    <Card><CardHeader title="Violet" subtitle="Population cible" icon="people" iconTone="violet" /></Card>
    <Card><CardHeader title="Green" subtitle="Objectifs atteints" icon="check" iconTone="green" /></Card>
    <Card><CardHeader title="Orange" subtitle="Alertes et ruptures" icon="alert" iconTone="orange" /></Card>
    <Card><CardHeader title="Blue" subtitle="Doses administrées" icon="syringe" iconTone="blue" /></Card>
    <Card><CardHeader title="Red" subtitle="Taux d'abandon" icon="down" iconTone="red" /></Card>
  </div>
);
