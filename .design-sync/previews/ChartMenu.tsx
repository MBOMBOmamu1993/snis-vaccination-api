import { ChartMenu } from "snis-vaccination-dhis2";

const option = {
  xAxis: { type: "category", data: ["BCG", "Penta1", "Penta3", "VAR1"] },
  yAxis: { type: "value" },
  series: [{ type: "bar", name: "Doses", data: [118204, 132880, 128450, 96204] }],
};

/** Le bouton ≡ se positionne en haut à droite de son conteneur relatif
 *  (chaque EChart l'intègre déjà — usage direct réservé aux charts custom). */
export const BoutonDansUnChart = () => (
  <div className="relative card" style={{ height: 120 }}>
    <ChartMenu getInstance={() => null} getContainer={() => null} option={option} title="Doses par antigène" />
    <div className="text-[11.5px] text-surface-700">
      Zone du graphique — le menu ≡ (en haut à droite) propose : plein écran, impression,
      PNG, JPEG, PDF, SVG, CSV, XLS et tableau de données.
    </div>
  </div>
);
