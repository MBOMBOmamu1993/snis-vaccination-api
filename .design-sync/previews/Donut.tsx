import "./_liveClock";
import { Donut } from "snis-vaccination-dhis2";

/** Anneau avec libellé central et légende (défaut). */
export const AvecLegende = () => (
  <Donut
    height={230}
    centerLabel="1 955 rapports"
    exportTitle="Statut des rapports"
    data={[
      { name: "Reçus à temps", value: 1537, color: "#1f9d57" },
      { name: "Reçus en retard", value: 305, color: "#f59e0b" },
      { name: "Manquants", value: 113, color: "#e23636" },
    ]}
  />
);

/** Sans légende, anneau centré (usage compact dans une carte KPI). */
export const SansLegende = () => (
  <Donut
    height={180}
    legend={false}
    centerLabel="87 %"
    data={[
      { name: "Couvert", value: 87, color: "#0093d5" },
      { name: "Restant", value: 13, color: "#e2e8f0" },
    ]}
  />
);

/** Couleurs de la palette PEV par défaut (aucune couleur fournie). */
export const PaletteParDefaut = () => (
  <Donut
    height={230}
    centerLabel="Stratégies"
    data={[
      { name: "Fixe", value: 68 },
      { name: "Avancée", value: 24 },
      { name: "Mobile", value: 8 },
    ]}
  />
);
