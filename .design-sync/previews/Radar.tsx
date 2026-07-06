import "./_liveClock";
import { Radar } from "snis-vaccination-dhis2";

const indicators = ["Complétude", "Promptitude", "Penta3", "VAR1", "BCG", "Abandon inversé"];

/** Comparaison de deux provinces sur 6 indicateurs (échelle 0–100). */
export const DeuxProvinces = () => (
  <Radar
    height={300}
    exportTitle="Profil qualité — comparaison"
    indicators={indicators}
    entities={[
      { name: "Kinshasa", values: [96, 88, 93, 89, 95, 91] },
      { name: "Tshuapa", values: [84, 59, 66, 62, 78, 70] },
    ]}
  />
);

/** Plusieurs entités : la palette PEV distingue chaque profil. */
export const MultiEntites = () => (
  <Radar
    height={300}
    exportTitle="Profil qualité — 4 provinces"
    indicators={indicators}
    entities={[
      { name: "Kinshasa", values: [96, 88, 93, 89, 95, 91] },
      { name: "Kongo Central", values: [93, 77, 85, 82, 90, 86] },
      { name: "Nord-Kivu", values: [92, 72, 79, 74, 88, 81] },
      { name: "Tshopo", values: [89, 64, 71, 68, 82, 75] },
    ]}
  />
);
