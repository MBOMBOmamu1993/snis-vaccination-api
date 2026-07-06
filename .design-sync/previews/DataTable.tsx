import { DataTable, type HeatLevel } from "snis-vaccination-dhis2";

const columns = ["Province", "Complétude (%)", "Promptitude (%)", "Penta3 (%)", "VAR1 (%)"];
const rows = [
  { "Province": "Kinshasa", "Complétude (%)": 96.4, "Promptitude (%)": 88.1, "Penta3 (%)": 92.7, "VAR1 (%)": 89.3 },
  { "Province": "Kongo Central", "Complétude (%)": 93.2, "Promptitude (%)": 76.5, "Penta3 (%)": 85.1, "VAR1 (%)": 81.9 },
  { "Province": "Tshopo", "Complétude (%)": 88.7, "Promptitude (%)": 64.2, "Penta3 (%)": 71.4, "VAR1 (%)": 68.0 },
  { "Province": "Tshuapa", "Complétude (%)": 84.5, "Promptitude (%)": 58.9, "Penta3 (%)": 66.2, "VAR1 (%)": 61.7 },
  { "Province": "Nord-Kivu", "Complétude (%)": 91.8, "Promptitude (%)": 72.3, "Penta3 (%)": 78.6, "VAR1 (%)": 74.4 },
  { "Province": "Haut-Katanga", "Complétude (%)": 95.1, "Promptitude (%)": 83.7, "Penta3 (%)": 90.2, "VAR1 (%)": 86.5 },
];

/** Seuils PEV classiques : ≥ 90 % bon, ≥ 70 % à surveiller, sinon critique. */
const heat = (col: string, v: unknown): HeatLevel =>
  col === "Province" || typeof v !== "number" ? null : v >= 90 ? "good" : v >= 70 ? "warn" : "bad";

/** Tableau « dtable » : en-tête navy, 1re colonne entité, zébrage. */
export const CouvertureParProvince = () => <DataTable columns={columns} rows={rows} />;

/** Coloration conditionnelle heatmap (seuils fournis par la page). */
export const AvecHeatmap = () => <DataTable columns={columns} rows={rows} heat={heat} />;

/** `exportFilename` ajoute automatiquement les boutons CSV / Excel. */
export const AvecExport = () => (
  <DataTable columns={columns} rows={rows} exportFilename="couverture_par_province" heat={heat} />
);

/** Sans données : bascule sur EmptyState. */
export const SansDonnees = () => <DataTable columns={columns} rows={[]} />;
