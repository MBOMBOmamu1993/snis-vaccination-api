import { Card, SectionBar, TableExportButtons } from "snis-vaccination-dhis2";

const data = {
  columns: ["Antigène", "Doses administrées", "Cible"],
  rows: [
    ["BCG", 118_204, 131_500],
    ["Penta1", 132_880, 131_500],
    ["Penta3", 128_450, 131_500],
    ["VAR1", 96_204, 131_500],
  ] as (string | number | null)[][],
};

/** Variante « card » (défaut) : bordure slate, pour un en-tête de carte. */
export const VarianteCard = () => (
  <Card className="max-w-md">
    <div className="card-header">
      <div className="card-title">Doses par antigène</div>
      <TableExportButtons filename="doses_par_antigene" data={data} />
    </div>
    <div className="text-[11.5px] text-surface-700">Le tableau exporté provient de `data` (ou du DOM le plus proche).</div>
  </Card>
);

/** Variante « bar » : bordure et texte blancs, pour le bandeau navy. */
export const VarianteBar = () => (
  <SectionBar
    icon="table"
    right={<TableExportButtons variant="bar" filename="triangulation_doses" data={data} />}
  >
    Triangulation des doses
  </SectionBar>
);
