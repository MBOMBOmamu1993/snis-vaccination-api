"use client";

import { EmptyState } from "@/components/ui/EmptyState";
import { TableExportButtons } from "@/components/ui/TableExport";

type Cell = string | number | null | undefined;

/** Niveau heatmap d'une cellule (façon Shiny PEV RDC). `null` = pas de coloration. */
export type HeatLevel = "good" | "warn" | "bad" | null;

const HEAT_CLASS: Record<NonNullable<HeatLevel>, string> = {
  good: "hm-good-soft",
  warn: "hm-warn-soft",
  bad: "hm-bad-soft",
};

/** Tableau générique au style « dtable » du dashboard (en-tête bleu marine,
 *  1re colonne entité navy, façon Shiny PEV RDC).
 *  - `exportFilename` affiche automatiquement les boutons CSV / Excel.
 *  - `heat` colore conditionnellement les cellules selon les seuils FOURNIS PAR
 *    LA PAGE (aucun seuil n'est inventé ici ; cf. logique des tons KPI). */
export function DataTable({
  columns,
  rows,
  maxRows = 60,
  format,
  exportFilename,
  heat,
}: {
  columns: string[];
  rows: Record<string, Cell>[];
  maxRows?: number;
  format?: (col: string, value: Cell) => React.ReactNode;
  exportFilename?: string;
  /** Coloration heatmap d'une cellule selon la colonne / la valeur (design only). */
  heat?: (col: string, value: Cell, row: Record<string, Cell>) => HeatLevel;
}) {
  if (!rows.length || !columns.length) return <EmptyState message="Aucune donnée disponible." />;
  const render = (col: string, v: Cell): React.ReactNode => {
    if (format) {
      const out = format(col, v);
      if (out !== undefined) return out;
    }
    if (v === null || v === undefined || v === "") return "—";
    if (typeof v === "number") return Number.isInteger(v) ? v.toLocaleString("fr-FR") : v.toLocaleString("fr-FR", { maximumFractionDigits: 1 });
    return String(v);
  };
  return (
    <div className="overflow-x-auto">
      {exportFilename ? (
        <div className="mb-2 flex justify-end">
          <TableExportButtons
            filename={exportFilename}
            data={{ columns, rows: rows.map((r) => columns.map((c) => (r[c] === undefined ? null : (r[c] as string | number | null)))) }}
          />
        </div>
      ) : null}
      <table className="dtable">
        <thead>
          <tr>
            {columns.map((c, i) => <th key={c} className={i === 0 ? "name" : undefined}>{c}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, maxRows).map((r, i) => (
            <tr key={i}>
              {columns.map((c, j) => {
                if (j === 0) return <td key={c} className="name">{render(c, r[c])}</td>;
                const lvl = heat ? heat(c, r[c], r) : null;
                return <td key={c} className={lvl ? HEAT_CLASS[lvl] : undefined}>{render(c, r[c])}</td>;
              })}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > maxRows ? (
        <div className="text-[11px] text-surface-500 mt-1.5 px-1">{rows.length - maxRows} ligne(s) supplémentaire(s) non affichée(s).</div>
      ) : null}
    </div>
  );
}
