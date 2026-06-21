"use client";

/**
 * Boutons « Télécharger CSV » / « Télécharger Excel » des tableaux (cf.
 * specs feedback TL 01). Deux modes d'alimentation :
 *  - data : colonnes/lignes passées explicitement par la page ;
 *  - DOM (par défaut) : à l'export, le tableau le plus proche est lu depuis le
 *    DOM (thead/tbody/tfoot) — couvre les ~40 tableaux « dtable » existants
 *    sans refactorer chaque page.
 * Deux variantes visuelles :
 *  - "bar"  : dans le bandeau bleu marine (SectionBar) — bordure/texte blancs ;
 *  - "card" : dans un en-tête de carte (CardTitle/CardHeader) — bordure slate.
 */
import { useRef } from "react";
import { Icon } from "@/components/ui/Icon";

export type TableData = { columns: string[]; rows: (string | number | null)[][] };

/** Slug de nom de fichier à partir du titre du tableau. */
export function slugify(s: string): string {
  return s
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 80) || "tableau";
}

const cellText = (el: Element): string => (el.textContent ?? "").replace(/\s+/g, " ").trim();

/** Sérialise un <table> du DOM (thead + tbody + tfoot, colspan développés). */
export function tableToData(table: HTMLTableElement): TableData {
  const headRows = Array.from(table.tHead?.rows ?? []);
  // En-tête : dernière ligne du thead (la plus détaillée), colspan développés.
  const lastHead = headRows[headRows.length - 1];
  const columns: string[] = [];
  if (lastHead) {
    for (const cell of Array.from(lastHead.cells)) {
      const span = cell.colSpan || 1;
      for (let i = 0; i < span; i++) columns.push(span > 1 ? `${cellText(cell)}${i ? ` (${i + 1})` : ""}` : cellText(cell));
    }
  }
  const rows: (string | number | null)[][] = [];
  const bodies = [...Array.from(table.tBodies), ...(table.tFoot ? [table.tFoot] : [])];
  // rowSpan : reporte la valeur des cellules fusionnées sur les lignes suivantes.
  const carry = new Map<number, { text: string; remaining: number }>();
  const consumeCarry = (out: (string | number | null)[], col: number): number => {
    while (carry.has(col)) {
      const held = carry.get(col)!;
      out.push(held.text);
      held.remaining -= 1;
      if (held.remaining <= 0) carry.delete(col);
      col++;
    }
    return col;
  };
  for (const body of bodies) {
    for (const tr of Array.from(body.rows)) {
      const out: (string | number | null)[] = [];
      let col = 0;
      for (const cell of Array.from(tr.cells)) {
        col = consumeCarry(out, col);
        const text = cellText(cell);
        if ((cell.rowSpan || 1) > 1) carry.set(col, { text, remaining: cell.rowSpan - 1 });
        for (let i = 0; i < (cell.colSpan || 1); i++) { out.push(text); col++; }
      }
      consumeCarry(out, col);
      if (out.some((v) => v !== "" && v !== null)) rows.push(out);
    }
  }
  return { columns, rows };
}

/** Téléchargement d'un Blob. */
function download(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 4000);
}

/** CSV : BOM UTF-8 + séparateur « ; » (Excel FR). */
export function downloadCsv(data: TableData, filename: string): void {
  const esc = (v: string | number | null): string => {
    const s = v === null || v === undefined ? "" : String(v);
    return /[";\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const lines = [data.columns.map(esc).join(";"), ...data.rows.map((r) => r.map(esc).join(";"))];
  download(new Blob(["﻿" + lines.join("\r\n")], { type: "text/csv;charset=utf-8" }), `${filename}.csv`);
}

/** Excel via SheetJS (import dynamique pour ne pas alourdir le bundle). */
export async function downloadXlsx(data: TableData, filename: string): Promise<void> {
  const XLSX = await import("xlsx");
  const ws = XLSX.utils.aoa_to_sheet([data.columns, ...data.rows]);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "Données");
  XLSX.writeFile(wb, `${filename}.xlsx`);
}

const VARIANT_CLS = {
  bar: "border border-white/70 text-white hover:bg-white/15",
  card: "border border-slate-300 text-navy-700 hover:border-oms-500 hover:text-oms-600 bg-white",
} as const;

/**
 * Les deux boutons d'export. Sans `data`, le tableau exporté est le premier
 * <table> de la carte (.card) ou de la <section> la plus proche du bouton.
 */
export function TableExportButtons({
  filename,
  data,
  variant = "card",
}: {
  filename: string;
  data?: TableData;
  variant?: "bar" | "card";
}) {
  const ref = useRef<HTMLSpanElement | null>(null);

  const resolveData = (): TableData | null => {
    if (data) return data;
    const el = ref.current;
    if (!el) return null;
    const host = el.closest(".card") ?? el.closest("section") ?? el.parentElement?.parentElement ?? null;
    // Bandeau de section : le tableau est dans le bloc qui suit le bandeau.
    const scope = host ?? document;
    let table = scope.querySelector("table");
    if (!table && el.closest(".section-bar")) {
      const bar = el.closest(".section-bar")!;
      let sib = bar.nextElementSibling;
      while (sib && !table) { table = sib.querySelector("table"); sib = sib.nextElementSibling; }
    }
    return table ? tableToData(table as HTMLTableElement) : null;
  };

  const onCsv = () => {
    const d = resolveData();
    if (d) downloadCsv(d, slugify(filename));
  };
  const onXlsx = () => {
    const d = resolveData();
    if (d) void downloadXlsx(d, slugify(filename));
  };

  const cls = `inline-flex items-center gap-1.5 rounded-md px-2.5 py-1.5 text-[11px] font-bold transition ${VARIANT_CLS[variant]}`;
  return (
    <span ref={ref} className="inline-flex max-w-full flex-wrap shrink-0 items-center gap-2 normal-case tracking-normal">
      <button type="button" className={cls} onClick={onCsv} title="Télécharger les données du tableau au format CSV">
        <Icon name="download" className="h-[12px] w-[12px]" strokeWidth={2.4} />
        Télécharger CSV
      </button>
      <button type="button" className={cls} onClick={onXlsx} title="Télécharger les données du tableau au format Excel">
        <Icon name="table" className="h-[12px] w-[12px]" strokeWidth={2.4} />
        Télécharger Excel
      </button>
    </span>
  );
}
