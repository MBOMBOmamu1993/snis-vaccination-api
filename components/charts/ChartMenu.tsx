"use client";

/**
 * Menu d'export des graphiques (bouton ≡ en haut à droite de CHAQUE chart,
 * cf. specs feedback TL 01 §2 — esprit du menu « exporting » Highcharts).
 *
 * Entrées : plein écran · impression · PNG · JPEG · PDF · SVG · CSV · XLS ·
 * tableau de données. Les données CSV/XLS/tableau proviennent soit de
 * `exportData` (fourni par la page), soit d'une extraction générique de
 * l'option ECharts (axes catégorie + séries, pie, radar).
 */
import { useEffect, useRef, useState } from "react";
import type { EChartsCoreOption } from "echarts/core";
import type * as echartsCore from "echarts/core";
import { downloadCsv, downloadXlsx, slugify, type TableData } from "@/components/ui/TableExport";

type EChartsInstance = ReturnType<typeof echartsCore.init>;

/* ----------------- Extraction générique des données du chart ----------------- */

type AnyRec = Record<string, unknown>;
const asArray = <T,>(v: T | T[] | undefined): T[] => (v === undefined ? [] : Array.isArray(v) ? v : [v]);
const pointValue = (d: unknown): string | number | null => {
  if (d === null || d === undefined) return null;
  if (typeof d === "number" || typeof d === "string") return d;
  if (typeof d === "object" && "value" in (d as AnyRec)) {
    const v = (d as AnyRec).value;
    return typeof v === "number" || typeof v === "string" ? v : Array.isArray(v) ? String(v) : null;
  }
  return null;
};

/** Construit columns/rows depuis l'option ECharts (bar/line/pie/radar). */
export function dataFromOption(option: EChartsCoreOption): TableData | null {
  const series = asArray(option.series as AnyRec | AnyRec[]);
  if (!series.length) return null;
  const first = series[0];

  if (first.type === "pie") {
    const data = asArray(first.data as AnyRec[]);
    return {
      columns: ["Catégorie", "Valeur"],
      rows: data.map((d) => [String((d as AnyRec).name ?? ""), pointValue(d)]),
    };
  }

  if (first.type === "radar") {
    const radar = (option.radar ?? {}) as AnyRec;
    const indicators = asArray(radar.indicator as AnyRec[]).map((i) => String(i.name ?? ""));
    const data = asArray(first.data as AnyRec[]);
    return {
      columns: ["Série", ...indicators],
      rows: data.map((d) => {
        const vals = Array.isArray((d as AnyRec).value) ? ((d as AnyRec).value as unknown[]) : [];
        return [String((d as AnyRec).name ?? ""), ...vals.map((v) => (typeof v === "number" ? v : v == null ? null : String(v)))];
      }),
    };
  }

  // Graphiques à axes : l'axe « category » porte les libellés.
  const xa = asArray(option.xAxis as AnyRec | AnyRec[])[0];
  const ya = asArray(option.yAxis as AnyRec | AnyRec[])[0];
  const catAxis = xa?.type === "category" ? xa : ya?.type === "category" ? ya : null;
  if (!catAxis) return null;
  const cats = asArray(catAxis.data as unknown[]).map((c) => String(c ?? ""));
  // Les barres horizontales inversent l'ordre des catégories pour l'affichage —
  // on restitue l'ordre naturel (du haut vers le bas du graphique).
  const horizontal = catAxis === ya;
  const idx = cats.map((_, i) => (horizontal ? cats.length - 1 - i : i));
  const columns = ["Catégorie", ...series.map((s, i) => String(s.name ?? `Série ${i + 1}`))];
  const rows = idx.map((i) => [
    cats[i],
    ...series.map((s) => pointValue(asArray(s.data as unknown[])[i])),
  ]);
  return { columns, rows };
}

/* --------------------------------- Menu --------------------------------- */

interface MenuProps {
  getInstance: () => EChartsInstance | null;
  getContainer: () => HTMLElement | null;
  option: EChartsCoreOption;
  title?: string;
  exportData?: TableData;
}

const ITEM_CLS = "block w-full px-3.5 py-[7px] text-left text-[12px] font-semibold text-slate-600 hover:bg-slate-100 hover:text-navy-700";

export default function ChartMenu({ getInstance, getContainer, option, title, exportData }: MenuProps) {
  const [open, setOpen] = useState(false);
  const [showTable, setShowTable] = useState(false);
  const rootRef = useRef<HTMLDivElement | null>(null);

  // Fermeture au clic extérieur et à Échap.
  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false);
    };
    document.addEventListener("mousedown", onDown);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDown);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);

  const fname = slugify(title ?? "graphique");
  const data = (): TableData | null => exportData ?? dataFromOption(option);

  const pngUrl = (type: "png" | "jpeg" = "png"): string | null => {
    const inst = getInstance();
    if (!inst) return null;
    return inst.getDataURL({ type, pixelRatio: 2, backgroundColor: "#fff" });
  };
  const downloadUrl = (url: string, name: string) => {
    const a = document.createElement("a");
    a.href = url;
    a.download = name;
    document.body.appendChild(a);
    a.click();
    a.remove();
  };

  const act = {
    fullscreen: () => {
      const el = getContainer();
      if (!el) return;
      if (document.fullscreenElement) void document.exitFullscreen();
      else void el.requestFullscreen().then(() => getInstance()?.resize());
    },
    print: () => {
      const url = pngUrl();
      if (!url) return;
      const w = window.open("", "_blank", "width=900,height=650");
      if (!w) return;
      w.document.write(`<html><head><title>${title ?? "Graphique"}</title></head><body style="margin:0;display:flex;align-items:center;justify-content:center"><img src="${url}" style="max-width:100%" onload="window.print();"/></body></html>`);
      w.document.close();
    },
    png: () => {
      const url = pngUrl();
      if (url) downloadUrl(url, `${fname}.png`);
    },
    jpeg: () => {
      const url = pngUrl("jpeg");
      if (url) downloadUrl(url, `${fname}.jpeg`);
    },
    pdf: async () => {
      const url = pngUrl();
      const inst = getInstance();
      if (!url || !inst) return;
      try {
        const { jsPDF } = await import("jspdf");
        const w = inst.getWidth();
        const h = inst.getHeight();
        const landscape = w >= h;
        const doc = new jsPDF({ orientation: landscape ? "landscape" : "portrait", unit: "pt", format: "a4" });
        const pw = doc.internal.pageSize.getWidth() - 60;
        const ph = doc.internal.pageSize.getHeight() - 80;
        const ratio = Math.min(pw / w, ph / h);
        if (title) {
          doc.setFontSize(12);
          doc.text(title, 30, 30);
        }
        doc.addImage(url, "PNG", 30, 44, w * ratio, h * ratio);
        doc.save(`${fname}.pdf`);
      } catch {
        // jspdf indisponible → repli : fenêtre d'impression (« Enregistrer en PDF »).
        act.print();
      }
    },
    svg: async () => {
      const el = getContainer();
      if (!el) return;
      // Instancie un chart jetable en renderer SVG avec la même option.
      const echarts = await import("echarts/core");
      const { SVGRenderer } = await import("echarts/renderers");
      echarts.use([SVGRenderer]);
      const tmp = document.createElement("div");
      tmp.style.cssText = `position:fixed;left:-10000px;top:0;width:${el.clientWidth || 800}px;height:${el.clientHeight || 400}px;`;
      document.body.appendChild(tmp);
      try {
        const inst = echarts.init(tmp, undefined, { renderer: "svg" });
        inst.setOption({ backgroundColor: "#fff", ...option, animation: false } as EChartsCoreOption, true);
        const svg = tmp.querySelector("svg");
        if (svg) {
          const blob = new Blob(['<?xml version="1.0" encoding="UTF-8"?>\n' + svg.outerHTML], { type: "image/svg+xml;charset=utf-8" });
          downloadUrl(URL.createObjectURL(blob), `${fname}.svg`);
        }
        inst.dispose();
      } finally {
        tmp.remove();
      }
    },
    csv: () => {
      const d = data();
      if (d) downloadCsv(d, fname);
    },
    xls: () => {
      const d = data();
      if (d) void downloadXlsx(d, fname);
    },
    table: () => setShowTable(true),
  };

  const item = (label: string, fn: () => void | Promise<void>) => (
    <button type="button" className={ITEM_CLS} onClick={() => { setOpen(false); void fn(); }}>
      {label}
    </button>
  );

  const tableData = showTable ? data() : null;

  return (
    <div ref={rootRef} className="absolute right-1.5 top-1.5 z-30 print:hidden">
      <button
        type="button"
        aria-label="Menu d'export du graphique"
        title="Exporter le graphique"
        onClick={() => setOpen((o) => !o)}
        className="flex h-[24px] w-[24px] items-center justify-center rounded-md text-slate-400 transition hover:bg-slate-100 hover:text-slate-600"
      >
        <svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" strokeWidth={2.4} strokeLinecap="round">
          <path d="M4 6h16M4 12h16M4 18h16" />
        </svg>
      </button>
      {open ? (
        <div className="absolute right-0 top-[28px] z-40 w-[230px] overflow-hidden rounded-lg border border-slate-200 bg-white py-1 shadow-[0_14px_34px_-12px_rgba(15,23,42,.35)]">
          {item("Afficher en plein écran", act.fullscreen)}
          {item("Imprimer le graphique", act.print)}
          <div className="my-1 border-t border-slate-100" />
          {item("Télécharger l'image PNG", act.png)}
          {item("Télécharger l'image JPEG", act.jpeg)}
          {item("Télécharger le document PDF", act.pdf)}
          {item("Télécharger l'image SVG", act.svg)}
          <div className="my-1 border-t border-slate-100" />
          {item("Télécharger CSV", act.csv)}
          {item("Télécharger XLS", act.xls)}
          {item("Afficher le tableau de données", act.table)}
        </div>
      ) : null}
      {showTable ? (
        <div
          className="fixed inset-0 z-[120] flex items-center justify-center bg-[rgba(0,19,47,.5)] p-6"
          onClick={() => setShowTable(false)}
          onKeyDown={(e) => { if (e.key === "Escape") setShowTable(false); }}
        >
          <div className="max-h-[80vh] w-full max-w-[760px] overflow-auto rounded-xl border border-slate-200 bg-white p-4 shadow-2xl" onClick={(e) => e.stopPropagation()}>
            <div className="mb-2 flex items-center justify-between gap-3">
              <div className="text-[13px] font-extrabold text-navy-700">{title ?? "Tableau de données"}</div>
              <button type="button" className="rounded-md border border-slate-200 px-2 py-1 text-[11px] font-bold text-slate-500 hover:border-oms-500 hover:text-oms-600" onClick={() => setShowTable(false)}>
                Fermer
              </button>
            </div>
            {tableData ? (
              <table className="dtable">
                <thead><tr>{tableData.columns.map((c, i) => <th key={i} className={i === 0 ? "name" : undefined}>{c}</th>)}</tr></thead>
                <tbody>
                  {tableData.rows.map((r, i) => (
                    <tr key={i}>{r.map((v, j) => <td key={j} className={j === 0 ? "name" : undefined}>{v === null || v === "" ? "—" : v}</td>)}</tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <div className="py-8 text-center text-[12px] text-surface-500">Données du graphique non disponibles.</div>
            )}
          </div>
        </div>
      ) : null}
    </div>
  );
}
