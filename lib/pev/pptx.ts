"use client";

import JSZip from "jszip";
import type { AggRecord } from "./types";
import { aggregate, dropout, cleanName, ymLabel } from "./calc";
import { num } from "./data";

const TEMPLATE_URL = "/canevas/canevas_revue_formative_pev.pptx";
const A_NS = "http://schemas.openxmlformats.org/drawingml/2006/main";
const P_NS = "http://schemas.openxmlformats.org/presentationml/2006/main";

/* ----------------------------- Construction des données ----------------------------- */

export interface RevueData {
  provinceLabel: string;
  periodLabel: string;
  prevPeriodLabel: string;
  // Diapo 10 — résultats de processus (2 colonnes : période n-1 / période courante)
  slide10: { label: string; prev: string; cur: string }[];
  // Diapo 11 — complétude/promptitude + doses par ZS (ou province)
  slide11: { head: string[]; rows: (string | number)[][] };
  // Diapo 12 — taux d'abandon par ZS (ou province)
  slide12: { head: string[]; rows: (string | number)[][] };
}

const shiftYear = (ym: string, delta: number): string => `${parseInt(ym.slice(0, 4), 10) + delta}${ym.slice(4)}`;

/** Calcule le contenu des diapos 10–12 depuis l'agrégat DHIS2 (province/ZS),
 *  pour la période filtrée et la même période de l'année précédente. */
export function computeRevueData(base: AggRecord[], filters: { province: string; antenne: string; zs: string; months: string[] }, allMonths: string[], groupByZs: boolean): RevueData {
  const scoped = base.filter((r) =>
    (!filters.province || r._Province === filters.province) &&
    (!filters.antenne || r._Antenne === filters.antenne) &&
    (!filters.zs || r._ZS === filters.zs));

  const curMonths = filters.months.length ? filters.months : allMonths;
  const prevMonths = curMonths.map((m) => shiftYear(m, -1));
  const cur = scoped.filter((r) => curMonths.includes(r._YM));
  const prev = scoped.filter((r) => prevMonths.includes(r._YM));

  const periodLabel = curMonths.length ? `${ymLabel(curMonths[0])} – ${ymLabel(curMonths[curMonths.length - 1])}` : "Toutes périodes";
  const prevPeriodLabel = prevMonths.length ? `${ymLabel(prevMonths[0])} – ${ymLabel(prevMonths[prevMonths.length - 1])}` : "—";

  // Proxy stratégie avancée/mobile : entités (CS/ZS) avec doses hors stratégie fixe.
  const countAvm = (recs: AggRecord[]) => {
    const byEnt = new Map<string, number>();
    for (const r of recs) {
      const k = String(r._AS ?? r._ZS ?? r._Province);
      const avm = num(r["DTC1_0_11"]) - num(r["DTC1_0_11_fixe"]);
      byEnt.set(k, (byEnt.get(k) ?? 0) + avm);
    }
    return Array.from(byEnt.values()).filter((v) => v > 0).length;
  };

  const slide10: RevueData["slide10"] = [
    { label: "Nombre de CS avec stratégie avancées", prev: String(countAvm(prev) || "—"), cur: String(countAvm(cur) || "—") },
    { label: "Nombre de CS avec stratégie mobile", prev: "—", cur: "—" },
    { label: "Nombre des séances de vaccination en stratégie fixe (Prévue/réalisée)", prev: "—", cur: "—" },
    { label: "Nombre des séances de vaccination en stratégie avancée (Prévue/réalisée)", prev: "—", cur: "—" },
    { label: "Nombre des séances de vaccination en stratégie mobile (Prévue/réalisée)", prev: "—", cur: "—" },
  ];

  // Diapos 11 & 12 — regroupement par ZS (si province choisie) ou par province.
  const key: keyof AggRecord = groupByZs ? "_ZS" : "_Province";
  const groups = new Map<string, AggRecord[]>();
  for (const r of cur) { const k = String(r[key] ?? "—"); if (!groups.has(k)) groups.set(k, []); groups.get(k)!.push(r); }
  const entries = Array.from(groups.entries()).map(([n, recs]) => ({ n: cleanName(n), a: aggregate(recs) })).sort((x, y) => x.n.localeCompare(y.n, "fr"));

  const slide11 = {
    head: [groupByZs ? "Zone de santé" : "Province", "Complétude %", "Promptitude %", "DTC3", "VAR1"],
    rows: entries.map((e) => [e.n, e.a.comp.toFixed(1), e.a.prompt.toFixed(1), Math.round(e.a.doses.DTC3 ?? 0), Math.round(e.a.doses.VAR1 ?? 0)] as (string | number)[]),
  };
  const slide12 = {
    head: [groupByZs ? "Zone de santé" : "Province", "Abandon DTC1–DTC3 %", "Abandon BCG–VAR1 %"],
    rows: entries.map((e) => [e.n, dropout(e.a.doses.DTC1, e.a.doses.DTC3).toFixed(1), dropout(e.a.doses.BCG, e.a.doses.VAR1).toFixed(1)] as (string | number)[]),
  };

  return {
    provinceLabel: filters.province ? cleanName(filters.province) : "Toutes les provinces",
    periodLabel, prevPeriodLabel, slide10, slide11, slide12,
  };
}

/* ----------------------------- Manipulation XML PPTX ----------------------------- */

const parse = (xml: string) => new DOMParser().parseFromString(xml, "application/xml");
const serialize = (doc: Document) => new XMLSerializer().serializeToString(doc);

function descByLocal(root: Element | Document, local: string): Element[] {
  const out: Element[] = [];
  const walk = (n: Element) => {
    for (const c of Array.from(n.children)) {
      if (c.localName === local) out.push(c);
      walk(c);
    }
  };
  walk(root instanceof Document ? root.documentElement : root);
  return out;
}

/** Affecte le texte d'une cellule (a:tc) en réutilisant/insérant un run a:r. */
function setCellText(doc: Document, tc: Element, text: string) {
  const p = descByLocal(tc, "p")[0];
  if (!p) return;
  let r = descByLocal(p, "r")[0];
  if (!r) {
    r = doc.createElementNS(A_NS, "a:r");
    const rPr = doc.createElementNS(A_NS, "a:rPr");
    rPr.setAttribute("lang", "fr-FR");
    rPr.setAttribute("sz", "1400");
    const latin = doc.createElementNS(A_NS, "a:latin");
    latin.setAttribute("typeface", "Times New Roman");
    rPr.appendChild(latin);
    const t = doc.createElementNS(A_NS, "a:t");
    r.appendChild(rPr); r.appendChild(t);
    // insérer avant un éventuel endParaRPr
    const endPr = descByLocal(p, "endParaRPr")[0];
    if (endPr) p.insertBefore(r, endPr); else p.appendChild(r);
  }
  let t = descByLocal(r, "t")[0];
  if (!t) { t = doc.createElementNS(A_NS, "a:t"); r.appendChild(t); }
  t.textContent = text;
}

/** Construit un graphicFrame contenant une table DrawingML (éditable). */
function tableGraphicFrame(head: string[], rows: (string | number)[][], yEmu: number): string {
  const W = 11277600, X = 457200; // largeur utile, marge gauche
  const first = Math.round(W * 0.42);
  const rest = Math.round((W - first) / Math.max(1, head.length - 1));
  const grid = head.map((_, i) => `<a:gridCol w="${i === 0 ? first : rest}"/>`).join("");
  const cell = (txt: string, bold: boolean, fill?: string) =>
    `<a:tc><a:txBody><a:bodyPr/><a:lstStyle/><a:p><a:pPr algn="${bold ? "ctr" : "l"}"/><a:r><a:rPr lang="fr-FR" sz="1100" b="${bold ? 1 : 0}"><a:solidFill><a:srgbClr val="${bold ? "FFFFFF" : "1E293B"}"/></a:solidFill><a:latin typeface="Calibri"/></a:rPr><a:t>${esc(txt)}</a:t></a:r></a:p></a:txBody>` +
    `<a:tcPr marL="45720" marR="45720" marT="22860" marB="22860" anchor="ctr">${fill ? `<a:solidFill><a:srgbClr val="${fill}"/></a:solidFill>` : ""}</a:tcPr></a:tc>`;
  const headRow = `<a:tr h="370000">${head.map((h) => cell(h, true, "00205C")).join("")}</a:tr>`;
  const bodyRows = rows.map((r, ri) =>
    `<a:tr h="300000">${r.map((c, ci) => cell(String(c), false, ri % 2 ? "EEF4FC" : "FFFFFF") .replace('val="1E293B"', `val="${ci === 0 ? "0A3A86" : "1E293B"}"`)).join("")}</a:tr>`).join("");
  return `<p:graphicFrame xmlns:p="${P_NS}" xmlns:a="${A_NS}" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">` +
    `<p:nvGraphicFramePr><p:cNvPr id="900" name="TableDHIS2"/><p:cNvGraphicFramePr><a:graphicFrameLocks noGrp="1"/></p:cNvGraphicFramePr><p:nvPr/></p:nvGraphicFramePr>` +
    `<p:xfrm><a:off x="${X}" y="${yEmu}"/><a:ext cx="${W}" cy="${300000 * (rows.length + 1)}"/></p:xfrm>` +
    `<a:graphic><a:graphicData uri="http://schemas.openxmlformats.org/drawingml/2006/table">` +
    `<a:tbl><a:tblPr firstRow="1" bandRow="1"><a:tableStyleId>{5C22544A-7EE6-4342-B048-85BDC9FD1C3A}</a:tableStyleId></a:tblPr>` +
    `<a:tblGrid>${grid}</a:tblGrid>${headRow}${bodyRows}</a:tbl></a:graphicData></a:graphic></p:graphicFrame>`;
}

const esc = (s: string) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

function appendTable(slideDoc: Document, head: string[], rows: (string | number)[][], yEmu: number) {
  const spTree = descByLocal(slideDoc, "spTree")[0];
  if (!spTree) return;
  const gf = parse(tableGraphicFrame(head, rows.slice(0, 16), yEmu)).documentElement;
  spTree.appendChild(slideDoc.importNode(gf, true));
}

/* ----------------------------- Génération ----------------------------- */

export async function generateRevuePptx(data: RevueData): Promise<void> {
  const buf = await fetch(TEMPLATE_URL).then((r) => {
    if (!r.ok) throw new Error("Modèle PPTX introuvable");
    return r.arrayBuffer();
  });
  const zip = await JSZip.loadAsync(buf);

  // --- Diapo 1 : page de garde (province + période) ---
  const s1 = zip.file("ppt/slides/slide1.xml");
  if (s1) {
    let xml = await s1.async("string");
    xml = xml.replace(/(ZONE DE SANTÉ DE\s*:?)([^<]*)/i, `$1 ${esc(data.provinceLabel)} — ${esc(data.periodLabel)}`);
    zip.file("ppt/slides/slide1.xml", xml);
  }

  // --- Diapo 10 : tableau de processus ---
  const s10 = zip.file("ppt/slides/slide10.xml");
  if (s10) {
    const doc = parse(await s10.async("string"));
    const tbl = descByLocal(doc, "tbl")[0];
    if (tbl) {
      const trs = descByLocal(tbl, "tr");
      // En-tête (R0) : libellés de période
      const head = descByLocal(trs[0], "tc");
      if (head[1]) setCellText(doc, head[1], data.prevPeriodLabel);
      if (head[2]) setCellText(doc, head[2], data.periodLabel);
      // Lignes de données
      data.slide10.forEach((row, i) => {
        const tr = trs[i + 1];
        if (!tr) return;
        const tcs = descByLocal(tr, "tc");
        if (tcs[1]) setCellText(doc, tcs[1], row.prev);
        if (tcs[2]) setCellText(doc, tcs[2], row.cur);
      });
    }
    zip.file("ppt/slides/slide10.xml", serialize(doc));
  }

  // --- Diapos 11 & 12 : tableaux DHIS2 ajoutés ---
  for (const [file, payload] of [["ppt/slides/slide11.xml", data.slide11], ["ppt/slides/slide12.xml", data.slide12]] as const) {
    const f = zip.file(file);
    if (!f) continue;
    const doc = parse(await f.async("string"));
    appendTable(doc, payload.head, payload.rows, 2600000);
    zip.file(file, serialize(doc));
  }

  const out = await zip.generateAsync({ type: "blob", mimeType: "application/vnd.openxmlformats-officedocument.presentationml.presentation" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(out);
  a.download = `Canevas_Revue_Formative_PEV_${data.provinceLabel.replace(/\s+/g, "_")}.pptx`;
  document.body.appendChild(a); a.click(); a.remove();
}
