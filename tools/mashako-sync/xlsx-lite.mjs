/** Lecteur XLSX minimal, sans dépendance (le projet n'a que Playwright).
 *
 *  Un .xlsx est une archive ZIP de fichiers XML. On n'a besoin que de :
 *    xl/workbook.xml         → noms des feuilles, dans l'ordre
 *    xl/sharedStrings.xml    → table des chaînes (les cellules texte y renvoient)
 *    xl/worksheets/sheetN.xml→ les cellules
 *  Les exports « Tableau croisé » de Tableau n'ont ni formules, ni dates, ni
 *  styles à interpréter : chaque cellule est une chaîne partagée (t="s") ou un
 *  nombre brut. C'est tout ce qu'on décode.
 */
import { inflateRawSync } from "node:zlib";

/* ── ZIP : on lit le répertoire central, seul endroit fiable pour les tailles
   (les en-têtes locaux peuvent renvoyer à un descripteur placé APRÈS les
   données). ── */
export function unzip(buf) {
  const files = {};
  /* fin du répertoire central : signature 0x06054b50, cherchée depuis la fin */
  let eocd = -1;
  for (let i = buf.length - 22; i >= 0 && i > buf.length - 66000; i--) {
    if (buf.readUInt32LE(i) === 0x06054b50) { eocd = i; break; }
  }
  if (eocd < 0) throw new Error("archive ZIP illisible (fin de répertoire absente)");
  const nEntries = buf.readUInt16LE(eocd + 10);
  let p = buf.readUInt32LE(eocd + 16);
  for (let k = 0; k < nEntries; k++) {
    if (buf.readUInt32LE(p) !== 0x02014b50) break;
    const method = buf.readUInt16LE(p + 10);
    const compSize = buf.readUInt32LE(p + 20);
    const nameLen = buf.readUInt16LE(p + 28);
    const extraLen = buf.readUInt16LE(p + 30);
    const cmtLen = buf.readUInt16LE(p + 32);
    const lho = buf.readUInt32LE(p + 42);
    const name = buf.toString("utf8", p + 46, p + 46 + nameLen);
    /* en-tête local : les longueurs nom/extra y diffèrent souvent du central */
    const lNameLen = buf.readUInt16LE(lho + 26);
    const lExtraLen = buf.readUInt16LE(lho + 28);
    const start = lho + 30 + lNameLen + lExtraLen;
    const raw = buf.subarray(start, start + compSize);
    if (!name.endsWith("/")) {
      files[name] = method === 0 ? Buffer.from(raw) : inflateRawSync(raw);
    }
    p += 46 + nameLen + extraLen + cmtLen;
  }
  return files;
}

const unesc = (s) => s
  .replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"')
  .replace(/&apos;/g, "'").replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCodePoint(parseInt(h, 16)))
  .replace(/&#(\d+);/g, (_, d) => String.fromCodePoint(+d))
  .replace(/&amp;/g, "&");

/* une chaîne partagée peut être découpée en plusieurs <t> (formatage riche) */
function sharedStrings(xml) {
  if (!xml) return [];
  const out = [];
  const re = /<si>([\s\S]*?)<\/si>/g;
  let m;
  while ((m = re.exec(xml))) {
    let s = "";
    const rt = /<t[^>]*>([\s\S]*?)<\/t>/g;
    let t;
    while ((t = rt.exec(m[1]))) s += unesc(t[1]);
    out.push(s);
  }
  return out;
}

const colIndex = (ref) => {
  const m = /^([A-Z]+)/.exec(ref);
  if (!m) return 0;
  let n = 0;
  for (const c of m[1]) n = n * 26 + (c.charCodeAt(0) - 64);
  return n - 1;
};

function sheetRows(xml, ss) {
  const rows = [];
  const rowRe = /<row[^>]*>([\s\S]*?)<\/row>/g;
  let r;
  while ((r = rowRe.exec(xml))) {
    const cells = [];
    /* ⚠ La cellule VIDE s'écrit `<c r="A3" s="1"/>`. Si l'alternative ouvrante
       est testée d'abord, `([^>]*)` avale le `/` final et `([\s\S]*?)</c>` va
       chercher la fermeture de la cellule SUIVANTE : deux cellules fusionnent,
       toute la ligne se décale d'une colonne et l'index de chaîne partagée de
       la voisine s'affiche à la place du libellé (constaté 28/07 sur les
       exports « Aire de Santé » : « 52, 55, 58 » au lieu des noms d'aires).
       L'alternative auto-fermante doit donc passer EN PREMIER. */
    const cRe = /<c([^>]*?)\/>|<c([^>]*?)>([\s\S]*?)<\/c>/g;
    let c;
    while ((c = cRe.exec(r[1]))) {
      const vide = c[1] !== undefined;
      const attrs = vide ? c[1] : (c[2] || "");
      const inner = vide ? "" : (c[3] || "");
      const ref = (/r="([A-Z]+\d+)"/.exec(attrs) || [])[1] || "";
      const type = (/t="([^"]+)"/.exec(attrs) || [])[1] || "";
      let v = "";
      if (type === "inlineStr") {
        const t = /<t[^>]*>([\s\S]*?)<\/t>/.exec(inner);
        v = t ? unesc(t[1]) : "";
      } else {
        const vm = /<v>([\s\S]*?)<\/v>/.exec(inner);
        const raw = vm ? unesc(vm[1]) : "";
        v = type === "s" ? (ss[+raw] ?? "") : raw;
      }
      const i = ref ? colIndex(ref) : cells.length;
      while (cells.length < i) cells.push("");
      cells[i] = v;
    }
    rows.push(cells);
  }
  return rows;
}

/** → [{ name, rows }] dans l'ordre du classeur. */
export function readXlsx(buf) {
  const f = unzip(buf);
  const dec = (n) => (f[n] ? f[n].toString("utf8") : "");
  const ss = sharedStrings(dec("xl/sharedStrings.xml"));

  /* nom de feuille ↔ fichier : workbook.xml donne l'ordre et les r:id,
     workbook.xml.rels fait le lien r:id → worksheets/sheetN.xml */
  const rels = {};
  for (const m of dec("xl/_rels/workbook.xml.rels").matchAll(/<Relationship([^>]*)\/>/g)) {
    const id = (/Id="([^"]+)"/.exec(m[1]) || [])[1];
    const tgt = (/Target="([^"]+)"/.exec(m[1]) || [])[1];
    if (id && tgt) rels[id] = tgt.replace(/^\/?xl\//, "").replace(/^\//, "");
  }
  const out = [];
  for (const m of dec("xl/workbook.xml").matchAll(/<sheet([^>]*)\/>/g)) {
    const name = unesc((/name="([^"]*)"/.exec(m[1]) || [])[1] || "");
    const rid = (/r:id="([^"]+)"/.exec(m[1]) || [])[1];
    const path = rels[rid];
    const xml = path ? dec("xl/" + path) : "";
    out.push({ name, rows: xml ? sheetRows(xml, ss) : [] });
  }
  return out;
}
