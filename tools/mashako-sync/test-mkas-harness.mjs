#!/usr/bin/env node
/** Harnais de test du rendu ZS « visuel unique » (demande Felly 31/07) :
 *  évalue les VRAIES fonctions de docs/index.html (MK_ZS_CFG, MK_ZS_BUILD,
 *  helpers mkZs*) sur les vrais JSON (out-zs/views/, branch mashako-data)
 *  et vérifie que chaque feuille produit la synthèse ZS en tête + les lignes
 *  d'aires de santé, avec les libellés de l'original.
 *  Usage : node test-mkas-harness.mjs  (depuis mashako-sync/)
 *  Fixtures (non versionnées, regénérables) : télécharger les JSON de
 *  https://github.com/MBOMBOmamu1993/snis-vaccination-api/tree/mashako-data/zs/views
 *  vers out-zs/views/ (vues principales + *_AS.json).
 */
import { readFileSync, existsSync, readdirSync } from "node:fs";
import vm from "node:vm";

const FIXDIR = "out-zs/views";
if (!existsSync(FIXDIR) || !readdirSync(FIXDIR).some((f) => f.endsWith(".json"))) {
  console.error("Fixtures absentes : telechargez les JSON de la branche mashako-data (zs/views/) vers " + FIXDIR + "/ — voir l'en-tete de ce fichier.");
  process.exit(2);
}

const HTML = readFileSync("C:/Users/felly/snis-vaccination-api/docs/index.html", "utf8");

/* Extraire les sources telles qu'écrites dans le fichier. */
const pick = (debut, fin) => {
  const i = HTML.indexOf(debut), j = HTML.indexOf(fin, i);
  if (i < 0 || j < 0) throw new Error("extrait introuvable : " + debut.slice(0, 60));
  return HTML.slice(i, j);
};
const src = [
  pick("function mkColorOf(name) {", "function mkColorColFor"),
  pick("function mkColorColFor(c, cols) {", "function mkColByNorm"),
  pick("function mkNorm(c) {", "function mkFmtPct"),
  pick("function mkFmtPct(v) {", "function mkPctNum"),
  pick("function mkPctNum(v) {", "function mkThresholdColor"),
  pick("function mkThresholdColor(v) {", "function mkColorColFor"),
  pick("function mkTruthy(v) {", "function mkNorm(c)"),
  pick("function mkAsVal(r, cle)", "function mkAsAppend"),
  pick("/* ═══ RAPPORT ZONE DE SANTÉ — visuel unique", "function mkDrawView(d, box, vm) {"),
].join("\n");

const echecs = [];
const ok = (cond, msg) => { if (!cond) { echecs.push(msg); console.log("  ✗", msg); } else console.log("  ✓", msg); };

/* Contexte : vraies constantes + stubs des dépendances DOM/réseau. */
const ctx = {
  MK_GREEN: "#2e9e48", MK_YELLOW: "#f2bd00", MK_RED: "#e03531", MK_BLUE: "#3b5cd6",
  esc: (s) => String(s == null ? "" : s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])),
  mkAnt: "Aba", mkPfx: "zs/",
  mkAllLbl: () => "Toutes les ZS",
  mkAsLoad: (u, cb) => cb(null),
  mkBannerHtml: () => "<div>BANNER</div>", mkFillPeriod: () => { },
  mkLegendHtml: () => "", mkExpBtns: () => "", mkWireExpBtns: () => { },
  mkFetchView: (n, cb) => cb(null), mkKpiCard: () => "<div>KPI</div>", mkDeltaHtml: () => "",
  mkHeatMonths: () => ({ mCol: "m", yCol: "y" }), mkMoisRank: () => 0, mkPeriodKeyOf: () => null,
  mkSelMois: "", mkSelAn: "",
  console,
};
vm.createContext(ctx);
vm.runInContext(src, ctx);

const ZONE = "Aba";
function fixture(f) {
  const p = "out-zs/views/" + f + ".json";
  return existsSync(p) ? JSON.parse(readFileSync(p, "utf8")) : null;
}
/* Construit le ctx builder exactement comme mkZsRender. */
function build(name, urlName, mainF, asF) {
  const d = mainF ? fixture(mainF) : { columns: [], rows: [] };
  const as = asF ? fixture(asF) : null;
  const main = { columns: (d && d.columns) || [], rows: (d && d.rows) || [] };
  const antCol = main.columns.filter((c) => /^antenne$/i.test(c))[0] || null;
  const mainRows = antCol ? main.rows.filter((r) => String(r[antCol] || "").trim() === ZONE) : main.rows;
  const asRows = as && as.rows ? as.rows.filter((r) => r._ZS === ZONE && r._ROLE === "AS" && r._AS) : [];
  asRows.sort((a, b) => String(a._AS).localeCompare(String(b._AS), "fr"));
  const hzRow = as && as.rows ? as.rows.filter((r) => r._ZS === ZONE && r._ROLE === "HZ")[0] || null : null;
  const cfg = vm.runInContext(`MK_ZS_CFG[${JSON.stringify(name)}]`, ctx);
  if (!cfg) throw new Error("pas de MK_ZS_CFG pour " + name);
  ctx.__bc = { cfg, name, zone: ZONE, main, mainRows, as, asRows, hzRow };
  return vm.runInContext(`MK_ZS_BUILD[${JSON.stringify(cfg.kind)}](__bc)`, ctx);
}
const rowsH = (b) => (b.rows || []).map((r) => r.h).join("");
const zsH = (b) => (b.zsRow || []).join("");

/* ── Supervision_HZ_P1 ── */
console.log("— Supervision_HZ_P1 (Aba) —");
{
  const b = build("Supervision_HZ_P1", "Supervision_HZ_P1", "Supervision_HZ_P1", "Supervision_HZ_P1_AS");
  ok(b.zsRow && zsH(b).includes("31%") && zsH(b).includes("(5/16)"), "ligne ZS : pastille 31% (5/16) depuis la ligne HZ");
  ok(b.zsRow && /% AS supervisées de qualité/.test(b.zsHead.join("|")) && /bonne localisation/.test(b.zsHead.join("|")), "en-têtes ZS de l'original");
  ok(b.rows.length === 16, `16 lignes AS pour Aba (trouvé ${b.rows.length})`);
  const h = rowsH(b);
  ok(h.includes("✓") || h.includes("✗"), "coches ✓/✗ dans la table AS");
  ok(h.includes("Centre de Santé"), "colonne Centre de Santé remplie");
  ok(!/SUPERVISION|b\d·/.test(h), "pas de fuite technique dans les cellules");
}
/* ── Supervision_HZ_P2 : ZS seule ── */
console.log("— Supervision_HZ_P2 (Aba, ZS seule) —");
{
  const b = build("Supervision_HZ_P2", "Supervision_HZ_P2", "Supervision_HZ_P2", "Supervision_HZ_P2_AS");
  ok(b.zsRow && zsH(b).includes("%"), "bloc synthèse ZS présent");
}
/* ── Séances_HZ_P1 ── */
console.log("— Séances_HZ_P1 (Aba) —");
{
  const b = build("Séances_HZ_P1", "Seances_HZ_P1", "Seances_HZ_P1", "Sances_HZ_P1_AS");
  ok(b.zsRow && zsH(b).includes("(1/2)"), "ligne ZS : fraction (1/2) depuis le JSON principal");
  ok(b.zsRow && zsH(b).includes("50%"), "ligne ZS : 50% présent");
  ok(b.rows.length > 0, `lignes AS présentes (${b.rows.length})`);
  ok(rowsH(b).includes("%"), "cellules % dans la table AS");
}
/* ── Taux d'abandon_HZ_P1 ── */
console.log("— Taux d'abandon_HZ_P1 (Aba) —");
{
  const b = build("Taux d'abandon_HZ_P1", "Taux_d_abandon_HZ_P1", "Taux_d_abandon_HZ_P1", "Tauxdabandon_HZ_P1_AS");
  ok(b.zsRow && zsH(b).includes("(2/2)"), "ligne ZS : fraction (2/2)");
  ok(b.rows.length > 0, `lignes AS présentes (${b.rows.length})`);
  ok(/>\s*8%|>\s*41%|>\s*18%/.test(rowsH(b)), "taux d'abandon réels par aire (8% / 41% / 18%)");
  ok(rowsH(b).includes("✗"), "objectif non atteint rendu en ✗");
}
/* ── Livraison_HZ_P1 ── */
console.log("— Livraison_HZ_P1 (Aba) —");
{
  const b = build("Livraison_HZ_P1", "Livraison_HZ_P1", "Livraison_HZ_P1", "Livraison_HZ_P1_AS");
  ok(b.head.join("|").includes("Approv. en vaccins et intrants") && b.head.join("|").includes("SAB 0,05ml"), "colonnes de l'original (Approv + 13 produits)");
  ok(b.rows.length > 0, `lignes AS (${b.rows.length})`);
  ok(rowsH(b).includes("102%"), "cellule SAB 0,5ml = 102% (donnée réelle Aba)");
  ok(/\(1[\s ]?690 \/ 1[\s ]?663\)/.test(rowsH(b)), "fraction (1690 / 1663) sous le %");
  ok(b.zsRow && b.zsInline === true, "ligne de synthèse ZS épinglée en tête de la même table");
  ok(zsH(b).includes("/"), "synthèse ZS en % d'aires conformes avec fraction");
}
/* ── CDF_HZ_P1 / P2 / NF ── */
console.log("— CDF_HZ_P1 (Aba) —");
{
  const b = build("CDF_HZ_P1", "CDF_HZ_P1", "CDF_HZ_P1", "CDF_HZ_P1_AS");
  ok(b.zsRow && b.zsHead.join("|").includes("Problème ZS"), "bloc ZS avec « Problème ZS »");
  ok(b.head.join("|").includes("Problème AS"), "table AS avec « Problème AS »");
  ok(b.rows.length >= 2, `lignes AS fusionnées (${b.rows.length})`);
  ok(zsH(b).includes("Pas de problème rapporté") || /problème rapporté/.test(zsH(b)), "synthèse ZS du problème");
}
console.log("— CDF_HZ_P2 (Aba, ZS seule) —");
{
  const b = build("CDF_HZ_P2", "CDF_HZ_P2", "CDF_HZ_P2", "CDF_HZ_P2_AS");
  ok(b.zsRow !== null || b.rows.length === 0, "bloc ZS seul (ou note si zone absente)");
}
console.log("— CDF_HZ_NF (Aba) —");
{
  const b = build("CDF_HZ_NF", "CDF_HZ_NF", "CDF_HZ_NF", "CDF_HZ_NF_AS");
  ok(b.head.join("|").includes("Compresseurs dysfonctionnels") && b.head.join("|").includes("givre"), "5 colonnes de pannes");
  ok(b.rows.length > 0 && b.zsRow, "lignes AS + ligne ZS (sommes)");
}
/* ── Vaccine_expiration_HZ_P1 ── */
console.log("— Vaccine_expiration_HZ_P1 (Aba) —");
{
  const b = build("Vaccine_expiration_HZ_P1", "Vaccine_expiration_HZ_P1", "Vaccine_expiration_HZ_P1", "Vaccine_expiration_HZ_P1_AS");
  ok(b.rows.length > 0, `lignes AS (${b.rows.length})`);
  ok(rowsH(b).includes("Normal"), "statut « Normal » vert");
  ok(b.zsRow, "ligne ZS (compteurs HZ)");
}
/* ── Vaccine_dispo_HZ_P1 (sans vue principale) ── */
console.log("— Vaccine_dispo_HZ_P1 (Aba, 100% crosstab) —");
{
  const b = build("Vaccine_dispo_HZ_P1", "Vaccine_dispo_HZ_P1", null, "Vaccine_dispo_HZ_P1_AS");
  ok(b.rows.length > 0, `lignes AS (${b.rows.length})`);
  ok(b.head.join("|").includes("Conditions de disponibilité des vaccins") && b.head.join("|").includes("Box de sécurité"), "colonnes de l'original");
  ok(/>\s*12\s*</.test(rowsH(b)), "compteur de conditions (12 pour Aba)");
  ok(b.zsRow, "ligne ZS (semaines de stock par produit)");
}
/* ── Infirmier_HZ_P1 ── */
console.log("— Infirmier_HZ_P1 (Aba) —");
{
  const b = build("Infirmier_HZ_P1", "Infirmier_HZ_P1", "Infirmier_HZ_P1", "Infirmier_HZ_P1_AS");
  ok(b.rows.length >= 2, `lignes AS (${b.rows.length}) — une seule table, pas de doublon`);
  ok(b.zsRow && /\d/.test(zsH(b)), "ligne ZS = totaux agents");
}
/* ── Supervision_HZ_P3 : constats ── */
console.log("— Supervision_HZ_P3 (Aba) —");
{
  const b = build("Supervision_HZ_P3", "Supervision_HZ_P3", "Supervision_HZ_P3", "Supervision_HZ_P3_AS");
  ok(b.head.join("|").includes("Constats et recommandations"), "colonnes constats");
  ok(b.rows.length > 0 && /22\/07\/2026|27\/07\/2026/.test(rowsH(b)), "dates + constats réels");
}

/* ── Carte de supervision : points + couleurs ── */
console.log("— mkZsCartePoints (Aba, geo extrait) —");
{
  const as = fixture("Supervision_HZ_P1_AS");
  const geo = { Aba: [["Aba", 22.0, 3.0], ["Atadra", 22.1, 3.1], ["Ataki", 22.2, 3.2], ["Baki", 22.3, 3.3], ["ZoneInconnue", 22.4, 3.4]] };
  Object.assign(ctx, { __as: as, __geo: geo });
  const pts = vm.runInContext(`mkZsCartePoints(__as, 'Aba', __geo)`, ctx);
  ok(pts.length >= 5, `points produits (${pts.length})`);
  const aba = pts.find((p) => p.name === "Aba");
  ok(aba && aba.color === "#2e9e48", "Aba (b6·1, qualité vraie) → vert");
  const at = pts.find((p) => p.name === "Atadra");
  ok(at && at.color === "#e03531", "Atadra (b6·0, 0 critère) → rouge");
  ok(aba && /Centre de Santé/.test(aba.label), "étiquette Centre de Santé");
  ok(pts.some((p) => p.etat === "Non supervisée"), "AS géolocalisée sans ligne de supervision → grise");
}
/* ── Onglet Vaccine_dispo_HZ_P1 couvert (réaffiché dans la barre de feuilles) ── */
{
  ok(!!vm.runInContext(`MK_ZS_CFG['Vaccine_dispo_HZ_P1']`, ctx), "MK_ZS_CFG couvre Vaccine_dispo_HZ_P1");
}

/* ── Test moteur : mkZsRender complet avec DOM simulé ── */console.log("— moteur mkZsRender (Séances_HZ_P1, Aba) —");
{
  const mainD = fixture("Seances_HZ_P1"), asD = fixture("Sances_HZ_P1_AS");
  ctx.mkAsLoad = (u, cb) => cb(asD);
  const stubs = {};
  const mkStub = (key) => (stubs[key] = { innerHTML: "", textContent: "", style: {}, disabled: false, onclick: null, oninput: null });
  const box = {
    isConnected: true, _h: "",
    set innerHTML(v) { this._h = v; }, get innerHTML() { return this._h; },
    querySelector: (s) => mkStub(s),
    querySelectorAll: () => [],
  };
  vm.runInContext(`mkZsRender({ name: 'Séances_HZ_P1', urlName: 'Seances_HZ_P1' }, __box, __d)`, Object.assign(ctx, { __box: box, __d: mainD }) || ctx);
  const card = stubs[".mk-zs-body"] ? stubs[".mk-zs-body"].innerHTML : "";
  ok(box._h.includes("pev-tbar") && box._h.includes("BANNER"), "cadre : barre d'outils + bandeau");
  ok(card.includes("50%") && card.includes("(1/2)"), "bloc synthèse ZS rendu dans la carte");
  ok((card.match(/<table/g) || []).length === 2, "bloc ZS + table AS dans la MÊME carte (2 tables empilées, 0 carte séparée)");
  ok(!/Détail par aire de santé —/.test(box._h + card), "plus de carte « Détail par aire de santé » séparée");
  ok(stubs[".mk-count"] && /5 ligne/.test(stubs[".mk-count"].textContent), "compteur sur les lignes AS (5)");
  ok(typeof box._mkXls === "function", "export Excel branché sur le visuel fusionné");
}

console.log(echecs.length ? `\n✗ ${echecs.length} échec(s)` : "\n✓ tous les contrôles passent");
process.exit(echecs.length ? 1 : 0);
