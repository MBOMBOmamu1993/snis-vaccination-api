#!/usr/bin/env node
/** Harnais de test mkAsAppend : évalue les VRAIES fonctions de docs/index.html
 *  sur les vrais JSON fusionnés (out-zs/views/) et vérifie le rendu corrigé.
 *  Usage : node test-mkas-harness.mjs  (depuis mashako-sync/)
 */
import { readFileSync } from "node:fs";
import vm from "node:vm";

const HTML = readFileSync("C:/Users/felly/snis-vaccination-api/docs/index.html", "utf8");

/* Extraire les sources des fonctions à tester, telles qu'écrites dans le fichier. */
const pick = (debut, fin) => {
  const i = HTML.indexOf(debut), j = HTML.indexOf(fin, i);
  if (i < 0 || j < 0) throw new Error("extrait introuvable : " + debut.slice(0, 50));
  return HTML.slice(i, j);
};
const src = [
  pick("var MK_AS_LABELS = {", "function mkAsVal"),
  pick("function mkAsJunk(c) {", "/* → [{ bloc"),
  pick("function mkAsBlocs(colonnes) {", "function mkAsVal"),
  pick("function mkAsVal(r, cle)", "function mkAsAppend"),
  pick("function mkAsAppend(v, box) {", "/* ═══ RENDU FIDÈLE"),
].join("\n");

const echecs = [];
const ok = (cond, msg) => { if (!cond) { echecs.push(msg); console.log("  ✗", msg); } else console.log("  ✓", msg); };

function rendre(fichier, zone) {
  const d = JSON.parse(readFileSync("out-zs/views/" + fichier, "utf8"));
  let htmlProduit = null;
  const ctx = {
    esc: (s) => String(s),
    mkColorOf: (n) => ({ green: "#2e9e48", red: "#e03531", yellow: "#f2bd00", GREEN: "#2e9e48" }[String(n || "").toLowerCase()] || null),
    mkPfx: "zs/", mkAnt: zone,
    mkAsLoad: (u, cb) => cb(d),
    document: { createElement: () => ({ set className(v) { }, set innerHTML(v) { htmlProduit = v; } }) },
    box: { isConnected: true, querySelector: () => null, appendChild: () => { } },
    console,
  };
  vm.createContext(ctx);
  vm.runInContext(src, ctx);
  vm.runInContext(`mkAsAppend({ name: 'x', urlName: '${fichier.replace("_AS.json", "")}' }, box)`, ctx);
  return htmlProduit;
}

console.log("— Supervision_HZ_P1, zone Aba —");
let h = rendre("Supervision_HZ_P1_AS.json", "Aba");
ok(h && h.includes("Bonne localisation") && h.includes("Cohérence des données") && h.includes("Bonne durée") && h.includes("Supervision de qualité"), "libellés Supervision dans l'ordre réel");
ok(h && !/>1<\/td>/.test(h) && !/· 1</.test(h), "plus de « 1 » brut dans les cellules");
ok(h && />(Oui|Non|Vrai|Faux|Qualité satisfaisante|Qualité insatisfaisante|0)</.test(h), "les critères pivot affichent leur libellé");
ok(h && !/SUPERVISION/.test(h), "pas de colonne technique SUPERVISION");

console.log("— Livraison_HZ_P1, zone Aba —");
h = rendre("Livraison_HZ_P1_AS.json", "Aba");
ok(h && h.includes("SAB 0,5ml") && h.includes("BCG") && h.includes("Rota") && h.includes("Toutes les conditions de livraison respectées") && h.includes("Approv. en vaccins et intrants"), "en-têtes antigènes Livraison présents");
/* la 1re ligne AS « Aba » a b2·_perc_05_delivery_new = 1,0162 → cellule « 102% » */
ok(h && />102%<\/td>/.test(h), "cellule antigène SAB 0,5ml = « 102% » (donnée réelle Aba)");
ok(h && !/· 0</.test(h), "pas de « · 0 » parasite (SUPERVISION filtré)");

console.log("— Infirmier_HZ_P1, zone Aba —");
h = rendre("Infirmier_HZ_P1_AS.json", "Aba");
ok(h && h.includes("Agents formés au PEV") && h.includes("Nombre d'agents"), "libellés Infirmier");
ok(h && !/SUPERVISION/.test(h) && !/· 0</.test(h), "pas de fuite technique");

console.log(echecs.length ? `\n✗ ${echecs.length} échec(s)` : "\n✓ tous les contrôles passent");
process.exit(echecs.length ? 1 : 0);
