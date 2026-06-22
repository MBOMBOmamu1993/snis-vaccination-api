/* -------------------------------------------------------------------------- */
/* Helpers de décoration visuelle des graphiques (DESIGN ONLY).                */
/* Génèrent des dégradés et ombres dérivés d'une couleur de base, pour donner  */
/* aux barres / donuts le rendu « lissé » du dashboard Shiny PEV RDC.          */
/* IMPORTANT : aucune donnée, aucun calcul, aucune couleur métier n'est        */
/* modifié — on ne fait qu'habiller une teinte existante (la teinte de base    */
/* fournie par la page est conservée comme couleur pleine du dégradé).         */
/* -------------------------------------------------------------------------- */

type ColorStop = { offset: number; color: string };
export type LinearGradient = {
  type: "linear";
  x: number;
  y: number;
  x2: number;
  y2: number;
  colorStops: ColorStop[];
};

function hexToRgb(hex: string): [number, number, number] {
  const h = hex.replace("#", "");
  const f = h.length === 3 ? h.split("").map((c) => c + c).join("") : h;
  return [parseInt(f.slice(0, 2), 16), parseInt(f.slice(2, 4), 16), parseInt(f.slice(4, 6), 16)];
}

/** Éclaircit une couleur hex vers le blanc (amount 0 → 1). */
function lighten(hex: string, amount: number): string {
  const [r, g, b] = hexToRgb(hex);
  const m = (c: number) => Math.round(c + (255 - c) * amount);
  return `rgb(${m(r)}, ${m(g)}, ${m(b)})`;
}

/** Couleur hex en rgba avec alpha. */
export function rgba(hex: string, a: number): string {
  const [r, g, b] = hexToRgb(hex);
  return `rgba(${r}, ${g}, ${b}, ${a})`;
}

/** Dégradé vertical pour barres montantes : clair en haut → teinte pleine en bas. */
export function barUp(base: string): LinearGradient {
  return {
    type: "linear", x: 0, y: 0, x2: 0, y2: 1,
    colorStops: [
      { offset: 0, color: lighten(base, 0.24) },
      { offset: 1, color: base },
    ],
  };
}

/** Dégradé horizontal pour barres couchées : teinte pleine → clair vers la droite. */
export function barAcross(base: string): LinearGradient {
  return {
    type: "linear", x: 0, y: 0, x2: 1, y2: 0,
    colorStops: [
      { offset: 0, color: base },
      { offset: 1, color: lighten(base, 0.28) },
    ],
  };
}

/**
 * Style d'`itemStyle` complet pour une barre : dégradé + coins arrondis + ombre
 * douce de la même teinte, + survol légèrement plus clair. `dir` choisit
 * l'orientation du dégradé/arrondi. `base` est la teinte d'origine de la page.
 */
export function barStyle(base: string, dir: "up" | "across") {
  const radius = dir === "up" ? [6, 6, 0, 0] : [0, 6, 6, 0];
  return {
    color: dir === "up" ? barUp(base) : barAcross(base),
    borderRadius: radius as [number, number, number, number],
    shadowColor: rgba(base, 0.28),
    shadowBlur: 8,
    shadowOffsetY: dir === "up" ? 3 : 0,
    shadowOffsetX: dir === "across" ? 3 : 0,
  };
}

/** Effet de survol commun aux barres (teinte plus lumineuse + ombre accentuée). */
export const barEmphasis = {
  itemStyle: { shadowBlur: 14, shadowColor: "rgba(0,32,92,0.30)" },
} as const;

/** Habillage commun des donuts : segments arrondis séparés par du blanc + ombre. */
export const donutItemStyle = {
  borderColor: "#ffffff",
  borderWidth: 2,
  borderRadius: 6,
  shadowBlur: 12,
  shadowColor: "rgba(0,32,92,0.18)",
} as const;
