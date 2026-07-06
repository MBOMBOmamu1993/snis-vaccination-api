import { Icon, type IconName } from "snis-vaccination-dhis2";

const cell = (name: IconName) => (
  <div key={name} style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 6, width: 74 }}>
    <Icon name={name} className="w-6 h-6 text-navy-700" />
    <span className="text-[10px] text-surface-700 font-medium">{name}</span>
  </div>
);

const NAVIGATION: IconName[] = ["home", "analyse", "report", "map", "pin", "calendar", "download", "refresh"];
const SANTE: IconName[] = ["syringe", "child", "people", "clinic", "hospital", "fridge", "truck", "shield"];
const DONNEES: IconName[] = ["clipboard", "doc", "database", "bars", "layers", "check", "alert", "trophy"];

/** Icônes de navigation et d'action (trait 24×24, hérite de currentColor). */
export const Navigation = () => (
  <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>{NAVIGATION.map(cell)}</div>
);

/** Icônes du domaine santé / vaccination / logistique. */
export const SanteEtLogistique = () => (
  <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>{SANTE.map(cell)}</div>
);

/** Icônes données & indicateurs. */
export const DonneesEtIndicateurs = () => (
  <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>{DONNEES.map(cell)}</div>
);

/** La couleur et la taille viennent du contexte (currentColor + className). */
export const TaillesEtCouleurs = () => (
  <div style={{ display: "flex", alignItems: "center", gap: 14 }}>
    <Icon name="syringe" className="w-4 h-4 text-surface-700" />
    <Icon name="syringe" className="w-6 h-6 text-oms-500" />
    <Icon name="syringe" className="w-8 h-8 text-navy-700" />
    <Icon name="syringe" className="w-10 h-10 text-good-500" />
    <span className="section-bar" style={{ display: "inline-flex", marginBottom: 0 }}>
      <Icon name="syringe" /> Dans un bandeau
    </span>
  </div>
);
