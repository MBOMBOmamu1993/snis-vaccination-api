export interface PageDef { id: string; label: string; icon: string; }
export interface ModuleDef {
  key: string; name: string; icon: string; tone: string; live: boolean; desc: string;
  pages: PageDef[];
}

/* Les 4 onglets du Dashboard PEV de routine (présentation type Tableau de bord
   Tshopo/Tshuapa). Source des données : exclusivement DHIS2. */
export const MODULES: ModuleDef[] = [
  {
    key: "qualite", name: "Contrôle qualité des données", icon: "quality", tone: "good", live: true,
    desc: "Complétude & promptitude des rapports · Évolution mensuelle de la qualité des données (DHIS2).",
    pages: [
      { id: "q_completude", label: "Complétude & promptitude", icon: "form" },
      { id: "q_tendance", label: "Évolution mensuelle", icon: "chart" },
    ],
  },
  {
    key: "vaccination", name: "Données de vaccination", icon: "syringe", tone: "navy", live: true,
    desc: "Doses administrées par antigène · Taux d'abandon (DTC1/DTC3, BCG/VAR) · Courbe de suivi mensuelle.",
    pages: [
      { id: "v_antigenes", label: "Vaccination par antigène", icon: "syringe" },
      { id: "v_abandon", label: "Taux d'abandon", icon: "down" },
      { id: "v_courbe", label: "Courbe de suivi", icon: "chart" },
    ],
  },
  {
    key: "logistique", name: "Logistique", icon: "route", tone: "teal", live: true,
    desc: "Suivi des intrants vaccinaux et de la stratégie de vaccination (fixe / avancée / mobile).",
    pages: [
      { id: "l_strategie", label: "Stratégie & disponibilité", icon: "truck" },
    ],
  },
  {
    key: "telecharger", name: "Télécharger canevas revue formative", icon: "download", tone: "warn", live: true,
    desc: "Génération automatique du canevas de revue formative PEV (PPTX éditable), diapos 10 à 12 renseignées avec les données DHIS2 de la période filtrée.",
    pages: [
      { id: "t_canevas", label: "Canevas revue formative", icon: "report" },
    ],
  },
];

export const moduleByKey = (k: string) => MODULES.find((m) => m.key === k);
export const findPage = (m: ModuleDef, id: string) => m.pages.find((p) => p.id === id);
