import { SectionBar, TableExportButtons } from "snis-vaccination-dhis2";

/** Bandeau de section navy OMS avec icône. */
export const BandeauSimple = () => (
  <SectionBar icon="clipboard">Contrôle qualité des données</SectionBar>
);

/** Avec actions alignées à droite (boutons d'export, variante « bar »). */
export const AvecExport = () => (
  <SectionBar
    icon="syringe"
    right={
      <TableExportButtons
        variant="bar"
        filename="doses_par_antigene"
        data={{ columns: ["Antigène", "Doses"], rows: [["BCG", 12040], ["Penta3", 9860]] }}
      />
    }
  >
    Données de vaccination — doses par antigène
  </SectionBar>
);

/** Plusieurs sections successives structurant une page. */
export const SectionsDePage = () => (
  <div>
    <SectionBar icon="clipboard">Complétude &amp; promptitude</SectionBar>
    <div className="mb-4 text-[12px] text-surface-700">Contenu de la section…</div>
    <SectionBar icon="analyse">Évolution mensuelle</SectionBar>
    <div className="mb-4 text-[12px] text-surface-700">Contenu de la section…</div>
    <SectionBar icon="truck">Logistique — stratégie fixe / avancée</SectionBar>
    <div className="text-[12px] text-surface-700">Contenu de la section…</div>
  </div>
);
