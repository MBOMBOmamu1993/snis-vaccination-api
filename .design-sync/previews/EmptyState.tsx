import { Card, EmptyState } from "snis-vaccination-dhis2";

/** État vide par défaut. */
export const ParDefaut = () => <EmptyState />;

/** Titre et message personnalisés. */
export const Personnalise = () => (
  <EmptyState
    title="Aucune province sélectionnée"
    message="Choisissez au moins une province et une période dans la barre de filtres pour afficher les données DHIS2."
  />
);

/** Dans une carte (usage type : visuel sans données pour le filtre courant). */
export const DansUneCarte = () => (
  <Card>
    <EmptyState message="Aucune donnée disponible pour la période sélectionnée." />
  </Card>
);
