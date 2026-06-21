"use client";

import { QualiteCompletude, QualiteTendance } from "./pages/Qualite";
import { VaccinationAntigenes, VaccinationSeances, VaccinationAbandon, VaccinationCourbe } from "./pages/Vaccination";
import { LogistiqueStrategie } from "./pages/Logistique";
import { TelechargerCanevas } from "./pages/Telecharger";

/** id de page → composant de rendu. */
export const PAGE_REGISTRY: Record<string, () => JSX.Element> = {
  q_completude: QualiteCompletude,
  q_tendance: QualiteTendance,
  v_antigenes: VaccinationAntigenes,
  v_seances: VaccinationSeances,
  v_abandon: VaccinationAbandon,
  v_courbe: VaccinationCourbe,
  l_strategie: LogistiqueStrategie,
  t_canevas: TelechargerCanevas,
};
