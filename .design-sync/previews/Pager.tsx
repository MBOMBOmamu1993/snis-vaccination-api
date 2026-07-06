import { Pager } from "snis-vaccination-dhis2";

const noop = (_p: number) => {};

/** Première page d'un tableau volumineux (30 lignes par page). */
export const PremierePage = () => (
  <Pager page={1} pageCount={12} setPage={noop} start={0} end={30} total={347} />
);

/** Page courante au milieu : fenêtre glissante + ellipses des deux côtés. */
export const PageIntermediaire = () => (
  <Pager page={6} pageCount={12} setPage={noop} start={150} end={180} total={347} />
);

/** Dernière page (bouton « Suivant » désactivé, tranche partielle). */
export const DernierePage = () => (
  <Pager page={12} pageCount={12} setPage={noop} start={330} end={347} total={347} />
);
