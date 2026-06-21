/** Enregistrement agrégé tel que produit par le pipeline DHIS2 (backfill →
 *  aggregate). On NE modifie PAS la structure produite par les scripts Python ;
 *  on la consomme telle quelle. */
export interface AggRecord {
  _Province: string;
  _Antenne?: string;
  _ZS?: string;
  _AS?: string;
  _YM: string;        // "AAAAMM"
  n: number;          // structures attendues
  rap: number;        // structures ayant rapporté
  comp: number;       // complétude % (déjà calculée par le pipeline)
  prompt: number;     // promptitude % (déjà calculée par le pipeline)
  [field: string]: string | number | undefined; // <Antigène>_0_11, _fixe, …
}

export interface Meta {
  generated_at: string;
  total_records: number;
  provinces: string[];
  province_slugs: Record<string, string>;
  antennes: string[];
  months: string[];
  include_pop: boolean;
}
