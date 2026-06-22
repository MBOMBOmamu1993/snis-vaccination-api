import { fmtPct } from "@/lib/client/format";
import { Icon, type IconName } from "@/components/ui/Icon";

export type KpiTone = "neutral" | "navy" | "good" | "warn" | "bad" | "brand" | "violet" | "teal";

/* Cartes KPI « pleines » fidèles au dashboard Shiny PEV RDC : fond saturé en
 * dégradé, texte blanc, grande icône en filigrane dans le coin haut-droit.
 * Seul l'habillage change — aucune donnée, aucun calcul, aucune prop ni logique
 * de choix de ton (good/warn/bad…) n'est touché. */
const TONE: Record<KpiTone, { from: string; to: string }> = {
  neutral: { from: "#5b6b86", to: "#46536b" },
  navy:    { from: "#0a3a86", to: "#00205c" },
  good:    { from: "#2bbd6b", to: "#1f9d57" },
  warn:    { from: "#fbab2d", to: "#f08c00" },
  bad:     { from: "#f0524f", to: "#dc2f2f" },
  brand:   { from: "#2bb0ee", to: "#0093d5" },
  violet:  { from: "#9d5cf5", to: "#7c3aed" },
  teal:    { from: "#19c2b1", to: "#0d9488" },
};

export function KpiCard({
  label,
  value,
  pct,
  tone = "neutral",
  icon,
  hint,
  sub,
}: {
  label: React.ReactNode;
  value: React.ReactNode;
  /** % de réalisation (affiché en sous-titre si fourni). */
  pct?: number | null;
  tone?: KpiTone;
  icon?: IconName;
  hint?: string;
  /** Sous-titre libre (alternatif à `pct`). */
  sub?: string;
}) {
  const t = TONE[tone];
  return (
    <div
      title={hint}
      className="kpi-tile relative overflow-hidden rounded-2xl px-4 py-3.5 flex flex-col justify-between min-h-[112px] text-white transition-transform duration-200 will-change-transform hover:-translate-y-0.5"
      style={{
        backgroundImage: `linear-gradient(150deg, ${t.from}, ${t.to})`,
        boxShadow: `0 10px 24px -14px ${t.to}cc, inset 0 1px 0 rgba(255,255,255,0.18)`,
      }}
    >
      {icon ? (
        <Icon
          name={icon}
          aria-hidden
          className="pointer-events-none absolute -right-3 -top-3 w-[82px] h-[82px] opacity-[0.16]"
          strokeWidth={1.4}
        />
      ) : null}
      <div className="relative text-[11px] font-bold uppercase tracking-[0.05em] leading-tight text-white/90">{label}</div>
      <div className="relative">
        <div className="text-[30px] font-extrabold leading-none tabular-nums drop-shadow-[0_1px_1px_rgba(0,0,0,0.18)]">{value}</div>
        {pct !== undefined ? (
          <div className="text-[11.5px] font-semibold mt-1.5">
            {pct === null ? (
              <span className="text-white/65">% réalisation : n/d</span>
            ) : (
              <span className="text-white/85">% réalisation : <span className="font-extrabold text-white">{fmtPct(pct)}</span></span>
            )}
          </div>
        ) : sub ? (
          <div className="text-[11.5px] font-medium text-white/85 mt-1.5">{sub}</div>
        ) : null}
      </div>
    </div>
  );
}
