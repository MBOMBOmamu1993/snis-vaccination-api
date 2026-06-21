import { cn } from "@/lib/client/cn";
import { fmtPct } from "@/lib/client/format";
import { Icon, type IconName } from "@/components/ui/Icon";

export type KpiTone = "neutral" | "navy" | "good" | "warn" | "bad" | "brand" | "violet" | "teal";

const TONE: Record<KpiTone, { ico: string; bg: string; border: string; text: string }> = {
  neutral: { ico: "#475569", bg: "#f6f8fb", border: "#e2e8f0", text: "#1e293b" },
  navy:    { ico: "#00205c", bg: "#e7ecf6", border: "#c5d2ea", text: "#00205c" },
  good:    { ico: "#1f9d57", bg: "#e9f8f0", border: "#bce6cf", text: "#178a44" },
  warn:    { ico: "#f59e0b", bg: "#fff5e4", border: "#fbe2ad", text: "#c87b04" },
  bad:     { ico: "#e23636", bg: "#fdecec", border: "#f6c4c4", text: "#c81e1e" },
  brand:   { ico: "#0093d5", bg: "#e6f3fb", border: "#bce0f3", text: "#0078ae" },
  violet:  { ico: "#7c3aed", bg: "#f2ecfe", border: "#ddc9fb", text: "#6d28d9" },
  teal:    { ico: "#0d9488", bg: "#e6f6f4", border: "#b6e3dd", text: "#0f766e" },
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
      className="relative rounded-2xl border p-4 pt-4 flex flex-col items-center text-center transition hover:-translate-y-0.5 hover:shadow-card"
      style={{ background: t.bg, borderColor: t.border }}
    >
      {icon ? (
        <div
          className="w-[52px] h-[52px] rounded-full flex items-center justify-center mb-2 shadow-[0_6px_14px_-6px_rgba(0,0,0,0.35)]"
          style={{ background: t.ico, color: "#fff" }}
        >
          <Icon name={icon} className="w-[25px] h-[25px]" strokeWidth={1.9} />
        </div>
      ) : null}
      <div className="text-[11.5px] font-bold leading-tight" style={{ color: t.text }}>{label}</div>
      <div className="text-[28px] font-extrabold leading-none mt-1.5 mb-1 tabular-nums" style={{ color: t.text }}>{value}</div>
      {pct !== undefined ? (
        <div className="text-[11.5px] text-surface-700 font-medium">
          {pct === null ? <span className="text-surface-400">% réalisation : n/d</span> : <>% réalisation : <span className="font-extrabold" style={{ color: t.text }}>{fmtPct(pct)}</span></>}
        </div>
      ) : sub ? (
        <div className="text-[11.5px] text-surface-700 font-medium">{sub}</div>
      ) : null}
    </div>
  );
}
