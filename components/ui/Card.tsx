import { cn } from "@/lib/client/cn";
import { Icon, type IconName } from "@/components/ui/Icon";

/** Tons d'icône colorés (badge plein dégradé) pour les sous-titres de visuels. */
export type HeaderTone = "navy" | "teal" | "violet" | "green" | "orange" | "blue" | "red";

export const HEADER_TONE: Record<HeaderTone, { from: string; to: string }> = {
  navy:   { from: "#0a3a86", to: "#00205c" },
  teal:   { from: "#19c2b1", to: "#0d9488" },
  violet: { from: "#9d5cf5", to: "#7c3aed" },
  green:  { from: "#2bbd6b", to: "#1f9d57" },
  orange: { from: "#fbbf24", to: "#f08c00" },
  blue:   { from: "#36b3ec", to: "#0093d5" },
  red:    { from: "#f4625f", to: "#e23636" },
};

export function Card({ className, children }: { className?: string; children: React.ReactNode }) {
  return <section className={cn("card", className)}>{children}</section>;
}

export function CardHeader({
  title,
  subtitle,
  right,
  icon,
  iconTone = "navy",
}: {
  title: string;
  subtitle?: string;
  right?: React.ReactNode;
  icon?: IconName;
  iconTone?: HeaderTone;
}) {
  const t = HEADER_TONE[iconTone];
  return (
    <header className="card-header">
      <div className="min-w-0 flex items-center gap-2.5">
        {icon ? (
          <span
            className="w-[34px] h-[34px] rounded-[10px] flex items-center justify-center shrink-0 text-white"
            style={{ backgroundImage: `linear-gradient(145deg, ${t.from}, ${t.to})`, boxShadow: `0 5px 13px -5px ${t.to}` }}
          >
            <Icon name={icon} className="w-[18px] h-[18px]" strokeWidth={2} />
          </span>
        ) : null}
        <div className="min-w-0">
          <h2 className="card-title">{title}</h2>
          {subtitle ? <div className="card-subtitle mt-0.5">{subtitle}</div> : null}
        </div>
      </div>
      {right ? <div className="flex items-center gap-2 shrink-0">{right}</div> : null}
    </header>
  );
}

/** Bandeau de section bleu marine OMS (cf. maquette). `right` : contenu aligné
 *  à droite (boutons d'export des tableaux, cf. specs feedback TL 01). */
export function SectionBar({ children, icon, right }: { children: React.ReactNode; icon?: IconName; right?: React.ReactNode }) {
  return (
    <div className="section-bar">
      {icon ? <Icon name={icon} /> : null}
      {children}
      {right ? <span className="ml-auto inline-flex items-center gap-2">{right}</span> : null}
    </div>
  );
}
