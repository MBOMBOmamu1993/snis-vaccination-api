"use client";

import { useFilters } from "@/lib/pev/store";
import { cleanName, ymLabel } from "@/lib/pev/calc";

/** Rappel du périmètre filtré (province / antenne / ZS / période). */
export function ScopeNote() {
  const f = useFilters();
  const parts: string[] = [];
  parts.push(f.province ? cleanName(f.province) : "Toutes les provinces");
  if (f.antenne) parts.push(`Antenne ${f.antenne}`);
  if (f.zs) parts.push(cleanName(f.zs));
  const per = f.months.length ? `${ymLabel(f.months[0])} → ${ymLabel(f.months[f.months.length - 1])}` : "Toutes les périodes";
  return (
    <div className="flex flex-wrap items-center gap-2 text-[11.5px] text-surface-600">
      <span className="chip-info">Périmètre</span>
      <b className="text-navy-700">{parts.join(" · ")}</b>
      <span className="text-surface-400">|</span>
      <b className="text-navy-700">{per}</b>
    </div>
  );
}

export function Loading() {
  return (
    <div className="flex flex-col items-center justify-center gap-3 py-20 text-surface-500">
      <div className="grid grid-cols-3 gap-[6px]">
        {Array.from({ length: 9 }).map((_, i) => (
          <i key={i} className="block h-[9px] w-[9px] rounded-full bg-oms-500" style={{ animation: "pevpulse 1.1s infinite ease-in-out", animationDelay: `${(((i % 3) + Math.floor(i / 3)) * 0.12).toFixed(2)}s` }} />
        ))}
      </div>
      <div className="text-[12.5px] font-semibold">Chargement des données DHIS2…</div>
      <style>{`@keyframes pevpulse{0%,80%,100%{opacity:.2;transform:scale(.7)}40%{opacity:1;transform:scale(1)}}`}</style>
    </div>
  );
}
