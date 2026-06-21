"use client";

import { useEffect, useMemo, useState } from "react";
import { useFilters } from "@/lib/pev/store";
import { loadMeta, loadZs, useAsync } from "@/lib/pev/data";
import { cleanName, ymLabel } from "@/lib/pev/calc";
import { Icon } from "@/components/ui/Icon";

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex min-w-0 flex-col gap-[3px]">
      <label className="px-0.5 text-[10px] font-extrabold uppercase tracking-[0.09em] text-slate-500">{label}</label>
      {children}
    </div>
  );
}

function Select({ value, onChange, options, placeholder, disabled }: {
  value: string; onChange: (v: string) => void; options: { v: string; l: string }[]; placeholder: string; disabled?: boolean;
}) {
  return (
    <div className={`flex items-center rounded-xl border border-slate-200 bg-white px-2.5 py-2 shadow-[0_1px_2px_rgba(15,23,42,0.04)] transition hover:border-oms-500 ${disabled ? "opacity-50" : ""}`}>
      <select className="w-full cursor-pointer bg-transparent text-[12.5px] font-bold text-navy-700 outline-none" value={value} disabled={disabled} onChange={(e) => onChange(e.target.value)}>
        <option value="">{placeholder}</option>
        {options.map((o) => <option key={o.v} value={o.v}>{o.l}</option>)}
      </select>
    </div>
  );
}

export function FilterBar() {
  const f = useFilters();
  const { data: meta } = useAsync(loadMeta, []);
  // by_zs : chargé seulement quand une province est sélectionnée (cascade Antenne/ZS).
  const { data: zs } = useAsync(async () => (f.province ? loadZs() : Promise.resolve(null)), [f.province]);

  const months = meta?.months ?? [];
  const [from, setFrom] = useState("");
  const [to, setTo] = useState("");

  // La plage Période début→fin alimente le store (months[]).
  useEffect(() => {
    if (!from && !to) { f.set({ months: [] }); return; }
    const a = from || months[0];
    const b = to || months[months.length - 1];
    const lo = a <= b ? a : b, hi = a <= b ? b : a;
    f.set({ months: months.filter((m) => m >= lo && m <= hi) });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [from, to, meta]);

  const provOptions = useMemo(() => (meta?.provinces ?? []).map((p) => ({ v: p, l: cleanName(p) })), [meta]);
  const antOptions = useMemo(() => {
    if (!zs || !f.province) return [];
    const set = new Set(zs.filter((r) => r._Province === f.province && r._Antenne).map((r) => r._Antenne as string));
    return Array.from(set).sort((a, b) => a.localeCompare(b, "fr")).map((a) => ({ v: a, l: a }));
  }, [zs, f.province]);
  const zsOptions = useMemo(() => {
    if (!zs || !f.province) return [];
    const set = new Set(zs.filter((r) => r._Province === f.province && (!f.antenne || r._Antenne === f.antenne) && r._ZS).map((r) => r._ZS as string));
    return Array.from(set).sort((a, b) => a.localeCompare(b, "fr")).map((z) => ({ v: z, l: cleanName(z) }));
  }, [zs, f.province, f.antenne]);

  const monthOpts = months.map((m) => ({ v: m, l: ymLabel(m) }));

  return (
    <div className="relative z-20 shrink-0 border-b border-slate-200 bg-white">
      <div className="flex flex-wrap items-end gap-2.5 px-4 py-2.5">
        <Field label="Province">
          <Select value={f.province} placeholder="Toutes les provinces" options={provOptions}
            onChange={(v) => f.set({ province: v, antenne: "", zs: "" })} />
        </Field>
        <Field label="Antenne">
          <Select value={f.antenne} placeholder={f.province ? "Toutes" : "— choisir province"} options={antOptions} disabled={!f.province}
            onChange={(v) => f.set({ antenne: v, zs: "" })} />
        </Field>
        <Field label="Zone de santé">
          <Select value={f.zs} placeholder={f.province ? "Toutes" : "— choisir province"} options={zsOptions} disabled={!f.province}
            onChange={(v) => f.set({ zs: v })} />
        </Field>
        <Field label="Période — début">
          <Select value={from} placeholder="Premier mois" options={monthOpts} onChange={setFrom} />
        </Field>
        <Field label="Période — fin">
          <Select value={to} placeholder="Dernier mois" options={monthOpts} onChange={setTo} />
        </Field>
        <button type="button" onClick={() => { setFrom(""); setTo(""); f.reset(); }}
          className="ml-auto inline-flex items-center gap-1.5 self-end rounded-lg border border-slate-200 bg-white px-3 py-2 text-[11.5px] font-bold text-slate-600 shadow-[0_1px_2px_rgba(15,23,42,0.04)] transition hover:border-oms-500 hover:text-oms-600">
          <Icon name="refresh" className="h-[13px] w-[13px]" strokeWidth={2.2} /> Réinitialiser
        </button>
      </div>
    </div>
  );
}
