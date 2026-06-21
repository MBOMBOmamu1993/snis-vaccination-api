"use client";

import { useState } from "react";

/**
 * Pagination réutilisable pour les tableaux volumineux (feedback TL) :
 * affiche `pageSize` lignes par page (30 par défaut) et expose la tranche
 * courante + l'état de page. Les tableaux énormes (ex. triangulation) restent
 * ainsi lisibles, page après page.
 */
export function usePaged<T>(rows: T[], pageSize = 30) {
  const [page, setPage] = useState(1);
  const total = rows.length;
  const pageCount = Math.max(1, Math.ceil(total / pageSize));
  const cur = Math.min(Math.max(1, page), pageCount);
  const start = (cur - 1) * pageSize;
  const slice = rows.slice(start, start + pageSize);
  return { page: cur, setPage, pageCount, slice, start, end: start + slice.length, total, pageSize };
}

/** Barre de navigation entre les pages d'un tableau paginé. */
export function Pager({
  page,
  pageCount,
  setPage,
  start,
  end,
  total,
}: {
  page: number;
  pageCount: number;
  setPage: (p: number) => void;
  start: number;
  end: number;
  total: number;
}) {
  if (pageCount <= 1) return null;
  // Fenêtre glissante de numéros de page autour de la page courante.
  const win = 2;
  const nums: number[] = [];
  for (let p = Math.max(1, page - win); p <= Math.min(pageCount, page + win); p++) nums.push(p);
  const btn = "inline-flex h-8 min-w-8 items-center justify-center rounded-lg border px-2.5 text-[12px] font-bold transition disabled:cursor-not-allowed disabled:opacity-40";
  return (
    <div className="mt-3 flex flex-wrap items-center justify-between gap-2">
      <div className="text-[11px] font-semibold text-surface-500">
        Lignes <b className="text-surface-700">{start + 1}</b>–<b className="text-surface-700">{end}</b> sur <b className="text-surface-700">{total}</b>
      </div>
      <div className="flex flex-wrap items-center gap-1.5">
        <button className={`${btn} border-surface-200 bg-white text-navy-700 hover:border-oms-500`} onClick={() => setPage(page - 1)} disabled={page <= 1}>‹ Précédent</button>
        {nums[0] > 1 ? (
          <>
            <button className={`${btn} border-surface-200 bg-white text-navy-700 hover:border-oms-500`} onClick={() => setPage(1)}>1</button>
            {nums[0] > 2 ? <span className="px-1 text-surface-400">…</span> : null}
          </>
        ) : null}
        {nums.map((p) => (
          <button
            key={p}
            onClick={() => setPage(p)}
            className={p === page ? `${btn} border-navy-700 bg-navy-700 text-white` : `${btn} border-surface-200 bg-white text-navy-700 hover:border-oms-500`}
            style={p === page ? { background: "#00205c", borderColor: "#00205c", color: "#fff" } : undefined}
          >
            {p}
          </button>
        ))}
        {nums[nums.length - 1] < pageCount ? (
          <>
            {nums[nums.length - 1] < pageCount - 1 ? <span className="px-1 text-surface-400">…</span> : null}
            <button className={`${btn} border-surface-200 bg-white text-navy-700 hover:border-oms-500`} onClick={() => setPage(pageCount)}>{pageCount}</button>
          </>
        ) : null}
        <button className={`${btn} border-surface-200 bg-white text-navy-700 hover:border-oms-500`} onClick={() => setPage(page + 1)} disabled={page >= pageCount}>Suivant ›</button>
      </div>
    </div>
  );
}
