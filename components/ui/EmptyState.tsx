export function EmptyState({ title = "Aucune donnée", message }: { title?: string; message?: string }) {
  return (
    <div className="flex flex-col items-center justify-center text-center py-10 px-4">
      <div className="text-surface-300 text-3xl mb-2">∅</div>
      <div className="text-[13px] font-semibold text-surface-700">{title}</div>
      {message ? <div className="text-[11.5px] text-surface-700/70 mt-1 max-w-sm">{message}</div> : null}
    </div>
  );
}
