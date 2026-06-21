import type { SVGProps } from "react";

export const DICONS: Record<string, string> = {
  // onglets
  link: '<path d="M11 11 8 8a2 2 0 0 0-3 3l4 4a3 3 0 0 0 4 0"/><path d="m13 13 3 3a2 2 0 0 0 3-3l-4-4a3 3 0 0 0-4 0"/>',
  quality: '<ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v6c0 1.6 3.6 3 8 3 1 0 1.9-.07 2.8-.2"/><path d="M4 11v6c0 1.6 3.6 3 8 3"/><path d="m15.5 18.5 2 2 4-4"/>',
  gauge: '<path d="M12 14a2 2 0 1 0 0-4 2 2 0 0 0 0 4Z"/><path d="m13.4 10.6 3.6-3.6"/><path d="M3.3 17A9 9 0 1 1 20.7 17"/>',
  route: '<circle cx="6" cy="19" r="3"/><path d="M9 19h8.5a3.5 3.5 0 0 0 0-7h-11a3.5 3.5 0 0 1 0-7H15"/><circle cx="18" cy="5" r="3"/>',
  report: '<rect x="4" y="3" width="16" height="18" rx="2"/><path d="M8 3v4M16 3v4M8 12h8M8 16h5"/>',
  eval: '<path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="m15 11 2 2 4-4"/>',
  // pages
  overview: '<rect x="3" y="3" width="7" height="9" rx="1"/><rect x="14" y="3" width="7" height="5" rx="1"/><rect x="14" y="12" width="7" height="9" rx="1"/><rect x="3" y="16" width="7" height="5" rx="1"/>',
  antenne: '<path d="M12 20v-7"/><circle cx="12" cy="10" r="2"/><path d="M7.5 14a6 6 0 0 1 0-8M16.5 6a6 6 0 0 1 0 8M4.5 16.5a9 9 0 0 1 0-13M19.5 3.5a9 9 0 0 1 0 13"/>',
  zs: '<path d="M3 21h18"/><path d="M5 21V5a1 1 0 0 1 1-1h12a1 1 0 0 1 1 1v16"/><path d="M12 7.5v4M10 9.5h4"/>',
  as: '<path d="M4 21V8l8-4 8 4v13"/><path d="M12 9v6M9 12h6"/>',
  synthese: '<path d="m12 2 9 5-9 5-9-5 9-5Z"/><path d="m3 12 9 5 9-5M3 17l9 5 9-5"/>',
  chart: '<path d="M3 3v18h18"/><rect x="7" y="10" width="3" height="7"/><rect x="12" y="6" width="3" height="11"/><rect x="17" y="13" width="3" height="4"/>',
  concord: '<path d="M8 7 4 11l4 4"/><path d="M4 11h12"/><path d="m16 17 4-4-4-4"/><path d="M20 13H8"/>',
  erreurs: '<path d="M10.3 3.3 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.3a2 2 0 0 0-3.4 0Z"/><path d="M12 9v4M12 17h.01"/>',
  form: '<rect x="4" y="3" width="16" height="18" rx="2"/><path d="M9 8h6M9 12h6M9 16h4"/>',
  enfants: '<circle cx="9" cy="7" r="3"/><path d="M3 21v-1a5 5 0 0 1 5-5h2a5 5 0 0 1 5 5v1"/><circle cx="17.5" cy="9" r="2"/><path d="M14.5 21v-1a4 4 0 0 1 6.5-3"/>',
  table: '<rect x="3" y="4" width="18" height="16" rx="2"/><path d="M3 10h18M9 4v16"/>',
  syringe: '<path d="m18 2 4 4M17 3l4 4-9 9-4 1 1-4 8-8Z"/><path d="m13 7 4 4M8 12l-5 5 1 3 3 1 5-5"/>',
  question: '<circle cx="12" cy="12" r="9"/><path d="M9.5 9a2.5 2.5 0 0 1 5 .2c0 1.8-2.5 2.3-2.5 3.8"/><path d="M12 17h.01"/>',
  reco: '<path d="M9 11l3 3 8-8"/><path d="M20 12v6a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h9"/>',
  check: '<circle cx="12" cy="12" r="9"/><path d="m8.5 12 2.5 2.5 4.5-5"/>',
  cotation: '<circle cx="12" cy="12" r="9"/><path d="M12 3a9 9 0 0 1 9 9h-9Z"/>',
  rank: '<path d="M3 3v18h18"/><path d="m7 14 3-3 3 3 4-5"/>',
  comment: '<path d="M21 15a2 2 0 0 1-2 2H8l-4 4V5a2 2 0 0 1 2-2h13a2 2 0 0 1 2 2Z"/>',
  message: '<path d="m3 11 18-5v12L3 14v-3Z"/><path d="M11.6 16.8a3 3 0 1 1-5.8-1.6"/>',
  legend: '<rect x="3" y="4" width="18" height="16" rx="2"/><path d="M7 9h.01M7 13h.01M7 17h.01M11 9h6M11 13h6M11 17h4"/>',
  calendar: '<rect x="3" y="4.5" width="18" height="16" rx="2"/><path d="M3 9h18M8 2.5v4M16 2.5v4"/>',
  penta: '<path d="m12 2 9 6.5-3.5 10.5h-11L3 8.5 12 2Z"/>',
  rr: '<circle cx="12" cy="12" r="9"/><path d="M12 7v10M8 9v6M16 9v6"/>',
  pin: '<path d="M12 21s-7-6.3-7-11a7 7 0 0 1 14 0c0 4.7-7 11-7 11Z"/><circle cx="12" cy="10" r="2.5"/>',
  down: '<path d="M12 5v14M5 12l7 7 7-7"/>',
  up: '<path d="M12 19V5M5 12l7-7 7 7"/>',
  home: '<path d="m3 10.5 9-7.5 9 7.5"/><path d="M5 9.5V20h14V9.5"/>',
  map: '<path d="m9 4-6 2.5v13L9 17l6 2.5 6-2.5v-13L15 6 9 4Z"/><path d="M9 4v13M15 6v13"/>',
  download: '<path d="M12 3v12"/><path d="m7 10 5 5 5-5"/><path d="M5 21h14"/>',
  emptyset: '<circle cx="12" cy="12" r="9"/><path d="m6 6 12 12"/>',
  target: '<circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="5"/><circle cx="12" cy="12" r="1.6"/>',
  compass: '<circle cx="12" cy="12" r="10"/><polygon points="16.2 7.8 14 14 7.8 16.2 10 10"/>',
  info: '<circle cx="12" cy="12" r="10"/><path d="M12 16v-4M12 8h.01"/>',
  code: '<rect x="2.5" y="4" width="19" height="12.5" rx="1.6"/><path d="M1.5 20.5h21"/><path d="m9.8 8.7-2.3 2.6 2.3 2.6"/><path d="m14.2 8.7 2.3 2.6-2.3 2.6"/>',
  hand: '<path d="m11 17 2 2a1 1 0 1 0 3-3"/><path d="m14 14 2.5 2.5a1 1 0 1 0 3-3l-3.88-3.88a3 3 0 0 0-4.24 0l-.88.88a1 1 0 1 1-3-3l2.81-2.81a5.8 5.8 0 0 1 7.06-.87l.47.28a2 2 0 0 0 1.42.25L21 4"/><path d="m21 3 1 11h-2"/><path d="M3 3 2 14l6.5 6.5a1 1 0 1 0 3-3"/><path d="M3 4h8"/>',
  rocket: '<path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.7-.84.7-2.13-.1-2.91a2.18 2.18 0 0 0-2.9-.09Z"/><path d="m12 15-3-3a22 22 0 0 1 2-3.95A12.9 12.9 0 0 1 22 2c0 2.72-.78 7.5-6 11a22 22 0 0 1-4 2Z"/><path d="M9 12H4s.55-3 2-4c1.6-1.08 5 0 5 0"/><path d="M12 15v5s3-.55 4-2c1.08-1.6 0-5 0-5"/>',
  checkmark: '<path d="m5 12 4 4 9-10"/>',
};

export type DIconName = keyof typeof DICONS | string;

export function DIcon({ name, ...props }: { name: DIconName } & SVGProps<SVGSVGElement>) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={1.9}
      strokeLinecap="round"
      strokeLinejoin="round"
      {...props}
      dangerouslySetInnerHTML={{ __html: DICONS[name] ?? "" }}
    />
  );
}

/** Tons de dégradé. */
export const DTONES: Record<string, [string, string]> = {
  navy: ["#1f54b8", "#00205c"], oms: ["#36b3ec", "#0078ae"], good: ["#2bbd6b", "#178a44"],
  violet: ["#9d5cf5", "#6d28d9"], warn: ["#fbbf24", "#c87b04"], teal: ["#19c2b1", "#0f766e"],
  danger: ["#f87171", "#c81e1e"],
};
