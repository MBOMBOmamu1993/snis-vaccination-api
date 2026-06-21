import type { SVGProps } from "react";
import { DICONS } from "@/components/dashboard/icons";

export type IconName =
  | "home" | "time" | "component" | "analyse" | "report"
  | "hands" | "people" | "person" | "clipboard" | "tower"
  | "pin" | "map" | "clinic" | "hospital" | "trophy" | "alert" | "shield"
  | "down" | "up" | "calendar" | "doc" | "bars" | "refresh"
  | "map-pin" | "layers" | "building" | "database" | "download"
  | "scale" | "child" | "pop" | "road" | "fridge" | "syringe" | "flag" | "truck" | "hand" | "clip"
  | "chevron-down" | "chevron-left" | "chevron-right" | "check"
  // Icônes du nouveau shell (rendues via DICONS en repli) :
  | "link" | "gauge" | "overview" | "antenne" | "zs" | "as" | "synthese"
  | "chart" | "concord" | "erreurs" | "form" | "enfants" | "table"
  | "question" | "reco" | "cotation" | "rank" | "comment" | "message"
  | "legend" | "penta" | "rr" | "quality" | "route" | "eval";

const P: Partial<Record<IconName, JSX.Element>> = {
  home: (<><path d="M3 10.5 12 3l9 7.5" /><path d="M5 9.5V20h14V9.5" /></>),
  time: (<><path d="M3 17l5-5 4 3 8-8" /><path d="M3 21h18" /></>),
  component: (<><circle cx="12" cy="12" r="3" /><path d="M12 3v3M12 18v3M3 12h3M18 12h3M5.6 5.6l2.1 2.1M16.3 16.3l2.1 2.1M18.4 5.6l-2.1 2.1M7.7 16.3l-2.1 2.1" /></>),
  analyse: (<><path d="M4 4v16h16" /><rect x="7" y="10" width="3" height="7" /><rect x="12" y="6" width="3" height="11" /><rect x="17" y="13" width="3" height="4" /></>),
  report: (<><path d="M7 3h7l5 5v13H7z" /><path d="M14 3v5h5" /><path d="M9 13h6M9 17h6" /></>),
  hands: (<><path d="M11 11 8 8a2 2 0 0 0-3 3l4 4a3 3 0 0 0 4 0" /><path d="m13 13 3 3a2 2 0 0 0 3-3l-4-4a3 3 0 0 0-4 0" /></>),
  people: (<><circle cx="9" cy="8" r="3" /><circle cx="17" cy="9" r="2.3" /><path d="M3.5 20a5.5 5.5 0 0 1 11 0M15 20a4.5 4.5 0 0 1 6 0" /></>),
  person: (<><circle cx="12" cy="7" r="3.4" /><path d="M5 20a7 7 0 0 1 14 0" /></>),
  clipboard: (<><rect x="6" y="4" width="12" height="17" rx="2" /><path d="M9 4V3h6v1" /><path d="M9 10h6M9 14h4" /></>),
  tower: (<><path d="M12 20v-7" /><circle cx="12" cy="10" r="2" /><path d="M7.5 14a6 6 0 0 1 0-8M16.5 6a6 6 0 0 1 0 8M5 16a9 9 0 0 1 0-12M19 4a9 9 0 0 1 0 12" /></>),
  pin: (<><path d="M12 21s-7-6.3-7-11a7 7 0 0 1 14 0c0 4.7-7 11-7 11Z" /><circle cx="12" cy="10" r="2.5" /></>),
  map: (<><path d="m9 4-6 2.5v13L9 17l6 2.5 6-2.5v-13L15 6 9 4Z" /><path d="M9 4v13M15 6v13" /></>),
  clinic: (<><path d="M4 21V8l8-4 8 4v13" /><path d="M12 9v6M9 12h6" /><path d="M9 21v-4h6v4" /></>),
  hospital: (<><path d="M3 21h18" /><path d="M5 21V5a1 1 0 0 1 1-1h12a1 1 0 0 1 1 1v16" /><path d="M12 7.5v4M10 9.5h4" /><path d="M9.5 21v-3.5h5V21" /></>),
  trophy: (<><path d="M7 4h10v4a5 5 0 0 1-10 0V4Z" /><path d="M7 6H4v1a3 3 0 0 0 3 3M17 6h3v1a3 3 0 0 1-3 3" /><path d="M9 21h6M12 14v4" /></>),
  alert: (<><path d="M12 3 2 20h20L12 3Z" /><path d="M12 10v4M12 17h.01" /></>),
  shield: (<><path d="M12 3 5 6v5c0 4.5 3 7.5 7 9 4-1.5 7-4.5 7-9V6l-7-3Z" /><path d="m9 12 2 2 4-4" /></>),
  down: (<><path d="M3 7l6 6 4-3 8 8" /><path d="M21 18v-5h-5" /></>),
  up: (<><path d="M3 17l6-6 4 3 8-8" /><path d="M21 6v5h-5" /></>),
  scale: (<><path d="M12 3v18M5 7h14M5 7l-2.5 6a3 3 0 0 0 5 0L5 7Zm14 0-2.5 6a3 3 0 0 0 5 0L19 7Z" /></>),
  child: (<><circle cx="12" cy="6" r="2.6" /><path d="M9 21v-5H7l2.2-5.2A3 3 0 0 1 12 9a3 3 0 0 1 2.8 1.8L17 16h-2v5" /></>),
  pop: (<><circle cx="9" cy="7" r="3" /><circle cx="17" cy="8" r="2.2" /><path d="M3 20a6 6 0 0 1 12 0M15 20a4.5 4.5 0 0 1 6 0" /></>),
  road: (<><path d="M6 3 4 21M18 3l2 18M12 4v3M12 11v3M12 18v2" /></>),
  fridge: (<><rect x="6" y="3" width="12" height="18" rx="2" /><path d="M6 10h12M9 6v2M9 13v3" /></>),
  syringe: (<><path d="m18 2 4 4M17 3l4 4M20.5 6.5 9 18l-5 1 1-5L16.5 2.5M11 9l4 4M7 13l4 4" /></>),
  flag: (<><path d="M4 21V4M4 4h13l-2 4 2 4H4" /></>),
  truck: (<><path d="M3 16V6h11v10M14 9h4l3 3v4h-7" /><circle cx="7" cy="18" r="2" /><circle cx="17" cy="18" r="2" /></>),
  hand: (<><path d="M7 11V6a1.5 1.5 0 0 1 3 0v5M10 11V4.5a1.5 1.5 0 0 1 3 0V11M13 11V6a1.5 1.5 0 0 1 3 0v7a6 6 0 0 1-6 6h-1a5 5 0 0 1-4-2l-3-4a1.6 1.6 0 0 1 2.5-2L7 13" /></>),
  clip: (<><rect x="6" y="4" width="12" height="17" rx="2" /><path d="M9 4V3h6v1" /><path d="M9 10h6M9 14h4" /></>),
  calendar: (<><rect x="3" y="4.5" width="18" height="16" rx="2" /><path d="M3 9h18M8 2.5v4M16 2.5v4" /></>),
  doc: (<><path d="M7 3h7l5 5v13H7z" /><path d="M14 3v5h5" /></>),
  bars: (<><path d="M4 20V9M9 20V4M14 20v-8M19 20v-5" /></>),
  refresh: (<><path d="M3 12a9 9 0 0 1 15-6.7L21 8" /><path d="M21 3v5h-5" /><path d="M21 12a9 9 0 0 1-15 6.7L3 16" /><path d="M3 21v-5h5" /></>),
  "map-pin": (<><path d="M12 21s-7-6.3-7-11a7 7 0 0 1 14 0c0 4.7-7 11-7 11Z" /><circle cx="12" cy="10" r="2.5" /></>),
  layers: (<><path d="m12 3 9 5-9 5-9-5 9-5Z" /><path d="m3 13 9 5 9-5" /></>),
  building: (<><rect x="5" y="3" width="14" height="18" rx="1.5" /><path d="M9 7h2M13 7h2M9 11h2M13 11h2M9 15h2M13 15h2" /><path d="M10 21v-3h4v3" /></>),
  database: (<><ellipse cx="12" cy="5" rx="8" ry="3" /><path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5" /><path d="M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6" /></>),
  download: (<><path d="M12 3v12" /><path d="m7 10 5 5 5-5" /><path d="M5 21h14" /></>),
  "chevron-down": (<><path d="m6 9 6 6 6-6" /></>),
  "chevron-left": (<><path d="m15 6-6 6 6 6" /></>),
  "chevron-right": (<><path d="m9 6 6 6-6 6" /></>),
  check: (<><path d="m5 12 5 5L20 7" /></>),
};

export function Icon({ name, ...props }: { name: IconName } & SVGProps<SVGSVGElement>) {
  const el = P[name];
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.8} strokeLinecap="round" strokeLinejoin="round" {...props}>
      {el ?? <g dangerouslySetInnerHTML={{ __html: DICONS[name] ?? "" }} />}
    </svg>
  );
}
