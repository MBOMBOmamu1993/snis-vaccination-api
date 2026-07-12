"use client";

import { useEffect, useState } from "react";
import { DIcon, DTONES } from "./icons";
import { MODULES, moduleByKey, findPage, type ModuleDef, type PageDef } from "./modules";
import { PAGE_REGISTRY } from "./registry";
import { FilterBar } from "./FilterBar";

const OMS = "/logo/pev-logo.svg";
const PEV = "/logo/pev-transparent.png";

function GradBox({ icon, tone, size = 40, radius = 11 }: { icon: string; tone: string; size?: number; radius?: number }) {
  const [a, b] = DTONES[tone] ?? DTONES.navy;
  return (
    <span className="flex shrink-0 items-center justify-center text-white" style={{ width: size, height: size, borderRadius: radius, background: `linear-gradient(145deg, ${a}, ${b})`, boxShadow: "0 6px 14px -7px rgba(0,0,0,.5)" }}>
      <DIcon name={icon} style={{ width: size * 0.52, height: size * 0.52 }} />
    </span>
  );
}

type Phase = "loading" | "welcome" | "home" | "module";

export default function Dashboard() {
  const [phase, setPhase] = useState<Phase>("loading");
  const [modKey, setModKey] = useState<string | null>(null);
  const [pageId, setPageId] = useState<string | null>(null);
  const [fade, setFade] = useState(false);

  useEffect(() => {
    const t = setTimeout(() => { setFade(true); setTimeout(() => setPhase("welcome"), 480); }, 2200);
    return () => clearTimeout(t);
  }, []);

  const mod = modKey ? moduleByKey(modKey) : null;
  const page = mod && pageId ? findPage(mod, pageId) : null;

  function openModule(key: string) {
    const m = moduleByKey(key);
    if (!m || !m.live) return;
    setModKey(key); setPageId(m.pages[0].id); setPhase("module");
  }
  function goHome() { setPhase("home"); }

  return (
    <div className="fixed inset-0 z-[70] overflow-hidden bg-surface-100 text-surface-900">
      {phase === "loading" && <Loading fade={fade} />}
      {phase === "welcome" && <Welcome onStart={() => setPhase("home")} />}
      {phase === "home" && <Home onOpen={openModule} />}
      {phase === "module" && mod && page && (
        <ModuleView mod={mod} page={page} onSelectPage={setPageId} onHome={goHome} />
      )}
    </div>
  );
}

/* ----------------------------- Phase 1 — Chargement ----------------------------- */
function Loading({ fade }: { fade: boolean }) {
  return (
    <div className="fixed inset-0 z-[90] flex flex-col items-center justify-center gap-6 transition-opacity duration-500" style={{ background: "#001a45", opacity: fade ? 0 : 1 }}>
      <div className="flex items-center gap-7 opacity-90">
        <img src={OMS} alt="OMS" className="h-[78px] w-auto" />
        <img src={PEV} alt="PEV" className="h-[78px] w-auto" />
      </div>
      <div className="flex flex-col items-center gap-3.5 text-center">
        <div className="text-[30px] font-extrabold text-white">Téléchargement du Dashboard PEV de routine en cours</div>
        <div className="grid grid-cols-3 gap-[7px]">
          {Array.from({ length: 9 }).map((_, i) => (
            <i key={i} className="block h-[11px] w-[11px] rounded-full bg-white" style={{ animation: "pevpulse 1.1s infinite ease-in-out", animationDelay: `${(((i % 3) + Math.floor(i / 3)) * 0.12).toFixed(2)}s` }} />
          ))}
        </div>
        <div className="text-[15px] text-white/65">Préparation des données de vaccination de routine (DHIS2)…</div>
      </div>
      <style>{`@keyframes pevpulse{0%,80%,100%{opacity:.2;transform:scale(.7)}40%{opacity:1;transform:scale(1)}}`}</style>
    </div>
  );
}

/* ----------------------------- Phase 2 — Bienvenue ----------------------------- */
function Welcome({ onStart }: { onStart: () => void }) {
  const SOURCES: [string, string][] = [
    ["Données de vaccination", "DHIS2 (SNIS)"],
    ["Complétude & promptitude des rapports", "DHIS2 (SNIS)"],
    ["Doses administrées & taux d'abandon", "DHIS2 (SNIS)"],
    ["Logistique & stratégie de vaccination", "DHIS2 (SNIS)"],
    ["Canevas de revue formative (PPTX)", "DHIS2 (SNIS)"],
  ];
  return (
    <div className="fixed inset-0 z-[80] flex items-start justify-center overflow-y-auto px-4 py-9" style={{ background: "rgba(0,19,47,.55)", backdropFilter: "blur(3px)" }}>
      <div className="w-full max-w-[960px] overflow-hidden rounded-[18px] border border-surface-200 bg-white shadow-[0_40px_90px_-30px_rgba(0,19,47,.7)]">
        <div className="flex items-center gap-4 px-8 py-6 text-white" style={{ background: "linear-gradient(120deg,#00205c,#0a3a86)", borderBottom: "5px solid #f5c518" }}>
          <span className="flex h-14 w-14 shrink-0 items-center justify-center rounded-[14px]" style={{ background: "rgba(255,255,255,.14)" }}>
            <DIcon name="syringe" style={{ width: 30, height: 30 }} strokeWidth={2.2} />
          </span>
          <div>
            <h1 className="text-[30px] font-extrabold leading-none">Dashboard PEV de routine</h1>
            <p className="mt-1 text-[16px] font-medium text-[#bcd6f5]">Vaccination de routine — Programme Élargi de Vaccination · OMS — RDC</p>
          </div>
        </div>

        <div className="px-8 pb-8 pt-6">
          <div className="rounded-[14px] px-6 py-5" style={{ background: "linear-gradient(110deg,#eaf4fd,#dcebfb)", borderLeft: "6px solid #0093d5" }}>
            <div className="flex items-start gap-3">
              <DIcon name="target" style={{ width: 21, height: 21, color: "#00205c", flex: "none", marginTop: 2 }} strokeWidth={2} />
              <p className="text-[16px] leading-relaxed text-[#27324a]"><b className="font-extrabold text-navy-700">Plateforme intégrée</b> de suivi de la <b className="font-extrabold text-navy-700">vaccination de routine</b> : contrôle de la qualité des données, données de vaccination, logistique et génération automatique du canevas de revue formative.</p>
            </div>
            <div className="mt-3 flex items-start gap-3">
              <DIcon name="compass" style={{ width: 19, height: 19, color: "#00205c", flex: "none", marginTop: 2 }} />
              <p className="text-[15px] italic leading-relaxed text-[#3a4a66]">Affichage par province et par période — conçu pour exploiter des milliers d'enregistrements sans surcharge.</p>
            </div>
          </div>

          <div className="mt-5 grid grid-cols-1 gap-5 md:grid-cols-2">
            <div>
              <h3 className="mb-2.5 flex items-center gap-2 text-[16px] font-extrabold text-navy-700"><DIcon name="quality" style={{ width: 18, height: 18 }} /> Source des données</h3>
              <div className="rounded-[14px] border border-surface-200 bg-[#f6f8fb] px-4 py-3.5 space-y-2.5">
                {SOURCES.map(([k, v]) => (
                  <div key={k} className="flex items-start gap-2.5 text-[13.5px] leading-snug text-[#475467]">
                    <span className="mt-0.5 flex h-[21px] w-[21px] shrink-0 items-center justify-center rounded-full" style={{ background: "#e7f6ec", color: "#178a44" }}><DIcon name="checkmark" style={{ width: 13, height: 13 }} strokeWidth={2.6} /></span>
                    <span><b className="font-extrabold text-surface-900">{k} :</b> {v}</span>
                  </div>
                ))}
              </div>
            </div>
            <div>
              <h3 className="mb-2.5 flex items-center gap-2 text-[16px] font-extrabold" style={{ color: "#e2700b" }}><DIcon name="erreurs" style={{ width: 18, height: 18 }} /> Avertissement</h3>
              <div className="rounded-[14px] border border-[#fbd88a] bg-[#fff8eb] px-4 py-3.5 text-[13.5px] leading-snug text-[#6b5326] space-y-3">
                <p>Les données présentées proviennent du <b className="font-extrabold" style={{ color: "#7a4a08" }}>DHIS2</b> et sont <b className="font-extrabold" style={{ color: "#7a4a08" }}>sujettes à validation</b> et à des <b className="font-extrabold" style={{ color: "#7a4a08" }}>modifications rétroactives</b>.</p>
                <p>L'interprétation doit tenir compte du <b className="font-extrabold" style={{ color: "#7a4a08" }}>contexte local</b>, de la <b className="font-extrabold" style={{ color: "#7a4a08" }}>complétude</b> et de la <b className="font-extrabold" style={{ color: "#7a4a08" }}>qualité des rapportages</b>.</p>
              </div>
            </div>
          </div>

          <div className="mt-4 rounded-[14px] border border-surface-200 bg-[#f6f8fb] px-6 py-4.5 text-center text-[13.5px] leading-relaxed text-[#475467]">
            <div className="flex flex-wrap items-center justify-center gap-2"><DIcon name="code" style={{ width: 18, height: 18, color: "#00205c" }} /> Plateforme du <b className="font-extrabold text-navy-700">Programme Élargi de Vaccination (PEV) de la RDC</b></div>
            <div className="mt-1.5 flex flex-wrap items-center justify-center gap-2"><DIcon name="hand" style={{ width: 18, height: 18, color: "#0093d5" }} /> avec le soutien technique de l'<b className="font-extrabold" style={{ color: "#0093d5" }}>Organisation Mondiale de la Santé (OMS)</b></div>
          </div>

          <button onClick={onStart} className="mx-auto mt-6 flex items-center gap-2.5 rounded-[14px] px-10 py-3.5 text-[17px] font-extrabold uppercase tracking-wide text-white transition hover:-translate-y-0.5"
            style={{ background: "linear-gradient(120deg,#00205c,#15479e)", boxShadow: "0 16px 32px -14px rgba(0,32,92,.7)" }}>
            <DIcon name="rocket" style={{ width: 20, height: 20 }} strokeWidth={2} /> Commencer l'exploration
          </button>
        </div>
      </div>
    </div>
  );
}

/* ----------------------------- Phase 3 — Accueil ----------------------------- */
function Home({ onOpen }: { onOpen: (key: string) => void }) {
  return (
    <div className="flex h-full flex-col overflow-y-auto">
      <div className="sticky top-0 z-30 flex h-24 shrink-0 items-center gap-5 px-8 text-white" style={{ background: "#00205c", borderBottom: "3px solid #0093d5" }}>
        <img src={OMS} alt="OMS" className="h-12 w-auto" />
        <div className="flex-1 text-center">
          <div className="text-[11px] font-bold uppercase tracking-[0.22em] text-white/60">Programme Élargi de Vaccination · OMS — République Démocratique du Congo</div>
          <h1 className="mt-0.5 text-[23px] font-extrabold">Dashboard PEV de routine</h1>
        </div>
        <img src={PEV} alt="PEV" className="h-[58px] w-auto" />
      </div>
      <div className="mx-auto w-full max-w-[1240px] px-8 py-10">
        <div className="rounded-2xl border border-surface-200 bg-white px-10 py-9 text-center shadow-[0_10px_30px_-18px_rgba(15,23,42,.25)]" style={{ borderTop: "5px solid #f5c518" }}>
          <h2 className="text-[34px] font-extrabold text-navy-700">Bienvenue sur le Dashboard PEV de routine</h2>
          <p className="mx-auto mt-3 max-w-[820px] text-[16.5px] leading-relaxed text-surface-700">Votre outil centralisé de suivi de la <b className="text-navy-700">vaccination de routine</b> : <b className="text-navy-700">contrôle qualité des données</b>, <b className="text-navy-700">données de vaccination</b>, <b className="text-navy-700">logistique</b> et <b className="text-navy-700">canevas de revue formative</b>. Données issues exclusivement du <b className="text-navy-700">DHIS2</b>. Sélectionnez un onglet pour commencer l'exploration.</p>
        </div>
        <div className="my-9 flex items-center gap-3"><span className="h-px flex-1 bg-[#d7dfea]" /><span className="text-[13px] font-extrabold uppercase tracking-[0.12em] text-surface-500">Onglets disponibles</span><span className="h-px flex-1 bg-[#d7dfea]" /></div>
        <div className="grid grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-4">
          {MODULES.map((m) => {
            const c = DTONES[m.tone]?.[1] ?? "#00205c";
            return (
              <button key={m.key} type="button" onClick={() => onOpen(m.key)}
                className="relative flex min-h-[210px] flex-col items-center rounded-[18px] border-2 bg-white px-5 pb-6 pt-7 text-center transition"
                style={{ borderColor: `${c}33` }}
                onMouseEnter={(e) => { e.currentTarget.style.transform = "translateY(-5px)"; e.currentTarget.style.boxShadow = "0 24px 46px -22px rgba(15,23,42,.4)"; e.currentTarget.style.borderColor = c; }}
                onMouseLeave={(e) => { e.currentTarget.style.transform = "none"; e.currentTarget.style.boxShadow = "none"; e.currentTarget.style.borderColor = `${c}33`; }}>
                <span className="mb-3.5" style={{ color: c }}><DIcon name={m.icon} style={{ width: 50, height: 50 }} strokeWidth={1.8} /></span>
                <h3 className="text-[17px] font-extrabold leading-tight" style={{ color: c }}>{m.name}</h3>
                <p className="mt-2.5 text-[12.5px] leading-relaxed text-surface-500">{m.desc}</p>
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}

/* ----------------------------- Phase 4 — Vue onglet ----------------------------- */
function ModuleView({ mod, page, onSelectPage, onHome }: { mod: ModuleDef; page: PageDef; onSelectPage: (id: string) => void; onHome: () => void }) {
  const Comp = PAGE_REGISTRY[page.id];

  const navLink = (p: PageDef) => (
    <button key={p.id} type="button" onClick={() => onSelectPage(p.id)}
      className="my-0.5 flex w-full items-center gap-2.5 rounded-[9px] border-l-[3px] px-3 py-2.5 text-left text-[13px] font-semibold transition"
      style={page.id === p.id ? { background: "rgba(255,255,255,.14)", color: "#fff", borderLeftColor: "#0093d5", fontWeight: 800 } : { background: "transparent", color: "rgba(255,255,255,.72)", borderLeftColor: "transparent" }}>
      <span className="inline-flex h-[17px] w-[17px] shrink-0"><DIcon name={p.icon} style={{ width: 17, height: 17 }} strokeWidth={2} /></span>{p.label}
    </button>
  );

  return (
    <div className="flex h-full">
      <aside className="flex w-[262px] shrink-0 flex-col text-white" style={{ background: "#001a45" }}>
        <div className="border-b border-white/10 px-[18px] pb-4 pt-[18px]">
          <button type="button" onClick={onHome} className="mb-3.5 inline-flex items-center gap-1.5 rounded-lg bg-white/[0.08] px-2.5 py-1.5 text-[11.5px] font-bold text-white/70 transition hover:bg-white/[0.16] hover:text-white">
            <svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" strokeWidth={2.2} strokeLinecap="round" strokeLinejoin="round"><path d="m15 18-6-6 6-6" /></svg> Accueil
          </button>
          <div className="flex items-center gap-2.5">
            <GradBox icon={mod.icon} tone={mod.tone} size={40} />
            <div><div className="text-[10px] font-bold uppercase tracking-[0.16em] text-white/50">Onglet</div><div className="text-[15px] font-extrabold leading-tight">{mod.name}</div></div>
          </div>
        </div>
        <nav className="flex-1 overflow-y-auto px-2 py-3">{mod.pages.map(navLink)}</nav>
        <div className="flex items-center gap-2 border-t border-white/10 px-[18px] py-3 text-[10.5px] text-white/40">
          <DIcon name="quality" style={{ width: 14, height: 14 }} /> Source : DHIS2 (SNIS)
        </div>
      </aside>

      <div className="flex min-w-0 flex-1 flex-col">
        <div className="flex h-16 shrink-0 items-center gap-4 px-6 text-white" style={{ background: "#00205c", borderBottom: "3px solid #0093d5" }}>
          <img src={OMS} alt="OMS" className="h-[38px] w-auto" />
          <div className="min-w-0 flex-1 text-center">
            <div className="text-[9.5px] font-bold uppercase tracking-[0.2em] text-white/60">Dashboard PEV de routine · PEV / OMS — RDC</div>
            <h1 className="mt-px truncate text-[16px] font-extrabold uppercase">{page.label} — {mod.name}</h1>
          </div>
          <img src={PEV} alt="PEV" className="h-[46px] w-auto" />
        </div>
        {mod.key !== "telecharger" || page.id === "t_canevas" ? <FilterBar /> : null}
        <div className="flex-1 overflow-y-auto px-5 py-5" style={{ background: "#eef2f7" }}>
          <div className="mx-auto max-w-[1200px]">
            {Comp ? <Comp /> : <div className="py-16 text-center text-surface-500">Page à concevoir.</div>}
          </div>
        </div>
      </div>
    </div>
  );
}
