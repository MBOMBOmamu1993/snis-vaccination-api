#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Construit le PowerPoint de revue formative PEV — Antenne Kinshasa Est.

- Diapo 10 : Résultats de processus (Jan-Avr 2025 vs Jan-Avr 2026) — table existante remplie.
- Diapo 11 : Performances par AS et ZS (Jan-Avr 2026) — complétude/promptitude, CV admin,
  enfants ZD/SV, catégorisation ACZ, disponibilité des intrants, taux de perte.
- Diapo 12 : Performance communication — enfants identifiés/récupérés (RECO),
  taux d'abandon BCG-VAR1 et DTC1-DTC3.

Sources (toutes dans le repo) :
  docs/data_as/dashboard/by_as/kn_kinshasa_province.json.gz   (AS, avec population)
  docs/data_as/monthly/<YM>/*.ndjson + docs/data_as/ou_map_as.json + antenne_rules.json
Formules : strictement alignées sur le dashboard (docs/index.html).
"""
import gzip, json, glob, re, copy, os
from pptx import Presentation
from pptx.util import Emu, Pt, Cm
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.chart.data import CategoryChartData
from pptx.enum.chart import XL_CHART_TYPE, XL_LEGEND_POSITION
from pptx.oxml.ns import qn

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROV_FILE = os.path.join(ROOT, "docs/data_as/dashboard/by_as/kn_kinshasa_province.json.gz")
OU_MAP = os.path.join(ROOT, "docs/data_as/ou_map_as.json")
ANT_RULES = os.path.join(ROOT, "docs/config/antenne_rules.json")
TEMPLATE = os.path.join(ROOT, "public/canevas/canevas_revue_formative_pev.pptx")
OUT = os.path.join(ROOT, "Canevas_Revue_Formative_PEV_Kinshasa_Est.pptx")

ANTENNE = "Kin Est"
M2026 = ["202601", "202602", "202603", "202604"]
M2025 = ["202501", "202502", "202503", "202504"]
NP = 4
NV_RATE, NS_RATE = 0.04, 0.0349
PER_LABEL_26 = "Janvier – Avril 2026"

# ---------- helpers ----------
def num(v):
    try:
        f = float(v)
        return f if f == f else 0.0
    except (TypeError, ValueError):
        return 0.0

def clean(s):
    s = re.sub(r'^[a-z]{2}\s+', '', str(s))
    s = re.sub(r'\s+(Province|Zone de Santé|Aire de Santé|Aire de santé)$', '', s, flags=re.I)
    return s.strip()

def s(recs, f):
    return sum(num(r.get(f)) for r in recs)

# ---------- load ----------
data = json.load(gzip.open(PROV_FILE))
est_all = [r for r in data if r.get("_Antenne") == ANTENNE]
cur = [r for r in est_all if r["_YM"] in M2026]
prev = [r for r in est_all if r["_YM"] in M2025]

# ---------- targets / CV ----------
def targets(recs):
    """Cible NV/NS sur la période : pop (dédupliquée par AS) × taux × NP/12."""
    seen, pop = set(), 0.0
    for r in recs:
        k = (r.get("_ZS"), r.get("_AS"))
        if k not in seen:
            seen.add(k); pop += num(r.get("Pop_par_AS"))
    return {"pop": pop, "nv": pop * NV_RATE * NP / 12.0, "ns": pop * NS_RATE * NP / 12.0}

# Antigènes affichés (ordre calendrier vaccinal) + champ doses + dénominateur
AG_FIELD = {
    "BCG": "BCG_0_11", "VPO0": "VPO0_0_11", "DTC1": "DTC1_0_11", "VPO1": "VPO1_0_11",
    "ROTA1": "ROTA1_0_11", "PCV13-1": "PCV13_1_0_11", "DTC2": "DTC2_0_11", "VPO2": "VPO2_0_11",
    "ROTA2": "ROTA2_0_11", "PCV13-2": "PCV13_2_0_11", "DTC3": "DTC3_0_11", "VPO3": "VPO3_0_11",
    "VPI1": "VPI1_0_11", "ROTA3": "ROTA3_0_11", "PCV13-3": "PCV13_3_0_11", "VAR1": "VAR1_0_11",
    "VPI2": "VPI2_0_11", "VAA": "VAA_0_11", "VAR2": "VAR2_12_23", "Td2+": "Td_2_plus",
}
AG_ORDER = list(AG_FIELD.keys())
AG_USE_NV = {"BCG", "Td2+"}

def cv_pct(recs, ag, tgt):
    den = tgt["nv"] if ag in AG_USE_NV else tgt["ns"]
    if den <= 0:
        return None
    return s(recs, AG_FIELD[ag]) / den * 100.0

def abandon(a, b):
    a, b = num(a), num(b)
    return None if a <= 0 else (a - b) / a * 100.0

def categorize(cv1, ta):
    if cv1 is None or ta is None or ta < 0:
        return "NA"
    h, l = cv1 >= 90, ta <= 10
    return "Cat. 1" if (h and l) else "Cat. 2" if (h and not l) else "Cat. 3" if (not h and l) else "Cat. 4"

def zd(recs):
    return sum(max(0.0, num(r.get("Pop_par_AS")) * NS_RATE / 12.0 - num(r.get("DTC1_0_11"))) for r in recs)

def sv(recs):
    return sum(max(0.0, num(r.get("Pop_par_AS")) * NS_RATE / 12.0 - num(r.get("DTC3_0_11"))) for r in recs)

# Disponibilité : (Σ jours non-rupture) / (Σ 30 sur mois présents) — aligné dashboard.
def dispo(recs, pfx):
    days, nonrup = 0.0, 0.0
    for r in recs:
        rcv = num(r.get(pfx + "_re_ues")); sf = num(r.get(pfx + "_stock_fin"))
        cmm = num(r.get(pfx + "_cmm")); rup = max(0.0, num(r.get(pfx + "_jours_rupture")))
        if rcv > 0 or sf > 0 or cmm > 0 or rup > 0:
            days += 30.0; nonrup += max(0.0, 30.0 - rup)
    return None if days <= 0 else nonrup / days * 100.0

# Taux de perte : pertes / (administrées + pertes), avec repli utilisées-administrées.
def perte(recs, pfx):
    adm = s(recs, pfx + "_administr_es"); util = s(recs, pfx + "_utilis_es"); pdu = s(recs, pfx + "_pertes")
    if pdu <= 0 and util > adm > 0:
        pdu = util - adm
    den = adm + pdu
    return None if den <= 0 else pdu / den * 100.0

# ---------- groupements ----------
def group(recs, by_zs):
    g = {}
    for r in recs:
        k = clean(r["_ZS"]) if by_zs else (clean(r["_ZS"]), clean(r["_AS"]))
        g.setdefault(k, []).append(r)
    return g

g_as = group(cur, False)
g_zs = group(cur, True)

# Étiquettes AS (désambiguïsation des doublons de noms entre ZS)
as_keys = sorted(g_as.keys(), key=lambda k: (k[0], k[1]))
from collections import Counter
name_count = Counter(k[1] for k in as_keys)
def as_label(k):
    return f"{k[1]} ({k[0]})" if name_count[k[1]] > 1 else k[1]
zs_keys = sorted(g_zs.keys())

# ---------- perdues de vue (RECO) depuis le brut mensuel ----------
oum = json.load(open(OU_MAP))
ar = json.load(open(ANT_RULES))
kin_key = [k for k in ar if "inshasa" in k.lower()][0]
zs2ant = ar[kin_key]
ID_F = ["Perdues_de_vue_identifi_s_Penta1_0_11mois", "Perdues_de_vue_identifi_s_Penta1_12_23mois",
        "Perdues_de_vue_identifi_s_Penta3_0_11mois", "Perdues_de_vue_identifi_s_Penta3_12_23mois",
        "Perdues_de_vue_identifi_s_Penta3_24_59mois", "Perdues_de_vue_identifi_s_24_59mois"]
REC_F = ["Perdues_de_vue_r_cup_r_s_Penta1_0_11mois", "Perdues_de_vue_r_cup_r_s_Penta1_12_23mois",
         "Perdues_de_vue_r_cup_r_s_Penta1_24_59mois", "Perdues_de_vue_r_cup_r_s_Penta3_0_11mois",
         "Perdues_de_vue_r_cup_r_s_Penta3_12_23mois", "Perdues_de_vue_r_cup_r_s_Penta3_24_59mois"]
VAR_REC = "Enfants_r_cup_r_s_VAR1_BCU_FAE"

pdv_as = {}   # (zs, as) -> {id, rec, var}
pdv_zs = {}   # zs -> {...}
for ym in M2026:
    for fn in glob.glob(os.path.join(ROOT, f"docs/data_as/monthly/{ym}/*.ndjson")):
        with open(fn) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                r = json.loads(line)
                m = oum.get(r.get("OrgUnit"))
                if not m:
                    continue
                zsc = clean(m.get("Org3", ""))
                if zs2ant.get(zsc) != ANTENNE:
                    continue
                asc = clean(m.get("Org4", ""))
                vid = sum(num(r.get(f)) for f in ID_F)
                vrec = sum(num(r.get(f)) for f in REC_F)
                vvar = num(r.get(VAR_REC))
                for d, k in ((pdv_as, (zsc, asc)), (pdv_zs, zsc)):
                    e = d.setdefault(k, {"id": 0.0, "rec": 0.0, "var": 0.0})
                    e["id"] += vid; e["rec"] += vrec; e["var"] += vvar

# ============================================================
#  CONSTRUCTION PPTX
# ============================================================
prs = Presentation(TEMPLATE)
EMU_W = prs.slide_width
EMU_H = prs.slide_height
# layout vierge : celui avec le moins de placeholders
blank = min(prs.slide_layouts, key=lambda L: len(L.placeholders))

NAVY = RGBColor(0x0B, 0x3A, 0x6F)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
DARK = RGBColor(0x1E, 0x29, 0x3B)
BAND = RGBColor(0xEE, 0xF4, 0xFC)

def cv_color(p):
    if p is None: return None
    if p > 100: return RGBColor(0x25, 0x63, 0xEB)
    if p >= 90: return RGBColor(0x16, 0xA3, 0x4A)
    if p >= 60: return RGBColor(0xEA, 0xB3, 0x08)
    return RGBColor(0xDC, 0x26, 0x26)

def ab_color(p):
    if p is None: return None
    if p < 0: return RGBColor(0x7F, 0x1D, 0x1D)
    if p <= 10: return RGBColor(0x16, 0xA3, 0x4A)
    if p < 20: return RGBColor(0xEA, 0xB3, 0x08)
    return RGBColor(0xEF, 0x44, 0x44)

def cat_color(c):
    return {"Cat. 1": RGBColor(0x16, 0xA3, 0x4A), "Cat. 2": RGBColor(0x25, 0x63, 0xEB),
            "Cat. 3": RGBColor(0xEA, 0xB3, 0x08), "Cat. 4": RGBColor(0xDC, 0x26, 0x26),
            "NA": RGBColor(0x7F, 0x1D, 0x1D)}.get(c)

def disp_color(p):
    if p is None: return None
    if p >= 90: return RGBColor(0x16, 0xA3, 0x4A)
    if p >= 60: return RGBColor(0xEA, 0xB3, 0x08)
    return RGBColor(0xDC, 0x26, 0x26)

def perte_color(p):
    if p is None: return None
    if p <= 5: return RGBColor(0x16, 0xA3, 0x4A)
    if p <= 10: return RGBColor(0xEA, 0xB3, 0x08)
    return RGBColor(0xDC, 0x26, 0x26)

def add_slide():
    sl = prs.slides.add_slide(blank)
    for ph in list(sl.placeholders):
        ph._element.getparent().remove(ph._element)
    return sl

def add_title(sl, title, subtitle=None):
    tb = sl.shapes.add_textbox(Cm(0.6), Cm(0.3), EMU_W - Cm(1.2), Cm(1.6))
    tf = tb.text_frame; tf.word_wrap = True
    p = tf.paragraphs[0]; r = p.add_run(); r.text = title
    r.font.size = Pt(18); r.font.bold = True; r.font.color.rgb = NAVY
    if subtitle:
        p2 = tf.add_paragraph(); r2 = p2.add_run(); r2.text = subtitle
        r2.font.size = Pt(11); r2.font.italic = True; r2.font.color.rgb = RGBColor(0x55, 0x55, 0x55)
    return tb

def style_cell(cell, text, *, bold=False, fill=None, color=None, align=PP_ALIGN.LEFT, size=8):
    cell.margin_left = Cm(0.08); cell.margin_right = Cm(0.08)
    cell.margin_top = Cm(0.02); cell.margin_bottom = Cm(0.02)
    cell.vertical_anchor = MSO_ANCHOR.MIDDLE
    if fill is not None:
        cell.fill.solid(); cell.fill.fore_color.rgb = fill
    else:
        cell.fill.solid(); cell.fill.fore_color.rgb = WHITE
    tf = cell.text_frame; tf.word_wrap = True
    p = tf.paragraphs[0]; p.alignment = align
    run = p.add_run(); run.text = text
    f = run.font; f.size = Pt(size); f.bold = bold
    f.color.rgb = color if color is not None else (WHITE if fill is not None and bold else DARK)

def add_table(sl, header, rows, *, top=Cm(2.0), size=8, col_w=None, cell_fills=None,
              first_align=PP_ALIGN.LEFT):
    """rows: list of list[str]. cell_fills[r][c] -> RGBColor or None (body only)."""
    nrows = len(rows) + 1
    ncols = len(header)
    left = Cm(0.5); width = EMU_W - Cm(1.0)
    height = min(EMU_H - top - Cm(0.6), Cm(0.62) * nrows)
    gf = sl.shapes.add_table(nrows, ncols, left, top, width, int(height))
    tbl = gf.table
    tbl.first_row = False; tbl.horz_banding = False
    if col_w:
        total = sum(col_w)
        for i, w in enumerate(col_w):
            tbl.columns[i].width = int(width * w / total)
    for c, h in enumerate(header):
        style_cell(tbl.cell(0, c), h, bold=True, fill=NAVY, color=WHITE,
                   align=PP_ALIGN.CENTER, size=size)
    for ri, row in enumerate(rows):
        band = BAND if ri % 2 else WHITE
        for ci, val in enumerate(row):
            fill = band
            color = DARK
            if cell_fills and cell_fills[ri][ci] is not None:
                fill = cell_fills[ri][ci]; color = WHITE
            al = first_align if ci == 0 else PP_ALIGN.CENTER
            style_cell(tbl.cell(ri + 1, ci), val, fill=fill, color=color, align=al, size=size,
                       bold=(ci == 0 and val == "TOTAL"))
    return gf

def paginate(items, per):
    for i in range(0, len(items), per):
        yield items[i:i + per]

def add_bar_chart(sl, categories, series, *, top=Cm(2.0), title=None, horizontal=False):
    cd = CategoryChartData()
    cd.categories = categories
    for nm, vals in series:
        cd.add_series(nm, vals)
    ctype = XL_CHART_TYPE.BAR_CLUSTERED if horizontal else XL_CHART_TYPE.COLUMN_CLUSTERED
    gf = sl.shapes.add_chart(ctype, Cm(0.5), top, EMU_W - Cm(1.0),
                             EMU_H - top - Cm(0.6), cd)
    ch = gf.chart
    ch.has_legend = True
    ch.legend.position = XL_LEGEND_POSITION.TOP
    ch.legend.include_in_layout = False
    try:
        ch.value_axis.tick_labels.font.size = Pt(8)
        ch.category_axis.tick_labels.font.size = Pt(7)
    except Exception:
        pass
    return gf

NEW = []  # (after_index_0based, slide_element)  -- collected for reordering

def remember(sl, after0):
    NEW.append((after0, sl))

# ------------------------------------------------------------
# DIAPO 10 — remplir la table existante (Kin Est, 2025 vs 2026)
# ------------------------------------------------------------
def count_cs(recs, field):
    seen = {}
    for r in recs:
        k = (r.get("_ZS"), r.get("_AS"))
        seen[k] = seen.get(k, 0) + num(r.get(field))
    return sum(1 for v in seen.values() if v > 0)

def pr(recs, p, rl):
    return f"{round(s(recs, p))} / {round(s(recs, rl))}"

def slide10_rows(recs):
    return [
        str(count_cs(recs, "seances_avancees_realisees") or "—"),
        str(count_cs(recs, "seances_mobiles_realisees") or "—"),
        pr(recs, "seances_fixes_prevues", "seances_fixes_realisees"),
        pr(recs, "seances_avancees_prevues", "seances_avancees_realisees"),
        pr(recs, "seances_mobiles_prevues", "seances_mobiles_realisees"),
    ]

col25 = slide10_rows(prev)
col26 = slide10_rows(cur)
sl10 = prs.slides[9]
tbl10 = None
for shp in sl10.shapes:
    if shp.has_table:
        tbl10 = shp.table; break
if tbl10 is not None:
    for i in range(5):
        for ci, txt in ((1, col25[i]), (2, col26[i])):
            cell = tbl10.cell(i + 1, ci)
            cell.text = txt
            for para in cell.text_frame.paragraphs:
                para.alignment = PP_ALIGN.CENTER
                for run in para.runs:
                    run.font.size = Pt(12); run.font.bold = True

# ------------------------------------------------------------
# DIAPO 11 — analyses par AS et ZS (Jan-Avr 2026)
# ------------------------------------------------------------
SUB = f"Antenne Kinshasa Est — {PER_LABEL_26}"

# 1. Complétude & promptitude — graphiques (AS paginé, ZS unique)
def comp_prompt(recs):
    n = s(recs, "n"); rap = s(recs, "rap")
    pw = sum(num(r.get("prompt")) * num(r.get("n")) for r in recs)
    comp = rap / n * 100 if n else 0
    prompt = pw / n if n else 0
    return round(comp, 1), round(prompt, 1)

# ZS chart
cats = [k for k in zs_keys]
comp_v = [comp_prompt(g_zs[k])[0] for k in zs_keys]
prom_v = [comp_prompt(g_zs[k])[1] for k in zs_keys]
sl = add_slide()
add_title(sl, "Complétude & Promptitude par Zone de Santé", SUB)
add_bar_chart(sl, cats, [("Complétude %", comp_v), ("Promptitude %", prom_v)], top=Cm(2.2))
remember(sl, 10)

# AS chart paginé
for idx, chunk in enumerate(paginate(as_keys, 40), 1):
    sl = add_slide()
    add_title(sl, f"Complétude & Promptitude par Aire de Santé ({idx})", SUB)
    cats = [as_label(k) for k in chunk]
    cv = [comp_prompt(g_as[k])[0] for k in chunk]
    pv = [comp_prompt(g_as[k])[1] for k in chunk]
    add_bar_chart(sl, cats, [("Complétude %", cv), ("Promptitude %", pv)], top=Cm(2.2))
    remember(sl, 10)

# 2. CV admin tous antigènes — tableaux (ZS unique, AS paginé)
def cv_row(recs):
    t = targets(recs)
    out = []
    fills = []
    for ag in AG_ORDER:
        p = cv_pct(recs, ag, t)
        out.append("–" if p is None else f"{p:.0f}")
        fills.append(cv_color(p))
    return out, fills

cv_header = ["Zone de Santé"] + AG_ORDER
rows, fills = [], []
# TOTAL
tot_cells, tot_fills = cv_row(cur)
rows.append(["TOTAL"] + tot_cells); fills.append([None] + tot_fills)
for k in zs_keys:
    c, fl = cv_row(g_zs[k]); rows.append([k] + c); fills.append([None] + fl)
sl = add_slide()
add_title(sl, "Couverture vaccinale administrative par ZS — tous antigènes (%)", SUB)
add_table(sl, cv_header, rows, top=Cm(2.0), size=7,
          col_w=[2.2] + [1] * len(AG_ORDER), cell_fills=fills)
remember(sl, 10)

cv_header_as = ["Aire de Santé"] + AG_ORDER
for idx, chunk in enumerate(paginate(as_keys, 26), 1):
    rows, fills = [], []
    for k in chunk:
        c, fl = cv_row(g_as[k]); rows.append([as_label(k)] + c); fills.append([None] + fl)
    sl = add_slide()
    add_title(sl, f"Couverture vaccinale administrative par AS — tous antigènes (%) ({idx})", SUB)
    add_table(sl, cv_header_as, rows, top=Cm(2.0), size=6,
              col_w=[2.6] + [1] * len(AG_ORDER), cell_fills=fills)
    remember(sl, 10)

# 3. Enfants ZD & SV — graphiques (ZS unique, AS paginé)
cats = list(zs_keys)
zd_v = [round(zd(g_zs[k])) for k in zs_keys]
sv_v = [round(sv(g_zs[k])) for k in zs_keys]
sl = add_slide()
add_title(sl, "Enfants Zéro-Dose (ZD) et Sous-Vaccinés (SV) par ZS", SUB)
add_bar_chart(sl, cats, [("Enfants ZD", zd_v), ("Enfants SV", sv_v)], top=Cm(2.2))
remember(sl, 10)

for idx, chunk in enumerate(paginate(as_keys, 40), 1):
    sl = add_slide()
    add_title(sl, f"Enfants Zéro-Dose (ZD) et Sous-Vaccinés (SV) par AS ({idx})", SUB)
    cats = [as_label(k) for k in chunk]
    zdv = [round(zd(g_as[k])) for k in chunk]
    svv = [round(sv(g_as[k])) for k in chunk]
    add_bar_chart(sl, cats, [("Enfants ZD", zdv), ("Enfants SV", svv)], top=Cm(2.2))
    remember(sl, 10)

# 4. Catégorisation ACZ — tableaux (ZS unique, AS paginé)
def cat_row(recs):
    t = targets(recs)
    d1 = s(recs, "DTC1_0_11"); d3 = s(recs, "DTC3_0_11")
    cv1 = d1 / t["ns"] * 100 if t["ns"] > 0 else None
    ta = abandon(d1, d3)
    cat = categorize(cv1, ta)
    return ["–" if cv1 is None else f"{cv1:.1f}%",
            "–" if ta is None else f"{ta:.1f}%", cat], cat

cat_header = ["{}", "CV Penta1 (%)", "Taux abandon DTC1-DTC3 (%)", "Catégorie ACZ"]
def build_cat(keys, label_fn, head_label, page=45):
    hdr = [head_label, "CV Penta1 (%)", "Taux abandon DTC1-DTC3 (%)", "Catégorie ACZ"]
    items = keys
    multipage = len(items) > page
    pi = 0
    for chunk in paginate(items, page):
        pi += 1
        rows, fills = [], []
        for k in chunk:
            cells, cat = cat_row(g_as[k] if head_label.startswith("Aire") else g_zs[k])
            rows.append([label_fn(k)] + cells)
            fills.append([None, None, None, cat_color(cat)])
        sl = add_slide()
        suff = f" ({pi})" if multipage else ""
        add_title(sl, f"Catégorisation ACZ par {'AS' if head_label.startswith('Aire') else 'ZS'}{suff}",
                  SUB + " — Cat.1: P1≥90 & TA≤10 · Cat.2: P1≥90 & TA>10 · Cat.3: P1<90 & TA≤10 · Cat.4: P1<90 & TA>10")
        add_table(sl, hdr, rows, top=Cm(2.4), size=9, col_w=[3, 2, 2.5, 2], cell_fills=fills)
        remember(sl, 10)

# ZS total row included
def cat_zs_rows():
    hdr = ["Zone de Santé", "CV Penta1 (%)", "Taux abandon DTC1-DTC3 (%)", "Catégorie ACZ"]
    rows, fills = [], []
    cells, cat = cat_row(cur); rows.append(["TOTAL"] + cells); fills.append([None, None, None, cat_color(cat)])
    for k in zs_keys:
        cells, cat = cat_row(g_zs[k]); rows.append([k] + cells); fills.append([None, None, None, cat_color(cat)])
    sl = add_slide()
    add_title(sl, "Catégorisation ACZ par ZS",
              SUB + " — Cat.1: P1≥90 & TA≤10 · Cat.2: P1≥90 & TA>10 · Cat.3: P1<90 & TA≤10 · Cat.4: P1<90 & TA>10")
    add_table(sl, hdr, rows, top=Cm(2.4), size=10, col_w=[3, 2, 2.5, 2], cell_fills=fills)
    remember(sl, 10)

cat_zs_rows()
build_cat(as_keys, as_label, "Aire de Santé", page=42)

# 5. Disponibilité des intrants (BCG, DTC, VAR, VPO) — tableaux
DISPO_V = [("BCG", "BCG"), ("DTC (Penta)", "DTC"), ("VAR", "VAR"), ("VPO", "VPO")]
def dispo_row(recs):
    cells, fills = [], []
    for _, pfx in DISPO_V:
        p = dispo(recs, pfx)
        cells.append("–" if p is None else f"{p:.1f}%"); fills.append(disp_color(p))
    return cells, fills

hdr = ["Zone de Santé"] + [l for l, _ in DISPO_V]
rows, fills = [], []
c, fl = dispo_row(cur); rows.append(["TOTAL"] + c); fills.append([None] + fl)
for k in zs_keys:
    c, fl = dispo_row(g_zs[k]); rows.append([k] + c); fills.append([None] + fl)
sl = add_slide()
add_title(sl, "Taux de disponibilité des intrants par ZS (BCG, DTC, VAR, VPO)", SUB)
add_table(sl, hdr, rows, top=Cm(2.0), size=10, col_w=[3, 1.5, 1.5, 1.5, 1.5], cell_fills=fills)
remember(sl, 10)

hdr = ["Aire de Santé"] + [l for l, _ in DISPO_V]
for idx, chunk in enumerate(paginate(as_keys, 40), 1):
    rows, fills = [], []
    for k in chunk:
        c, fl = dispo_row(g_as[k]); rows.append([as_label(k)] + c); fills.append([None] + fl)
    sl = add_slide()
    add_title(sl, f"Taux de disponibilité des intrants par AS (BCG, DTC, VAR, VPO) ({idx})", SUB)
    add_table(sl, hdr, rows, top=Cm(2.0), size=8, col_w=[3, 1.5, 1.5, 1.5, 1.5], cell_fills=fills)
    remember(sl, 10)

# 6. Taux de perte (VPO, DTC, VAR, VAA, Td) — tableaux
PERTE_V = [("VPO", "VPO"), ("DTC (Penta)", "DTC"), ("VAR", "VAR"), ("VAA", "VAA"), ("Td", "Td")]
def perte_row(recs):
    cells, fills = [], []
    for _, pfx in PERTE_V:
        p = perte(recs, pfx)
        cells.append("–" if p is None else f"{p:.1f}%"); fills.append(perte_color(p))
    return cells, fills

hdr = ["Zone de Santé"] + [l for l, _ in PERTE_V]
rows, fills = [], []
c, fl = perte_row(cur); rows.append(["TOTAL"] + c); fills.append([None] + fl)
for k in zs_keys:
    c, fl = perte_row(g_zs[k]); rows.append([k] + c); fills.append([None] + fl)
sl = add_slide()
add_title(sl, "Taux de perte par ZS et antigène traceur (VPO, DTC, VAR, VAA, Td)", SUB)
add_table(sl, hdr, rows, top=Cm(2.0), size=10, col_w=[3, 1.4, 1.4, 1.4, 1.4, 1.4], cell_fills=fills)
remember(sl, 10)

hdr = ["Aire de Santé"] + [l for l, _ in PERTE_V]
for idx, chunk in enumerate(paginate(as_keys, 40), 1):
    rows, fills = [], []
    for k in chunk:
        c, fl = perte_row(g_as[k]); rows.append([as_label(k)] + c); fills.append([None] + fl)
    sl = add_slide()
    add_title(sl, f"Taux de perte par AS et antigène traceur (VPO, DTC, VAR, VAA, Td) ({idx})", SUB)
    add_table(sl, hdr, rows, top=Cm(2.0), size=8, col_w=[3, 1.4, 1.4, 1.4, 1.4, 1.4], cell_fills=fills)
    remember(sl, 10)

# ------------------------------------------------------------
# DIAPO 12 — performance communication
# ------------------------------------------------------------
# slide12 original index : 11 (0-based). Find current index after additions? We append then reorder.
S12_AFTER = 11

# 1. Enfants identifiés & récupérés (RECO) — tableaux DTC + VAR
def pdv_rows(keys, label_fn, src):
    hdr = ["Entité", "Identifiés DTC (Penta1+3)", "Récupérés DTC (Penta1+3)",
           "% récup. DTC", "Récupérés VAR1 (BCU-FAE)"]
    rows = []
    tid = trec = tvar = 0.0
    for k in keys:
        e = src.get(k if not isinstance(k, tuple) else k, {"id": 0, "rec": 0, "var": 0})
        i, rc, vr = e["id"], e["rec"], e["var"]
        tid += i; trec += rc; tvar += vr
        pct = f"{rc / i * 100:.0f}%" if i > 0 else "–"
        rows.append([label_fn(k), f"{round(i)}", f"{round(rc)}", pct, f"{round(vr)}"])
    total = ["TOTAL", f"{round(tid)}", f"{round(trec)}",
             f"{trec / tid * 100:.0f}%" if tid > 0 else "–", f"{round(tvar)}"]
    return hdr, [total] + rows

# ZS
hdr, rows = pdv_rows(zs_keys, lambda k: k, pdv_zs)
sl = add_slide()
add_title(sl, "Enfants perdus de vue identifiés et récupérés par les RECO — par ZS",
          SUB + " — DTC = Penta1+Penta3 (RECO). VAR : récupérés VAR1 BCU-FAE (identifiés non collectés dans DHIS2).")
add_table(sl, hdr, rows, top=Cm(2.4), size=9, col_w=[2.6, 2, 2, 1.4, 2])
remember(sl, S12_AFTER)

# AS
hdr = ["Aire de Santé", "Identifiés DTC (Penta1+3)", "Récupérés DTC (Penta1+3)", "% récup. DTC", "Récupérés VAR1 (BCU-FAE)"]
for idx, chunk in enumerate(paginate(as_keys, 30), 1):
    rows = []
    for k in chunk:
        e = pdv_as.get(k, {"id": 0, "rec": 0, "var": 0})
        i, rc, vr = e["id"], e["rec"], e["var"]
        pct = f"{rc / i * 100:.0f}%" if i > 0 else "–"
        rows.append([as_label(k), f"{round(i)}", f"{round(rc)}", pct, f"{round(vr)}"])
    sl = add_slide()
    add_title(sl, f"Enfants perdus de vue identifiés et récupérés par les RECO — par AS ({idx})",
              SUB + " — DTC = Penta1+Penta3 (RECO). VAR : récupérés VAR1 BCU-FAE.")
    add_table(sl, hdr, rows, top=Cm(2.4), size=8, col_w=[2.6, 2, 2, 1.4, 2])
    remember(sl, S12_AFTER)

# 2. Taux d'abandon BCG-VAR1 & DTC1-DTC3 — graphiques (ZS unique, AS paginé)
def ab_pair(recs):
    return (round(abandon(s(recs, "DTC1_0_11"), s(recs, "DTC3_0_11")) or 0, 1),
            round(abandon(s(recs, "BCG_0_11"), s(recs, "VAR1_0_11")) or 0, 1))

cats = list(zs_keys)
ab_dtc = [ab_pair(g_zs[k])[0] for k in zs_keys]
ab_bcg = [ab_pair(g_zs[k])[1] for k in zs_keys]
sl = add_slide()
add_title(sl, "Taux d'abandon DTC1-DTC3 et BCG-VAR1(RR1) par ZS", SUB)
add_bar_chart(sl, cats, [("Abandon DTC1-DTC3 %", ab_dtc), ("Abandon BCG-VAR1 %", ab_bcg)], top=Cm(2.2))
remember(sl, S12_AFTER)

for idx, chunk in enumerate(paginate(as_keys, 40), 1):
    sl = add_slide()
    add_title(sl, f"Taux d'abandon DTC1-DTC3 et BCG-VAR1(RR1) par AS ({idx})", SUB)
    cats = [as_label(k) for k in chunk]
    d = [ab_pair(g_as[k])[0] for k in chunk]
    b = [ab_pair(g_as[k])[1] for k in chunk]
    add_bar_chart(sl, cats, [("Abandon DTC1-DTC3 %", d), ("Abandon BCG-VAR1 %", b)], top=Cm(2.2))
    remember(sl, S12_AFTER)

# ------------------------------------------------------------
#  RÉORDONNANCEMENT : insérer les nouvelles diapos après 11 / 12
# ------------------------------------------------------------
sldIdLst = prs.slides._sldIdLst
ids = list(sldIdLst)              # ordre courant (27 base + nouvelles appendues)
base = ids[:27]                   # 27 diapos d'origine
# map slide element -> sldId
def sldid_of(slide):
    rId = None
    for sid in ids:
        if sid.get(qn('r:id')) == slide.part.relate_to.__self__ if False else False:
            pass
    return None

# Construire ordre : pour chaque diapo de base, l'ajouter ; après index 10 (diapo11)
# insérer les NEW after0==10 ; après index 11 (diapo12) insérer NEW after0==11.
# On retrouve l'élément sldId de chaque slide via son position dans prs.slides.
slide_to_sldId = {}
for sid in ids:
    slide_to_sldId[sid] = sid
# Build position list of new slides grouped
new11 = [sl for (a, sl) in NEW if a == 10]
new12 = [sl for (a, sl) in NEW if a == 11]

def sldId_for_slide(slide):
    target_rid = None
    part = slide.part
    # find rId in presentation rels pointing to this part
    for rid, rel in prs.part.rels.items():
        if rel._target is part:
            target_rid = rid; break
    for sid in list(sldIdLst):
        if sid.get(qn('r:id')) == target_rid:
            return sid
    return None

order = []
for i, sid in enumerate(base):
    order.append(sid)
    if i == 10:   # diapo 11
        for sl in new11:
            order.append(sldId_for_slide(sl))
    if i == 11:   # diapo 12
        for sl in new12:
            order.append(sldId_for_slide(sl))

# Réécrire sldIdLst dans le nouvel ordre
for sid in list(sldIdLst):
    sldIdLst.remove(sid)
for sid in order:
    sldIdLst.append(sid)

prs.save(OUT)
print("OK ->", OUT)
print("Diapos totales:", len(prs.slides._sldIdLst))
print("Nouvelles diapo 11:", len(new11), "| Nouvelles diapo 12:", len(new12))
