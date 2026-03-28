from __future__ import annotations

import json
import gzip
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# ============================================================
# PATHS  (everything relative to repo root)
# ============================================================

SCRIPT_DIR  = Path(__file__).resolve().parent
REPO_ROOT   = SCRIPT_DIR.parent
DATA_DIR    = REPO_ROOT / "docs" / "data"
MONTHLY_DIR = DATA_DIR / "monthly"
DASHBOARD_DIR = DATA_DIR / "dashboard"
OU_MAP_PATH = DATA_DIR / "ou_map.json"
INDEX_PATH  = DATA_DIR / "index.json"
ANTENNE_RULES_PATH = DATA_DIR / "antenne_rules.json"
RENAME_MAP_PATH    = DATA_DIR / "rename_map.json"

MAX_JSON_FILE_BYTES = 50_000_000


# ============================================================
# PERIOD PARSING
# ============================================================

_MONTH_ABBR = {
    "jan":"01","feb":"02","mar":"03","apr":"04",
    "may":"05","jun":"06","jul":"07","aug":"08",
    "sep":"09","oct":"10","nov":"11","dec":"12",
}

def parse_period(raw: Any) -> Optional[str]:
    if not raw or not isinstance(raw, str):
        return None
    s = raw.strip()
    if len(s) == 6 and s.isdigit():
        return s
    if len(s) >= 9 and s[2] == "-" and s[6] == "-":
        mm = _MONTH_ABBR.get(s[3:6].lower())
        yyyy = s[7:11]
        if mm and yyyy.isdigit():
            return f"{yyyy}{mm}"
    if len(s) >= 7 and s[4] == "-":
        yyyy, mm = s[0:4], s[5:7]
        if yyyy.isdigit() and mm.isdigit():
            return f"{yyyy}{mm}"
    return None

def period_display(yyyymm: str) -> str:
    months = ["Jan","Feb","Mar","Apr","May","Jun",
              "Jul","Aug","Sep","Oct","Nov","Dec"]
    try:
        y, m = int(yyyymm[:4]), int(yyyymm[4:6])
        return f"{months[m-1]} {y}"
    except Exception:
        return yyyymm


# ============================================================
# OU_MAP PARSING
# ============================================================

_PROV_SUFFIX = re.compile(r"\s+Province\s*$", re.I)
_ZS_SUFFIX   = re.compile(r"\s+Zone\s+de\s+Sant[ée]\s*$", re.I)
_AS_SUFFIX   = re.compile(r"\s+Aire\s+de\s+Sant[ée]\s*$", re.I)
_FOSA_SUFFIX = re.compile(
    r"\s+(Centre|Poste|H[oô]pital|H[oô]p\.?)\s+de\s+Sant[ée]\s*$", re.I)
_PREFIX      = re.compile(r"^[a-zA-Z]{2,3}\s+")

def _clean(raw: str, sfx: Optional[re.Pattern] = None) -> str:
    s = (raw or "").strip()
    s = _PREFIX.sub("", s)
    if sfx:
        s = sfx.sub("", s)
    return s.strip()

def load_ou_map(path: Path) -> Dict[str, dict]:
    if not path.exists():
        print(f"  ERROR: {path} not found"); return {}
    raw = json.loads(path.read_text("utf-8"))
    if not isinstance(raw, dict):
        print(f"  ERROR: ou_map is {type(raw).__name__}, expected dict"); return {}

    ou = {}
    for uid, info in raw.items():
        if not isinstance(info, dict): continue
        prov_raw = info.get("Org2","")
        ou[uid] = {
            "province":     _clean(prov_raw, _PROV_SUFFIX),
            "zone_sante":   _clean(info.get("Org3",""), _ZS_SUFFIX),
            "aire_sante":   _clean(info.get("Org4",""), _AS_SUFFIX),
            "fosa":         _clean(info.get("Org5",""), _FOSA_SUFFIX),
            "province_key": prov_raw.strip(),
        }
    print(f"  ou_map: {len(ou):,} org units")
    if ou:
        k, v = next(iter(ou.items()))
        print(f"    sample: {k} → {v['province']} > {v['zone_sante']} > {v['aire_sante']}")
    return ou


# ============================================================
# ANTENNE RULES
# ============================================================

def load_antenne_rules(path: Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        print(f"  antenne_rules not found (optional)"); return {}
    data = json.loads(path.read_text("utf-8"))
    total_zs = sum(len(v) for v in data.values())
    print(f"  antenne_rules: {len(data)} provinces, {total_zs} ZS mappings")
    return data

def resolve_antenne(rules: Dict, prov_key: str, zs: str) -> str:
    pr = rules.get(prov_key)
    if not pr:
        pk_low = prov_key.lower()
        for ak, av in rules.items():
            if ak.lower() == pk_low:
                pr = av; break
    if not pr:
        return ""
    ant = pr.get(zs, "")
    if not ant:
        zs_low = zs.lower()
        for k, v in pr.items():
            if k.lower() == zs_low:
                return v
    return ant


# ============================================================
# RENAME MAP
# ============================================================

def load_rename_map(path: Path) -> Dict[str, str]:
    """Returns label_to_zoho: {"BCG fixe1": "BCG_fixe1", ...}"""
    if not path.exists():
        print(f"  ERROR: {path} not found"); return {}
    data = json.loads(path.read_text("utf-8"))
    print(f"  rename_map: {len(data)} entries")
    return dict(data)


# ============================================================
# VACCINE GROUP DEFINITIONS
# ============================================================

VACCINE_GROUPS = {
    "BCG": {
        "labels": [
            "BCG fixe1","BCG fixe2","BCG avancé1","BCG avancé2",
            "BCG mobile1","BCG mobile2"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "DTC1": {
        "labels": [
            "Penta1 fixe1","Penta1 fixe2","Penta1 avancé1","Penta1 avancé2",
            "Penta1 mobile1","Penta1 mobile2"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "DTC2": {
        "labels": [
            "Penta2 fixe1","Penta2 fixe2","Penta2 avancé1","Penta2 avancé2",
            "Penta2 mobile1","Penta2 mobile2"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "DTC3": {
        "labels": [
            "Penta3 fixe1","Penta3 fixe2","Penta3 avancé1","Penta3 avancé2",
            "Penta3 mobile1","Penta3 mobile2"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "VPO0": {
        "labels": [
            "VPO0 0-11 mois fixe1","VPO0 0-11 mois fixe2",
            "VPO0 0-11 mois avancée1","VPO0 0-11 mois avancée2",
            "VPO0 0-11 mois mobile1","VPO0 0-11 mois mobile2"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "VPO1": {
        "labels": [
            "VPO1 0-11 mois fixe1","VPO1 0-11 mois fixe2",
            "VPO1 0-11 mois avancée1","VPO1 0-11 mois avancée2",
            "VPO1 0-11 mois mobile1","VPO1 0-11 mois mobile2"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "VPO2": {
        "labels": [
            "VPO2 0-11 mois fixe1","VPO2 0-11 mois fixe2",
            "VPO2 0-11 mois avancée1","VPO2 0-11 mois avancée2",
            "VPO2 0-11 mois mobile1","VPO2 0-11 mois mobile2"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "VPO3": {
        "labels": [
            "VPO3 fixe1","VPO3 fixe2","VPO3 avancé1","VPO3 avancé2",
            "VPO3 mobile1","VPO3 mobile2"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "VPI1": {
        "labels": [
            "VPI1 fixe1","VPI1 fixe2","VPI1 avancé1","VPI1 avancé2",
            "VPI1 mobile1","VPI1 mobile2"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "VPI2": {
        "labels": [
            "VPI2 fixe1","VPI2 fixe2","VPI2 avancé1","VPI2 avancé2",
            "VPI2 mobile1","VPI2 mobile2"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "ROTA1": {
        "labels": [
            "ROTA1 0-11 mois fixe","ROTA1 0-11 mois avancée",
            "ROTA1 0-11 mois mobile"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "ROTA2": {
        "labels": [
            "ROTA2 0-11 mois fixe","ROTA2 0-11 mois avancée",
            "ROTA2 0-11 mois mobile"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "ROTA3": {
        "labels": ["ROTA3 fixe","ROTA3 avancé","ROTA3 mobile"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "PCV13_1": {
        "labels": [
            "PCV13(1) 0-11 mois fixe1","PCV13(1) 0-11 mois fixe2",
            "PCV13(1) 0-11 mois avancée1","PCV13(1) 0-11 mois avancée2",
            "PCV13(1) 0-11 mois mobile1","PCV13(1) 0-11 mois mobile2"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "PCV13_2": {
        "labels": [
            "PCV13(2) 0-11 mois fixe1","PCV13(2) 0-11 mois fixe2",
            "PCV13(2) 0-11 mois avancée1","PCV13(2) 0-11 mois avancée2",
            "PCV13(2) 0-11 mois mobile1","PCV13(2) 0-11 mois mobile2"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "PCV13_3": {
        "labels": [
            "PCV13 fixe1","PCV13 fixe2","PCV13 avancé1","PCV13 avancé2",
            "PCV13 mobile1","PCV13 mobile2"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "VAR1": {
        "labels": [
            "VAR1 fixe1","VAR1 fixe2","VAR1 avancé1","VAR1 avancé2",
            "VAR1 mobile1","VAR1 mobile2"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "VAR2": {
        "labels": [
            "VAR2 fixe1","VAR2 fixe2","VAR2 avancé",
            "VAR2 mobile1","VAR2 mobile2",
            "VAR2 0-11 mois fixe","VAR2 0-11 mois avancée","VAR2 0-11 mois mobile"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "VAP1": {
        "labels": [
            "VAP1 0-11 mois fixe","VAP1 0-11 mois avancée"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "VAP2": {
        "labels": [
            "VAP2 0-11 mois fixe","VAP2 0-11 mois avancée",
            "VAP2 0-11 mois mobile"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "VAP3": {
        "labels": [
            "VAP3 0-11 mois fixe","VAP3 0-11 mois avancée",
            "VAP3 0-11 mois mobile"],
        "denom": "Pop. 0-11m (survivants)",
    },
    "VAP4": {
        "labels": [
            "VAP4 12-23 mois fixe","VAP4 12-23 mois avancée",
            "VAP4 12-23 mois mobile"],
        "denom": "Pop. 12-59m",
    },
}

# Extra numeric indicators (sum)
EXTRA_SUMS = {
    "seances_prevues":   "Séances prévues",
    "seances_realisees": "Séances réalisées",
    "seances_fixes_prevues":     "Séances fixes prévues",
    "seances_fixes_realisees":   "Séances fixes réalisées",
    "seances_avancees_prevues":  "Séances avancées prévues",
    "seances_avancees_realisees":"Séances avancées réalisées",
    "seances_mobiles_prevues":   "Séances mobiles prévues",
    "seances_mobiles_realisees": "Séances mobiles réalisées",
    "ecv":          "ECV",
    "hpv":          "HPV",
    "pop_totale":   "Pop. totale",
    "naissances_vivantes": "Naissances vivantes",
    "pop_0_11m_nv":   "Pop. 0-11m (nv)",
    "pop_0_11m_surv": "Pop. 0-11m (survivants)",
    "pop_0_59m":      "Pop. 0-59m",
    "pop_12_59m":     "Pop. 12-59m",
    "rapports_attendus": "Rapports attendus",
}

# Percentage fields (average across facilities)
PCT_FIELDS = {
    "completude":  "Complétude",
    "promptitude": "Promptitude",
}


# ============================================================
# FIELD ACCESSOR
# ============================================================

def safe_num(v: Any) -> float:
    if v is None: return 0.0
    if isinstance(v, (int, float)): return float(v)
    if isinstance(v, str):
        try: return float(v.replace(",", "."))
        except ValueError: return 0.0
    return 0.0

def get_field(rec: dict, label: str, l2z: Dict[str, str]) -> float:
    zoho = l2z.get(label)
    if zoho and zoho in rec:
        return safe_num(rec[zoho])
    if label in rec:
        return safe_num(rec[label])
    return 0.0


# ============================================================
# READ ALL NDJSON
# ============================================================

def read_all_ndjson(mdir: Path) -> List[dict]:
    records: List[dict] = []
    if not mdir.exists():
        print(f"  ERROR: {mdir} does not exist"); return records

    month_dirs = sorted(d for d in mdir.iterdir() if d.is_dir())
    print(f"  Found {len(month_dirs)} month folders")

    for md in month_dirs:
        count = 0
        # Plain .ndjson
        for nf in sorted(md.glob("*.ndjson")):
            if nf.name.endswith(".gz"): continue
            try:
                with open(nf, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line: continue
                        try:
                            records.append(json.loads(line))
                            count += 1
                        except json.JSONDecodeError:
                            pass
            except Exception as e:
                print(f"    WARN: {nf}: {e}")

        # Fallback .gz
        if count == 0:
            for gf in sorted(md.glob("*.ndjson.gz")):
                try:
                    with gzip.open(gf, "rt", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if not line: continue
                            try:
                                records.append(json.loads(line))
                                count += 1
                            except json.JSONDecodeError:
                                pass
                except Exception as e:
                    print(f"    WARN: {gf}: {e}")

        if count:
            print(f"    {md.name}: {count:,} records")

    print(f"  TOTAL: {len(records):,} records")
    return records


# ============================================================
# BUCKET ACCUMULATOR
# ============================================================

class Bucket:
    __slots__ = ("doses","denoms","extras","pct_sum","n")

    def __init__(self):
        self.doses:   Dict[str,float] = defaultdict(float)
        self.denoms:  Dict[str,float] = defaultdict(float)
        self.extras:  Dict[str,float] = defaultdict(float)
        self.pct_sum: Dict[str,float] = defaultdict(float)
        self.n: int = 0

    def add(self, rec: dict, l2z: Dict[str,str]):
        for vname, vdef in VACCINE_GROUPS.items():
            total = sum(get_field(rec, lb, l2z) for lb in vdef["labels"])
            self.doses[vname] += total
            dlab = vdef.get("denom")
            if dlab:
                self.denoms[vname] += get_field(rec, dlab, l2z)

        for key, label in EXTRA_SUMS.items():
            self.extras[key] += get_field(rec, label, l2z)

        for key, label in PCT_FIELDS.items():
            self.pct_sum[key] += get_field(rec, label, l2z)

        self.n += 1

    def to_dict(self) -> dict:
        vax = {}
        for vname in VACCINE_GROUPS:
            adm = self.doses.get(vname, 0)
            den = self.denoms.get(vname, 0)
            e: dict = {"administered": round(adm)}
            if den > 0:
                e["target"] = round(den)
                e["coverage"] = round(adm / den * 100, 1)
            vax[vname] = e

        ind = {}
        for key in EXTRA_SUMS:
            v = self.extras.get(key, 0)
            ind[key] = round(v) if v == int(v) else round(v, 1)

        nn = max(1, self.n)
        rpt = {
            "completeness": round(self.pct_sum.get("completude", 0) / nn, 1),
            "promptness":   round(self.pct_sum.get("promptitude", 0) / nn, 1),
            "facilities":   self.n,
        }

        return {"vaccines": vax, "indicators": ind, "reporting": rpt}


# ============================================================
# AGGREGATION
# ============================================================

def aggregate(
    records: List[dict],
    ou_map: Dict[str, dict],
    l2z: Dict[str, str],
    ant_rules: Dict[str, Dict[str, str]],
) -> dict:

    buckets: Dict[Tuple[str,str,str], Bucket] = {}

    def bkt(stype: str, skey: str, pe: str) -> Bucket:
        k = (stype, skey, pe)
        if k not in buckets:
            buckets[k] = Bucket()
        return buckets[k]

    unresolved: Set[str] = set()
    pe_fail = 0

    for rec in records:
        uid = rec.get("OrgUnit","")
        pe  = parse_period(rec.get("Period",""))
        if not pe:
            pe_fail += 1; continue

        info = ou_map.get(uid)
        if not info:
            unresolved.add(uid); continue

        prov = info["province"]
        zs   = info["zone_sante"]
        ars  = info["aire_sante"]
        pkey = info["province_key"]
        if not prov:
            unresolved.add(uid); continue

        ant = resolve_antenne(ant_rules, pkey, zs)

        bkt("national","national",pe).add(rec,l2z)
        bkt("province",prov,pe).add(rec,l2z)

        if ant:
            bkt("antenne", f"{prov}|{ant}", pe).add(rec,l2z)
        if zs:
            bkt("zs", f"{prov}|{zs}", pe).add(rec,l2z)
        if ars:
            bkt("as", f"{prov}|{zs}|{ars}", pe).add(rec,l2z)

    if unresolved:
        print(f"  WARN: {len(unresolved)} UIDs not in ou_map (samples: {list(unresolved)[:5]})")
    if pe_fail:
        print(f"  WARN: {pe_fail} records with unparseable period")

    # ---- Build output structure ----
    all_pe:  Set[str] = set()
    all_prov: Set[str] = set()
    for (st,sk,pe) in buckets:
        all_pe.add(pe)
        if st == "province": all_prov.add(sk)

    spe = sorted(all_pe)
    sprov = sorted(all_prov)

    # National
    nat_pe = {}
    for pe in spe:
        b = buckets.get(("national","national",pe))
        if b: nat_pe[pe] = b.to_dict()

    # Provinces
    provs_out = {}
    for prov in sprov:
        # Province periods
        pp = {}
        for pe in spe:
            b = buckets.get(("province",prov,pe))
            if b: pp[pe] = b.to_dict()

        # Antennes
        ant_names: Set[str] = set()
        for (st,sk,pe) in buckets:
            if st=="antenne" and sk.startswith(f"{prov}|"):
                ant_names.add(sk.split("|",1)[1])

        ants_out = {}
        for an in sorted(ant_names):
            ap = {}
            for pe in spe:
                b = buckets.get(("antenne",f"{prov}|{an}",pe))
                if b: ap[pe] = b.to_dict()
            if ap: ants_out[an] = {"periods": ap}

        # ZS
        zs_names: Set[str] = set()
        for (st,sk,pe) in buckets:
            if st=="zs" and sk.startswith(f"{prov}|"):
                zs_names.add(sk.split("|",1)[1])

        zs_out = {}
        for zsn in sorted(zs_names):
            zp = {}
            for pe in spe:
                b = buckets.get(("zs",f"{prov}|{zsn}",pe))
                if b: zp[pe] = b.to_dict()

            # AS under this ZS
            as_names: Set[str] = set()
            for (st,sk,pe) in buckets:
                if st=="as" and sk.startswith(f"{prov}|{zsn}|"):
                    as_names.add(sk.split("|",2)[2])

            as_out = {}
            for asn in sorted(as_names):
                asp = {}
                for pe in spe:
                    b = buckets.get(("as",f"{prov}|{zsn}|{asn}",pe))
                    if b: asp[pe] = b.to_dict()
                if asp: as_out[asn] = {"periods": asp}

            zse: dict = {"periods": zp}
            if as_out: zse["aires_sante"] = as_out
            zs_out[zsn] = zse

        pe_entry: dict = {"periods": pp}
        if ants_out: pe_entry["antennes"] = ants_out
        if zs_out:   pe_entry["zones_sante"] = zs_out
        provs_out[prov] = pe_entry

    resolved = len(records) - len(unresolved) - pe_fail

    return {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "periods": spe,
            "periods_display": {pe: period_display(pe) for pe in spe},
            "provinces": sprov,
            "vaccine_groups": list(VACCINE_GROUPS.keys()),
            "total_records": len(records),
            "resolved": resolved,
        },
        "national": {"periods": nat_pe},
        "provinces": provs_out,
    }


# ============================================================
# OUTPUT
# ============================================================

def write_output(data: dict, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob("*.json"):
        old.unlink()

    full = json.dumps(data, ensure_ascii=False, separators=(",",":"))
    sz   = len(full.encode("utf-8"))

    if sz < MAX_JSON_FILE_BYTES:
        p = out_dir / "dashboard.json"
        p.write_text(full, "utf-8")
        print(f"  Written: dashboard.json ({sz:,} bytes)")
        idx = {"files":["dashboard.json"],"split":False,"metadata":data["metadata"]}
        (out_dir/"index.json").write_text(json.dumps(idx,ensure_ascii=False,separators=(",",":")),"utf-8")
        return

    print(f"  Full data = {sz:,} bytes → splitting by province")
    files = []

    nat = {"metadata":data["metadata"],"national":data["national"]}
    nj  = json.dumps(nat,ensure_ascii=False,separators=(",",":"))
    (out_dir/"national.json").write_text(nj,"utf-8")
    files.append("national.json")
    print(f"    national.json: {len(nj):,} bytes")

    for pn, pd in sorted(data.get("provinces",{}).items()):
        safe = re.sub(r"[^a-z0-9]+","_",pn.lower()).strip("_")
        fn   = f"prov_{safe}.json"
        pj   = json.dumps({"province_name":pn,**pd},ensure_ascii=False,separators=(",",":"))
        (out_dir/fn).write_text(pj,"utf-8")
        files.append(fn)
        psz = len(pj.encode("utf-8"))
        flag = " ⚠️ LARGE!" if psz > MAX_JSON_FILE_BYTES else ""
        print(f"    {fn}: {psz:,} bytes{flag}")

    idx = {"files":files,"split":True,"metadata":data["metadata"]}
    (out_dir/"index.json").write_text(json.dumps(idx,ensure_ascii=False,separators=(",",":")),"utf-8")


def validate(out_dir: Path) -> bool:
    ok = True
    for jf in sorted(out_dir.glob("*.json")):
        try:
            txt = jf.read_text("utf-8")
            json.loads(txt)
            print(f"  ✅ {jf.name}: {jf.stat().st_size:,} bytes")
        except Exception as e:
            print(f"  ❌ {jf.name}: {e}")
            ok = False
    return ok


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    print("="*60)
    print("AGGREGATE DASHBOARD  v3.1")
    print("="*60)

    print("\n[1] Paths")
    for lbl, p in [("MONTHLY",MONTHLY_DIR),("OU_MAP",OU_MAP_PATH),
                    ("RENAME_MAP",RENAME_MAP_PATH),("ANTENNE",ANTENNE_RULES_PATH)]:
        e = "✅" if p.exists() else "❌"
        print(f"  {e} {lbl}: {p}")

    print("\n[2] rename_map")
    l2z = load_rename_map(RENAME_MAP_PATH)
    if not l2z:
        print("FATAL: rename_map empty"); return 1

    # Check vaccine labels
    missing = []
    for vn, vd in VACCINE_GROUPS.items():
        for lb in vd["labels"]:
            if lb not in l2z: missing.append(f"{vn}:{lb}")
        dlab = vd.get("denom")
        if dlab and dlab not in l2z: missing.append(f"{vn}:denom:{dlab}")
    for key,lab in EXTRA_SUMS.items():
        if lab not in l2z: missing.append(f"extra:{lab}")
    for key,lab in PCT_FIELDS.items():
        if lab not in l2z: missing.append(f"pct:{lab}")

    if missing:
        print(f"  WARN: {len(missing)} labels not in rename_map:")
        for m in missing[:15]:
            print(f"    - {m}")
        print("  (Will try to read by label name directly)")

    print("\n[3] ou_map")
    ou_map = load_ou_map(OU_MAP_PATH)
    if not ou_map:
        print("FATAL: ou_map empty"); return 1

    print("\n[4] antenne_rules")
    ant_rules = load_antenne_rules(ANTENNE_RULES_PATH)

    print("\n[5] Reading NDJSON")
    records = read_all_ndjson(MONTHLY_DIR)

    if not records:
        print("\nWARN: No records — writing empty dashboard")
        DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
        empty = {
            "metadata": {
                "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "error":"no_data","periods":[],"provinces":[]},
            "national":{"periods":{}},"provinces":{}}
        (DASHBOARD_DIR/"dashboard.json").write_text(
            json.dumps(empty,ensure_ascii=False,separators=(",",":")),"utf-8")
        (DASHBOARD_DIR/"index.json").write_text(
            json.dumps({"files":["dashboard.json"],"split":False,"metadata":empty["metadata"]},
                       ensure_ascii=False,separators=(",",":")),"utf-8")
        return 0

    # Sample
    r0 = records[0]
    print(f"  Sample fields ({len(r0)} total): {list(r0.keys())[:10]}...")
    print(f"  OrgUnit={r0.get('OrgUnit','?')} Period={r0.get('Period','?')}")

    print("\n[6] Aggregating")
    dashboard = aggregate(records, ou_map, l2z, ant_rules)

    meta = dashboard["metadata"]
    print(f"  Periods: {meta['periods']}")
    print(f"  Provinces: {len(meta['provinces'])}")
    print(f"  Resolved: {meta['resolved']:,}/{meta['total_records']:,}")

    print(f"\n[7] Writing → {DASHBOARD_DIR}")
    write_output(dashboard, DASHBOARD_DIR)

    print(f"\n[8] Validating")
    ok = validate(DASHBOARD_DIR)

    print(f"\n{'='*60}")
    if ok:
        print("✅ SUCCESS")
    else:
        print("❌ ERRORS")
    print(f"{'='*60}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
