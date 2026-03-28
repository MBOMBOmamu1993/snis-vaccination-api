from __future__ import annotations

import json
import gzip
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

# ============================================================
# PATHS (everything relative to repo root)
# ============================================================

SCRIPT_DIR         = Path(__file__).resolve().parent
REPO_ROOT          = SCRIPT_DIR.parent
DOCS_DIR           = REPO_ROOT / "docs"
DATA_DIR           = DOCS_DIR / "data"
CONFIG_DIR         = DOCS_DIR / "config"
MONTHLY_DIR        = DATA_DIR / "monthly"
DASHBOARD_DIR      = DATA_DIR / "dashboard"
HEATMAP_DIR        = DASHBOARD_DIR / "heatmap"
OU_MAP_PATH        = DATA_DIR / "ou_map.json"
RENAME_MAP_PATH    = CONFIG_DIR / "rename_map.json"
ANTENNE_RULES_PATH = CONFIG_DIR / "antenne_rules.json"

MAX_HEATMAP_CHUNK  = 5_000_000  # 5 MB per heatmap split file


# ============================================================
# PERIOD PARSING
# ============================================================

_MONTH_ABBR = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
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
        print(f"  ERROR: {path} not found")
        return {}
    raw = json.loads(path.read_text("utf-8"))
    if not isinstance(raw, dict):
        print(f"  ERROR: ou_map is {type(raw).__name__}, expected dict")
        return {}
    ou = {}
    for uid, info in raw.items():
        if not isinstance(info, dict):
            continue
        prov_raw = info.get("Org2", "")
        ou[uid] = {
            "province":     _clean(prov_raw, _PROV_SUFFIX),
            "zone_sante":   _clean(info.get("Org3", ""), _ZS_SUFFIX),
            "aire_sante":   _clean(info.get("Org4", ""), _AS_SUFFIX),
            "fosa":         _clean(info.get("Org5", ""), _FOSA_SUFFIX),
            "province_key": prov_raw.strip(),
        }
    print(f"  ou_map: {len(ou):,} org units")
    return ou


# ============================================================
# ANTENNE RULES
# ============================================================

def load_antenne_rules(path: Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        print(f"  antenne_rules not found (optional)")
        return {}
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
                pr = av
                break
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
    if not path.exists():
        print(f"  ERROR: {path} not found")
        return {}
    data = json.loads(path.read_text("utf-8"))
    print(f"  rename_map: {len(data)} entries")
    return dict(data)


# ============================================================
# ANTIGENS the HTML expects (AG_MAP keys)
# ============================================================

# The HTML's AG_MAP maps these short names to flat field names like "BCG_0_11"
# We define which DHIS2 labels (from rename_map) sum into each.

ANTIGEN_DEFS = {
    "BCG_0_11": [
        "BCG fixe1", "BCG fixe2", "BCG avancé1", "BCG avancé2",
        "BCG mobile1", "BCG mobile2"],
    "DTC1_0_11": [
        "Penta1 fixe1", "Penta1 fixe2", "Penta1 avancé1", "Penta1 avancé2",
        "Penta1 mobile1", "Penta1 mobile2"],
    "DTC2_0_11": [
        "Penta2 fixe1", "Penta2 fixe2", "Penta2 avancé1", "Penta2 avancé2",
        "Penta2 mobile1", "Penta2 mobile2"],
    "DTC3_0_11": [
        "Penta3 fixe1", "Penta3 fixe2", "Penta3 avancé1", "Penta3 avancé2",
        "Penta3 mobile1", "Penta3 mobile2"],
    "VPO0_0_11": [
        "VPO0 0-11 mois fixe1", "VPO0 0-11 mois fixe2",
        "VPO0 0-11 mois avancée1", "VPO0 0-11 mois avancée2",
        "VPO0 0-11 mois mobile1", "VPO0 0-11 mois mobile2"],
    "VPO1_0_11": [
        "VPO1 0-11 mois fixe1", "VPO1 0-11 mois fixe2",
        "VPO1 0-11 mois avancée1", "VPO1 0-11 mois avancée2",
        "VPO1 0-11 mois mobile1", "VPO1 0-11 mois mobile2"],
    "VPO2_0_11": [
        "VPO2 0-11 mois fixe1", "VPO2 0-11 mois fixe2",
        "VPO2 0-11 mois avancée1", "VPO2 0-11 mois avancée2",
        "VPO2 0-11 mois mobile1", "VPO2 0-11 mois mobile2"],
    "VPO3_0_11": [
        "VPO3 fixe1", "VPO3 fixe2", "VPO3 avancé1", "VPO3 avancé2",
        "VPO3 mobile1", "VPO3 mobile2"],
    "VPI1_0_11": [
        "VPI1 fixe1", "VPI1 fixe2", "VPI1 avancé1", "VPI1 avancé2",
        "VPI1 mobile1", "VPI1 mobile2"],
    "VPI2_0_11": [
        "VPI2 fixe1", "VPI2 fixe2", "VPI2 avancé1", "VPI2 avancé2",
        "VPI2 mobile1", "VPI2 mobile2"],
    "ROTA1_0_11": [
        "ROTA1 0-11 mois fixe", "ROTA1 0-11 mois avancée",
        "ROTA1 0-11 mois mobile"],
    "ROTA2_0_11": [
        "ROTA2 0-11 mois fixe", "ROTA2 0-11 mois avancée",
        "ROTA2 0-11 mois mobile"],
    "ROTA3_0_11": [
        "ROTA3 fixe", "ROTA3 avancé", "ROTA3 mobile"],
    "PCV13_1_0_11": [
        "PCV13(1) 0-11 mois fixe1", "PCV13(1) 0-11 mois fixe2",
        "PCV13(1) 0-11 mois avancée1", "PCV13(1) 0-11 mois avancée2",
        "PCV13(1) 0-11 mois mobile1", "PCV13(1) 0-11 mois mobile2"],
    "PCV13_2_0_11": [
        "PCV13(2) 0-11 mois fixe1", "PCV13(2) 0-11 mois fixe2",
        "PCV13(2) 0-11 mois avancée1", "PCV13(2) 0-11 mois avancée2",
        "PCV13(2) 0-11 mois mobile1", "PCV13(2) 0-11 mois mobile2"],
    "PCV13_3_0_11": [
        "PCV13 fixe1", "PCV13 fixe2", "PCV13 avancé1", "PCV13 avancé2",
        "PCV13 mobile1", "PCV13 mobile2"],
    "VAR1_0_11": [
        "VAR1 fixe1", "VAR1 fixe2", "VAR1 avancé1", "VAR1 avancé2",
        "VAR1 mobile1", "VAR1 mobile2"],
    "VAR2_0_11": [
        "VAR2 fixe1", "VAR2 fixe2", "VAR2 avancé",
        "VAR2 mobile1", "VAR2 mobile2",
        "VAR2 0-11 mois fixe", "VAR2 0-11 mois avancée",
        "VAR2 0-11 mois mobile"],
    "VAA_0_11": [
        "VAP1 0-11 mois fixe", "VAP1 0-11 mois avancée",
        "VAP2 0-11 mois fixe", "VAP2 0-11 mois avancée",
        "VAP2 0-11 mois mobile"],
}

# Labels for reporting indicators
LABEL_COMPLETUDE  = "Complétude"
LABEL_PROMPTITUDE = "Promptitude"
LABEL_RAP_ATTENDU = "Rapports attendus"


# ============================================================
# FIELD ACCESSOR
# ============================================================

def safe_num(v: Any) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v.replace(",", "."))
        except ValueError:
            return 0.0
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
        print(f"  ERROR: {mdir} does not exist")
        return records

    month_dirs = sorted(d for d in mdir.iterdir() if d.is_dir())
    print(f"  Found {len(month_dirs)} month folders")

    for md in month_dirs:
        count = 0
        for nf in sorted(md.glob("*.ndjson")):
            if nf.name.endswith(".gz"):
                continue
            try:
                with open(nf, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            records.append(json.loads(line))
                            count += 1
                        except json.JSONDecodeError:
                            pass
            except Exception as e:
                print(f"    WARN: {nf}: {e}")

        if count == 0:
            for gf in sorted(md.glob("*.ndjson.gz")):
                try:
                    with gzip.open(gf, "rt", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
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
# AGGREGATE into flat rows the HTML expects
# ============================================================

def aggregate(
    records: List[dict],
    ou_map: Dict[str, dict],
    l2z: Dict[str, str],
    ant_rules: Dict[str, Dict[str, str]],
) -> dict:
    """
    Returns:
      {
        "meta": {...},
        "by_province": [ {_Province, _Antenne, _YM, n, rap, comp, prompt, BCG_0_11, ...}, ... ],
        "by_zs":       [ {_Province, _Antenne, _ZS, _YM, n, rap, comp, prompt, BCG_0_11, ...}, ... ],
        "heatmap":     { province: { fosa_name: { "YYYYMM": 1, ... }, ... }, ... }
      }
    """

    # Accumulators keyed by (province, antenne, ym) and (province, antenne, zs, ym)
    # Each stores: n (facilities expected), rap (reported), comp_sum, prompt_sum,
    #              and antigen dose sums

    ag_keys = list(ANTIGEN_DEFS.keys())

    prov_bkt: Dict[tuple, dict] = {}   # (prov, ant, ym) -> bucket
    zs_bkt:   Dict[tuple, dict] = {}   # (prov, ant, zs, ym) -> bucket
    heatmap:  Dict[str, Dict[str, Dict[str, int]]] = {}  # prov -> fosa -> ym -> 1

    all_provinces: Set[str] = set()
    all_antennes:  Set[str] = set()
    all_zs:        Set[str] = set()
    all_months:    Set[str] = set()
    all_fosa:      Set[str] = set()

    unresolved: Set[str] = set()
    pe_fail = 0

    def new_bucket() -> dict:
        b = {"n": 0, "rap": 0, "comp_sum": 0.0, "prompt_sum": 0.0}
        for ak in ag_keys:
            b[ak] = 0.0
        return b

    def add_to_bucket(b: dict, rec: dict):
        b["n"] += 1
        # A facility "reported" if completude > 0
        comp_val = get_field(rec, LABEL_COMPLETUDE, l2z)
        prompt_val = get_field(rec, LABEL_PROMPTITUDE, l2z)
        if comp_val > 0:
            b["rap"] += 1
        b["comp_sum"] += comp_val
        b["prompt_sum"] += prompt_val
        for ak, labels in ANTIGEN_DEFS.items():
            b[ak] += sum(get_field(rec, lb, l2z) for lb in labels)

    for rec in records:
        uid = rec.get("OrgUnit", "")
        ym = parse_period(rec.get("Period", ""))
        if not ym:
            pe_fail += 1
            continue

        info = ou_map.get(uid)
        if not info:
            unresolved.add(uid)
            continue

        prov = info["province"]
        zs   = info["zone_sante"]
        fosa = info["fosa"] or info["aire_sante"]
        pkey = info["province_key"]
        if not prov:
            unresolved.add(uid)
            continue

        ant = resolve_antenne(ant_rules, pkey, zs)

        all_provinces.add(prov)
        if ant:
            all_antennes.add(ant)
        if zs:
            all_zs.add(zs)
        all_months.add(ym)
        if fosa:
            all_fosa.add(fosa)

        # Province level
        pk = (prov, ant, ym)
        if pk not in prov_bkt:
            prov_bkt[pk] = new_bucket()
        add_to_bucket(prov_bkt[pk], rec)

        # ZS level
        if zs:
            zk = (prov, ant, zs, ym)
            if zk not in zs_bkt:
                zs_bkt[zk] = new_bucket()
            add_to_bucket(zs_bkt[zk], rec)

        # Heatmap: per province -> fosa -> ym
        if fosa and prov:
            if prov not in heatmap:
                heatmap[prov] = {}
            if fosa not in heatmap[prov]:
                heatmap[prov][fosa] = {}
            heatmap[prov][fosa][ym] = 1

    if unresolved:
        print(f"  WARN: {len(unresolved)} UIDs not in ou_map (samples: {list(unresolved)[:5]})")
    if pe_fail:
        print(f"  WARN: {pe_fail} records with unparseable period")

    # ---- Build flat arrays ----
    def bucket_to_row(b: dict, extra: dict) -> dict:
        nn = max(1, b["n"])
        row = dict(extra)
        row["n"] = b["n"]
        row["rap"] = b["rap"]
        row["comp"] = round(b["comp_sum"] / nn, 1)
        row["prompt"] = round(b["prompt_sum"] / nn, 1)
        for ak in ag_keys:
            row[ak] = round(b[ak])
        return row

    by_province = []
    for (prov, ant, ym), b in sorted(prov_bkt.items()):
        by_province.append(bucket_to_row(b, {
            "_Province": prov,
            "_Antenne": ant,
            "_YM": ym,
        }))

    by_zs = []
    for (prov, ant, zs, ym), b in sorted(zs_bkt.items()):
        by_zs.append(bucket_to_row(b, {
            "_Province": prov,
            "_Antenne": ant,
            "_ZS": zs,
            "_YM": ym,
        }))

    sorted_months = sorted(all_months)
    sorted_provs  = sorted(all_provinces)
    sorted_ants   = sorted(all_antennes)
    sorted_zs     = sorted(all_zs)

    meta = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "months": sorted_months,
        "provinces": sorted_provs,
        "antennes": sorted_ants,
        "zs": sorted_zs,
        "nb_fosa": len(all_fosa),
        "total_records": len(records),
        "resolved": len(records) - len(unresolved) - pe_fail,
        "antigen_fields": ag_keys,
    }

    print(f"  Months: {len(sorted_months)}")
    print(f"  Provinces: {len(sorted_provs)}")
    print(f"  Antennes: {len(sorted_ants)}")
    print(f"  ZS: {len(sorted_zs)}")
    print(f"  FOSA: {len(all_fosa)}")
    print(f"  by_province rows: {len(by_province)}")
    print(f"  by_zs rows: {len(by_zs)}")
    print(f"  heatmap provinces: {len(heatmap)}")

    return {
        "meta": meta,
        "by_province": by_province,
        "by_zs": by_zs,
        "heatmap": heatmap,
    }


# ============================================================
# OUTPUT
# ============================================================

def write_json(path: Path, data: Any):
    txt = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    path.write_text(txt, "utf-8")
    sz = path.stat().st_size
    print(f"  Written: {path.name} ({sz:,} bytes)")
    return sz


def write_output(result: dict, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    # Clean old files
    for old in out_dir.glob("*.json"):
        old.unlink()
    hm_dir = out_dir / "heatmap"
    if hm_dir.exists():
        for old in hm_dir.glob("*.json"):
            old.unlink()
    else:
        hm_dir.mkdir(parents=True, exist_ok=True)

    # 1) meta.json
    write_json(out_dir / "meta.json", result["meta"])

    # 2) by_province.json
    write_json(out_dir / "by_province.json", result["by_province"])

    # 3) by_zs.json
    write_json(out_dir / "by_zs.json", result["by_zs"])

    # 4) heatmap/_index.json + per-province split files
    heatmap = result["heatmap"]
    index = {}
    for prov in sorted(heatmap.keys()):
        safe = re.sub(r"[^a-z0-9]+", "_", prov.lower()).strip("_")
        fn = f"{safe}.json"
        write_json(hm_dir / fn, heatmap[prov])
        index[prov] = fn

    write_json(hm_dir / "_index.json", index)
    print(f"  Heatmap: {len(index)} province files")


def validate(out_dir: Path) -> bool:
    ok = True
    for jf in sorted(out_dir.rglob("*.json")):
        try:
            txt = jf.read_text("utf-8")
            json.loads(txt)
            rel = jf.relative_to(out_dir)
            print(f"  OK {rel}: {jf.stat().st_size:,} bytes")
        except Exception as e:
            rel = jf.relative_to(out_dir)
            print(f"  FAIL {rel}: {e}")
            ok = False
    return ok


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    print("=" * 60)
    print("AGGREGATE DASHBOARD  v4.0")
    print("  Output: meta.json, by_province.json, by_zs.json,")
    print("          heatmap/_index.json + heatmap/<prov>.json")
    print("=" * 60)

    print("\n[1] Paths")
    for lbl, p in [("MONTHLY", MONTHLY_DIR), ("OU_MAP", OU_MAP_PATH),
                    ("RENAME_MAP", RENAME_MAP_PATH), ("ANTENNE", ANTENNE_RULES_PATH)]:
        e = "OK" if p.exists() else "MISSING"
        print(f"  [{e}] {lbl}: {p}")

    print("\n[2] rename_map")
    l2z = load_rename_map(RENAME_MAP_PATH)
    if not l2z:
        print("FATAL: rename_map empty")
        return 1

    # Check antigen labels coverage
    missing = []
    for ak, labels in ANTIGEN_DEFS.items():
        for lb in labels:
            if lb not in l2z:
                missing.append(f"{ak}:{lb}")
    for lb in [LABEL_COMPLETUDE, LABEL_PROMPTITUDE]:
        if lb not in l2z:
            missing.append(f"indicator:{lb}")

    if missing:
        print(f"  WARN: {len(missing)} labels not in rename_map:")
        for m in missing[:20]:
            print(f"    - {m}")
        print("  (Will try to read by label name directly)")

    print("\n[3] ou_map")
    ou_map = load_ou_map(OU_MAP_PATH)
    if not ou_map:
        print("FATAL: ou_map empty")
        return 1

    print("\n[4] antenne_rules")
    ant_rules = load_antenne_rules(ANTENNE_RULES_PATH)

    print("\n[5] Reading NDJSON")
    records = read_all_ndjson(MONTHLY_DIR)

    if not records:
        print("\nWARN: No records - writing empty dashboard")
        DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
        HEATMAP_DIR.mkdir(parents=True, exist_ok=True)
        empty_meta = {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "error": "no_data", "months": [], "provinces": [],
            "antennes": [], "zs": [], "nb_fosa": 0,
            "total_records": 0, "resolved": 0, "antigen_fields": [],
        }
        write_json(DASHBOARD_DIR / "meta.json", empty_meta)
        write_json(DASHBOARD_DIR / "by_province.json", [])
        write_json(DASHBOARD_DIR / "by_zs.json", [])
        write_json(HEATMAP_DIR / "_index.json", {})
        return 0

    r0 = records[0]
    print(f"  Sample fields ({len(r0)} total): {list(r0.keys())[:10]}...")
    print(f"  OrgUnit={r0.get('OrgUnit', '?')} Period={r0.get('Period', '?')}")

    print("\n[6] Aggregating")
    result = aggregate(records, ou_map, l2z, ant_rules)

    print(f"\n[7] Writing -> {DASHBOARD_DIR}")
    write_output(result, DASHBOARD_DIR)

    print(f"\n[8] Validating")
    ok = validate(DASHBOARD_DIR)

    print(f"\n{'=' * 60}")
    if ok:
        print("SUCCESS")
    else:
        print("ERRORS DETECTED")
    print(f"{'=' * 60}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
