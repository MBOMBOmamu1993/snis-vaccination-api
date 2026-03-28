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
# PATHS
# ============================================================

SCRIPT_DIR = Path(__file__).resolve().parent          # scripts/
REPO_ROOT  = SCRIPT_DIR.parent                        # repo root
DATA_DIR   = REPO_ROOT / "docs" / "data"
MONTHLY_DIR = DATA_DIR / "monthly"
DASHBOARD_DIR = DATA_DIR / "dashboard"
OU_MAP_PATH = DATA_DIR / "ou_map.json"
INDEX_PATH  = DATA_DIR / "index.json"
ANTENNE_RULES_PATH = DATA_DIR / "antenne_rules.json"
RENAME_MAP_PATH = REPO_ROOT / "docs" / "config" / "rename_map.json"

MAX_JSON_FILE_BYTES = 50_000_000  # 50 MB safety limit per file

# ============================================================
# PERIOD PARSING  ("01-Jan-2025" → "202501")
# ============================================================

_MONTH_ABBR = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}

def parse_period(raw: Any) -> Optional[str]:
    """Return YYYYMM or None."""
    if not raw or not isinstance(raw, str):
        return None
    s = raw.strip()

    # "202501"
    if len(s) == 6 and s.isdigit():
        return s

    # "01-Jan-2025"
    if len(s) >= 9 and s[2] == "-" and s[6] == "-":
        mm = _MONTH_ABBR.get(s[3:6].lower())
        yyyy = s[7:11]
        if mm and yyyy.isdigit():
            return f"{yyyy}{mm}"

    # "2025-01" or "2025-01-01"
    if len(s) >= 7 and s[4] == "-":
        yyyy, mm = s[0:4], s[5:7]
        if yyyy.isdigit() and mm.isdigit():
            return f"{yyyy}{mm}"

    return None


def period_display(yyyymm: str) -> str:
    """'202501' → 'Jan 2025'"""
    months = ["Jan","Feb","Mar","Apr","May","Jun",
              "Jul","Aug","Sep","Oct","Nov","Dec"]
    try:
        y, m = int(yyyymm[:4]), int(yyyymm[4:6])
        return f"{months[m-1]} {y}"
    except Exception:
        return yyyymm


# ============================================================
# OU_MAP PARSING  –  clean prefixes/suffixes
# ============================================================

_PROV_SUFFIX_RE = re.compile(r"\s+Province\s*$", re.IGNORECASE)
_ZS_SUFFIX_RE   = re.compile(r"\s+Zone\s+de\s+Sant[ée]\s*$", re.IGNORECASE)
_AS_SUFFIX_RE   = re.compile(r"\s+Aire\s+de\s+Sant[ée]\s*$", re.IGNORECASE)
_FOSA_SUFFIX_RE = re.compile(
    r"\s+(Centre|Poste|Hôpital|Hospital|H[oô]p\.?)\s+de\s+Sant[ée]\s*$",
    re.IGNORECASE,
)
# Prefix like "hk ", "ll ", "kn " (2-3 lowercase letters + space)
_PREFIX_RE = re.compile(r"^[a-z]{2,3}\s+")


def _clean_org_name(raw: str, suffix_re: Optional[re.Pattern] = None) -> str:
    """Remove prefix (e.g. 'hk ') and optional suffix (e.g. ' Province')."""
    s = (raw or "").strip()
    # Remove 2-3 letter lowercase prefix
    s = _PREFIX_RE.sub("", s)
    # Remove suffix
    if suffix_re:
        s = suffix_re.sub("", s)
    return s.strip()


def load_ou_map(path: Path) -> Dict[str, dict]:
    """
    Load ou_map.json → {uid: {province, zone_sante, aire_sante, fosa, province_raw}}
    """
    if not path.exists():
        print(f"ERROR: {path} not found")
        return {}

    raw = json.loads(path.read_text(encoding="utf-8"))

    if not isinstance(raw, dict):
        print(f"ERROR: ou_map.json is not a dict (got {type(raw).__name__})")
        return {}

    ou_map: Dict[str, dict] = {}

    for uid, info in raw.items():
        if not isinstance(info, dict):
            continue

        province_raw = info.get("Org2", "")
        zs_raw       = info.get("Org3", "")
        as_raw       = info.get("Org4", "")
        fosa_raw     = info.get("Org5", "")

        province = _clean_org_name(province_raw, _PROV_SUFFIX_RE)
        zone_sante = _clean_org_name(zs_raw, _ZS_SUFFIX_RE)
        aire_sante = _clean_org_name(as_raw, _AS_SUFFIX_RE)
        fosa = _clean_org_name(fosa_raw, _FOSA_SUFFIX_RE)

        # Keep the raw province key (with prefix) for antenne_rules matching
        ou_map[uid] = {
            "province": province,
            "zone_sante": zone_sante,
            "aire_sante": aire_sante,
            "fosa": fosa,
            "province_key": province_raw.strip(),  # e.g. "hk Haut Katanga Province"
        }

    print(f"  ou_map: {len(ou_map)} org units loaded")

    # Show sample
    samples = list(ou_map.items())[:3]
    for uid, info in samples:
        print(f"    {uid}: {info['province']} > {info['zone_sante']} > "
              f"{info['aire_sante']} > {info['fosa']}")

    return ou_map


# ============================================================
# ANTENNE RULES
# ============================================================

def load_antenne_rules(path: Path) -> Dict[str, Dict[str, str]]:
    """
    antenne_rules.json:
    { "hk Haut Katanga Province": { "Lubumbashi": "Lubumbashi", ... } }

    But keys in the file use the short format "hk Haut Katanga Province"
    while ou_map province_key also has this format.

    Returns: { province_key: { zs_name: antenne_name } }
    """
    if not path.exists():
        print(f"  antenne_rules.json not found (optional)")
        return {}

    data = json.loads(path.read_text(encoding="utf-8"))
    print(f"  antenne_rules: {len(data)} provinces loaded")
    return data


def resolve_antenne(
    antenne_rules: Dict[str, Dict[str, str]],
    province_key: str,
    zone_sante: str,
) -> str:
    """Look up the antenne for a ZS. Returns antenne name or ''."""
    # Try exact province_key match
    prov_rules = antenne_rules.get(province_key)

    if not prov_rules:
        # Try matching by the cleaned province name portion
        # province_key = "hk Haut Katanga Province"
        # antenne key  = "hk Haut Katanga Province"  → should match
        # Also try with just the prefix portion
        for ak, av in antenne_rules.items():
            # Compare lowercased
            if ak.lower() == province_key.lower():
                prov_rules = av
                break

    if not prov_rules:
        return ""

    # Look up ZS name
    antenne = prov_rules.get(zone_sante, "")
    if not antenne:
        # Try case-insensitive
        zs_lower = zone_sante.lower()
        for zs_name, ant_name in prov_rules.items():
            if zs_name.lower() == zs_lower:
                return ant_name

    return antenne


# ============================================================
# RENAME MAP (reverse: Zoho link → DHIS2 label)
# ============================================================

def load_rename_map(path: Path) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    rename_map.json: { "DHIS2 label": "zoho_link_name" }
    Returns:
      label_to_zoho: {"BCG fixe1": "BCG_fixe1"}
      zoho_to_label: {"BCG_fixe1": "BCG fixe1"}
    """
    if not path.exists():
        print(f"ERROR: {path} not found")
        return {}, {}

    data = json.loads(path.read_text(encoding="utf-8"))
    label_to_zoho = dict(data)
    zoho_to_label = {v: k for k, v in data.items()}
    print(f"  rename_map: {len(label_to_zoho)} entries")
    return label_to_zoho, zoho_to_label


# ============================================================
# VACCINE DEFINITIONS
# ============================================================

# Each vaccine group maps to DHIS2 labels.
# The fetch script converts these to Zoho link names in the NDJSON.
# We read from NDJSON using the Zoho link names, but define groups using labels.

VACCINE_GROUPS = {
    "BCG": {
        "dose_labels": [
            "BCG fixe1", "BCG fixe2", "BCG avancé1", "BCG avancé2",
            "BCG mobile1", "BCG mobile2",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "DTC1": {
        "dose_labels": [
            "Penta1 fixe1", "Penta1 fixe2", "Penta1 avancé1", "Penta1 avancé2",
            "Penta1 mobile1", "Penta1 mobile2",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "DTC2": {
        "dose_labels": [
            "Penta2 fixe1", "Penta2 fixe2", "Penta2 avancé1", "Penta2 avancé2",
            "Penta2 mobile1", "Penta2 mobile2",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "DTC3": {
        "dose_labels": [
            "Penta3 fixe1", "Penta3 fixe2", "Penta3 avancé1", "Penta3 avancé2",
            "Penta3 mobile1", "Penta3 mobile2",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "VPO0": {
        "dose_labels": [
            "VPO0 0-11 mois fixe1", "VPO0 0-11 mois fixe2",
            "VPO0 0-11 mois avancée1", "VPO0 0-11 mois avancée2",
            "VPO0 0-11 mois mobile1", "VPO0 0-11 mois mobile2",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "VPO1": {
        "dose_labels": [
            "VPO1 0-11 mois fixe1", "VPO1 0-11 mois fixe2",
            "VPO1 0-11 mois avancée1", "VPO1 0-11 mois avancée2",
            "VPO1 0-11 mois mobile1", "VPO1 0-11 mois mobile2",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "VPO2": {
        "dose_labels": [
            "VPO2 0-11 mois fixe1", "VPO2 0-11 mois fixe2",
            "VPO2 0-11 mois avancée1", "VPO2 0-11 mois avancée2",
            "VPO2 0-11 mois mobile1", "VPO2 0-11 mois mobile2",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "VPO3": {
        "dose_labels": [
            "VPO3 fixe1", "VPO3 fixe2", "VPO3 avancé1", "VPO3 avancé2",
            "VPO3 mobile1", "VPO3 mobile2",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "VPI1": {
        "dose_labels": [
            "VPI1 fixe1", "VPI1 fixe2", "VPI1 avancé1", "VPI1 avancé2",
            "VPI1 mobile1", "VPI1 mobile2",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "VPI2": {
        "dose_labels": [
            "VPI2 fixe1", "VPI2 fixe2", "VPI2 avancé1", "VPI2 avancé2",
            "VPI2 mobile1", "VPI2 mobile2",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "ROTA1": {
        "dose_labels": [
            "ROTA1 0-11 mois fixe", "ROTA1 0-11 mois avancée",
            "ROTA1 0-11 mois mobile",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "ROTA2": {
        "dose_labels": [
            "ROTA2 0-11 mois fixe", "ROTA2 0-11 mois avancée",
            "ROTA2 0-11 mois mobile",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "ROTA3": {
        "dose_labels": ["ROTA3 fixe", "ROTA3 avancé", "ROTA3 mobile"],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "PCV13_1": {
        "dose_labels": [
            "PCV13(1) 0-11 mois fixe1", "PCV13(1) 0-11 mois fixe2",
            "PCV13(1) 0-11 mois avancée1", "PCV13(1) 0-11 mois avancée2",
            "PCV13(1) 0-11 mois mobile1", "PCV13(1) 0-11 mois mobile2",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "PCV13_2": {
        "dose_labels": [
            "PCV13(2) 0-11 mois fixe1", "PCV13(2) 0-11 mois fixe2",
            "PCV13(2) 0-11 mois avancée1", "PCV13(2) 0-11 mois avancée2",
            "PCV13(2) 0-11 mois mobile1", "PCV13(2) 0-11 mois mobile2",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "PCV13_3": {
        "dose_labels": [
            "PCV13 fixe1", "PCV13 fixe2", "PCV13 avancé1", "PCV13 avancé2",
            "PCV13 mobile1", "PCV13 mobile2",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "VAR1": {
        "dose_labels": [
            "VAR1 fixe1", "VAR1 fixe2", "VAR1 avancé1", "VAR1 avancé2",
            "VAR1 mobile1", "VAR1 mobile2",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "VAR2": {
        "dose_labels": [
            "VAR2 fixe1", "VAR2 fixe2", "VAR2 avancé",
            "VAR2 mobile1", "VAR2 mobile2",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "VAA": {
        "dose_labels": [
            "VAA fixe1", "VAA fixe2", "VAA avancé1", "VAA avancé2",
            "VAA mobile1", "VAA mobile2",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "VAP1": {
        "dose_labels": [
            "VAP1 0-11 mois fixe", "VAP1 0-11 mois avancée",
            "VAP1 0-11 mois mobile",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "VAP2": {
        "dose_labels": [
            "VAP2 0-11 mois fixe", "VAP2 0-11 mois avancée",
            "VAP2 0-11 mois mobile",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "VAP3": {
        "dose_labels": [
            "VAP3 0-11 mois fixe", "VAP3 0-11 mois avancée",
            "VAP3 0-11 mois mobile",
        ],
        "denominator_label": "Pop. 0-11m (survivants)",
    },
    "VAP4": {
        "dose_labels": [
            "VAP4 12-23 mois fixe", "VAP4 12-23 mois avancée",
            "VAP4 12-23 mois mobile",
        ],
        "denominator_label": "Pop. 12-59m",
    },
    "Td2": {
        "dose_labels": ["Td 2"],
        "denominator_label": None,
    },
    "Td3": {
        "dose_labels": ["Td 3"],
        "denominator_label": None,
    },
    "Td4": {
        "dose_labels": ["Td 4"],
        "denominator_label": None,
    },
    "Td5": {
        "dose_labels": ["Td 5"],
        "denominator_label": None,
    },
}

# Extra indicators to sum (not vaccine doses)
EXTRA_INDICATORS = {
    "seances_prevues":   "Séances prévues",
    "seances_realisees": "Séances réalisées",
    "seances_fixes_prevues":    "Séances fixes prévues",
    "seances_fixes_realisees":  "Séances fixes réalisées",
    "seances_avancees_prevues":   "Séances avancées prévues",
    "seances_avancees_realisees": "Séances avancées réalisées",
    "seances_mobiles_prevues":    "Séances mobiles prévues",
    "seances_mobiles_realisees":  "Séances mobiles réalisées",
    "ecv": "ECV",
    "hpv": "HPV",
    "pop_totale": "Pop. totale",
    "naissances_vivantes": "Naissances vivantes",
    "pop_0_11m_nv": "Pop. 0-11m (nv)",
    "pop_0_11m_surv": "Pop. 0-11m (survivants)",
    "pop_0_59m": "Pop. 0-59m",
    "pop_12_59m": "Pop. 12-59m",
    "rapports_attendus": "Rapports attendus",
}

# Completeness & promptness (percentage fields → average, not sum)
REPORTING_FIELDS = {
    "completude": "Complétude",
    "promptitude": "Promptitude",
}


# ============================================================
# HELPERS
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


def get_field(rec: dict, label: str, label_to_zoho: Dict[str, str]) -> float:
    """Read a field from the record using either the Zoho name or the label."""
    zoho_name = label_to_zoho.get(label)
    if zoho_name and zoho_name in rec:
        return safe_num(rec[zoho_name])
    # Fallback: try the label directly
    if label in rec:
        return safe_num(rec[label])
    return 0.0


# ============================================================
# READ ALL NDJSON
# ============================================================

def read_all_ndjson(monthly_dir: Path) -> List[dict]:
    records: List[dict] = []

    if not monthly_dir.exists():
        print(f"ERROR: {monthly_dir} does not exist")
        return records

    month_dirs = sorted(d for d in monthly_dir.iterdir() if d.is_dir())
    print(f"  Found {len(month_dirs)} month folders")

    for md in month_dirs:
        count = 0

        # Prefer plain .ndjson
        ndjson_files = sorted(md.glob("*.ndjson"))
        # Exclude .ndjson.gz from this glob (pathlib *.ndjson won't match .ndjson.gz)

        for nf in ndjson_files:
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
                print(f"    WARN: {nf.name}: {e}")

        # Fallback to .gz if no plain records
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
                    print(f"    WARN: {gf.name}: {e}")

        if count:
            print(f"    {md.name}: {count:,} records")

    print(f"  TOTAL: {len(records):,} records")
    return records


# ============================================================
# AGGREGATION ENGINE
# ============================================================

@dataclass
class Bucket:
    """Accumulator for one (scope, period)."""
    # Vaccine doses: {vax_group: total}
    doses: Dict[str, float]
    # Denominators: {vax_group: total}
    denoms: Dict[str, float]
    # Extra indicators: {key: total}
    extras: Dict[str, float]
    # Reporting: {field: sum}
    reporting_sum: Dict[str, float]
    # Count of FOSA (for averaging reporting %)
    fosa_count: int

    @classmethod
    def empty(cls) -> "Bucket":
        return cls(
            doses=defaultdict(float),
            denoms=defaultdict(float),
            extras=defaultdict(float),
            reporting_sum=defaultdict(float),
            fosa_count=0,
        )

    def add_record(
        self,
        rec: dict,
        label_to_zoho: Dict[str, str],
    ) -> None:
        # Vaccine doses
        for vax_name, vax_def in VACCINE_GROUPS.items():
            total = 0.0
            for label in vax_def["dose_labels"]:
                total += get_field(rec, label, label_to_zoho)
            self.doses[vax_name] += total

            denom_label = vax_def.get("denominator_label")
            if denom_label:
                self.denoms[vax_name] += get_field(rec, denom_label, label_to_zoho)

        # Extra indicators
        for key, label in EXTRA_INDICATORS.items():
            self.extras[key] += get_field(rec, label, label_to_zoho)

        # Reporting (to average later)
        for key, label in REPORTING_FIELDS.items():
            self.reporting_sum[key] += get_field(rec, label, label_to_zoho)

        self.fosa_count += 1

    def to_dict(self) -> dict:
        result: dict = {}

        # Vaccines
        vaccines: dict = {}
        for vax_name in VACCINE_GROUPS:
            administered = self.doses.get(vax_name, 0)
            denominator = self.denoms.get(vax_name, 0)

            entry: dict = {"administered": round(administered)}
            if denominator > 0:
                entry["target"] = round(denominator)
                entry["coverage"] = round(administered / denominator * 100, 1)
            vaccines[vax_name] = entry

        result["vaccines"] = vaccines

        # Extra indicators
        extras_out: dict = {}
        for key in EXTRA_INDICATORS:
            val = self.extras.get(key, 0)
            extras_out[key] = round(val) if val == int(val) else round(val, 1)
        result["indicators"] = extras_out

        # Reporting
        n = max(1, self.fosa_count)
        result["reporting"] = {
            "completeness": round(self.reporting_sum.get("completude", 0) / n, 1),
            "promptness":   round(self.reporting_sum.get("promptitude", 0) / n, 1),
            "facilities":   self.fosa_count,
        }

        return result


# Use a simple class instead of dataclass to avoid import issues
class Bucket:
    """Accumulator for one (scope, period)."""
    __slots__ = ("doses", "denoms", "extras", "reporting_sum", "fosa_count")

    def __init__(self):
        self.doses: Dict[str, float] = defaultdict(float)
        self.denoms: Dict[str, float] = defaultdict(float)
        self.extras: Dict[str, float] = defaultdict(float)
        self.reporting_sum: Dict[str, float] = defaultdict(float)
        self.fosa_count: int = 0

    def add_record(self, rec: dict, label_to_zoho: Dict[str, str]) -> None:
        for vax_name, vax_def in VACCINE_GROUPS.items():
            total = 0.0
            for label in vax_def["dose_labels"]:
                total += get_field(rec, label, label_to_zoho)
            self.doses[vax_name] += total

            denom_label = vax_def.get("denominator_label")
            if denom_label:
                self.denoms[vax_name] += get_field(rec, denom_label, label_to_zoho)

        for key, label in EXTRA_INDICATORS.items():
            self.extras[key] += get_field(rec, label, label_to_zoho)

        for key, label in REPORTING_FIELDS.items():
            self.reporting_sum[key] += get_field(rec, label, label_to_zoho)

        self.fosa_count += 1

    def to_dict(self) -> dict:
        result: dict = {}

        vaccines: dict = {}
        for vax_name in VACCINE_GROUPS:
            administered = self.doses.get(vax_name, 0)
            denominator = self.denoms.get(vax_name, 0)
            entry: dict = {"administered": round(administered)}
            if denominator > 0:
                entry["target"] = round(denominator)
                entry["coverage"] = round(administered / denominator * 100, 1)
            vaccines[vax_name] = entry
        result["vaccines"] = vaccines

        extras_out: dict = {}
        for key in EXTRA_INDICATORS:
            val = self.extras.get(key, 0)
            extras_out[key] = round(val) if val == int(val) else round(val, 1)
        result["indicators"] = extras_out

        n = max(1, self.fosa_count)
        result["reporting"] = {
            "completeness": round(self.reporting_sum.get("completude", 0) / n, 1),
            "promptness":   round(self.reporting_sum.get("promptitude", 0) / n, 1),
            "facilities":   self.fosa_count,
        }

        return result


def aggregate(
    records: List[dict],
    ou_map: Dict[str, dict],
    label_to_zoho: Dict[str, str],
    antenne_rules: Dict[str, Dict[str, str]],
) -> dict:
    """Main aggregation. Returns the full dashboard structure."""

    # Accumulators: { (scope_type, scope_key, period): Bucket }
    # scope_type: "national", "province", "antenne", "zs", "as"
    buckets: Dict[Tuple[str, str, str], Bucket] = {}

    def get_bucket(scope_type: str, scope_key: str, period: str) -> Bucket:
        k = (scope_type, scope_key, period)
        if k not in buckets:
            buckets[k] = Bucket()
        return buckets[k]

    unresolved = set()
    period_fail = 0

    for rec in records:
        uid = rec.get("OrgUnit", "")
        period = parse_period(rec.get("Period", ""))

        if not period:
            period_fail += 1
            continue

        info = ou_map.get(uid)
        if not info:
            unresolved.add(uid)
            continue

        province    = info["province"]
        zone_sante  = info["zone_sante"]
        aire_sante  = info["aire_sante"]
        province_key = info["province_key"]

        if not province:
            unresolved.add(uid)
            continue

        # Antenne
        antenne = resolve_antenne(antenne_rules, province_key, zone_sante)

        # National
        get_bucket("national", "national", period).add_record(rec, label_to_zoho)

        # Province
        get_bucket("province", province, period).add_record(rec, label_to_zoho)

        # Antenne (if resolved)
        if antenne:
            ant_key = f"{province}|{antenne}"
            get_bucket("antenne", ant_key, period).add_record(rec, label_to_zoho)

        # Zone de Santé
        if zone_sante:
            zs_key = f"{province}|{zone_sante}"
            get_bucket("zs", zs_key, period).add_record(rec, label_to_zoho)

        # Aire de Santé
        if aire_sante:
            as_key = f"{province}|{zone_sante}|{aire_sante}"
            get_bucket("as", as_key, period).add_record(rec, label_to_zoho)

    if unresolved:
        print(f"  WARN: {len(unresolved)} UIDs not found in ou_map")
    if period_fail:
        print(f"  WARN: {period_fail} records with unparseable period")

    # ---- Build output ----
    all_periods: Set[str] = set()
    province_names: Set[str] = set()

    for (scope_type, scope_key, period) in buckets:
        all_periods.add(period)
        if scope_type == "province":
            province_names.add(scope_key)

    sorted_periods = sorted(all_periods)
    sorted_provinces = sorted(province_names)

    # National
    national_periods: dict = {}
    for pe in sorted_periods:
        b = buckets.get(("national", "national", pe))
        if b:
            national_periods[pe] = b.to_dict()

    # Provinces (with nested antennes, ZS, AS)
    provinces_out: dict = {}

    for prov in sorted_provinces:
        prov_periods: dict = {}
        for pe in sorted_periods:
            b = buckets.get(("province", prov, pe))
            if b:
                prov_periods[pe] = b.to_dict()

        # Find antennes for this province
        antennes_out: dict = {}
        antenne_names: Set[str] = set()
        for (st, sk, pe) in buckets:
            if st == "antenne" and sk.startswith(f"{prov}|"):
                ant_name = sk.split("|", 1)[1]
                antenne_names.add(ant_name)

        for ant_name in sorted(antenne_names):
            ant_key = f"{prov}|{ant_name}"
            ant_periods: dict = {}
            for pe in sorted_periods:
                b = buckets.get(("antenne", ant_key, pe))
                if b:
                    ant_periods[pe] = b.to_dict()
            if ant_periods:
                antennes_out[ant_name] = {"periods": ant_periods}

        # Find ZS for this province
        zs_out: dict = {}
        zs_names: Set[str] = set()
        for (st, sk, pe) in buckets:
            if st == "zs" and sk.startswith(f"{prov}|"):
                zs_name = sk.split("|", 1)[1]
                zs_names.add(zs_name)

        for zs_name in sorted(zs_names):
            zs_key = f"{prov}|{zs_name}"
            zs_periods: dict = {}
            for pe in sorted_periods:
                b = buckets.get(("zs", zs_key, pe))
                if b:
                    zs_periods[pe] = b.to_dict()

            # Find AS for this ZS
            as_out: dict = {}
            as_names: Set[str] = set()
            for (st, sk, pe) in buckets:
                if st == "as" and sk.startswith(f"{prov}|{zs_name}|"):
                    as_name = sk.split("|", 2)[2]
                    as_names.add(as_name)

            for as_name in sorted(as_names):
                as_key = f"{prov}|{zs_name}|{as_name}"
                as_periods: dict = {}
                for pe in sorted_periods:
                    b = buckets.get(("as", as_key, pe))
                    if b:
                        as_periods[pe] = b.to_dict()
                if as_periods:
                    as_out[as_name] = {"periods": as_periods}

            zs_entry: dict = {"periods": zs_periods}
            if as_out:
                zs_entry["aires_sante"] = as_out
            zs_out[zs_name] = zs_entry

        prov_entry: dict = {"periods": prov_periods}
        if antennes_out:
            prov_entry["antennes"] = antennes_out
        if zs_out:
            prov_entry["zones_sante"] = zs_out
        provinces_out[prov] = prov_entry

    result = {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "periods": sorted_periods,
            "periods_display": {pe: period_display(pe) for pe in sorted_periods},
            "provinces": sorted_provinces,
            "vaccine_groups": list(VACCINE_GROUPS.keys()),
            "total_records": len(records),
            "total_fosa_resolved": len(records) - len(unresolved) - period_fail,
        },
        "national": {"periods": national_periods},
        "provinces": provinces_out,
    }

    return result


# ============================================================
# OUTPUT: split by province if too large
# ============================================================

def write_output(data: dict, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # Clean old files
    for old in out_dir.glob("*.json"):
        old.unlink()

    full_json = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    full_size = len(full_json.encode("utf-8"))

    if full_size < MAX_JSON_FILE_BYTES:
        # Single file
        p = out_dir / "dashboard.json"
        p.write_text(full_json, encoding="utf-8")
        print(f"  Written: dashboard.json ({full_size:,} bytes)")

        idx = {
            "files": ["dashboard.json"],
            "split": False,
            "metadata": data["metadata"],
        }
        (out_dir / "index.json").write_text(
            json.dumps(idx, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        return

    # Split mode
    print(f"  Full data = {full_size:,} bytes → splitting by province")
    files: List[str] = []

    # National
    nat = {
        "metadata": data["metadata"],
        "national": data["national"],
    }
    nat_json = json.dumps(nat, ensure_ascii=False, separators=(",", ":"))
    (out_dir / "national.json").write_text(nat_json, encoding="utf-8")
    files.append("national.json")
    print(f"    national.json: {len(nat_json):,} bytes")

    # Per province
    for prov_name, prov_data in sorted(data.get("provinces", {}).items()):
        safe = re.sub(r"[^a-z0-9]+", "_", prov_name.lower()).strip("_")
        fname = f"prov_{safe}.json"

        pj = json.dumps(
            {"province_name": prov_name, **prov_data},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        (out_dir / fname).write_text(pj, encoding="utf-8")
        files.append(fname)
        sz = len(pj.encode("utf-8"))
        print(f"    {fname}: {sz:,} bytes")
        if sz > MAX_JSON_FILE_BYTES:
            print(f"    ⚠️  {fname} exceeds {MAX_JSON_FILE_BYTES:,} bytes!")

    idx = {
        "files": files,
        "split": True,
        "metadata": data["metadata"],
    }
    (out_dir / "index.json").write_text(
        json.dumps(idx, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    print(f"  Total: {len(files)} province files + index.json")


# ============================================================
# VALIDATION
# ============================================================

def validate(out_dir: Path) -> bool:
    ok = True
    for jf in sorted(out_dir.glob("*.json")):
        try:
            txt = jf.read_text(encoding="utf-8")
            if not txt.strip():
                print(f"  ❌ {jf.name}: EMPTY")
                ok = False
                continue
            first = txt.strip()[0]
            if first not in ("{", "["):
                print(f"  ❌ {jf.name}: starts with '{first}' — not JSON")
                ok = False
                continue
            json.loads(txt)
            print(f"  ✅ {jf.name}: {jf.stat().st_size:,} bytes OK")
        except json.JSONDecodeError as e:
            print(f"  ❌ {jf.name}: INVALID JSON — {e}")
            ok = False
        except Exception as e:
            print(f"  ❌ {jf.name}: {e}")
            ok = False
    return ok


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    print("=" * 60)
    print("AGGREGATE DASHBOARD  v3.0")
    print("=" * 60)

    print(f"\n[1] Checking paths...")
    for label, p in [
        ("DATA_DIR",     DATA_DIR),
        ("MONTHLY_DIR",  MONTHLY_DIR),
        ("OU_MAP",       OU_MAP_PATH),
        ("RENAME_MAP",   RENAME_MAP_PATH),
        ("ANTENNE_RULES", ANTENNE_RULES_PATH),
    ]:
        exists = "✅" if p.exists() else "❌"
        print(f"  {exists} {label}: {p}")

    # Load rename map
    print(f"\n[2] Loading rename_map.json...")
    label_to_zoho, zoho_to_label = load_rename_map(RENAME_MAP_PATH)
    if not label_to_zoho:
        print("FATAL: rename_map.json missing or empty")
        return 1

    # Verify vaccine label → zoho mappings
    missing_labels: List[str] = []
    for vax_name, vax_def in VACCINE_GROUPS.items():
        for label in vax_def["dose_labels"]:
            if label not in label_to_zoho:
                missing_labels.append(f"{vax_name}: {label}")
    if missing_labels:
        print(f"  WARN: {len(missing_labels)} vaccine labels not in rename_map:")
        for ml in missing_labels[:10]:
            print(f"    - {ml}")

    # Load ou_map
    print(f"\n[3] Loading ou_map.json...")
    ou_map = load_ou_map(OU_MAP_PATH)
    if not ou_map:
        print("FATAL: ou_map.json missing or empty")
        return 1

    # Load antenne rules
    print(f"\n[4] Loading antenne_rules.json...")
    antenne_rules = load_antenne_rules(ANTENNE_RULES_PATH)

    # Read NDJSON
    print(f"\n[5] Reading NDJSON data...")
    records = read_all_ndjson(MONTHLY_DIR)

    if not records:
        print("\nWARN: No records found — writing empty (but valid) dashboard")
        DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
        empty = {
            "metadata": {
                "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "error": "no_data_found",
                "periods": [],
                "provinces": [],
            },
            "national": {"periods": {}},
            "provinces": {},
        }
        (DASHBOARD_DIR / "dashboard.json").write_text(
            json.dumps(empty, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        (DASHBOARD_DIR / "index.json").write_text(
            json.dumps({"files": ["dashboard.json"], "split": False, "metadata": empty["metadata"]}),
            encoding="utf-8",
        )
        return 0

    # Show sample
    print(f"\n  Sample record fields: {list(records[0].keys())[:12]}...")

    # Aggregate
    print(f"\n[6] Aggregating...")
    dashboard = aggregate(records, ou_map, label_to_zoho, antenne_rules)

    meta = dashboard.get("metadata", {})
    print(f"  Periods: {meta.get('periods', [])}")
    print(f"  Provinces: {len(meta.get('provinces', []))}")
    print(f"  Records resolved: {meta.get('total_fosa_resolved', '?')}")

    # Write
    print(f"\n[7] Writing output to {DASHBOARD_DIR}...")
    write_output(dashboard, DASHBOARD_DIR)

    # Validate
    print(f"\n[8] Validating output...")
    ok = validate(DASHBOARD_DIR)

    if ok:
        print(f"\n{'='*60}")
        print(f"✅ SUCCESS — Dashboard data ready")
        print(f"{'='*60}")
        return 0
    else:
        print(f"\n{'='*60}")
        print(f"❌ ERRORS in output files")
        print(f"{'='*60}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
