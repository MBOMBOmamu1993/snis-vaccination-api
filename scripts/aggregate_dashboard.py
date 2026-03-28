from __future__ import annotations

import json
import gzip
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ============================================================
# CONFIG
# ============================================================

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "docs" / "data"
MONTHLY_DIR = DATA_DIR / "monthly"
DASHBOARD_DIR = DATA_DIR / "dashboard"
OU_MAP_PATH = DATA_DIR / "ou_map.json"
INDEX_PATH = DATA_DIR / "index.json"
ANTENNE_RULES_PATH = DATA_DIR / "antenne_rules.json"
RENAME_MAP_PATH = REPO_ROOT / "docs" / "config" / "rename_map.json"

# Max JSON file size (GitHub Pages limit ~100MB, we stay under 50MB per file)
MAX_FILE_BYTES = 50_000_000

# Month abbreviations for parsing "01-Jan-2025" format
MONTH_ABBR = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}

# ============================================================
# VACCINE DEFINITIONS
# Maps Zoho field link names back to vaccine groups.
# We need rename_map.json to know what fields exist.
# ============================================================

# These are the DHIS2 labels -> vaccine group mappings
# The Zoho link names are resolved at runtime from rename_map.json
VACCINE_GROUPS = {
    "BCG": {
        "labels": [
            "BCG fixe1", "BCG fixe2", "BCG avancé1", "BCG avancé2",
            "BCG mobile1", "BCG mobile2"
        ],
        "denominator": "Pop. 0-11m (survivants)",
    },
    "DTC1": {
        "labels": [
            "Penta1 fixe1", "Penta1 fixe2", "Penta1 avancé1", "Penta1 avancé2",
            "Penta1 mobile1", "Penta1 mobile2"
        ],
        "denominator": "Pop. 0-11m (survivants)",
    },
    "DTC3": {
        "labels": [
            "Penta3 fixe1", "Penta3 fixe2", "Penta3 avancé1", "Penta3 avancé2",
            "Penta3 mobile1", "Penta3 mobile2"
        ],
        "denominator": "Pop. 0-11m (survivants)",
    },
    "VPO3": {
        "labels": [
            "VPO3 fixe1", "VPO3 fixe2", "VPO3 avancé1", "VPO3 avancé2",
            "VPO3 mobile1", "VPO3 mobile2"
        ],
        "denominator": "Pop. 0-11m (survivants)",
    },
    "VPI1": {
        "labels": [
            "VPI1 fixe1", "VPI1 fixe2", "VPI1 avancé1", "VPI1 avancé2",
            "VPI1 mobile1", "VPI1 mobile2"
        ],
        "denominator": "Pop. 0-11m (survivants)",
    },
    "ROTA3": {
        "labels": ["ROTA3 fixe", "ROTA3 avancé", "ROTA3 mobile"],
        "denominator": "Pop. 0-11m (survivants)",
    },
    "PCV13": {
        "labels": [
            "PCV13 fixe1", "PCV13 fixe2", "PCV13 avancé1", "PCV13 avancé2",
            "PCV13 mobile1", "PCV13 mobile2"
        ],
        "denominator": "Pop. 0-11m (survivants)",
    },
    "VAR1": {
        "labels": [
            "VAR1 fixe1", "VAR1 fixe2", "VAR1 avancé1", "VAR1 avancé2",
            "VAR1 mobile1", "VAR1 mobile2"
        ],
        "denominator": "Pop. 0-11m (survivants)",
    },
    "VAR2": {
        "labels": [
            "VAR2 fixe1", "VAR2 fixe2", "VAR2 avancé", "VAR2 mobile1", "VAR2 mobile2"
        ],
        "denominator": "Pop. 0-11m (survivants)",
    },
    "VAA": {
        "labels": [
            "VAA fixe1", "VAA fixe2", "VAA avancé1", "VAA avancé2",
            "VAA mobile1", "VAA mobile2"
        ],
        "denominator": "Pop. 0-11m (survivants)",
    },
    "VAP": {
        "labels": [
            "VAP1 0-11 mois fixe", "VAP1 0-11 mois avancée", "VAP1 0-11 mois mobile",
            "VAP2 0-11 mois fixe", "VAP2 0-11 mois avancée", "VAP2 0-11 mois mobile",
            "VAP3 0-11 mois fixe", "VAP3 0-11 mois avancée", "VAP3 0-11 mois mobile",
        ],
        "denominator": "Pop. 0-11m (survivants)",
    },
    "Td": {
        "labels": ["Td 2", "Td 3", "Td 4", "Td 5"],
        "denominator": None,
    },
}


# ============================================================
# HELPERS
# ============================================================

def parse_period(period_str: str) -> Optional[str]:
    """
    Convert various period formats to YYYYMM.
    Handles:
      - "01-Jan-2025" (Zoho format from fetch script)
      - "202501" (already YYYYMM)
      - "2025-01" or "2025-01-01" (ISO)
      - "January 2025"
    """
    if not period_str or not isinstance(period_str, str):
        return None

    s = period_str.strip()

    # Already YYYYMM
    if len(s) == 6 and s.isdigit():
        return s

    # "01-Jan-2025" format
    if len(s) >= 9 and s[2] == "-" and s[6] == "-":
        month_abbr = s[3:6].lower()
        year = s[7:11]
        mm = MONTH_ABBR.get(month_abbr)
        if mm and year.isdigit():
            return f"{year}{mm}"

    # ISO "2025-01-01" or "2025-01"
    if len(s) >= 7 and s[4] == "-":
        year = s[0:4]
        month = s[5:7]
        if year.isdigit() and month.isdigit():
            return f"{year}{month}"

    # "January 2025"
    full_months = {
        "january": "01", "february": "02", "march": "03", "april": "04",
        "may": "05", "june": "06", "july": "07", "august": "08",
        "september": "09", "october": "10", "november": "11", "december": "12",
    }
    parts = s.lower().split()
    if len(parts) == 2 and parts[0] in full_months and parts[1].isdigit():
        return f"{parts[1]}{full_months[parts[0]]}"

    print(f"  WARN: cannot parse period '{period_str}'")
    return None


def safe_float(v: Any) -> float:
    """Convert value to float, returning 0.0 for non-numeric."""
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


def normalize_name(name: str) -> str:
    """Normalize org unit name for matching."""
    if not name:
        return ""
    import unicodedata
    s = unicodedata.normalize("NFC", name.strip())
    return s


# ============================================================
# LOAD OU MAP
# ============================================================

def load_ou_map(path: Path) -> Dict[str, dict]:
    """
    Load ou_map.json which maps UID -> org unit info.
    Expected format:
    {
      "uid1": {
        "name": "CS Abc",
        "level": 5,
        "parent": "uid_parent",
        "ancestors": {
            "province": "Province X",
            "zone_sante": "ZS Y",
            "aire_sante": "AS Z"
        }
      },
      ...
    }
    or list format:
    [
      {"id": "uid1", "name": "CS Abc", "level": 5, "parent": "...", ...}
    ]
    """
    if not path.exists():
        print(f"ERROR: ou_map.json not found at {path}")
        return {}

    data = json.loads(path.read_text(encoding="utf-8"))

    ou_map = {}

    if isinstance(data, dict):
        # Could be {"organisationUnits": [...]} or direct {uid: info}
        if "organisationUnits" in data:
            for ou in data["organisationUnits"]:
                uid = ou.get("id") or ou.get("uid")
                if uid:
                    ou_map[uid] = ou
        else:
            # Assume {uid: info} format
            ou_map = data

    elif isinstance(data, list):
        for ou in data:
            uid = ou.get("id") or ou.get("uid")
            if uid:
                ou_map[uid] = ou

    print(f"  Loaded {len(ou_map)} org units from ou_map.json")
    return ou_map


def get_hierarchy(ou_map: Dict[str, dict], uid: str) -> Dict[str, str]:
    """
    Get the hierarchy for an org unit.
    Returns: {"province": "...", "zone_sante": "...", "aire_sante": "...", "fosa": "..."}
    """
    info = ou_map.get(uid, {})

    # Try structured ancestors
    ancestors = info.get("ancestors", {})
    if ancestors:
        return {
            "province": ancestors.get("province", ""),
            "zone_sante": ancestors.get("zone_sante", ancestors.get("zs", "")),
            "aire_sante": ancestors.get("aire_sante", ancestors.get("as", "")),
            "fosa": info.get("name", ""),
            "uid": uid,
        }

    # Try path-based resolution
    path = info.get("path", "")
    name = info.get("name", "")
    level = info.get("level", 0)

    # Walk up the parent chain
    province = zone_sante = aire_sante = fosa = ""

    if level == 5:
        fosa = name
    elif level == 4:
        aire_sante = name
    elif level == 3:
        zone_sante = name
    elif level == 2:
        province = name

    # Resolve ancestors via parent chain
    current_uid = uid
    visited = set()
    while current_uid and current_uid not in visited:
        visited.add(current_uid)
        current_info = ou_map.get(current_uid, {})
        current_level = current_info.get("level", 0)
        current_name = current_info.get("name", "")

        if current_level == 2:
            province = current_name
        elif current_level == 3:
            zone_sante = current_name
        elif current_level == 4:
            aire_sante = current_name

        current_uid = current_info.get("parent", {})
        if isinstance(current_uid, dict):
            current_uid = current_uid.get("id", "")

    return {
        "province": province,
        "zone_sante": zone_sante,
        "aire_sante": aire_sante,
        "fosa": fosa,
        "uid": uid,
    }


# ============================================================
# LOAD RENAME MAP (reverse: Zoho link name -> DHIS2 label)
# ============================================================

def load_rename_maps(path: Path) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    rename_map.json: {"DHIS2 label": "zoho_link_name", ...}
    Returns:
      label_to_zoho: {"BCG fixe1": "BCG_fixe_1", ...}
      zoho_to_label: {"BCG_fixe_1": "BCG fixe1", ...}
    """
    if not path.exists():
        print(f"WARN: rename_map.json not found at {path}")
        return {}, {}

    data = json.loads(path.read_text(encoding="utf-8"))
    label_to_zoho = data  # {"DHIS2 label": "zoho_link_name"}
    zoho_to_label = {v: k for k, v in data.items()}

    print(f"  Loaded {len(label_to_zoho)} rename mappings")
    return label_to_zoho, zoho_to_label


# ============================================================
# READ ALL NDJSON DATA
# ============================================================

def read_all_ndjson(monthly_dir: Path) -> List[dict]:
    """Read all NDJSON files from monthly subfolders."""
    all_records = []

    if not monthly_dir.exists():
        print(f"ERROR: monthly directory not found: {monthly_dir}")
        return all_records

    month_dirs = sorted([d for d in monthly_dir.iterdir() if d.is_dir()])
    print(f"  Found {len(month_dirs)} month directories")

    for month_dir in month_dirs:
        month_records = 0

        # Read .ndjson files (plain text)
        for ndjson_file in sorted(month_dir.glob("*.ndjson")):
            if ndjson_file.suffix == ".gz":
                continue
            try:
                with open(ndjson_file, "r", encoding="utf-8") as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                            all_records.append(rec)
                            month_records += 1
                        except json.JSONDecodeError as e:
                            if line_num <= 3:
                                print(f"  WARN: bad JSON line {line_num} in {ndjson_file.name}: {e}")
            except Exception as e:
                print(f"  WARN: error reading {ndjson_file}: {e}")

        # If no plain files found, try .ndjson.gz
        if month_records == 0:
            for gz_file in sorted(month_dir.glob("*.ndjson.gz")):
                try:
                    with gzip.open(gz_file, "rt", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                rec = json.loads(line)
                                all_records.append(rec)
                                month_records += 1
                            except json.JSONDecodeError:
                                pass
                except Exception as e:
                    print(f"  WARN: error reading {gz_file}: {e}")

        if month_records > 0:
            print(f"    {month_dir.name}: {month_records} records")

    print(f"  Total: {len(all_records)} records from all months")
    return all_records


# ============================================================
# AGGREGATE
# ============================================================

def aggregate_data(
    records: List[dict],
    ou_map: Dict[str, dict],
    zoho_to_label: Dict[str, str],
    label_to_zoho: Dict[str, str],
) -> Dict[str, Any]:
    """
    Aggregate records into dashboard structure.

    Returns nested dict:
    {
      "national": { "periods": { "202501": { "BCG": {"administered": N, ...}, ... } } },
      "provinces": {
        "Province X": {
          "periods": { "202501": { ... } },
          "zones_sante": {
            "ZS Y": { "periods": { ... }, "aires_sante": { ... } }
          }
        }
      },
      "metadata": { ... }
    }
    """

    # Build vaccine field mapping: vaccine_group -> list of zoho field names
    vaccine_fields = {}
    denom_fields = {}

    for vax_name, vax_def in VACCINE_GROUPS.items():
        fields = []
        for label in vax_def["labels"]:
            zoho_name = label_to_zoho.get(label, label)
            fields.append(zoho_name)
        vaccine_fields[vax_name] = fields

        if vax_def.get("denominator"):
            denom_fields[vax_name] = label_to_zoho.get(
                vax_def["denominator"], vax_def["denominator"]
            )

    # Completeness/promptitude field names
    completude_field = label_to_zoho.get("Complétude", "Complétude")
    promptitude_field = label_to_zoho.get("Promptitude", "Promptitude")
    rapports_attendus_field = label_to_zoho.get("Rapports attendus", "Rapports attendus")

    # Aggregation accumulators
    # Key: (level_path, period) -> {vaccine: total, denom: total, ...}
    # level_path examples: ("national",), ("Province X",), ("Province X", "ZS Y"), etc.

    accum = defaultdict(lambda: defaultdict(float))
    counts = defaultdict(lambda: defaultdict(int))  # for averaging completeness

    unresolved_uids = set()
    parsed_ok = 0
    parsed_fail = 0

    for rec in records:
        ou_uid = rec.get("OrgUnit", "")
        period_raw = rec.get("Period", "")

        period = parse_period(period_raw)
        if not period:
            parsed_fail += 1
            continue
        parsed_ok += 1

        # Resolve hierarchy
        hier = get_hierarchy(ou_map, ou_uid)
        province = hier["province"]
        zs = hier["zone_sante"]
        as_ = hier["aire_sante"]
        fosa = hier["fosa"]

        if not province and not zs:
            if ou_uid not in unresolved_uids:
                unresolved_uids.add(ou_uid)
            continue

        # Define aggregation keys
        keys = []
        keys.append(("national", period))
        if province:
            keys.append((f"prov:{province}", period))
            if zs:
                keys.append((f"prov:{province}|zs:{zs}", period))
                if as_:
                    keys.append((f"prov:{province}|zs:{zs}|as:{as_}", period))

        for key in keys:
            bucket = accum[key]

            # Accumulate vaccine doses
            for vax_name, fields in vaccine_fields.items():
                total = 0.0
                for field_name in fields:
                    total += safe_float(rec.get(field_name, 0))
                bucket[f"{vax_name}_administered"] += total

            # Accumulate denominators
            for vax_name, denom_field in denom_fields.items():
                bucket[f"{vax_name}_denominator"] += safe_float(rec.get(denom_field, 0))

            # Completeness/promptitude
            bucket["completude_sum"] += safe_float(rec.get(completude_field, 0))
            bucket["promptitude_sum"] += safe_float(rec.get(promptitude_field, 0))
            bucket["rapports_attendus"] += safe_float(rec.get(rapports_attendus_field, 0))
            counts[key]["facilities"] += 1

    if unresolved_uids:
        print(f"  WARN: {len(unresolved_uids)} org unit UIDs not found in ou_map")
        if len(unresolved_uids) <= 10:
            for uid in list(unresolved_uids)[:10]:
                print(f"    - {uid}")

    print(f"  Records parsed OK: {parsed_ok}, failed: {parsed_fail}")

    # ============================================================
    # Build output structure
    # ============================================================

    def build_period_data(key_prefix: str) -> Dict[str, dict]:
        """Build period data for a given key prefix."""
        period_data = {}

        for (key, period), bucket in accum.items():
            if key != key_prefix:
                continue

            n_fac = max(1, counts[(key, period)]["facilities"])

            pd = {}
            for vax_name in VACCINE_GROUPS:
                administered = bucket.get(f"{vax_name}_administered", 0)
                denominator = bucket.get(f"{vax_name}_denominator", 0)

                entry = {
                    "administered": round(administered),
                }
                if denominator > 0:
                    entry["denominator"] = round(denominator)
                    entry["coverage_pct"] = round(administered / denominator * 100, 1)
                pd[vax_name] = entry

            # Completeness
            comp_sum = bucket.get("completude_sum", 0)
            prompt_sum = bucket.get("promptitude_sum", 0)
            rapp_att = bucket.get("rapports_attendus", 0)

            pd["reporting"] = {
                "completeness_avg": round(comp_sum / n_fac, 1) if n_fac else 0,
                "promptness_avg": round(prompt_sum / n_fac, 1) if n_fac else 0,
                "expected_reports": round(rapp_att),
                "facilities_count": n_fac,
            }

            period_data[period] = pd

        return dict(sorted(period_data.items()))

    # National level
    national = {"periods": build_period_data("national")}

    # Province level
    provinces = {}
    province_names = set()

    for key, _ in accum.keys():
        if isinstance(key, str) and key.startswith("prov:") and "|" not in key:
            prov_name = key[5:]
            province_names.add(prov_name)

    for prov_name in sorted(province_names):
        prov_key = f"prov:{prov_name}"
        prov_data = {
            "periods": build_period_data(prov_key),
            "zones_sante": {},
        }

        # Find ZS under this province
        zs_names = set()
        for key, _ in accum.keys():
            if isinstance(key, str) and key.startswith(f"{prov_key}|zs:"):
                parts = key.split("|")
                if len(parts) >= 2:
                    zs_name = parts[1][3:]  # remove "zs:"
                    zs_names.add(zs_name)

        for zs_name in sorted(zs_names):
            zs_key = f"{prov_key}|zs:{zs_name}"
            zs_data = {
                "periods": build_period_data(zs_key),
                "aires_sante": {},
            }

            # Find AS under this ZS
            as_names = set()
            for key, _ in accum.keys():
                if isinstance(key, str) and key.startswith(f"{zs_key}|as:"):
                    parts = key.split("|")
                    if len(parts) >= 3:
                        as_name = parts[2][3:]  # remove "as:"
                        as_names.add(as_name)

            for as_name in sorted(as_names):
                as_key = f"{zs_key}|as:{as_name}"
                zs_data["aires_sante"][as_name] = {
                    "periods": build_period_data(as_key),
                }

            prov_data["zones_sante"][zs_name] = zs_data

        provinces[prov_name] = prov_data

    # Available periods
    all_periods = set()
    for (key, period) in accum.keys():
        all_periods.add(period)

    result = {
        "metadata": {
            "generated_at": __import__("datetime").datetime.utcnow().isoformat() + "Z",
            "periods": sorted(all_periods),
            "provinces": sorted(province_names),
            "vaccine_groups": list(VACCINE_GROUPS.keys()),
        },
        "national": national,
        "provinces": provinces,
    }

    return result


# ============================================================
# WRITE OUTPUT (split by province if needed)
# ============================================================

def write_dashboard_json(data: Dict[str, Any], output_dir: Path) -> None:
    """Write dashboard JSON, splitting by province if too large."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Clean old files
    for old_file in output_dir.glob("*.json"):
        old_file.unlink()

    # Try writing as single file first
    full_json = json.dumps(data, ensure_ascii=False, separators=(",", ":"))

    if len(full_json.encode("utf-8")) < MAX_FILE_BYTES:
        out_path = output_dir / "dashboard.json"
        out_path.write_text(full_json, encoding="utf-8")
        print(f"  Written: {out_path} ({len(full_json):,} bytes)")

        # Write index
        index = {
            "files": ["dashboard.json"],
            "split_by_province": False,
            "metadata": data["metadata"],
        }
        (output_dir / "index.json").write_text(
            json.dumps(index, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        return

    # Split: national + one file per province
    print("  Data too large for single file, splitting by province...")

    files = []

    # National file
    national_data = {
        "metadata": data["metadata"],
        "national": data["national"],
    }
    nat_json = json.dumps(national_data, ensure_ascii=False, separators=(",", ":"))
    nat_path = output_dir / "national.json"
    nat_path.write_text(nat_json, encoding="utf-8")
    files.append("national.json")
    print(f"    {nat_path.name}: {len(nat_json):,} bytes")

    # Per-province files
    for prov_name, prov_data in sorted(data.get("provinces", {}).items()):
        # Create safe filename
        safe_name = (
            prov_name.lower()
            .replace(" ", "_")
            .replace("'", "")
            .replace("/", "_")
            .replace("é", "e")
            .replace("è", "e")
            .replace("ê", "e")
            .replace("à", "a")
            .replace("â", "a")
            .replace("ô", "o")
            .replace("î", "i")
            .replace("ù", "u")
            .replace("ç", "c")
        )
        # Remove any remaining non-ASCII
        safe_name = "".join(c for c in safe_name if c.isalnum() or c == "_")
        filename = f"province_{safe_name}.json"

        prov_json = json.dumps(
            {"province_name": prov_name, **prov_data},
            ensure_ascii=False,
            separators=(",", ":"),
        )

        prov_path = output_dir / filename
        prov_path.write_text(prov_json, encoding="utf-8")
        files.append(filename)
        print(f"    {filename}: {len(prov_json):,} bytes")

        # Safety check
        if len(prov_json.encode("utf-8")) > MAX_FILE_BYTES:
            print(f"    WARN: {filename} exceeds {MAX_FILE_BYTES:,} bytes!")

    # Write index
    index = {
        "files": files,
        "split_by_province": True,
        "metadata": data["metadata"],
    }
    index_path = output_dir / "index.json"
    index_path.write_text(
        json.dumps(index, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    files.append("index.json")
    print(f"    index.json written")
    print(f"  Total: {len(files)} files written to {output_dir}")


# ============================================================
# VALIDATION
# ============================================================

def validate_output(output_dir: Path) -> bool:
    """Validate all generated JSON files."""
    ok = True
    for json_file in output_dir.glob("*.json"):
        try:
            content = json_file.read_text(encoding="utf-8")
            if not content.strip():
                print(f"  ❌ {json_file.name}: EMPTY FILE")
                ok = False
                continue

            # Verify it's valid JSON
            parsed = json.loads(content)

            # Verify first character
            first_char = content.strip()[0]
            if first_char not in ("{", "["):
                print(f"  ❌ {json_file.name}: starts with '{first_char}' (not JSON)")
                ok = False
                continue

            size = json_file.stat().st_size
            print(f"  ✅ {json_file.name}: {size:,} bytes, valid JSON")

        except json.JSONDecodeError as e:
            print(f"  ❌ {json_file.name}: INVALID JSON - {e}")
            ok = False
        except Exception as e:
            print(f"  ❌ {json_file.name}: ERROR - {e}")
            ok = False

    return ok


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    print("=" * 60)
    print("AGGREGATE DASHBOARD DATA")
    print("=" * 60)

    # Verify paths
    print(f"\nPaths:")
    print(f"  REPO_ROOT:    {REPO_ROOT}")
    print(f"  DATA_DIR:     {DATA_DIR}")
    print(f"  MONTHLY_DIR:  {MONTHLY_DIR} (exists: {MONTHLY_DIR.exists()})")
    print(f"  OU_MAP:       {OU_MAP_PATH} (exists: {OU_MAP_PATH.exists()})")
    print(f"  RENAME_MAP:   {RENAME_MAP_PATH} (exists: {RENAME_MAP_PATH.exists()})")
    print(f"  DASHBOARD_DIR:{DASHBOARD_DIR}")

    # Load rename map
    print(f"\n--- Loading rename map ---")
    label_to_zoho, zoho_to_label = load_rename_maps(RENAME_MAP_PATH)
    if not label_to_zoho:
        print("ERROR: No rename mappings. Cannot map NDJSON fields to vaccines.")
        return 1

    # Load org unit map
    print(f"\n--- Loading org unit map ---")
    ou_map = load_ou_map(OU_MAP_PATH)
    if not ou_map:
        print("ERROR: No org unit data. Cannot build hierarchy.")
        return 1

    # Read NDJSON data
    print(f"\n--- Reading NDJSON data ---")
    records = read_all_ndjson(MONTHLY_DIR)
    if not records:
        print("ERROR: No records found in monthly data.")
        # Write empty but valid dashboard
        DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
        empty = {"metadata": {"error": "no_data"}, "national": {}, "provinces": {}}
        (DASHBOARD_DIR / "dashboard.json").write_text(
            json.dumps(empty), encoding="utf-8"
        )
        (DASHBOARD_DIR / "index.json").write_text(
            json.dumps({"files": ["dashboard.json"], "split_by_province": False, "metadata": empty["metadata"]}),
            encoding="utf-8",
        )
        print("  Written empty dashboard.json (valid JSON)")
        return 0

    # Show sample record for debugging
    print(f"\n--- Sample record (first) ---")
    sample = records[0]
    sample_keys = list(sample.keys())[:10]
    for k in sample_keys:
        print(f"  {k}: {sample[k]}")
    if len(sample.keys()) > 10:
        print(f"  ... and {len(sample.keys()) - 10} more fields")

    # Aggregate
    print(f"\n--- Aggregating ---")
    dashboard_data = aggregate_data(records, ou_map, zoho_to_label, label_to_zoho)

    # Write
    print(f"\n--- Writing output ---")
    write_dashboard_json(dashboard_data, DASHBOARD_DIR)

    # Validate
    print(f"\n--- Validation ---")
    valid = validate_output(DASHBOARD_DIR)

    if valid:
        print(f"\n✅ Dashboard data generated successfully")
        return 0
    else:
        print(f"\n❌ Some output files are invalid!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
