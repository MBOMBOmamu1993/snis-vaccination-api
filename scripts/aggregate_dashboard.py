#!/usr/bin/env python3
"""Aggregate Dashboard v5.0 – reads NDJSON files from docs/data/monthly/"""

import json, os, glob, sys
from datetime import datetime, timezone
from collections import defaultdict

# ── Paths ──
BASE = os.path.join("docs", "data", "monthly")
OUT  = os.path.join("docs", "data", "dashboard")
os.makedirs(os.path.join(OUT, "heatmap"), exist_ok=True)

# ── Antigen definitions ──
# Maps display name → list of column prefixes to sum
ANTIGENS = {
    "BCG":    ["BCG"],
    "VPO0":   ["VPO0"],
    "Penta1": ["Penta1"],
    "Penta2": ["Penta2"],
    "Penta3": ["Penta3"],
    "VPO3":   ["VPO3"],
    "VPI1":   ["VPI1"],
    "VPI2":   ["VPI2"],
    "PCV13_1":["PCV13_1"],
    "PCV13_2":["PCV13_2"],
    "PCV13_3":["PCV13_3"],
    "ROTA1":  ["ROTA1"],
    "ROTA2":  ["ROTA2"],
    "ROTA3":  ["ROTA3"],
    "VAR1":   ["VAR1"],
    "VAR2":   ["VAR2"],
    "VAA":    ["VAA"],
    "VPI2_r": ["VPI2"],
    "VAP1":   ["VAP1"],
    "VAP2":   ["VAP2"],
    "VAP3":   ["VAP3"],
    "VAP4":   ["VAP4"],
    "Td2":    ["Td_2", "Td 2"],
    "Td3":    ["Td_3", "Td 3"],
    "Td4":    ["Td_4", "Td 4"],
    "Td5":    ["Td_5", "Td 5"],
}

# Suffixes that represent vaccination strategies
SUFFIXES = ["_fixe1", "_fixe2", "_avanc_1", "_avanc_2", "_avanc1",
            "_avanc2", "_mobile1", "_mobile2", "_fixe", "_avanc",
            "_mobile", "_avanc_e1", "_avanc_e2"]

def sum_antigen(row, prefixes):
    """Sum all columns matching any prefix+suffix combination."""
    total = 0
    for prefix in prefixes:
        for suffix in SUFFIXES:
            key = prefix + suffix
            if key in row:
                try:
                    total += int(float(row[key]))
                except (ValueError, TypeError):
                    pass
        # Also try exact match (e.g., "BCG" alone)
        if prefix in row:
            try:
                val = int(float(row[prefix]))
                # Only add if it looks like a count, not a string
                total += val
            except (ValueError, TypeError):
                pass
    return total

def parse_period(period_str):
    """Convert 'Period' field to YYYY-MM. Handles '01-Jan-2025' etc."""
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%Y%m%d", "%b-%Y"):
        try:
            dt = datetime.strptime(period_str.strip(), fmt)
            return dt.strftime("%Y-%m")
        except ValueError:
            continue
    return None

# ── Load all NDJSON files ──
print(f"Scanning {BASE} for NDJSON files...")
files = sorted(glob.glob(os.path.join(BASE, "**", "*.ndjson"), recursive=True))
print(f"Found {len(files)} files")

if not files:
    print("ERROR: No NDJSON files found!")
    sys.exit(1)

rows = []
for f in files:
    with open(f, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"  WARN: bad JSON in {f}: {e}")
                continue

print(f"Loaded {len(rows)} rows total")

# ── Extract province from OrgUnit (first token before "/") ──
# OrgUnit format varies; we use it as Zone de Santé
# We need a Province field – check if it exists or derive it
# For now, use OrgUnit as ZS name

# ── Aggregate ──
# by_province[month][province][antigen] = count
# by_zs[month][zs][antigen] = count
by_province = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
by_zs       = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

all_months = set()
all_provinces = set()
all_zs = set()

for row in rows:
    period = parse_period(row.get("Period", ""))
    if not period:
        continue

    org = row.get("OrgUnit", "Inconnu")
    # Use Province if available, otherwise derive from org
    province = row.get("Province", "")
    zs = row.get("ZS", "") or row.get("Zone_de_sante", "") or org

    if not province:
        province = "Non classé"

    all_months.add(period)
    all_provinces.add(province)
    all_zs.add(zs)

    for antigen_name, prefixes in ANTIGENS.items():
        val = sum_antigen(row, prefixes)
        if val > 0:
            by_province[period][province][antigen_name] += val
            by_zs[period][zs][antigen_name] += val

print(f"Months: {sorted(all_months)}")
print(f"Provinces: {len(all_provinces)}, ZS: {len(all_zs)}")

# ── Build output ──
# meta.json
meta = {
    "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "months": sorted(all_months),
    "provinces": sorted(all_provinces),
    "zs": sorted(all_zs),
    "antigens": list(ANTIGENS.keys()),
    "row_count": len(rows),
    "file_count": len(files),
}

# by_province.json – flat array
prov_out = []
for month in sorted(all_months):
    for prov in sorted(by_province[month].keys()):
        entry = {"month": month, "province": prov}
        entry.update(by_province[month][prov])
        prov_out.append(entry)

# by_zs.json – flat array
zs_out = []
for month in sorted(all_months):
    for zs in sorted(by_zs[month].keys()):
        entry = {"month": month, "zs": zs}
        entry.update(by_zs[month][zs])
        zs_out.append(entry)

# ── Write files ──
def write_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    size = os.path.getsize(path)
    print(f"  {path} -> {size:,} bytes")

write_json(os.path.join(OUT, "meta.json"), meta)
write_json(os.path.join(OUT, "by_province.json"), prov_out)
write_json(os.path.join(OUT, "by_zs.json"), zs_out)

# Heatmap per antigen
heatmap_index = {}
for ag in ANTIGENS.keys():
    hm = []
    for month in sorted(all_months):
        for zs in sorted(by_zs[month].keys()):
            val = by_zs[month][zs].get(ag, 0)
            if val > 0:
                hm.append({"month": month, "zs": zs, "value": val})
    fname = f"{ag}.json"
    write_json(os.path.join(OUT, "heatmap", fname), hm)
    heatmap_index[ag] = f"heatmap/{fname}"

write_json(os.path.join(OUT, "heatmap", "_index.json"), heatmap_index)

print("\n✅ Aggregation complete!")
print(f"   {len(prov_out)} province rows, {len(zs_out)} ZS rows")
