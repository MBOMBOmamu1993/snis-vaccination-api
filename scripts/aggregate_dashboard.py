import json, gzip, os, math, sys
from pathlib import Path
from collections import defaultdict

DOCS = Path("docs")
DATA = DOCS / "data"
MONTHLY = DATA / "monthly"
DASH = DATA / "dashboard"
DASH.mkdir(parents=True, exist_ok=True)

# ── Load ou_map ──
with open(DATA / "ou_map.json") as f:
    OU_MAP = json.load(f)

# ── Load antenne_rules ──
ANT_RULES = {}
ant_path = DOCS / "config" / "antenne_rules.json"
if ant_path.exists():
    with open(ant_path) as f:
        ANT_RULES = json.load(f)

# ── Field specs ──
SUM_SPECS = {
    'BCG_0_11': ['BCG_fixe1','BCG_fixe2','BCG_avanc_1','BCG_avanc_2','BCG_mobile1','BCG_mobile2'],
    'BCG_0_11_fixe': ['BCG_fixe1','BCG_fixe2'],
    'DTC1_0_11': ['Penta1_fixe1','Penta1_fixe2','Penta1_avanc_1','Penta1_avanc_2','Penta1_mobile1','Penta1_mobile2'],
    'DTC1_0_11_fixe': ['Penta1_fixe1','Penta1_fixe2'],
    'DTC2_0_11': ['Penta2_fixe1','Penta2_fixe2','Penta2_avanc_1','Penta2_avanc_2','Penta2_mobile1','Penta2_mobile2'],
    'DTC2_0_11_fixe': ['Penta2_fixe1','Penta2_fixe2'],
    'DTC3_0_11': ['Penta3_fixe1','Penta3_fixe2','Penta3_avanc_1','Penta3_avanc_2','Penta3_mobile1','Penta3_mobile2'],
    'DTC3_0_11_fixe': ['Penta3_fixe1','Penta3_fixe2'],
    'VPO0_0_11': ['VPO0_0_11_mois_fixe1','VPO0_0_11_mois_fixe2','VPO0_0_11_mois_avanc_e1','VPO0_0_11_mois_avanc_e2','VPO0_0_11_mois_mobile1','VPO0_0_11_mois_mobile2'],
    'VPO0_0_11_fixe': ['VPO0_0_11_mois_fixe1','VPO0_0_11_mois_fixe2'],
    'VPO1_0_11': ['VPO1_0_11_mois_fixe1','VPO1_0_11_mois_fixe2','VPO1_0_11_mois_avanc_e1','VPO1_0_11_mois_avanc_e2','VPO1_0_11_mois_mobile1','VPO1_0_11_mois_mobile2'],
    'VPO1_0_11_fixe': ['VPO1_0_11_mois_fixe1','VPO1_0_11_mois_fixe2'],
    'VPO2_0_11': ['VPO2_0_11_mois_fixe1','VPO2_0_11_mois_fixe2','VPO2_0_11_mois_avanc_e1','VPO2_0_11_mois_avanc_e2','VPO2_0_11_mois_mobile1','VPO2_0_11_mois_mobile2'],
    'VPO2_0_11_fixe': ['VPO2_0_11_mois_fixe1','VPO2_0_11_mois_fixe2'],
    'VPO3_0_11': ['VPO3_fixe1','VPO3_fixe2','VPO3_avanc_1','VPO3_avanc_2','VPO3_mobile1','VPO3_mobile2'],
    'VPO3_0_11_fixe': ['VPO3_fixe1','VPO3_fixe2'],
    'VPI1_0_11': ['VPI1_fixe1','VPI1_fixe2','VPI1_avanc_1','VPI1_avanc_2','VPI1_mobile1','VPI1_mobile2'],
    'VPI1_0_11_fixe': ['VPI1_fixe1','VPI1_fixe2'],
    'VPI2_0_11': ['VPI2_fixe1','VPI2_fixe2','VPI2_avanc_1','VPI2_avanc_2','VPI2_mobile1','VPI2_mobile2'],
    'VPI2_0_11_fixe': ['VPI2_fixe1','VPI2_fixe2'],
    'ROTA1_0_11': ['ROTA1_0_11_mois_fixe','ROTA1_0_11_mois_avanc_e','ROTA1_0_11_mois_mobile'],
    'ROTA1_0_11_fixe': ['ROTA1_0_11_mois_fixe'],
    'ROTA2_0_11': ['ROTA2_0_11_mois_fixe','ROTA2_0_11_mois_avanc_e','ROTA2_0_11_mois_mobile'],
    'ROTA2_0_11_fixe': ['ROTA2_0_11_mois_fixe'],
    'ROTA3_0_11': ['ROTA3_fixe','ROTA3_avanc_','ROTA3_mobile'],
    'ROTA3_0_11_fixe': ['ROTA3_fixe'],
    'PCV13_1_0_11': ['PCV13_1_0_11_mois_fixe1','PCV13_1_0_11_mois_fixe2','PCV13_1_0_11_mois_avanc_e1','PCV13_1_0_11_mois_avanc_e2','PCV13_1_0_11_mois_mobile1','PCV13_1_0_11_mois_mobile2'],
    'PCV13_1_0_11_fixe': ['PCV13_1_0_11_mois_fixe1','PCV13_1_0_11_mois_fixe2'],
    'PCV13_2_0_11': ['PCV13_2_0_11_mois_fixe1','PCV13_2_0_11_mois_fixe2','PCV13_2_0_11_mois_avanc_e1','PCV13_2_0_11_mois_avanc_e2','PCV13_2_0_11_mois_mobile1','PCV13_2_0_11_mois_mobile2'],
    'PCV13_2_0_11_fixe': ['PCV13_2_0_11_mois_fixe1','PCV13_2_0_11_mois_fixe2'],
    'PCV13_3_0_11': ['PCV13_fixe1','PCV13_fixe2','PCV13_avanc_1','PCV13_avanc_2','PCV13_mobile1','PCV13_mobile2'],
    'PCV13_3_0_11_fixe': ['PCV13_fixe1','PCV13_fixe2'],
    'VAR1_0_11': ['VAR1_fixe1','VAR1_fixe2','VAR1_avanc_1','VAR1_avanc_2','VAR1_mobile1','VAR1_mobile2'],
    'VAR1_0_11_fixe': ['VAR1_fixe1','VAR1_fixe2'],
    'VAR2_0_11': ['VAR2_0_11_mois_fixe','VAR2_0_11_mois_avanc_e','VAR2_0_11_mois_mobile'],
    'VAR2_0_11_fixe': ['VAR2_0_11_mois_fixe'],
    'VAA_0_11': ['VAA_fixe1','VAA_fixe2','VAA_avanc_1','VAA_avanc_2','VAA_mobile1','VAA_mobile2'],
    'VAA_0_11_fixe': ['VAA_fixe1','VAA_fixe2'],
    'VAP1_0_11': ['VAP1_0_11_mois_fixe','VAP1_0_11_mois_avanc_e'],
    'VAP1_0_11_fixe': ['VAP1_0_11_mois_fixe'],
    'VAP2_0_11': ['VAP2_0_11_mois_fixe','VAP2_0_11_mois_avanc_e','VAP2_0_11_mois_mobile'],
    'VAP2_0_11_fixe': ['VAP2_0_11_mois_fixe'],
    'VAP3_0_11': ['VAP3_0_11_mois_fixe','VAP3_0_11_mois_avanc_e','VAP3_0_11_mois_mobile'],
    'VAP3_0_11_fixe': ['VAP3_0_11_mois_fixe'],
    'VAP4_12_23': ['VAP4_12_23_mois_fixe','VAP4_12_23_mois_avanc_e','VAP4_12_23_mois_mobile'],
    'VAP4_12_23_fixe': ['VAP4_12_23_mois_fixe'],
    'Td_2_plus': ['Td_2','Td_3','Td_4','Td_5']
}

AG_KEYS = ['BCG_0_11','BCG_0_11_fixe','DTC1_0_11','DTC1_0_11_fixe','DTC2_0_11','DTC2_0_11_fixe',
           'DTC3_0_11','DTC3_0_11_fixe','VPO0_0_11','VPO0_0_11_fixe','VPO1_0_11','VPO1_0_11_fixe',
           'VPO2_0_11','VPO2_0_11_fixe','VPO3_0_11','VPO3_0_11_fixe','VPI1_0_11','VPI1_0_11_fixe',
           'VPI2_0_11','VPI2_0_11_fixe','ROTA1_0_11','ROTA1_0_11_fixe','ROTA2_0_11','ROTA2_0_11_fixe',
           'ROTA3_0_11','ROTA3_0_11_fixe','PCV13_1_0_11','PCV13_1_0_11_fixe','PCV13_2_0_11','PCV13_2_0_11_fixe',
           'PCV13_3_0_11','PCV13_3_0_11_fixe','VAR1_0_11','VAR1_0_11_fixe','VAR2_0_11','VAR2_0_11_fixe',
           'VAA_0_11','VAA_0_11_fixe','VAP1_0_11','VAP1_0_11_fixe','VAP2_0_11','VAP2_0_11_fixe',
           'VAP3_0_11','VAP3_0_11_fixe','VAP4_12_23','VAP4_12_23_fixe','Td_2_plus']

def nv(row, field):
    v = row.get(field)
    if v is None or v == '': return 0
    try: return float(v)
    except: return 0

def normalize_org3(org3):
    s = (org3 or '').strip()
    if len(s) > 3 and s[2] == ' ':
        s = s[3:].strip()
    for suf in [' Zone de Santé', ' Zone de Sante']:
        if s.endswith(suf):
            s = s[:-len(suf)].strip()
            break
    return s

def resolve_antenne(province, zs):
    rules = ANT_RULES.get(province, {})
    norm = normalize_org3(zs)
    return rules.get(norm, rules.get(zs, ''))

def period_to_ym(p):
    p = (p or '').strip()
    if len(p) >= 7 and p[4] == '-':
        return p[:4] + p[5:7]
    if len(p) >= 6 and p.isdigit():
        return p[:6]
    mmm = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',
           'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}
    parts = p.split('-')
    if len(parts) == 3 and parts[1] in mmm:
        return parts[2] + mmm[parts[1]]
    return p

# ── Load all records ──
print("Loading all NDJSON files...")
with open(DATA / "index.json") as f:
    index = json.load(f)

all_records = []
months_list = sorted(index.get("months", {}).keys())

for month in months_list:
    parts = index["months"][month].get("parts", [])
    for part in parts:
        fname = part.get("plain") or part.get("file", "")
        if not fname:
            continue
        fpath = MONTHLY / month / fname
        gz_path = MONTHLY / month / part.get("file", "")

        lines = []
        if fpath.exists() and not fname.endswith('.gz'):
            with open(fpath) as f:
                lines = f.readlines()
        elif gz_path.exists() and str(gz_path).endswith('.gz'):
            with gzip.open(gz_path, 'rt') as f:
                lines = f.readlines()
        elif fpath.exists():
            try:
                with gzip.open(fpath, 'rt') as f:
                    lines = f.readlines()
            except:
                with open(fpath) as f:
                    lines = f.readlines()

        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                ou = row.get('OrgUnit', '')
                meta = OU_MAP.get(ou, {})
                row['_Province'] = meta.get('Org2', '')
                row['_ZS'] = meta.get('Org3', '')
                row['_AS'] = meta.get('Org4', '')
                row['_FOSA'] = meta.get('Org5', '')
                row['_Antenne'] = resolve_antenne(row['_Province'], row['_ZS'])
                row['_YM'] = period_to_ym(row.get('Period', ''))
                # Compute aggregated fields
                for sf, sources in SUM_SPECS.items():
                    row[sf] = sum(nv(row, s) for s in sources)
                all_records.append(row)
            except:
                pass

    print(f"  {month}: {len(all_records)} total records")

print(f"Total: {len(all_records)} records")

# ── Helper: aggregate by group ──
def agg_group(records, group_fields):
    """Group records and compute aggregates"""
    groups = defaultdict(lambda: {
        'n': 0, 'rap': 0,
        'sum_comp': 0, 'sum_prompt': 0,
        **{k: 0 for k in AG_KEYS}
    })

    for r in records:
        key = tuple(r.get(f, '') for f in group_fields)
        g = groups[key]
        g['n'] += 1
        comp = nv(r, 'Compl_tude')
        if comp > 0:
            g['rap'] += 1
        g['sum_comp'] += comp
        g['sum_prompt'] += nv(r, 'Promptitude')
        for k in AG_KEYS:
            g[k] += nv(r, k)

    result = []
    for key, g in groups.items():
        entry = {}
        for i, f in enumerate(group_fields):
            entry[f] = key[i]
        entry['n'] = g['n']
        entry['rap'] = g['rap']
        entry['comp'] = round(g['sum_comp'] / g['n'], 2) if g['n'] > 0 else 0
        entry['prompt'] = round(g['sum_prompt'] / g['n'], 2) if g['n'] > 0 else 0
        for k in AG_KEYS:
            if g[k] > 0:
                entry[k] = g[k]
        result.append(entry)

    return result

# ── Generate aggregated files ──
print("Aggregating...")

# 1. By Province × Month
prov_month = agg_group(all_records, ['_Province', '_YM'])
print(f"  Province×Month: {len(prov_month)} rows")

# 2. By ZS × Month
zs_month = agg_group(all_records, ['_Province', '_ZS', '_Antenne', '_YM'])
print(f"  ZS×Month: {len(zs_month)} rows")

# 3. By AS × Month
as_month = agg_group(all_records, ['_Province', '_ZS', '_Antenne', '_AS', '_YM'])
print(f"  AS×Month: {len(as_month)} rows")

# 4. By FOSA × Month (for heatmap + detail)
fosa_month = agg_group(all_records, ['_Province', '_ZS', '_Antenne', '_AS', '_FOSA', '_YM'])
print(f"  FOSA×Month: {len(fosa_month)} rows")

# 5. Metadata
all_provinces = sorted(set(r['_Province'] for r in all_records if r.get('_Province')))
all_antennes = sorted(set(r['_Antenne'] for r in all_records if r.get('_Antenne')))
all_zs = sorted(set(r['_ZS'] for r in all_records if r.get('_ZS')))
all_as = sorted(set(r['_AS'] for r in all_records if r.get('_AS')))
all_fosa = sorted(set(r['_FOSA'] for r in all_records if r.get('_FOSA')))
all_months = sorted(set(r['_YM'] for r in all_records if r.get('_YM')))

meta = {
    'generated_at': index.get('generated_at', ''),
    'total_records': len(all_records),
    'provinces': all_provinces,
    'antennes': all_antennes,
    'zs': all_zs,
    'as': all_as,
    'months': all_months,
    'nb_fosa': len(all_fosa)
}

# ── Write output files ──
def write_json(path, data):
    with open(path, 'w') as f:
        json.dump(data, f, separators=(',', ':'), ensure_ascii=False)
    size = os.path.getsize(path)
    print(f"  Written {path} ({size/1024:.0f} KB)")

write_json(DASH / "meta.json", meta)
write_json(DASH / "by_province.json", prov_month)
write_json(DASH / "by_zs.json", zs_month)
write_json(DASH / "by_as.json", as_month)
write_json(DASH / "by_fosa.json", fosa_month)

# 6. Heatmap data (FOSA × Month → reported yes/no) - compact format
heatmap = {}
for r in fosa_month:
    fosa = r.get('_FOSA', '')
    ym = r.get('_YM', '')
    if fosa and ym:
        if fosa not in heatmap:
            heatmap[fosa] = {}
        heatmap[fosa][ym] = 1 if r['rap'] > 0 else 0

write_json(DASH / "heatmap.json", heatmap)

print("\n✅ Dashboard aggregation complete!")
print(f"Files in {DASH}/:")
for f in sorted(DASH.iterdir()):
    print(f"  {f.name}: {os.path.getsize(f)/1024:.0f} KB")
