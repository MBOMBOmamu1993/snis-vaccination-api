import json, gzip, os, re, sys
from pathlib import Path
from collections import defaultdict
import shutil

DOCS = Path("docs")
DATA = DOCS / "data"
MONTHLY = DATA / "monthly"
DASH = DATA / "dashboard"

# Clean previous dashboard
if DASH.exists():
    shutil.rmtree(DASH)
DASH.mkdir(parents=True, exist_ok=True)

# ◄ FIX: empêcher Jekyll d'ignorer les fichiers
(DOCS / ".nojekyll").touch()

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
    'VAR2_fixe': ['VAR2_fixe1','VAR2_fixe2'],
    'VAR2': ['VAR2_fixe1','VAR2_fixe2','VAR2_avanc_','VAR2_mobile1','VAR2_mobile2'],
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

AG_KEYS = list(SUM_SPECS.keys())

def nv(row, field):
    v = row.get(field)
    if v is None or v == '':
        return 0
    try:
        return float(v)
    except:
        return 0

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

def slug(name):
    s = re.sub(r'[^\w\s-]', '', (name or 'unknown').strip())
    s = re.sub(r'[\s-]+', '_', s)
    return s.lower() or 'unknown'

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
                for sf, sources in SUM_SPECS.items():
                    row[sf] = sum(nv(row, s) for s in sources)
                all_records.append(row)
            except:
                pass

    print(f"  {month}: {len(all_records)} total records")

print(f"Total: {len(all_records)} records")

# ── Helper: aggregate by group ──
def agg_group(records, group_fields):
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

# ── Write helpers ──
def write_json(path, data):
    with open(path, 'w') as f:
        json.dump(data, f, separators=(',', ':'), ensure_ascii=False)
    size = os.path.getsize(path)
    print(f"  Written {path} ({size/1024:.0f} KB)")

def write_split(base_dir, records, key_field='_Province'):
    base_dir.mkdir(parents=True, exist_ok=True)
    by_key = defaultdict(list)
    for r in records:
        by_key[r.get(key_field, '') or 'unknown'].append(r)

    manifest = {}
    for k, rows in sorted(by_key.items()):
        fname = slug(k) + '.json'
        write_json(base_dir / fname, rows)
        manifest[k] = fname

    # ◄ FIX: manifest.json au lieu de _index.json
    write_json(base_dir / "manifest.json", manifest)
    return manifest

# ── Generate aggregated files ──
print("Aggregating...")

prov_month = agg_group(all_records, ['_Province', '_YM'])
print(f"  Province×Month: {len(prov_month)} rows")

zs_month = agg_group(all_records, ['_Province', '_ZS', '_Antenne', '_YM'])
print(f"  ZS×Month: {len(zs_month)} rows")

as_month = agg_group(all_records, ['_Province', '_ZS', '_Antenne', '_AS', '_YM'])
print(f"  AS×Month: {len(as_month)} rows")

fosa_month = agg_group(all_records, ['_Province', '_ZS', '_Antenne', '_AS', '_FOSA', '_YM'])
print(f"  FOSA×Month: {len(fosa_month)} rows")

all_provinces = sorted(set(r['_Province'] for r in all_records if r.get('_Province')))
all_antennes = sorted(set(r['_Antenne'] for r in all_records if r.get('_Antenne')))
all_zs = sorted(set(r['_ZS'] for r in all_records if r.get('_ZS')))
all_as_list = sorted(set(r['_AS'] for r in all_records if r.get('_AS')))
all_fosa = sorted(set(r['_FOSA'] for r in all_records if r.get('_FOSA')))
all_months = sorted(set(r['_YM'] for r in all_records if r.get('_YM')))

prov_slugs = {p: slug(p) for p in all_provinces}

meta_data = {
    'generated_at': index.get('generated_at', ''),
    'total_records': len(all_records),
    'provinces': all_provinces,
    'province_slugs': prov_slugs,
    'antennes': all_antennes,
    'zs': all_zs,
    'as': all_as_list,
    'months': all_months,
    'nb_fosa': len(all_fosa)
}

print("\nWriting output files...")

write_json(DASH / "meta.json", meta_data)
write_json(DASH / "by_province.json", prov_month)
write_json(DASH / "by_zs.json", zs_month)

print("\nSplitting by_as by province...")
write_split(DASH / "by_as", as_month)

print("\nSplitting by_fosa by province...")
write_split(DASH / "by_fosa", fosa_month)

print("\nBuilding heatmap by province...")
hm_dir = DASH / "heatmap"
hm_dir.mkdir(parents=True, exist_ok=True)
hm_by_prov = defaultdict(dict)
for r in fosa_month:
    prov = r.get('_Province', '') or 'unknown'
    fosa = r.get('_FOSA', '')
    ym = r.get('_YM', '')
    if fosa and ym:
        if fosa not in hm_by_prov[prov]:
            hm_by_prov[prov][fosa] = {}
        hm_by_prov[prov][fosa][ym] = 1 if r['rap'] > 0 else 0

hm_manifest = {}
for prov, data in sorted(hm_by_prov.items()):
    fname = slug(prov) + '.json'
    write_json(hm_dir / fname, data)
    hm_manifest[prov] = fname

# ◄ FIX: manifest.json au lieu de _index.json
write_json(hm_dir / "manifest.json", hm_manifest)

print("\n✅ Dashboard aggregation complete!")
total_size = 0
file_count = 0
for p in DASH.rglob("*.json"):
    total_size += os.path.getsize(p)
    file_count += 1
print(f"  {file_count} files, total {total_size/1024/1024:.1f} MB")

for p in DASH.rglob("*.json"):
    sz = os.path.getsize(p)
    if sz > 90_000_000:
        print(f"  ⚠️  WARNING: {p} is {sz/1024/1024:.0f} MB (near GitHub limit)")
