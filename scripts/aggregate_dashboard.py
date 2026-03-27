#!/usr/bin/env python3
"""Agrège les données NDJSON en un seul fichier JSON léger pour le dashboard."""

import json, os, glob
from collections import defaultdict

DOCS = os.path.join(os.path.dirname(__file__), '..', 'docs')
MONTHLY = os.path.join(DOCS, 'data', 'monthly')
OU_MAP_PATH = os.path.join(DOCS, 'data', 'ou_map.json')
ANTENNE_PATH = os.path.join(DOCS, 'config', 'antenne_rules.json')
OUTPUT = os.path.join(DOCS, 'data', 'dashboard_agg.json')

# Champs antigènes à sommer
SUM_FIELDS = [
    'BCG_fixe1','BCG_fixe2','BCG_avanc_1','BCG_avanc_2','BCG_mobile1','BCG_mobile2',
    'Penta1_fixe1','Penta1_fixe2','Penta1_avanc_1','Penta1_avanc_2','Penta1_mobile1','Penta1_mobile2',
    'Penta2_fixe1','Penta2_fixe2','Penta2_avanc_1','Penta2_avanc_2','Penta2_mobile1','Penta2_mobile2',
    'Penta3_fixe1','Penta3_fixe2','Penta3_avanc_1','Penta3_avanc_2','Penta3_mobile1','Penta3_mobile2',
    'VPO0_0_11_mois_fixe1','VPO0_0_11_mois_fixe2','VPO0_0_11_mois_avanc_e1','VPO0_0_11_mois_avanc_e2',
    'VPO0_0_11_mois_mobile1','VPO0_0_11_mois_mobile2',
    'VPO1_0_11_mois_fixe1','VPO1_0_11_mois_fixe2','VPO1_0_11_mois_avanc_e1','VPO1_0_11_mois_avanc_e2',
    'VPO1_0_11_mois_mobile1','VPO1_0_11_mois_mobile2',
    'VPO2_0_11_mois_fixe1','VPO2_0_11_mois_fixe2','VPO2_0_11_mois_avanc_e1','VPO2_0_11_mois_avanc_e2',
    'VPO2_0_11_mois_mobile1','VPO2_0_11_mois_mobile2',
    'VPO3_fixe1','VPO3_fixe2','VPO3_avanc_1','VPO3_avanc_2','VPO3_mobile1','VPO3_mobile2',
    'VPI1_fixe1','VPI1_fixe2','VPI1_avanc_1','VPI1_avanc_2','VPI1_mobile1','VPI1_mobile2',
    'VPI2_fixe1','VPI2_fixe2','VPI2_avanc_1','VPI2_avanc_2','VPI2_mobile1','VPI2_mobile2',
    'ROTA1_0_11_mois_fixe','ROTA1_0_11_mois_avanc_e','ROTA1_0_11_mois_mobile',
    'ROTA2_0_11_mois_fixe','ROTA2_0_11_mois_avanc_e','ROTA2_0_11_mois_mobile',
    'ROTA3_fixe','ROTA3_avanc_','ROTA3_mobile',
    'PCV13_1_0_11_mois_fixe1','PCV13_1_0_11_mois_fixe2','PCV13_1_0_11_mois_avanc_e1',
    'PCV13_1_0_11_mois_avanc_e2','PCV13_1_0_11_mois_mobile1','PCV13_1_0_11_mois_mobile2',
    'PCV13_2_0_11_mois_fixe1','PCV13_2_0_11_mois_fixe2','PCV13_2_0_11_mois_avanc_e1',
    'PCV13_2_0_11_mois_avanc_e2','PCV13_2_0_11_mois_mobile1','PCV13_2_0_11_mois_mobile2',
    'PCV13_fixe1','PCV13_fixe2','PCV13_avanc_1','PCV13_avanc_2','PCV13_mobile1','PCV13_mobile2',
    'VAR1_fixe1','VAR1_fixe2','VAR1_avanc_1','VAR1_avanc_2','VAR1_mobile1','VAR1_mobile2',
    'VAR2_0_11_mois_fixe','VAR2_0_11_mois_avanc_e','VAR2_0_11_mois_mobile',
    'VAR2_fixe1','VAR2_fixe2','VAR2_avanc_','VAR2_mobile1','VAR2_mobile2',
    'VAA_fixe1','VAA_fixe2','VAA_avanc_1','VAA_avanc_2','VAA_mobile1','VAA_mobile2',
    'VAP1_0_11_mois_fixe','VAP1_0_11_mois_avanc_e',
    'VAP2_0_11_mois_fixe','VAP2_0_11_mois_avanc_e','VAP2_0_11_mois_mobile',
    'VAP3_0_11_mois_fixe','VAP3_0_11_mois_avanc_e','VAP3_0_11_mois_mobile',
    'VAP4_12_23_mois_fixe','VAP4_12_23_mois_avanc_e','VAP4_12_23_mois_mobile',
    'Td_2','Td_3','Td_4','Td_5',
    'Compl_tude','Promptitude',
    'Rapports_attendus','Naissances_vivantes',
    'S_ances_pr_vues','S_ances_r_alis_es',
    'S_ances_fixes_pr_vues','S_ances_fixes_r_alis_es',
    'S_ances_avanc_es_pr_vues','S_ances_avanc_es_r_alis_es',
    'S_ances_mobiles_pr_vues','S_ances_mobiles_r_alis_es',
    'Pop_totale','Pop_0_11m_nv','Pop_0_11m_survivants'
]

def nval(row, f):
    v = row.get(f)
    if v is None or v == '':
        return 0
    try:
        return float(v)
    except:
        return 0

def normalize_org3(org3):
    s = (org3 or '').strip()
    if not s:
        return ''
    if len(s) > 3 and s[2] == ' ':
        s = s[3:].strip()
    for suf in [' Zone de Santé', ' Zone de Sante']:
        if s.endswith(suf):
            s = s[:-len(suf)].strip()
            break
    return s

def main():
    # Charger ou_map
    with open(OU_MAP_PATH, 'r') as f:
        ou_map = json.load(f)
    
    # Charger antenne_rules
    antenne_rules = {}
    if os.path.exists(ANTENNE_PATH):
        with open(ANTENNE_PATH, 'r') as f:
            antenne_rules = json.load(f)
    
    def resolve_antenne(prov, zs):
        rules = antenne_rules.get(prov, {})
        norm = normalize_org3(zs)
        return rules.get(norm, rules.get(zs, ''))
    
    # Résultat : agrégé par (province, antenne, zs, as, fosa, period_ym)
    agg = defaultdict(lambda: {'count': 0, 'reporting': 0})
    
    # Aussi agréger par niveaux supérieurs
    agg_zs = defaultdict(lambda: {'count': 0, 'reporting': 0})
    agg_prov = defaultdict(lambda: {'count': 0, 'reporting': 0})
    
    month_dirs = sorted(glob.glob(os.path.join(MONTHLY, '*')))
    total_rows = 0
    
    for mdir in month_dirs:
        if not os.path.isdir(mdir):
            continue
        month = os.path.basename(mdir)
        ndjson_files = sorted(glob.glob(os.path.join(mdir, '*.ndjson')))
        
        for fpath in ndjson_files:
            if fpath.endswith('.gz'):
                continue
            with open(fpath, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except:
                        continue
                    
                    ou = row.get('OrgUnit', '')
                    meta = ou_map.get(ou, {})
                    prov = meta.get('Org2', '')
                    zs = meta.get('Org3', '')
                    ars = meta.get('Org4', '')
                    fosa = meta.get('Org5', '')
                    antenne = resolve_antenne(prov, zs)
                    period_ym = month  # dossier = YYYYMM
                    
                    is_reporting = nval(row, 'Compl_tude') > 0
                    
                    # Clé FOSA
                    key = f"{prov}|{antenne}|{zs}|{ars}|{fosa}|{period_ym}"
                    agg[key]['count'] += 1
                    if is_reporting:
                        agg[key]['reporting'] += 1
                    for sf in SUM_FIELDS:
                        agg[key][sf] = agg[key].get(sf, 0) + nval(row, sf)
                    
                    # Clé ZS
                    key_zs = f"{prov}|{antenne}|{zs}|{period_ym}"
                    agg_zs[key_zs]['count'] += 1
                    if is_reporting:
                        agg_zs[key_zs]['reporting'] += 1
                    for sf in SUM_FIELDS:
                        agg_zs[key_zs][sf] = agg_zs[key_zs].get(sf, 0) + nval(row, sf)
                    
                    # Clé Province
                    key_prov = f"{prov}|{period_ym}"
                    agg_prov[key_prov]['count'] += 1
                    if is_reporting:
                        agg_prov[key_prov]['reporting'] += 1
                    for sf in SUM_FIELDS:
                        agg_prov[key_prov][sf] = agg_prov[key_prov].get(sf, 0) + nval(row, sf)
                    
                    total_rows += 1
    
    # Convertir en listes
    def to_list(d, key_fields):
        result = []
        for k, v in d.items():
            parts = k.split('|')
            rec = dict(zip(key_fields, parts))
            rec.update(v)
            result.append(rec)
        return result
    
    output = {
        'generated_at': __import__('datetime').datetime.utcnow().isoformat(),
        'total_raw_rows': total_rows,
        'fosa': to_list(agg, ['province','antenne','zs','as','fosa','period']),
        'zs': to_list(agg_zs, ['province','antenne','zs','period']),
        'province': to_list(agg_prov, ['province','period'])
    }
    
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, 'w') as f:
        json.dump(output, f, separators=(',', ':'))
    
    size_mb = os.path.getsize(OUTPUT) / 1024 / 1024
    print(f"✅ dashboard_agg.json: {total_rows} rows → {len(agg)} FOSA-periods, {len(agg_zs)} ZS-periods, {len(agg_prov)} Prov-periods ({size_mb:.1f} MB)")

if __name__ == '__main__':
    main()
