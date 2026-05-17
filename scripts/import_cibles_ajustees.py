#!/usr/bin/env python3
"""Import des cibles ajustées (CSV PEV-RDC) vers JSON dashboard.

Usage:
    python3 scripts/import_cibles_ajustees.py <fichier.csv> <annee>

Lit un CSV avec colonnes "Province", "Zone de Santé" et
"Survivants ajustés <annee>" puis produit
docs/data/cibles_ajustees/<annee>.json indexé par ZS du dashboard.

Conventions PEV-RDC :
  NS_rate = 3.49 %, NV_rate = 4.00 %
  Donc Naissances_vivantes_ajustées = Survivants_ajustés / 0.8725
"""
import csv
import gzip
import json
import os
import re
import sys
import unicodedata
from datetime import datetime

NS_OVER_NV = 0.0349 / 0.04  # = 0.8725

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def norm(s: str) -> str:
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s.lower().strip()


def norm_prov(s: str) -> str:
    s = norm(s)
    s = re.sub(r"^[a-z]{2}\s+", "", s)
    s = re.sub(r"\s+province$", "", s)
    return s.strip()


def norm_zs(s: str) -> str:
    s = norm(s)
    s = re.sub(r"^[a-z]{2}\s+", "", s)
    s = re.sub(r"\s+zone\s+de\s+sante$", "", s)
    return s.strip()


def load_dashboard_zs():
    path = os.path.join(ROOT, "docs/data_zs/dashboard/by_zs.json.gz")
    with gzip.open(path) as f:
        d = json.load(f)
    out = {}
    for r in d:
        k = (norm_prov(r["_Province"]), norm_zs(r["_ZS"]))
        if k not in out:
            out[k] = {
                "_Province": r["_Province"],
                "_ZS": r["_ZS"],
                "_Antenne": r["_Antenne"],
            }
    return out


def read_csv(path: str):
    enc = "cp1252"
    try:
        with open(path, encoding="utf-8") as f:
            f.read()
        enc = "utf-8"
    except UnicodeDecodeError:
        pass
    with open(path, encoding=enc) as f:
        return list(csv.reader(f))


def find_col(headers, *needles):
    norm_h = [norm(h) for h in headers]
    for needle in needles:
        n = norm(needle)
        for i, h in enumerate(norm_h):
            if n in h:
                return i
    return -1


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    csv_path, year = sys.argv[1], sys.argv[2]
    if not year.isdigit():
        print(f"Année invalide: {year}")
        sys.exit(1)

    rows = read_csv(csv_path)
    if not rows:
        print("CSV vide")
        sys.exit(1)
    headers = rows[0]

    iProv = find_col(headers, "Province")
    iZS = find_col(headers, "Zone de Sant")
    iSV = find_col(headers, f"Survivants ajust {year}", f"Survivants ajustes {year}")
    if iSV < 0:
        iSV = find_col(headers, f"ajust {year}")
    if iProv < 0 or iZS < 0 or iSV < 0:
        print(f"Colonnes manquantes (Province={iProv}, ZS={iZS}, Survivants ajustés={iSV})")
        sys.exit(1)

    dash = load_dashboard_zs()
    records, unmatched = [], []
    for row in rows[1:]:
        if len(row) <= max(iProv, iZS, iSV):
            continue
        prov, zs, sv = row[iProv].strip(), row[iZS].strip(), row[iSV].strip()
        if not prov or not zs or not sv:
            continue
        try:
            sv_val = float(sv.replace(",", "").replace(" ", ""))
        except ValueError:
            continue
        k = (norm_prov(prov), norm_zs(zs))
        ref = dash.get(k)
        if not ref:
            unmatched.append((prov, zs))
            continue
        records.append({
            "_Province": ref["_Province"],
            "_Antenne": ref["_Antenne"],
            "_ZS": ref["_ZS"],
            "SV_ajust": round(sv_val, 2),
            "NV_ajust": round(sv_val / NS_OVER_NV, 2),
        })

    out = {
        "year": int(year),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "source": os.path.basename(csv_path),
        "ns_rate": 0.0349,
        "nv_rate": 0.04,
        "records": records,
    }
    out_dir = os.path.join(ROOT, "docs/data/cibles_ajustees")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{year}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))

    print(f"OK -> {out_path}")
    print(f"  ZS importées : {len(records)} / {len(dash)} (dashboard)")
    if unmatched:
        print(f"  Non matché : {len(unmatched)}")
        for u in unmatched[:5]:
            print("    -", u)


if __name__ == "__main__":
    main()
