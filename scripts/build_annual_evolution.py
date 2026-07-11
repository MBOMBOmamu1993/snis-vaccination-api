#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pré-calcul de l'ÉVOLUTION ANNUELLE des couvertures vaccinales, 2020 → année en
cours, par antigène, au niveau NATIONAL, PROVINCE et ZONE DE SANTÉ.

Pourquoi ce script existe
-------------------------
Les données brutes FOSA×mois du DHIS2 représentent des millions de lignes et
ne peuvent pas être embarquées dans le dépôt (elles ne couvrent que 2025-2026
côté fichiers). Or les requêtes analytics multi-années sont trop lentes pour
être faites à la volée dans le navigateur. Ce script tourne en CI (avec les
secrets DHIS2), interroge les INDICATEURS de couverture (dénominateurs
intégrés → % comparables entre années) par année, et écrit un fichier compact
`docs/data/annual/evolution.json(.gz)` (~quelques centaines de Ko) que le
dashboard charge instantanément pour la page « Évolution annuelle par antigène ».

Sortie : docs/data/annual/evolution.json et .json.gz
  {
    "generated_at": "...Z",
    "years": [2020, ..., 2026],
    "antigens": [{"key": "BCG", "label": "BCG", "uid": "..."}, ...],
    "national": {"BCG": {"2020": 84.1, ...}, ...},
    "provinces": [
       {"id": "...", "name": "...",
        "data": {"BCG": {"2020": 83.2, ...}, ...},
        "zs": [{"id": "...", "name": "...", "data": {...}}, ...]}
    ]
  }

Secrets requis (env) : DHIS2_BASE_URL, DHIS2_USERNAME, DHIS2_PASSWORD
"""
import os
import sys
import json
import gzip
import time
import datetime
from typing import Dict, List

import requests
from requests.adapters import HTTPAdapter

try:
    from urllib3.util.retry import Retry
except Exception:  # pragma: no cover
    from requests.packages.urllib3.util.retry import Retry  # type: ignore

# ── Antigènes suivis : indicateurs « Taux de couverture » du DHIS2 SNIS RDC ──
# (UID confirmés via l'API métadonnées). L'ordre définit l'affichage.
ANTIGENS: List[Dict[str, str]] = [
    {"key": "BCG",     "label": "BCG",              "uid": "cWJXYKCXrLx"},
    {"key": "Penta1",  "label": "Penta 1 (DTC1)",   "uid": "k26tJ22Ncpd"},
    {"key": "Penta3",  "label": "Penta 3 (DTC3)",   "uid": "v9b9aYsA5PM"},
    {"key": "VPO3",    "label": "VPO 3",            "uid": "PR35pwZEAny"},
    {"key": "VPI1",    "label": "VPI 1",            "uid": "F35ZsGcz2Lv"},
    {"key": "VPI2",    "label": "VPI 2",            "uid": "khtnBZ1Ispo"},
    {"key": "PCV13_1", "label": "PCV-13 (1)",       "uid": "Dx7fV8rLg5F"},
    {"key": "PCV13_3", "label": "PCV-13 (3)",       "uid": "biBItnhkMUe"},
    {"key": "ROTA1",   "label": "Rota 1",           "uid": "k8womY2OabA"},
    {"key": "ROTA3",   "label": "Rota 3",           "uid": "HX6kfUl1auv"},
    {"key": "VAR1",    "label": "VAR 1 / RR 1",     "uid": "OKaDG2t6LtN"},
    {"key": "VAA",     "label": "VAA",              "uid": "j2RPbYTGkAf"},
    {"key": "VAP1",    "label": "VAP 1",            "uid": "RZuHoUKI3fc"},
    {"key": "VAP4",    "label": "VAP 4",            "uid": "tNZxs1kE6fy"},
    {"key": "Td2",     "label": "Td 2+",            "uid": "z4Iqy42FhNf"},
]
UID2KEY = {a["uid"]: a["key"] for a in ANTIGENS}
DX = ";".join(a["uid"] for a in ANTIGENS)

START_YEAR = 2020


def make_session() -> requests.Session:
    retry = Retry(
        total=6, connect=6, read=6, status=6, backoff_factor=3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"], raise_on_status=False,
    )
    s = requests.Session()
    ad = HTTPAdapter(max_retries=retry)
    s.mount("https://", ad)
    s.mount("http://", ad)
    return s


def get_json(sess, base, user, pw, path, params) -> dict:
    url = base.rstrip("/") + "/" + path.lstrip("/")
    for attempt in range(1, 8):
        r = sess.get(url, params=params, auth=(user, pw),
                     headers={"Accept": "application/json"}, timeout=1200)
        if 200 <= r.status_code < 300:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            slp = min(90.0, 6.0 * attempt)
            print(f"WARN DHIS2 {r.status_code} attempt {attempt}/7 sleep {slp}s {path}", flush=True)
            time.sleep(slp)
            continue
        r.raise_for_status()
    raise RuntimeError("Échec DHIS2 après plusieurs tentatives : " + path)


def analytics_year(sess, base, user, pw, level: int, year: int) -> Dict[str, Dict[str, float]]:
    """Retourne {ouUid: {antigenKey: valeur}} pour un niveau et une année."""
    data = get_json(sess, base, user, pw, "api/analytics.json", {
        "dimension": ["dx:" + DX, "ou:LEVEL-" + str(level)],
        "filter": "pe:" + str(year),
        "skipMeta": "true",
        "outputIdScheme": "UID",
    })
    out: Dict[str, Dict[str, float]] = {}
    for row in data.get("rows", []):
        dx, ou, val = row[0], row[1], row[2]
        key = UID2KEY.get(dx)
        if not key:
            continue
        try:
            v = round(float(val), 1)
        except (TypeError, ValueError):
            continue
        out.setdefault(ou, {})[key] = v
    return out


def main() -> int:
    base = os.environ.get("DHIS2_BASE_URL")
    user = os.environ.get("DHIS2_USERNAME")
    pw = os.environ.get("DHIS2_PASSWORD")
    if not (base and user and pw):
        print("Secrets manquants : DHIS2_BASE_URL, DHIS2_USERNAME, DHIS2_PASSWORD", file=sys.stderr)
        return 1

    this_year = datetime.date.today().year
    years = list(range(START_YEAR, this_year + 1))
    sess = make_session()

    # 1) Métadonnées organisationnelles : provinces (niveau 2) et ZS (niveau 3).
    print("Métadonnées : provinces (niveau 2)…", flush=True)
    prov_meta = get_json(sess, base, user, pw, "api/organisationUnits.json", {
        "level": 2, "fields": "id,name", "paging": "false",
    })["organisationUnits"]
    prov_name = {p["id"]: p["name"] for p in prov_meta}

    print("Métadonnées : zones de santé (niveau 3)…", flush=True)
    zs_meta = get_json(sess, base, user, pw, "api/organisationUnits.json", {
        "level": 3, "fields": "id,name,ancestors[id,level]", "paging": "false",
    })["organisationUnits"]
    zs_parent = {}
    for z in zs_meta:
        prov = next((a["id"] for a in z.get("ancestors", []) if a.get("level") == 2), None)
        zs_parent[z["id"]] = {"name": z["name"], "prov": prov}

    # 2) Analytics année par année (rapide et sûr : 1 requête / niveau / année).
    nat: Dict[str, Dict[str, float]] = {}       # antigen -> year -> val
    prov_data: Dict[str, Dict[str, Dict[str, float]]] = {}  # prov -> antigen -> year -> val
    zs_data: Dict[str, Dict[str, Dict[str, float]]] = {}    # zs -> antigen -> year -> val

    for y in years:
        print(f"Analytics national + provinces {y}…", flush=True)
        lvl2 = analytics_year(sess, base, user, pw, 2, y)
        # national = moyenne simple des provinces renseignées (indicateur = %)
        agg: Dict[str, List[float]] = {}
        for ou, d in lvl2.items():
            pd = prov_data.setdefault(ou, {})
            for k, v in d.items():
                pd.setdefault(k, {})[str(y)] = v
                agg.setdefault(k, []).append(v)
        for k, vals in agg.items():
            if vals:
                nat.setdefault(k, {})[str(y)] = round(sum(vals) / len(vals), 1)

        print(f"Analytics zones de santé {y}…", flush=True)
        lvl3 = analytics_year(sess, base, user, pw, 3, y)
        for ou, d in lvl3.items():
            zd = zs_data.setdefault(ou, {})
            for k, v in d.items():
                zd.setdefault(k, {})[str(y)] = v

    # 3) Assemblage compact, ordonné par nom de province puis de ZS.
    provinces = []
    for pid in sorted(prov_name, key=lambda i: prov_name[i]):
        zs_list = []
        for zid in sorted(
            [z for z, m in zs_parent.items() if m["prov"] == pid and z in zs_data],
            key=lambda i: zs_parent[i]["name"],
        ):
            zs_list.append({"id": zid, "name": zs_parent[zid]["name"], "data": zs_data[zid]})
        provinces.append({
            "id": pid, "name": prov_name[pid],
            "data": prov_data.get(pid, {}), "zs": zs_list,
        })

    payload = {
        "generated_at": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "DHIS2 (SNIS RDC) — indicateurs Taux de couverture",
        "years": years,
        "antigens": ANTIGENS,
        "national": nat,
        "provinces": provinces,
    }

    out_dir = os.path.join("docs", "data", "annual")
    os.makedirs(out_dir, exist_ok=True)
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    with open(os.path.join(out_dir, "evolution.json"), "wb") as f:
        f.write(raw)
    with gzip.open(os.path.join(out_dir, "evolution.json.gz"), "wb") as f:
        f.write(raw)

    nz = sum(1 for p in provinces for _ in p["zs"])
    print(f"OK → {len(raw)} octets · {len(years)} années · {len(provinces)} provinces · "
          f"{nz} ZS · {len(ANTIGENS)} antigènes", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
