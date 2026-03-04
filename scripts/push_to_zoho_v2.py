#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import time
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests


# =========================
# CONFIG (à adapter)
# =========================
OWNER = os.getenv("ZC_OWNER", "drcongo")
APP_LINK = os.getenv("ZC_APP_LINK", "vaccination-de-routine-dhis2-rdc")
FORM_LINK = os.getenv("ZC_FORM_LINK", "Donn_es_PEV_FOSA")
REPORT_LINK = os.getenv("ZC_REPORT_LINK", "Donn_es_PEV_FOSA_Report")

# Base API Creator (souvent creator.zoho.com)
CREATOR_API_BASE = os.getenv("ZC_API_BASE", "https://creator.zoho.com")

# Data folder (repo checkout)
DATA_ROOT = Path(os.getenv("DATA_ROOT", "docs/data"))
INDEX_JSON = DATA_ROOT / "index.json"
MONTHLY_DIR = DATA_ROOT / "monthly"

# Rate limit pacing
BATCH_SIZE = 200
SLEEP_SECONDS = 10


# =========================
# OAUTH (refresh -> access token)
# =========================
def zoho_accounts_domain() -> str:
    # Exemple: accounts.zoho.com (ou accounts.zoho.eu, etc.)
    dc = os.getenv("ZOHO_DC", "com").strip().lower()
    return "accounts.zoho.com" if dc == "com" else f"accounts.zoho.{dc}"


def get_access_token() -> str:
    """
    Échange refresh_token -> access_token via OAuth v2.
    """
    client_id = os.environ["ZOHO_CLIENT_ID"]
    client_secret = os.environ["ZOHO_CLIENT_SECRET"]
    refresh_token = os.environ["ZOHO_REFRESH_TOKEN"]

    token_url = f"https://{zoho_accounts_domain()}/oauth/v2/token"
    resp = requests.post(
        token_url,
        data={
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "refresh_token",
        },
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError(f"No access_token in response: {data}")
    return token


# =========================
# Utils dates (3 derniers mois)
# =========================
def yyyymm(d: date) -> str:
    return f"{d.year:04d}{d.month:02d}"


def add_months_yyyymm(yyyymm_str: str, delta: int) -> str:
    y = int(yyyymm_str[:4])
    m = int(yyyymm_str[4:6])
    m0 = (y * 12 + (m - 1)) + delta
    y2 = m0 // 12
    m2 = (m0 % 12) + 1
    return f"{y2:04d}{m2:02d}"


def last_n_months(n: int) -> List[str]:
    today = date.today()
    cur = yyyymm(today)
    out = []
    for i in range(n - 1, -1, -1):
        out.append(add_months_yyyymm(cur, -i))
    return out


# =========================
# Read NDJSON parts from repo (plain or gz)
# =========================
def iter_ndjson_lines(path: Path) -> Iterable[dict]:
    """
    Lit un fichier .ndjson ou .ndjson.gz et yield chaque objet JSON.
    """
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if not ln:
                    continue
                yield json.loads(ln)
    else:
        with path.open("r", encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if not ln:
                    continue
                yield json.loads(ln)


def stable_rowhash(obj: dict) -> str:
    """
    Hash stable: JSON canonical (trié), sans champs volatiles.
    """
    clean = dict(obj)
    clean.pop("RowHash", None)
    clean.pop("ID", None)
    # Key est stable, tu peux le garder; mais on le recalcule ensuite
    s = json.dumps(clean, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def build_key(orgunit: str, period: str) -> str:
    return f"{orgunit}|{period}"


# =========================
# Zoho Creator REST v2.1 helpers
# =========================
def zc_headers(access_token: str) -> dict:
    return {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def zc_get_all_records_for_period(
    access_token: str,
    period_zoho: str,
    fields: Optional[List[str]] = None,
) -> Dict[str, Tuple[str, str]]:
    """
    Retourne map: Key -> (ID, RowHash) pour un Period donné.
    On lit par pages (pageSize 200).
    """
    # NB: l’API Creator v2.1 supporte criteria, page, pageSize (comme l’SDK/Deluge).
    # On utilise la même logique que ton widget.
    params_base = {
        "criteria": f'(Period == "{period_zoho}")',
        "pageSize": 200,
    }
    if fields:
        params_base["fields"] = ",".join(fields)

    out: Dict[str, Tuple[str, str]] = {}
    page = 1
    while True:
        params = dict(params_base)
        params["page"] = page

        url = f"{CREATOR_API_BASE}/api/v2.1/{OWNER}/{APP_LINK}/report/{REPORT_LINK}"
        r = requests.get(url, headers=zc_headers(access_token), params=params, timeout=120)

        # 3100 (no records) est fréquent dans Creator; selon tenant, ça peut être 404.
        if r.status_code in (404,):
            return out

        r.raise_for_status()
        data = r.json()
        rows = data.get("data") or []
        if not rows:
            break

        for rec in rows:
            k = rec.get("Key")
            rid = rec.get("ID")
            rh = rec.get("RowHash") or ""
            if k and rid:
                out[str(k)] = (str(rid), str(rh))

        if len(rows) < 200:
            break

        page += 1
        if page > 500:
            break

    return out


def zc_add_records(access_token: str, records: List[dict]) -> dict:
    """
    Bulk add (jusqu’à 200).
    """
    url = f"{CREATOR_API_BASE}/api/v2.1/{OWNER}/{APP_LINK}/form/{FORM_LINK}"
    payload = {"data": records}
    r = requests.post(url, headers=zc_headers(access_token), data=json.dumps(payload, ensure_ascii=False), timeout=300)
    r.raise_for_status()
    return r.json()


def zc_update_records(access_token: str, records: List[dict]) -> dict:
    """
    Bulk update (jusqu’à 200) via report endpoint.
    Chaque record doit contenir "ID" + champs à mettre à jour.
    """
    # D’après la doc “Update Records” max 200 records / request. :contentReference[oaicite:2]{index=2}
    url = f"{CREATOR_API_BASE}/api/v2.1/{OWNER}/{APP_LINK}/report/{REPORT_LINK}"
    payload = {"data": records}
    r = requests.put(url, headers=zc_headers(access_token), data=json.dumps(payload, ensure_ascii=False), timeout=300)
    r.raise_for_status()
    return r.json()


# =========================
# Main sync logic
# =========================
def month_parts_from_index(yyyymm_str: str) -> List[Path]:
    idx = json.loads(INDEX_JSON.read_text(encoding="utf-8"))
    months = (idx.get("months") or {})
    m = months.get(yyyymm_str)
    if not m:
        return []
    parts = m.get("parts") or []

    # On préfère plain .ndjson pour Deluge; ici, on peut lire plain OU gz.
    # On prend plain si présent, sinon gz.
    out: List[Path] = []
    for p in parts:
        plain = p.get("plain")
        gz = p.get("file")
        if plain:
            out.append(MONTHLY_DIR / yyyymm_str / plain)
        elif gz:
            out.append(MONTHLY_DIR / yyyymm_str / gz)
    return out


def normalize_incoming_record(obj: dict) -> dict:
    """
    - Assure OrgUnit, Period, Key, RowHash
    - Convertit None/"" en 0 pour les champs numériques (si tes NDJSON le font déjà, ça ne change rien)
    """
    org = str(obj.get("OrgUnit") or "").strip()
    per = str(obj.get("Period") or "").strip()

    # On garde Period déjà au format Zoho: 01-Jan-2025 (comme ton widget)
    rec = dict(obj)
    rec["OrgUnit"] = org
    rec["Period"] = per

    rec["Key"] = build_key(org, per)
    rec["RowHash"] = stable_rowhash(rec)

    # Normalisation simple: None -> 0 sur le reste des champs (optionnel)
    for k, v in list(rec.items()):
        if k in ("OrgUnit", "Period", "Key", "RowHash"):
            continue
        if v is None or (isinstance(v, str) and v.strip() == ""):
            rec[k] = 0

    return rec


def chunked(items: List[dict], size: int) -> Iterable[List[dict]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def sync_last_3_months() -> None:
    if not INDEX_JSON.exists():
        raise RuntimeError(f"Missing {INDEX_JSON}")

    access = get_access_token()

    months = last_n_months(3)
    print(f"[SYNC] months={months}")

    for m in months:
        parts = month_parts_from_index(m)
        if not parts:
            print(f"[{m}] no parts in index.json -> skip")
            continue

        # Charger toutes les lignes du mois
        incoming: List[dict] = []
        for part_path in parts:
            if not part_path.exists():
                print(f"[{m}] missing file: {part_path} -> skip")
                continue
            for obj in iter_ndjson_lines(part_path):
                incoming.append(normalize_incoming_record(obj))

        if not incoming:
            print(f"[{m}] 0 records -> skip")
            continue

        # period string (Zoho) = la Period de la 1ère ligne
        period_zoho = str(incoming[0]["Period"])
        print(f"[{m}] loaded records={len(incoming)} period={period_zoho}")

        # Charger existants (Key -> (ID, RowHash)) pour ce Period
        existing = zc_get_all_records_for_period(
            access_token=access,
            period_zoho=period_zoho,
            fields=["ID", "Key", "RowHash"],
        )
        print(f"[{m}] existing keys={len(existing)}")

        to_insert: List[dict] = []
        to_update: List[dict] = []
        skipped = 0

        for rec in incoming:
            key = rec["Key"]
            rh = rec["RowHash"]
            hit = existing.get(key)

            if not hit:
                # INSERT: on doit envoyer champs du form (pas "ID")
                # IMPORTANT: Creator v2.1 form insert n’accepte pas des champs inconnus.
                # Ici on envoie tout sauf "ID" (absent) – ok.
                to_insert.append(rec)
            else:
                rec_id, old_rh = hit
                if old_rh and old_rh == rh:
                    skipped += 1
                else:
                    # UPDATE: il faut inclure ID
                    upd = dict(rec)
                    upd["ID"] = rec_id
                    to_update.append(upd)

        print(f"[{m}] plan: insert={len(to_insert)} update={len(to_update)} skip={skipped}")

        # ---- INSERT batches
        for batch in chunked(to_insert, BATCH_SIZE):
            print(f"[{m}] ADD batch size={len(batch)}")
            _ = zc_add_records(access, batch)
            time.sleep(SLEEP_SECONDS)

        # ---- UPDATE batches
        for batch in chunked(to_update, BATCH_SIZE):
            print(f"[{m}] UPDATE batch size={len(batch)}")
            _ = zc_update_records(access, batch)
            time.sleep(SLEEP_SECONDS)

        print(f"[{m}] done")

    print("[SYNC] OK")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sleep", type=float, default=SLEEP_SECONDS, help="Sleep seconds between API calls")
    ap.add_argument("--batch", type=int, default=BATCH_SIZE, help="Batch size (max 200 recommended)")
    args = ap.parse_args()

    global SLEEP_SECONDS, BATCH_SIZE
    SLEEP_SECONDS = float(args.sleep)
    BATCH_SIZE = int(args.batch)

    if BATCH_SIZE > 200:
        raise SystemExit("Batch size must be <= 200")

    sync_last_3_months()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
