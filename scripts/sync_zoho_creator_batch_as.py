from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


# ---------------------------
# Zoho DC routing
# ---------------------------
def zoho_apis_domain(dc: str) -> str:
    dc = (dc or "com").strip().lower()
    if dc == "us":
        dc = "com"
    return f"www.zohoapis.{dc}"


def zoho_accounts_domain(dc: str) -> str:
    dc = (dc or "com").strip().lower()
    if dc == "us":
        dc = "com"
    return f"accounts.zoho.{dc}"


@dataclass
class ZohoConfig:
    dc: str
    client_id: str
    client_secret: str
    refresh_token: str
    owner: str
    app_link_name: str
    form_link_name: str
    report_link_name: str

    @property
    def creator_base(self) -> str:
        return f"https://{zoho_apis_domain(self.dc)}/creator/v2.1"

    @property
    def oauth_token_url(self) -> str:
        return f"https://{zoho_accounts_domain(self.dc)}/oauth/v2/token"


class ZohoCreatorClient:
    """
    Robuste:
    - On évite DELETE bulk via API en routine.
    - GET avec criteria peut renvoyer 400 code=9280 => traité comme "0 record".
    - Pagination parfois instable:
        * certains DC supportent record_cursor
        * d'autres supportent page/per_page
    """

    def __init__(self, cfg: ZohoConfig, timeout_s: int = 180) -> None:
        self.cfg = cfg
        self.timeout_s = timeout_s
        self.session = requests.Session()
        self._access_token: Optional[str] = None
        self._access_token_expire_at: float = 0.0

    def _refresh_access_token(self) -> str:
        params = {
            "refresh_token": self.cfg.refresh_token,
            "client_id": self.cfg.client_id,
            "client_secret": self.cfg.client_secret,
            "grant_type": "refresh_token",
        }
        r = self.session.post(self.cfg.oauth_token_url, params=params, timeout=self.timeout_s)
        r.raise_for_status()
        data = r.json()
        token = data.get("access_token")
        if not token:
            raise RuntimeError(f"Zoho token refresh failed: {data}")
        expires_in = float(data.get("expires_in", 3000))
        self._access_token = token
        self._access_token_expire_at = time.time() + max(60.0, expires_in - 60.0)
        return token

    def _auth_header(self) -> Dict[str, str]:
        if (not self._access_token) or (time.time() >= self._access_token_expire_at):
            self._refresh_access_token()
        return {"Authorization": f"Zoho-oauthtoken {self._access_token}"}

    @staticmethod
    def _try_parse_json(text: str) -> Dict[str, Any] | None:
        try:
            return json.loads(text)
        except Exception:
            return None

    def _req(
        self,
        method: str,
        url: str,
        *,
        params: Dict[str, Any] | None = None,
        json_body: Any | None = None,
    ) -> Dict[str, Any]:
        last_text = ""
        for attempt in range(1, 7):
            r = self.session.request(
                method,
                url,
                headers={**self._auth_header(), "Accept": "application/json"},
                params=params,
                json=json_body,
                timeout=self.timeout_s,
            )
            last_text = r.text or ""

            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(60.0, 3.0 * attempt))
                continue

            if r.status_code == 400 and method.upper() == "GET":
                j = self._try_parse_json(last_text)
                if isinstance(j, dict):
                    code = str(j.get("code") or "")
                    msg = (j.get("message") or j.get("description") or "")
                    if code == "9280" and "No records found" in str(msg):
                        return {"data": [], "info": {}}

            if r.status_code >= 400:
                raise RuntimeError(f"Zoho API error {r.status_code} {method} {url}: {last_text[:900]}")

            if last_text.strip() == "":
                return {}

            try:
                return r.json()
            except Exception:
                return {"_raw": last_text}

        raise RuntimeError(f"Zoho API failed after retries: {method} {url}: {last_text[:900]}")

    def add_records(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not records:
            return {}
        url = f"{self.cfg.creator_base}/data/{self.cfg.owner}/{self.cfg.app_link_name}/form/{self.cfg.form_link_name}"
        return self._req("POST", url, json_body={"data": records})

    def delete_record_by_id(self, record_id: str) -> Dict[str, Any]:
        url = f"{self.cfg.creator_base}/data/{self.cfg.owner}/{self.cfg.app_link_name}/report/{self.cfg.report_link_name}/{record_id}"
        return self._req("DELETE", url)

    def fetch_records_page(
        self,
        *,
        criteria: str = "",
        fields: str = "ID",
        per_page: int = 200,
        page: Optional[int] = None,
        record_cursor: Optional[str] = None,
    ) -> Dict[str, Any]:
        url = f"{self.cfg.creator_base}/data/{self.cfg.owner}/{self.cfg.app_link_name}/report/{self.cfg.report_link_name}"

        params: Dict[str, Any] = {"fields": fields}
        if criteria:
            params["criteria"] = criteria

        if record_cursor is not None:
            params["max_records"] = str(int(per_page))
            params["record_cursor"] = record_cursor
            return self._req("GET", url, params=params)

        if page is None:
            params["max_records"] = str(int(per_page))
            return self._req("GET", url, params=params)

        params["page"] = str(int(page))
        params["per_page"] = str(int(per_page))
        return self._req("GET", url, params=params)

    @staticmethod
    def _extract_data_list(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not isinstance(resp, dict):
            return []
        if "data" in resp and isinstance(resp["data"], list):
            return resp["data"]
        if "response" in resp and isinstance(resp["response"], dict) and isinstance(resp["response"].get("data"), list):
            return resp["response"]["data"]
        return []

    @staticmethod
    def _extract_info(resp: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(resp, dict):
            return {}
        if "info" in resp and isinstance(resp["info"], dict):
            return resp["info"]
        if "response" in resp and isinstance(resp["response"], dict) and isinstance(resp["response"].get("info"), dict):
            return resp["response"]["info"]
        return {}

    def iter_records(
        self,
        *,
        criteria: str,
        fields: str,
        per_page: int = 200,
        throttle_s: float = 0.0,
        hard_max_pages: int = 2000,
    ) -> Iterable[Dict[str, Any]]:
        cursor: Optional[str] = None
        first = True
        pages = 0

        while True:
            pages += 1
            if pages > hard_max_pages:
                raise RuntimeError("iter_records: too many pages, aborting for safety.")

            resp = self.fetch_records_page(
                criteria=criteria,
                fields=fields,
                per_page=per_page,
                page=None,
                record_cursor=cursor if not first else None,
            )
            data = self._extract_data_list(resp)
            info = self._extract_info(resp)

            for r in data:
                yield r

            next_cursor = info.get("record_cursor") or info.get("next_record_cursor") or info.get("next_cursor")

            if next_cursor:
                cursor = str(next_cursor)
                first = False
                if throttle_s > 0:
                    time.sleep(throttle_s)
                continue

            if len(data) < per_page:
                return

            break

        page = 1
        while True:
            pages += 1
            if pages > hard_max_pages:
                raise RuntimeError("iter_records(page): too many pages, aborting for safety.")

            resp = self.fetch_records_page(
                criteria=criteria,
                fields=fields,
                per_page=per_page,
                page=page,
                record_cursor=None,
            )
            data = self._extract_data_list(resp)

            for r in data:
                yield r

            if not data or len(data) < per_page:
                return

            page += 1
            if throttle_s > 0:
                time.sleep(throttle_s)


# ---------------------------
# Local repo data helpers
# ---------------------------
def load_index(index_path: Path) -> Dict[str, Any]:
    return json.loads(index_path.read_text(encoding="utf-8"))


def sorted_months(index: Dict[str, Any]) -> List[str]:
    months = list((index.get("months") or {}).keys())
    months.sort()
    return months


def last_n_previous_months(months_sorted: List[str], n: int) -> List[str]:
    """
    "Deux derniers mois précédents" = on EXCLUT le dernier mois de l'index,
    puis on prend les N derniers dans le reste.
    Ex: [..., 202601, 202602, 202603] et n=2 => [202601, 202602]
    """
    if n <= 0:
        return []
    if len(months_sorted) <= 1:
        return []
    base = months_sorted[:-1]
    if not base:
        return []
    return base[-n:] if len(base) >= n else base[:]


def load_json_file(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise RuntimeError(f"Unable to load JSON file: {path} ({e})") from e


def load_ou_map(repo_root: Path) -> Dict[str, Dict[str, str]]:
    candidates = [
        repo_root / "docs" / "data_as" / "ou_map_as.json.gz",
        repo_root / "docs" / "data_as" / "ou_map_as.json",
        repo_root / "docs" / "data_as" / "ou_map.json.gz",
        repo_root / "docs" / "data_as" / "ou_map.json",
        repo_root / "docs" / "data" / "ou_map.json.gz",
        repo_root / "docs" / "data" / "ou_map.json",
    ]

    for p in candidates:
        if not p.exists():
            continue
        try:
            if p.suffix == ".gz":
                raw = gzip.open(p, "rb").read()
                return json.loads(raw.decode("utf-8"))
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue

    raise FileNotFoundError(
        "Missing docs/data_as/ou_map_as.json(.gz) and docs/data/ou_map.json(.gz). "
        "Run build_ou_map.py first."
    )


def iter_ndjson_with_raw(file_path: Path) -> List[Tuple[Dict[str, Any], str]]:
    out: List[Tuple[Dict[str, Any], str]] = []
    with file_path.open("r", encoding="utf-8") as f:
        for ln in f:
            raw = ln.strip()
            if not raw:
                continue
            out.append((json.loads(raw), raw))
    return out


def chunk_records(records: List[Dict[str, Any]], size: int = 200) -> List[List[Dict[str, Any]]]:
    return [records[i:i + size] for i in range(0, len(records), size)]


def load_state(state_path: Path) -> Dict[str, Any]:
    if not state_path.exists():
        return {"done_months": {}, "last_run_at": None}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {"done_months": {}, "last_run_at": None}


def save_state(state_path: Path, state: Dict[str, Any]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def reset_state_file(state_path: Path) -> None:
    save_state(state_path, {"done_months": {}, "last_run_at": None})


# ---------------------------
# Transform helpers
# ---------------------------
def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def _to_number(v: Any) -> float:
    if v is None:
        return 0.0
    if isinstance(v, bool):
        return float(int(v))
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if not s or s.lower() == "null":
            return 0.0
        s = s.replace(",", ".")
        try:
            return float(s)
        except Exception:
            return 0.0
    return 0.0


def _number_out(v: float) -> int | float:
    if float(v).is_integer():
        return int(v)
    return v


def _sum_by_link(row: Dict[str, Any], *links: str) -> int | float:
    total = 0.0
    for link in links:
        total += _to_number(row.get(link))
    return _number_out(total)


def _normalize_org3_name(org3: str) -> str:
    s = str(org3 or "").strip()
    if not s:
        return ""

    if len(s) > 3 and s[2] == " ":
        s = s[3:].strip()

    suffixes = [
        " Zone de Santé",
        " Zone de Sante",
    ]
    for suf in suffixes:
        if s.endswith(suf):
            s = s[: -len(suf)].strip()
            break

    return s


def antenne_from(org2: str, org3: str, antenne_rules: Dict[str, Dict[str, str]]) -> str:
    province = str(org2 or "").strip()
    zone_raw = str(org3 or "").strip()
    zone = _normalize_org3_name(zone_raw)

    if not province or not zone:
        return ""

    province_rules = antenne_rules.get(province)
    if not isinstance(province_rules, dict):
        return ""

    val = province_rules.get(zone)
    if val:
        return str(val).strip()

    val = province_rules.get(zone_raw)
    if val:
        return str(val).strip()

    return ""


GLOBAL_SUM_SPECS: Dict[str, List[str]] = {
    "BCG_0_11": [
        "BCG fixe1", "BCG fixe2",
        "BCG avancé1", "BCG avancé2",
        "BCG mobile1", "BCG mobile2",
    ],
    "DTC1_0_11": [
        "Penta1 fixe1", "Penta1 fixe2",
        "Penta1 avancé1", "Penta1 avancé2",
        "Penta1 mobile1", "Penta1 mobile2",
    ],
    "DTC2_0_11": [
        "Penta2 fixe1", "Penta2 fixe2",
        "Penta2 avancé1", "Penta2 avancé2",
        "Penta2 mobile1", "Penta2 mobile2",
    ],
    "DTC3_0_11": [
        "Penta3 fixe1", "Penta3 fixe2",
        "Penta3 avancé1", "Penta3 avancé2",
        "Penta3 mobile1", "Penta3 mobile2",
    ],
    "VPO3_0_11": [
        "VPO3 fixe1", "VPO3 fixe2",
        "VPO3 avancé1", "VPO3 avancé2",
        "VPO3 mobile1", "VPO3 mobile2",
    ],
    "VPI1_0_11": [
        "VPI1 fixe1", "VPI1 fixe2",
        "VPI1 avancé1", "VPI1 avancé2",
        "VPI1 mobile1", "VPI1 mobile2",
    ],
    "VPI2_0_11": [
        "VPI2 fixe1", "VPI2 fixe2",
        "VPI2 avancé1", "VPI2 avancé2",
        "VPI2 mobile1", "VPI2 mobile2",
    ],
    "ROTA3_0_11": [
        "ROTA3 fixe", "ROTA3 avancé", "ROTA3 mobile",
    ],
    "PCV13_3_0_11": [
        "PCV13 fixe1", "PCV13 fixe2",
        "PCV13 avancé1", "PCV13 avancé2",
        "PCV13 mobile1", "PCV13 mobile2",
    ],
    "VAR1_0_11": [
        "VAR1 fixe1", "VAR1 fixe2",
        "VAR1 avancé1", "VAR1 avancé2",
        "VAR1 mobile1", "VAR1 mobile2",
    ],
    "VAR2_total": [
        "VAR2 fixe1", "VAR2 fixe2",
        "VAR2 avancé",
        "VAR2 mobile1", "VAR2 mobile2",
    ],
    "VAR2_0_11": [
        "VAR2 0-11 mois fixe", "VAR2 0-11 mois avancée", "VAR2 0-11 mois mobile",
    ],
    "VPO0_0_11": [
        "VPO0 0-11 mois fixe1", "VPO0 0-11 mois fixe2",
        "VPO0 0-11 mois avancée1", "VPO0 0-11 mois avancée2",
        "VPO0 0-11 mois mobile1", "VPO0 0-11 mois mobile2",
    ],
    "VPO1_0_11": [
        "VPO1 0-11 mois fixe1", "VPO1 0-11 mois fixe2",
        "VPO1 0-11 mois avancée1", "VPO1 0-11 mois avancée2",
        "VPO1 0-11 mois mobile1", "VPO1 0-11 mois mobile2",
    ],
    "VPO2_0_11": [
        "VPO2 0-11 mois fixe1", "VPO2 0-11 mois fixe2",
        "VPO2 0-11 mois avancée1", "VPO2 0-11 mois avancée2",
        "VPO2 0-11 mois mobile1", "VPO2 0-11 mois mobile2",
    ],
    "PCV13_1_0_11": [
        "PCV13(1) 0-11 mois fixe1", "PCV13(1) 0-11 mois fixe2",
        "PCV13(1) 0-11 mois avancée1", "PCV13(1) 0-11 mois avancée2",
        "PCV13(1) 0-11 mois mobile1", "PCV13(1) 0-11 mois mobile2",
    ],
    "PCV13_2_0_11": [
        "PCV13(2) 0-11 mois fixe1", "PCV13(2) 0-11 mois fixe2",
        "PCV13(2) 0-11 mois avancée1", "PCV13(2) 0-11 mois avancée2",
        "PCV13(2) 0-11 mois mobile1", "PCV13(2) 0-11 mois mobile2",
    ],
    "ROTA1_0_11": [
        "ROTA1 0-11 mois fixe", "ROTA1 0-11 mois avancée", "ROTA1 0-11 mois mobile",
    ],
    "ROTA2_0_11": [
        "ROTA2 0-11 mois fixe", "ROTA2 0-11 mois avancée", "ROTA2 0-11 mois mobile",
    ],
    "VAP1_0_11": [
        "VAP1 0-11 mois fixe", "VAP1 0-11 mois avancée", "VAP1 0-11 mois mobile",
    ],
    "VAP2_0_11": [
        "VAP2 0-11 mois fixe", "VAP2 0-11 mois avancée", "VAP2 0-11 mois mobile",
    ],
    "VAP3_0_11": [
        "VAP3 0-11 mois fixe", "VAP3 0-11 mois avancée", "VAP3 0-11 mois mobile",
    ],
    "VAP4_12_23": [
        "VAP4 12-23 mois fixe", "VAP4 12-23 mois avancée", "VAP4 12-23 mois mobile",
    ],
    "VAA_0_11": [
        "VAA fixe1", "VAA fixe2",
        "VAA avancé1", "VAA avancé2",
        "VAA mobile1", "VAA mobile2",
    ],
    "Td_2_plus": [
        "Td 2", "Td 3", "Td 4", "Td 5",
    ],
}


def build_excluded_raw_links(zoho_rename_map: Dict[str, str]) -> set[str]:
    excluded: set[str] = set()
    for labels in GLOBAL_SUM_SPECS.values():
        for label in labels:
            excluded.add(zoho_rename_map.get(label, label))
    return excluded


def build_zoho_record(
    row: Dict[str, Any],
    raw_line: str,
    ou_map: Dict[str, Dict[str, str]],
    zoho_rename_map: Dict[str, str],
    antenne_rules: Dict[str, Dict[str, str]],
    excluded_raw_links: set[str],
) -> Dict[str, Any]:
    ou = str(row.get("OrgUnit") or "").strip()
    period = str(row.get("Period") or "").strip()

    if not ou or not period:
        return {}

    meta = ou_map.get(ou) or {}
    rec: Dict[str, Any] = {}

    rec["Org4_UID"] = ou
    rec["Period"] = period
    rec["Key"] = f"{ou}|{period}"

    rh = row.get("RowHash")
    rec["RowHash"] = str(rh).strip() if rh else md5_hex(raw_line)

    rec["Org2"] = meta.get("Org2", "")
    rec["Org3"] = meta.get("Org3", "")
    rec["Org4"] = meta.get("Org4", "")
    rec["Antenne"] = antenne_from(rec["Org2"], rec["Org3"], antenne_rules)

    for k, v in row.items():
        if k in ("OrgUnit", "Period", "Key", "Org4_UID", "Org2", "Org3", "Org4", "RowHash"):
            continue
        if k in excluded_raw_links:
            continue
        rec[k] = v

    for out_field, source_labels in GLOBAL_SUM_SPECS.items():
        source_links = [zoho_rename_map.get(label, label) for label in source_labels]
        rec[out_field] = _sum_by_link(row, *source_links)

    return rec


# ---------------------------
# PURGE helpers (manual only)
# ---------------------------
def purge_by_criteria_until_empty(
    client: ZohoCreatorClient,
    *,
    criteria: str,
    throttle_s: float,
    batch_limit: int,
    hard_limit_loops: int = 200000,
) -> int:
    deleted = 0
    loops = 0
    while True:
        loops += 1
        if loops > hard_limit_loops:
            raise RuntimeError("purge_by_criteria_until_empty: too many loops, aborting for safety.")

        ids: List[str] = []
        for r in client.iter_records(criteria=criteria, fields="ID", per_page=min(200, batch_limit), throttle_s=0.0):
            rid = r.get("ID") or r.get("id")
            if rid:
                ids.append(str(rid))
            if len(ids) >= batch_limit:
                break

        if not ids:
            break

        for rid in ids:
            client.delete_record_by_id(rid)
            deleted += 1
            if throttle_s > 0:
                time.sleep(throttle_s)

        if throttle_s > 0:
            time.sleep(min(0.5, throttle_s))

    return deleted


def purge_all_records(client: ZohoCreatorClient, *, throttle_s: float, batch_limit: int) -> int:
    return purge_by_criteria_until_empty(client, criteria="", throttle_s=throttle_s, batch_limit=batch_limit)


# ---------------------------
# ADD-only import for a month (batch) with safe fallback
# ---------------------------
def _robust_add_records(
    client: ZohoCreatorClient,
    records: List[Dict[str, Any]],
    *,
    throttle_s: float,
    depth: int = 0,
    max_depth: int = 10,
) -> Tuple[int, int]:
    if not records:
        return (0, 0)

    try:
        client.add_records(records)
        if throttle_s > 0:
            time.sleep(throttle_s)
        return (len(records), 0)
    except Exception as e:
        if len(records) == 1 or depth >= max_depth:
            print(f"Add failed (record skipped). reason={e}", flush=True)
            return (0, len(records))

        mid = len(records) // 2
        left = records[:mid]
        right = records[mid:]

        ins_l, fail_l = _robust_add_records(client, left, throttle_s=throttle_s, depth=depth + 1, max_depth=max_depth)
        ins_r, fail_r = _robust_add_records(client, right, throttle_s=throttle_s, depth=depth + 1, max_depth=max_depth)
        return (ins_l + ins_r, fail_l + fail_r)


def insert_month_from_parts_add_only(
    client: ZohoCreatorClient,
    repo_root: Path,
    yyyymm: str,
    parts_meta: List[Dict[str, Any]],
    ou_map: Dict[str, Dict[str, str]],
    zoho_rename_map: Dict[str, str],
    antenne_rules: Dict[str, Dict[str, str]],
    excluded_raw_links: set[str],
    *,
    batch_size: int,
    throttle_s: float,
) -> Dict[str, Any]:
    month_dir = repo_root / "docs" / "data_as" / "monthly" / yyyymm

    total_local = 0
    skipped_no_key = 0
    inserted = 0
    failed = 0
    batches = 0

    for p in parts_meta:
        plain = p.get("plain")
        if not plain:
            continue

        fp = month_dir / str(plain)
        if not fp.exists():
            continue

        rows = iter_ndjson_with_raw(fp)

        zoho_recs: List[Dict[str, Any]] = []
        for obj, raw in rows:
            rec = build_zoho_record(
                row=obj,
                raw_line=raw,
                ou_map=ou_map,
                zoho_rename_map=zoho_rename_map,
                antenne_rules=antenne_rules,
                excluded_raw_links=excluded_raw_links,
            )
            if rec:
                zoho_recs.append(rec)
            else:
                skipped_no_key += 1

        total_local += len(zoho_recs)

        for batch in chunk_records(zoho_recs, batch_size):
            if not batch:
                continue
            batches += 1
            ins, fail = _robust_add_records(client, batch, throttle_s=throttle_s)
            inserted += ins
            failed += fail

    return {
        "month": yyyymm,
        "local_rows": total_local,
        "batches": batches,
        "inserted": inserted,
        "failed": failed,
        "skipped_no_key": skipped_no_key,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo_root", default=".", help="Repository root (default: .)")
    ap.add_argument("--index", default="docs/data_as/index.json")
    ap.add_argument("--state", default="docs/data_as/zoho_sync_state.json")

    ap.add_argument(
        "--refresh_last_n",
        type=int,
        default=2,
        help="ADD-ONLY for the last N PREVIOUS months (M-1..). Recommended=2.",
    )
    ap.add_argument("--batch_size", type=int, default=200)
    ap.add_argument("--throttle_seconds", type=float, default=1.5)

    ap.add_argument("--purge_all", action="store_true")
    ap.add_argument("--purge_only", action="store_true")
    ap.add_argument("--purge_batch_limit", type=int, default=200)

    ap.add_argument("--import_historical", action="store_true", help="Import all historical months (add only).")

    args = ap.parse_args()

    cfg = ZohoConfig(
        dc=os.environ.get("ZOHO_DC", "com"),
        client_id=os.environ["ZOHO_CLIENT_ID"],
        client_secret=os.environ["ZOHO_CLIENT_SECRET"],
        refresh_token=os.environ["ZOHO_REFRESH_TOKEN"],
        owner=os.environ.get("ZOHO_OWNER", "drcongo"),
        app_link_name=os.environ.get("ZOHO_APP_LINK", "vaccination-de-routine-dhis2-rdc"),
        form_link_name=os.environ.get("ZOHO_FORM_LINK", "Donn_es_PEV_AS"),
        report_link_name=os.environ.get("ZOHO_REPORT_LINK", "Donn_es_PEV_AS_Report"),
    )

    repo_root = Path(args.repo_root).resolve()
    index_path = repo_root / args.index
    state_path = repo_root / args.state

    rename_map_path = repo_root / "docs" / "config" / "rename_map.json"
    antenne_rules_path = repo_root / "docs" / "config" / "antenne_rules.json"

    zoho_rename_map = load_json_file(rename_map_path)
    antenne_rules = load_json_file(antenne_rules_path)
    excluded_raw_links = build_excluded_raw_links(zoho_rename_map)

    client = ZohoCreatorClient(cfg)

    if args.purge_all:
        print("PURGE ALL: deleting all records in Zoho report until empty...", flush=True)
        deleted = purge_all_records(
            client,
            throttle_s=args.throttle_seconds,
            batch_limit=args.purge_batch_limit,
        )
        print(f"PURGE ALL done. deleted={deleted}", flush=True)

        remaining = []
        for r in client.iter_records(criteria="", fields="ID", per_page=1, throttle_s=0.0):
            rid = r.get("ID") or r.get("id")
            if rid:
                remaining.append(str(rid))
            break
        if remaining:
            raise RuntimeError(f"PURGE ALL verification failed: still found records (example ID={remaining[0]}).")

        print("PURGE ALL verification OK: report is empty.", flush=True)
        print("Resetting zoho_sync_state.json ...", flush=True)
        reset_state_file(state_path)

        if args.purge_only:
            print("PURGE ONLY => exit.", flush=True)
            return 0

    index = load_index(index_path)
    months_sorted = sorted_months(index)
    if not months_sorted:
        print("No months found in index.json")
        return 0

    months_obj = index.get("months") or {}

    ou_map = load_ou_map(repo_root)
    print(f"OU_MAP loaded: org4={len(ou_map)}", flush=True)
    print(f"rename_map loaded: {len(zoho_rename_map)} fields", flush=True)
    print(f"antenne_rules loaded: {len(antenne_rules)} provinces", flush=True)
    print(f"excluded raw links for Zoho payload: {len(excluded_raw_links)}", flush=True)

    state = load_state(state_path)
    done_months: Dict[str, Any] = state.get("done_months") or {}

    refresh_months = last_n_previous_months(months_sorted, args.refresh_last_n)

    print(f"Months in index: {months_sorted[0]} -> {months_sorted[-1]} (count={len(months_sorted)})", flush=True)
    print(f"ADD-only months (previous N={args.refresh_last_n}) = {refresh_months}", flush=True)
    print(
        f"batch_size={args.batch_size} throttle_seconds={args.throttle_seconds} import_historical={args.import_historical}",
        flush=True,
    )

    for m in refresh_months:
        parts = (months_obj.get(m) or {}).get("parts") or []
        if not parts:
            print(f"[{m}] skip: no parts", flush=True)
            continue

        print(f"[{m}] add-only import (assumes Zoho month already deleted by Deluge)...", flush=True)
        stats = insert_month_from_parts_add_only(
            client=client,
            repo_root=repo_root,
            yyyymm=m,
            parts_meta=parts,
            ou_map=ou_map,
            zoho_rename_map=zoho_rename_map,
            antenne_rules=antenne_rules,
            excluded_raw_links=excluded_raw_links,
            batch_size=args.batch_size,
            throttle_s=args.throttle_seconds,
        )
        print(
            f"[{m}] local={stats['local_rows']} batches={stats['batches']} "
            f"inserted={stats['inserted']} failed={stats['failed']} skipped_no_key={stats['skipped_no_key']}",
            flush=True,
        )
        done_months[m] = {
            "done": True,
            "refreshed": True,
            "last_sync": time.time(),
            "mode": "daily_add_only_previous",
        }

    if args.import_historical:
        for m in months_sorted:
            if m in refresh_months:
                continue

            parts = (months_obj.get(m) or {}).get("parts") or []
            if not parts:
                continue

            if done_months.get(m, {}).get("done") is True:
                print(f"[{m}] skip: already done (historical)", flush=True)
                continue

            print(f"[{m}] import historical month (add only)...", flush=True)
            stats = insert_month_from_parts_add_only(
                client=client,
                repo_root=repo_root,
                yyyymm=m,
                parts_meta=parts,
                ou_map=ou_map,
                zoho_rename_map=zoho_rename_map,
                antenne_rules=antenne_rules,
                excluded_raw_links=excluded_raw_links,
                batch_size=args.batch_size,
                throttle_s=args.throttle_seconds,
            )
            print(
                f"[{m}] local={stats['local_rows']} batches={stats['batches']} "
                f"inserted={stats['inserted']} failed={stats['failed']} skipped_no_key={stats['skipped_no_key']}",
                flush=True,
            )
            done_months[m] = {
                "done": True,
                "refreshed": False,
                "last_sync": time.time(),
                "mode": "historical_add_only",
            }

    state["done_months"] = done_months
    state["last_run_at"] = time.time()
    save_state(state_path, state)

    print("OK: Zoho sync completed.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
