from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional, Iterable

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
    - Certains DC n'acceptent pas DELETE bulk avec criteria (code 1060) => on ne l'utilise pas en routine.
    - Certains renvoient 400 code=9280 quand aucun record ne matche un criteria (GET) => on traite comme "0 record".
    - Pagination parfois instable:
        * certains DC supportent record_cursor
        * d'autres supportent page/per_page
      => iter_records essaie cursor, sinon fallback page/per_page.
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

            # retry transient
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(60.0, 3.0 * attempt))
                continue

            # Zoho: GET with criteria can return 400 code=9280 = "No records found"
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

    # ---- Records APIs
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
        """
        Fetch records from report.
        Compat DC:
          - Cursor mode: max_records + record_cursor (no page/per_page)
          - Page mode: page + per_page (no record_cursor)
        """
        url = f"{self.cfg.creator_base}/data/{self.cfg.owner}/{self.cfg.app_link_name}/report/{self.cfg.report_link_name}"

        params: Dict[str, Any] = {"fields": fields}
        if criteria:
            params["criteria"] = criteria

        # Cursor mode
        if record_cursor is not None:
            params["max_records"] = str(int(per_page))
            params["record_cursor"] = record_cursor
            return self._req("GET", url, params=params)

        # First page attempt for cursor mode (some DC returns cursor without needing to pass it)
        if page is None:
            params["max_records"] = str(int(per_page))
            return self._req("GET", url, params=params)

        # Page/per_page mode
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
        """
        Iterate all records matching criteria.
        Strategy:
          1) Try cursor mode
          2) If cursor absent, fallback to page/per_page mode
        """
        # --- cursor mode
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
                record_cursor=cursor if not first else None,  # first call without cursor
            )
            data = self._extract_data_list(resp)
            info = self._extract_info(resp)

            for r in data:
                yield r

            # find next cursor
            next_cursor = (
                info.get("record_cursor")
                or info.get("next_record_cursor")
                or info.get("next_cursor")
            )

            # If we got a cursor, continue cursor mode
            if next_cursor:
                cursor = str(next_cursor)
                first = False
                if throttle_s > 0:
                    time.sleep(throttle_s)
                continue

            # No cursor
            # If the first call returned < per_page => we're done (no more pages)
            if len(data) < per_page:
                return

            # If first call returned exactly per_page but no cursor => fallback to page/per_page mode
            break

        # --- fallback page/per_page
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

    def update_record_by_id(self, record_id: str, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        ✅ FIX IMPORTANT:
        Update by ID must use REPORT endpoint in v2.1:
          PATCH .../report/<report>/<record_id>
        (Form endpoint causes 404 "Invalid API URL format" on your tenant)
        """
        url = f"{self.cfg.creator_base}/data/{self.cfg.owner}/{self.cfg.app_link_name}/report/{self.cfg.report_link_name}/{record_id}"
        return self._req("PATCH", url, json_body={"data": record})


# ---------------------------
# Local repo data helpers
# ---------------------------
def load_index(index_path: Path) -> Dict[str, Any]:
    return json.loads(index_path.read_text(encoding="utf-8"))


def sorted_months(index: Dict[str, Any]) -> List[str]:
    months = list((index.get("months") or {}).keys())
    months.sort()
    return months


def last_n_months(months_sorted: List[str], n: int = 3) -> List[str]:
    if n <= 0:
        return []
    return months_sorted[-n:] if len(months_sorted) >= n else months_sorted[:]


def load_ou_map(repo_root: Path) -> Dict[str, Dict[str, str]]:
    gz_path = repo_root / "docs" / "data" / "ou_map.json.gz"
    js_path = repo_root / "docs" / "data" / "ou_map.json"

    if gz_path.exists():
        raw = gzip.open(gz_path, "rb").read()
        return json.loads(raw.decode("utf-8"))

    if js_path.exists():
        return json.loads(js_path.read_text(encoding="utf-8"))

    raise FileNotFoundError("Missing docs/data/ou_map.json(.gz). Run build_ou_map.py first.")


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


def period_yyyymm_to_zoho(yyyymm: str) -> str:
    MMM = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    y = int(yyyymm[:4])
    m = int(yyyymm[4:6])
    return f"01-{MMM[m-1]}-{y:04d}"


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
# Transform: NDJSON row -> Zoho record
# ---------------------------
def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def build_zoho_record(row: Dict[str, Any], raw_line: str, ou_map: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    ou = str(row.get("OrgUnit") or "").strip()
    period = str(row.get("Period") or "").strip()

    if not ou or not period:
        return {}

    meta = ou_map.get(ou) or {}
    rec: Dict[str, Any] = {}

    rec["Org5_UID"] = ou
    rec["Period"] = period
    rec["Key"] = f"{ou}|{period}"

    rh = row.get("RowHash")
    rec["RowHash"] = str(rh).strip() if rh else md5_hex(raw_line)

    rec["Org2"] = meta.get("Org2", "")
    rec["Org3"] = meta.get("Org3", "")
    rec["Org4"] = meta.get("Org4", "")
    rec["Org5"] = meta.get("Org5", "")

    for k, v in row.items():
        if k in ("OrgUnit", "Period", "Key", "Org5_UID", "Org2", "Org3", "Org4", "Org5", "RowHash"):
            continue
        rec[k] = v

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
    """
    Safe purge:
      - fetch N IDs
      - delete them
      - repeat
    """
    deleted = 0
    loops = 0
    while True:
        loops += 1
        if loops > hard_limit_loops:
            raise RuntimeError("purge_by_criteria_until_empty: too many loops, aborting for safety.")

        ids: List[str] = []
        for r in client.iter_records(
            criteria=criteria,
            fields="ID",
            per_page=min(200, batch_limit),
            throttle_s=0.0,
        ):
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
# Incremental sync (RowHash compare)
# ---------------------------
def load_local_month_records(
    repo_root: Path,
    yyyymm: str,
    parts_meta: List[Dict[str, Any]],
    ou_map: Dict[str, Dict[str, str]],
) -> Tuple[Dict[str, Dict[str, Any]], int]:
    """
    Returns:
      local_by_key: Key -> ZohoRecord
      skipped_no_key
    """
    month_dir = repo_root / "docs" / "data" / "monthly" / yyyymm
    local_by_key: Dict[str, Dict[str, Any]] = {}
    skipped = 0

    for p in parts_meta:
        plain = p.get("plain")
        if not plain:
            continue
        fp = month_dir / str(plain)
        if not fp.exists():
            continue

        rows = iter_ndjson_with_raw(fp)
        for obj, raw in rows:
            rec = build_zoho_record(obj, raw, ou_map)
            if not rec:
                skipped += 1
                continue
            key = str(rec.get("Key") or "").strip()
            if not key:
                skipped += 1
                continue
            # Key unique -> overwrite ok
            local_by_key[key] = rec

    return local_by_key, skipped


def fetch_remote_month_index(
    client: ZohoCreatorClient,
    yyyymm: str,
    *,
    per_page: int,
    throttle_s: float,
) -> Dict[str, Tuple[str, str]]:
    """
    Returns:
      remote_by_key: Key -> (ID, RowHash)
    """
    period = period_yyyymm_to_zoho(yyyymm)
    criteria = f'(Period == "{period}")'
    remote_by_key: Dict[str, Tuple[str, str]] = {}

    fields = "ID,Key,RowHash"
    for r in client.iter_records(criteria=criteria, fields=fields, per_page=per_page, throttle_s=throttle_s):
        rid = r.get("ID") or r.get("id")
        key = r.get("Key")
        rh = r.get("RowHash") or ""
        if rid and key:
            remote_by_key[str(key)] = (str(rid), str(rh))

    return remote_by_key


def incremental_sync_month(
    client: ZohoCreatorClient,
    repo_root: Path,
    yyyymm: str,
    parts_meta: List[Dict[str, Any]],
    ou_map: Dict[str, Dict[str, str]],
    *,
    batch_size: int,
    throttle_s: float,
    per_page: int,
) -> Dict[str, Any]:
    """
    Incremental:
      - compare Key/RowHash
      - add missing keys
      - update changed rowhash
    """
    local_by_key, skipped_no_key = load_local_month_records(repo_root, yyyymm, parts_meta, ou_map)
    remote_by_key = fetch_remote_month_index(client, yyyymm, per_page=per_page, throttle_s=throttle_s)

    to_add: List[Dict[str, Any]] = []
    to_update: List[Tuple[str, Dict[str, Any]]] = []

    for key, rec in local_by_key.items():
        loc_rh = str(rec.get("RowHash") or "")
        remote = remote_by_key.get(key)
        if not remote:
            to_add.append(rec)
            continue

        rid, rem_rh = remote
        if loc_rh and rem_rh and (loc_rh != str(rem_rh)):
            to_update.append((rid, rec))

    # Add in batches
    add_batches = 0
    add_records = 0
    for batch in chunk_records(to_add, batch_size):
        if not batch:
            continue
        client.add_records(batch)
        add_batches += 1
        add_records += len(batch)
        if throttle_s > 0:
            time.sleep(throttle_s)

    # Update one-by-one (tenant-compatible)
    upd_records = 0
    for rid, rec in to_update:
        rec2 = dict(rec)
        rec2.pop("ID", None)
        client.update_record_by_id(rid, rec2)
        upd_records += 1
        if throttle_s > 0:
            time.sleep(throttle_s)

    return {
        "month": yyyymm,
        "local_rows": len(local_by_key),
        "remote_rows": len(remote_by_key),
        "add_records": add_records,
        "add_batches": add_batches,
        "update_records": upd_records,
        "skipped_no_key": skipped_no_key,
    }


# ---------------------------
# Full import (historical) - add only
# ---------------------------
def insert_month_from_parts(
    client: ZohoCreatorClient,
    repo_root: Path,
    yyyymm: str,
    parts_meta: List[Dict[str, Any]],
    ou_map: Dict[str, Dict[str, str]],
    *,
    batch_size: int,
    throttle_s: float,
) -> Tuple[int, int, int]:
    month_dir = repo_root / "docs" / "data" / "monthly" / yyyymm
    batches = 0
    total = 0
    skipped_no_key = 0

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
            rec = build_zoho_record(obj, raw, ou_map)
            if rec:
                zoho_recs.append(rec)
            else:
                skipped_no_key += 1

        total += len(zoho_recs)

        for batch in chunk_records(zoho_recs, batch_size):
            if not batch:
                continue
            client.add_records(batch)
            batches += 1
            if throttle_s > 0:
                time.sleep(throttle_s)

    return batches, total, skipped_no_key


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo_root", default=".", help="Repository root (default: .)")
    ap.add_argument("--index", default="docs/data/index.json")
    ap.add_argument("--state", default="docs/data/zoho_sync_state.json")

    # Routine knobs
    ap.add_argument("--refresh_last_n", type=int, default=2, help="Incremental refresh for last N months (recommended 1-2).")
    ap.add_argument("--batch_size", type=int, default=200)
    ap.add_argument("--throttle_seconds", type=float, default=1.5)
    ap.add_argument("--per_page", type=int, default=200, help="Zoho report page size (usually <=200).")

    # Manual purge
    ap.add_argument("--purge_all", action="store_true")
    ap.add_argument("--purge_only", action="store_true")
    ap.add_argument("--purge_batch_limit", type=int, default=200)

    # Full import mode (initialization)
    ap.add_argument("--import_historical", action="store_true", help="Import all historical months (slow). Default: false.")

    args = ap.parse_args()

    cfg = ZohoConfig(
        dc=os.environ.get("ZOHO_DC", "com"),
        client_id=os.environ["ZOHO_CLIENT_ID"],
        client_secret=os.environ["ZOHO_CLIENT_SECRET"],
        refresh_token=os.environ["ZOHO_REFRESH_TOKEN"],
        owner=os.environ.get("ZOHO_OWNER", "drcongo"),
        app_link_name=os.environ.get("ZOHO_APP_LINK", "vaccination-de-routine-dhis2-rdc"),
        form_link_name=os.environ.get("ZOHO_FORM_LINK", "Donn_es_PEV_FOSA"),
        report_link_name=os.environ.get("ZOHO_REPORT_LINK", "Donn_es_PEV_FOSA_Report"),
    )

    repo_root = Path(args.repo_root).resolve()
    index_path = repo_root / args.index
    state_path = repo_root / args.state

    client = ZohoCreatorClient(cfg)

    # PURGE ALL (manual)
    if args.purge_all:
        print("PURGE ALL: deleting all records in Zoho report until empty...", flush=True)
        deleted = purge_all_records(
            client,
            throttle_s=args.throttle_seconds,
            batch_limit=args.purge_batch_limit,
        )
        print(f"PURGE ALL done. deleted={deleted}", flush=True)

        # verify empty
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

    # Load index
    index = load_index(index_path)
    months_sorted = sorted_months(index)
    if not months_sorted:
        print("No months found in index.json")
        return 0

    months_obj = index.get("months") or {}

    # Load OU map
    ou_map = load_ou_map(repo_root)
    print(f"OU_MAP loaded: level5={len(ou_map)}", flush=True)

    # State
    state = load_state(state_path)
    done_months: Dict[str, Any] = state.get("done_months") or {}

    refresh_months = last_n_months(months_sorted, args.refresh_last_n)

    print(f"Months in index: {months_sorted[0]} -> {months_sorted[-1]} (count={len(months_sorted)})", flush=True)
    print(f"Refresh months (incremental, RowHash) = {refresh_months}", flush=True)
    print(
        f"batch_size={args.batch_size} throttle_seconds={args.throttle_seconds} per_page={args.per_page} "
        f"import_historical={args.import_historical}",
        flush=True,
    )

    # 1) Incremental refresh for last N months (NO DELETE)
    for m in refresh_months:
        parts = (months_obj.get(m) or {}).get("parts") or []
        if not parts:
            print(f"[{m}] skip: no parts", flush=True)
            continue

        print(f"[{m}] incremental sync (compare RowHash, add/update only)...", flush=True)
        stats = incremental_sync_month(
            client=client,
            repo_root=repo_root,
            yyyymm=m,
            parts_meta=parts,
            ou_map=ou_map,
            batch_size=args.batch_size,
            throttle_s=args.throttle_seconds,
            per_page=args.per_page,
        )
        print(
            f"[{m}] local={stats['local_rows']} remote={stats['remote_rows']} "
            f"add={stats['add_records']} (batches={stats['add_batches']}) "
            f"update={stats['update_records']} skipped_no_key={stats['skipped_no_key']}",
            flush=True,
        )
        done_months[m] = {"done": True, "refreshed": True, "last_sync": time.time(), "mode": "incremental"}

    # 2) Historical import (only if requested)
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
            batches, total, skipped = insert_month_from_parts(
                client, repo_root, m, parts, ou_map,
                batch_size=args.batch_size,
                throttle_s=args.throttle_seconds,
            )
            print(f"[{m}] inserted_records={total} batches={batches} skipped_no_key={skipped}", flush=True)
            done_months[m] = {"done": True, "refreshed": False, "last_sync": time.time(), "mode": "historical_add_only"}

    state["done_months"] = done_months
    state["last_run_at"] = time.time()
    save_state(state_path, state)

    print("OK: Zoho sync completed.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
