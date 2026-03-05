from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

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
    - Certains DC n'acceptent pas DELETE bulk avec criteria (code 1060).
    - Certains renvoient 400 code=9280 quand aucun record ne matche un criteria (GET).
    - Pagination parfois instable. Ici on privilégie la MAJ incrémentale => besoin de lire records existants.
      => on tente plusieurs variantes de pagination en GET:
         - page/per_page
         - from/limit
         - record_cursor (si présent)
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

            # Zoho renvoie parfois "400 code=9280" pour dire "0 résultat"
            if r.status_code == 400 and method.upper() == "GET":
                j = self._try_parse_json(last_text)
                if isinstance(j, dict):
                    code = str(j.get("code") or "")
                    msg = (j.get("message") or j.get("description") or "")
                    if code == "9280" and "No records found" in str(msg):
                        return {"data": []}

            if r.status_code >= 400:
                raise RuntimeError(f"Zoho API error {r.status_code} {method} {url}: {last_text[:900]}")

            if last_text.strip() == "":
                return {}
            try:
                return r.json()
            except Exception:
                return {"_raw": last_text}

        raise RuntimeError(f"Zoho API failed after retries: {method} {url}: {last_text[:900]}")

    # ---- endpoints

    @property
    def _report_url(self) -> str:
        return f"{self.cfg.creator_base}/data/{self.cfg.owner}/{self.cfg.app_link_name}/report/{self.cfg.report_link_name}"

    @property
    def _form_url(self) -> str:
        return f"{self.cfg.creator_base}/data/{self.cfg.owner}/{self.cfg.app_link_name}/form/{self.cfg.form_link_name}"

    # ---- Records APIs

    def add_records(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not records:
            return {}
        return self._req("POST", self._form_url, json_body={"data": records})

    def update_record_by_id(self, record_id: str, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update (PUT) un record existant par ID.
        On utilise l'endpoint FORM (plus stable selon tenants).
        """
        url = f"{self._form_url}/{record_id}"
        return self._req("PUT", url, json_body={"data": record})

    def delete_record_by_id(self, record_id: str) -> Dict[str, Any]:
        url = f"{self._report_url}/{record_id}"
        return self._req("DELETE", url)

    def delete_records_by_criteria(self, *, criteria: str) -> Dict[str, Any]:
        # bulk delete si supporté (souvent non chez toi)
        return self._req("DELETE", self._report_url, params={"criteria": criteria})

    # ---- GET helpers

    @staticmethod
    def _extract_data_list(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not isinstance(resp, dict):
            return []
        if "data" in resp and isinstance(resp["data"], list):
            return resp["data"]
        if "response" in resp and isinstance(resp["response"], dict) and isinstance(resp["response"].get("data"), list):
            return resp["response"]["data"]
        return []

    def fetch_records_page(
        self,
        *,
        criteria: str = "",
        fields: str = "ID",
        page: int = 1,
        per_page: int = 200,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Tentative pagination via page/per_page.
        Retourne (records, raw_response).
        """
        params: Dict[str, Any] = {"fields": fields, "page": str(int(page)), "per_page": str(int(per_page))}
        if criteria:
            params["criteria"] = criteria
        resp = self._req("GET", self._report_url, params=params)
        return self._extract_data_list(resp), resp

    def fetch_records_from_offset(
        self,
        *,
        criteria: str = "",
        fields: str = "ID",
        offset: int = 0,
        limit: int = 200,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Tentative pagination via from/limit.
        """
        params: Dict[str, Any] = {"fields": fields, "from": str(int(offset)), "limit": str(int(limit))}
        if criteria:
            params["criteria"] = criteria
        resp = self._req("GET", self._report_url, params=params)
        return self._extract_data_list(resp), resp

    def fetch_records_with_cursor(
        self,
        *,
        criteria: str = "",
        fields: str = "ID",
        limit: int = 200,
        record_cursor: str | None = None,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Tentative via record_cursor.
        """
        params: Dict[str, Any] = {"fields": fields, "limit": str(int(limit))}
        if criteria:
            params["criteria"] = criteria
        if record_cursor:
            params["record_cursor"] = record_cursor
        resp = self._req("GET", self._report_url, params=params)
        return self._extract_data_list(resp), resp


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
    return months_sorted[-n:] if len(months_sorted) >= n else months_sorted[:]


def last_n_excluding_latest(months_sorted: List[str], n: int) -> List[str]:
    """
    Ex: months=[...,'202601','202602','202603'] et n=2 => ['202601','202602'] (exclut le dernier).
    Si pas assez de mois => renvoie ce qui est possible.
    """
    if not months_sorted:
        return []
    if len(months_sorted) <= 1:
        return []
    pool = months_sorted[:-1]  # exclure le dernier (mois courant)
    return pool[-n:] if len(pool) >= n else pool[:]


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

    # Bloquer lignes sans Key (OrgUnit/Period)
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
# PURGE helpers (safe) - utilisé uniquement pour purge_all ou fallback exceptionnel
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

        # on lit des IDs via une requête simple (sans pagination complexe),
        # mais ici on garde la méthode historique "fetch_ids_batch" via max_records/limit.
        ids = fetch_ids_batch_compat(client, criteria=criteria, limit=batch_limit)
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


def fetch_ids_batch_compat(client: ZohoCreatorClient, *, criteria: str = "", limit: int = 200) -> List[str]:
    """
    Méthode compat minimale (sans pagination): récupère jusqu'à limit IDs.
    On réutilise report GET avec max_records/limit.
    """
    url = client._report_url

    # try 1: max_records
    params1: Dict[str, Any] = {"max_records": str(int(limit)), "fields": "ID"}
    if criteria:
        params1["criteria"] = criteria

    try:
        resp = client._req("GET", url, params=params1)
        data = client._extract_data_list(resp)
        return [str(r.get("ID") or r.get("id")) for r in data if (r.get("ID") or r.get("id"))]
    except Exception as e1:
        # try 2: limit
        params2: Dict[str, Any] = {"limit": str(int(limit)), "fields": "ID"}
        if criteria:
            params2["criteria"] = criteria
        try:
            resp = client._req("GET", url, params=params2)
            data = client._extract_data_list(resp)
            return [str(r.get("ID") or r.get("id")) for r in data if (r.get("ID") or r.get("id"))]
        except Exception as e2:
            raise RuntimeError(f"fetch_ids_batch_compat failed. e1={e1} e2={e2}")


# ---------------------------
# Incremental sync (RowHash)
# ---------------------------
def load_month_records_from_parts(
    repo_root: Path,
    yyyymm: str,
    parts_meta: List[Dict[str, Any]],
    ou_map: Dict[str, Dict[str, str]],
) -> Tuple[Dict[str, Dict[str, Any]], int]:
    """
    Retourne:
      - dict Key -> record Zoho-ready (avec RowHash)
      - skipped_no_key
    """
    month_dir = repo_root / "docs" / "data" / "monthly" / yyyymm
    desired: Dict[str, Dict[str, Any]] = {}
    skipped_no_key = 0

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
                skipped_no_key += 1
                continue
            key = str(rec.get("Key") or "").strip()
            if not key:
                skipped_no_key += 1
                continue
            desired[key] = rec

    return desired, skipped_no_key


def fetch_existing_key_rowhash_id_for_month(
    client: ZohoCreatorClient,
    yyyymm: str,
    *,
    per_page: int,
) -> Dict[str, Dict[str, str]]:
    """
    Récupère pour un mois donné: Key -> {id, rowhash}
    Essaie plusieurs méthodes de pagination.
    """
    period = period_yyyymm_to_zoho(yyyymm)
    criteria = f'(Period == "{period}")'
    fields = "ID,Key,RowHash"

    # 1) page/per_page
    existing: Dict[str, Dict[str, str]] = {}
    try:
        page = 1
        while True:
            recs, _raw = client.fetch_records_page(criteria=criteria, fields=fields, page=page, per_page=per_page)
            if not recs:
                break
            for r in recs:
                k = str(r.get("Key") or "").strip()
                rid = str(r.get("ID") or r.get("id") or "").strip()
                rh = str(r.get("RowHash") or "").strip()
                if k and rid:
                    existing[k] = {"id": rid, "rowhash": rh}
            if len(recs) < per_page:
                break
            page += 1
        return existing
    except Exception:
        pass

    # 2) from/limit
    try:
        offset = 0
        while True:
            recs, _raw = client.fetch_records_from_offset(criteria=criteria, fields=fields, offset=offset, limit=per_page)
            if not recs:
                break
            for r in recs:
                k = str(r.get("Key") or "").strip()
                rid = str(r.get("ID") or r.get("id") or "").strip()
                rh = str(r.get("RowHash") or "").strip()
                if k and rid:
                    existing[k] = {"id": rid, "rowhash": rh}
            if len(recs) < per_page:
                break
            offset += per_page
        return existing
    except Exception:
        pass

    # 3) record_cursor
    try:
        cursor: Optional[str] = None
        while True:
            recs, raw = client.fetch_records_with_cursor(criteria=criteria, fields=fields, limit=per_page, record_cursor=cursor)
            if not recs:
                break
            for r in recs:
                k = str(r.get("Key") or "").strip()
                rid = str(r.get("ID") or r.get("id") or "").strip()
                rh = str(r.get("RowHash") or "").strip()
                if k and rid:
                    existing[k] = {"id": rid, "rowhash": rh}

            # trouver un cursor
            cursor = None
            if isinstance(raw, dict):
                cursor = raw.get("record_cursor") or raw.get("recordCursor") or None
                if not cursor and "info" in raw and isinstance(raw["info"], dict):
                    cursor = raw["info"].get("record_cursor") or raw["info"].get("recordCursor") or None

            if not cursor:
                break
        return existing
    except Exception:
        pass

    raise RuntimeError("Unable to paginate Zoho records for month. (page/per_page, from/limit, record_cursor all failed)")


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
    Incrémental:
      - charge desired Key->record (RowHash)
      - charge existing Key->{id,rowhash}
      - add: keys absentes
      - update: rowhash différent
    """
    desired, skipped_no_key = load_month_records_from_parts(repo_root, yyyymm, parts_meta, ou_map)

    # si rien à envoyer
    if not desired:
        return {
            "month": yyyymm,
            "desired": 0,
            "added": 0,
            "updated": 0,
            "unchanged": 0,
            "skipped_no_key": skipped_no_key,
        }

    existing = fetch_existing_key_rowhash_id_for_month(client, yyyymm, per_page=per_page)

    to_add: List[Dict[str, Any]] = []
    to_update: List[Tuple[str, Dict[str, Any]]] = []
    unchanged = 0

    for key, rec in desired.items():
        ex = existing.get(key)
        if not ex:
            to_add.append(rec)
            continue
        old_rh = str(ex.get("rowhash") or "").strip()
        new_rh = str(rec.get("RowHash") or "").strip()
        if old_rh != new_rh:
            to_update.append((ex["id"], rec))
        else:
            unchanged += 1

    # ADD (batches)
    added = 0
    for batch in chunk_records(to_add, batch_size):
        if not batch:
            continue
        client.add_records(batch)
        added += len(batch)
        if throttle_s > 0:
            time.sleep(throttle_s)

    # UPDATE (record by record; updates sont généralement moins nombreux)
    updated = 0
    for rid, rec in to_update:
        client.update_record_by_id(rid, rec)
        updated += 1
        if throttle_s > 0:
            time.sleep(throttle_s)

    return {
        "month": yyyymm,
        "desired": len(desired),
        "added": added,
        "updated": updated,
        "unchanged": unchanged,
        "skipped_no_key": skipped_no_key,
    }


# ---------------------------
# MAIN
# ---------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo_root", default=".", help="Repository root (default: .)")
    ap.add_argument("--index", default="docs/data/index.json")
    ap.add_argument("--state", default="docs/data/zoho_sync_state.json")

    # Routine: mettre 2 (mais on EXCLUT le mois courant par défaut)
    ap.add_argument("--refresh_last_n", type=int, default=2, help="How many previous months to refresh (incremental). Default=2")
    ap.add_argument("--include_latest_in_refresh", action="store_true", help="If set, include latest month (current month) in refresh.")

    ap.add_argument("--batch_size", type=int, default=200)
    ap.add_argument("--throttle_seconds", type=float, default=1.5, help="Sleep between API calls (rate limit safety).")
    ap.add_argument("--per_page", type=int, default=200, help="Zoho page size when listing existing rows.")

    # Manual purge
    ap.add_argument("--purge_all", action="store_true")
    ap.add_argument("--purge_only", action="store_true")
    ap.add_argument("--purge_batch_limit", type=int, default=200)

    # Full import historical (rare)
    ap.add_argument("--import_historical", action="store_true", help="Import all historical months (slow). Default false.")

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

    # PURGE ALL mode
    if args.purge_all:
        print("PURGE ALL: deleting all records in Zoho report until empty...", flush=True)
        deleted = purge_all_records(client, throttle_s=args.throttle_seconds, batch_limit=args.purge_batch_limit)
        print(f"PURGE ALL done. deleted={deleted}", flush=True)

        # verify empty
        remain = fetch_ids_batch_compat(client, criteria="", limit=1)
        if remain:
            raise RuntimeError(f"PURGE ALL verification failed: still found records (example ID={remain[0]}).")
        print("PURGE ALL verification OK: report is empty.", flush=True)

        print("Resetting zoho_sync_state.json ...", flush=True)
        reset_state_file(state_path)

        if args.purge_only:
            print("PURGE ONLY => exit.", flush=True)
            return 0

    # read index
    index = load_index(index_path)
    months_sorted = sorted_months(index)
    if not months_sorted:
        print("No months found in index.json")
        return 0

    months_obj = index.get("months") or {}

    # OU map
    ou_map = load_ou_map(repo_root)
    print(f"OU_MAP loaded: level5={len(ou_map)}", flush=True)

    state = load_state(state_path)
    done_months: Dict[str, Any] = state.get("done_months") or {}

    # Refresh months: previous months by default (exclude latest/current month)
    if args.include_latest_in_refresh:
        refresh_months = last_n_months(months_sorted, args.refresh_last_n)
    else:
        refresh_months = last_n_excluding_latest(months_sorted, args.refresh_last_n)

    print(f"Months in index: {months_sorted[0]} -> {months_sorted[-1]} (count={len(months_sorted)})", flush=True)
    print(f"Refresh months (incremental, RowHash) = {refresh_months}", flush=True)
    print(
        f"batch_size={args.batch_size} throttle_seconds={args.throttle_seconds} per_page={args.per_page} "
        f"import_historical={args.import_historical}",
        flush=True,
    )

    # 1) Incremental refresh for selected months (NO DELETE)
    for m in refresh_months:
        parts = (months_obj.get(m) or {}).get("parts") or []
        if not parts:
            print(f"[{m}] skip: no parts", flush=True)
            continue

        print(f"[{m}] incremental sync (compare RowHash, add/update only)...", flush=True)
        stats = incremental_sync_month(
            client,
            repo_root,
            m,
            parts,
            ou_map,
            batch_size=args.batch_size,
            throttle_s=args.throttle_seconds,
            per_page=args.per_page,
        )
        print(
            f"[{m}] desired={stats['desired']} added={stats['added']} updated={stats['updated']} "
            f"unchanged={stats['unchanged']} skipped_no_key={stats['skipped_no_key']}",
            flush=True,
        )
        done_months[m] = {"done": True, "refreshed": True, "last_sync": time.time(), "mode": "incremental"}

    # 2) Historical import (optionnel / manuel)
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

            print(f"[{m}] import historical month (first time) ...", flush=True)
            # import historique = simple insert (Key unique protège des doublons si rerun)
            desired, skipped = load_month_records_from_parts(repo_root, m, parts, ou_map)
            recs = list(desired.values())
            batches = 0
            inserted = 0
            for batch in chunk_records(recs, args.batch_size):
                if not batch:
                    continue
                client.add_records(batch)
                batches += 1
                inserted += len(batch)
                if args.throttle_seconds > 0:
                    time.sleep(args.throttle_seconds)
            print(f"[{m}] inserted_records={inserted} batches={batches} skipped_no_key={skipped}", flush=True)
            done_months[m] = {"done": True, "refreshed": False, "last_sync": time.time(), "mode": "historical_insert"}

    # save state
    state["done_months"] = done_months
    state["last_run_at"] = time.time()
    save_state(state_path, state)

    print("OK: Zoho sync completed.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
