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
    def __init__(self, cfg: ZohoConfig, timeout_s: int = 120) -> None:
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

    def _req(
        self,
        method: str,
        url: str,
        *,
        params: Dict[str, Any] | None = None,
        json_body: Any | None = None,
    ) -> Dict[str, Any]:
        # retry/backoff only on 429/5xx
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
                sleep_s = min(30.0, 2.0 * attempt)
                time.sleep(sleep_s)
                continue
            if r.status_code >= 400:
                # helpful error
                raise RuntimeError(f"Zoho API error {r.status_code} {method} {url}: {last_text[:500]}")
            if last_text.strip() == "":
                return {}
            return r.json()
        raise RuntimeError(f"Zoho API failed after retries: {method} {url}: {last_text[:500]}")

    # ---- Records APIs
    def add_records(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not records:
            return {}
        url = f"{self.cfg.creator_base}/data/{self.cfg.owner}/{self.cfg.app_link_name}/form/{self.cfg.form_link_name}"
        body = {"data": records}
        return self._req("POST", url, json_body=body)

    def get_records_page(self, *, criteria: str, page: int = 1, per_page: int = 200) -> Dict[str, Any]:
        url = f"{self.cfg.creator_base}/data/{self.cfg.owner}/{self.cfg.app_link_name}/report/{self.cfg.report_link_name}"
        params = {"criteria": criteria, "page": page, "per_page": per_page}
        return self._req("GET", url, params=params)

    def delete_record_by_id(self, record_id: str) -> Dict[str, Any]:
        url = f"{self.cfg.creator_base}/data/{self.cfg.owner}/{self.cfg.app_link_name}/report/{self.cfg.report_link_name}/{record_id}"
        return self._req("DELETE", url)

    def delete_records_by_criteria(self, *, criteria: str) -> Dict[str, Any]:
        """
        Certains comptes Zoho supportent DELETE sur report avec criteria.
        Si ton DC l’accepte, ça supprime en masse (beaucoup plus rapide).
        """
        url = f"{self.cfg.creator_base}/data/{self.cfg.owner}/{self.cfg.app_link_name}/report/{self.cfg.report_link_name}"
        params = {"criteria": criteria}
        return self._req("DELETE", url, params=params)


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


def load_ou_map(repo_root: Path) -> Dict[str, Dict[str, str]]:
    """
    Charge docs/data/ou_map.json.gz si présent, sinon ou_map.json
    """
    gz_path = repo_root / "docs" / "data" / "ou_map.json.gz"
    js_path = repo_root / "docs" / "data" / "ou_map.json"

    if gz_path.exists():
        raw = gzip.open(gz_path, "rb").read()
        return json.loads(raw.decode("utf-8"))

    if js_path.exists():
        return json.loads(js_path.read_text(encoding="utf-8"))

    raise FileNotFoundError("Missing docs/data/ou_map.json(.gz). Run build_ou_map workflow first.")


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


# ---------------------------
# Transform: NDJSON row -> Zoho record (IMPORTANT)
# ---------------------------
def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def build_zoho_record(row: Dict[str, Any], raw_line: str, ou_map: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    """
    GitHub NDJSON contient "OrgUnit" + "Period" + indicateurs.
    Zoho attend les link names:
      - Key, Org5_UID, Period, RowHash, Org2..Org5 + indicateurs
    """
    ou = str(row.get("OrgUnit") or "").strip()
    period = str(row.get("Period") or "").strip()  # chez toi déjà "01-Apr-2025"

    if not ou or not period:
        # on renvoie quand même quelque chose minimal si besoin
        return {}

    meta = ou_map.get(ou) or {}
    rec: Dict[str, Any] = {}

    rec["Org5_UID"] = ou
    rec["Period"] = period
    rec["Key"] = f"{ou}|{period}"

    # RowHash: prendre celui du fichier si existant, sinon md5(raw_line)
    rh = row.get("RowHash")
    rec["RowHash"] = str(rh).strip() if rh else md5_hex(raw_line)

    # Org2..Org5
    rec["Org2"] = meta.get("Org2", "")
    rec["Org3"] = meta.get("Org3", "")
    rec["Org4"] = meta.get("Org4", "")
    rec["Org5"] = meta.get("Org5", "")

    # Copier tous les autres champs (indicateurs) tels quels
    for k, v in row.items():
        if k in ("OrgUnit", "Period", "Key", "Org5_UID", "Org2", "Org3", "Org4", "Org5", "RowHash"):
            continue
        rec[k] = v

    return rec


# ---------------------------
# Main sync logic
# ---------------------------
def delete_month_records(client: ZohoCreatorClient, yyyymm: str) -> int:
    """
    Delete ALL records for the month.
    1) try bulk delete by criteria (fast)
    2) fallback per-record delete (slow but works)
    """
    period = period_yyyymm_to_zoho(yyyymm)
    criteria = f'(Period == "{period}")'

    # 1) bulk delete (if supported)
    try:
        client.delete_records_by_criteria(criteria=criteria)
        return -1  # unknown exact count, but done
    except Exception as e:
        print(f"[{yyyymm}] bulk delete not available, fallback per-record delete. reason={e}")

    # 2) fallback
    deleted = 0
    page = 1
    per_page = 200
    while True:
        resp = client.get_records_page(criteria=criteria, page=page, per_page=per_page)
        data = resp.get("data") or []
        if not data:
            break
        for r in data:
            rid = r.get("ID") or r.get("id")
            if not rid:
                continue
            client.delete_record_by_id(str(rid))
            deleted += 1
        if len(data) < per_page:
            break
        page += 1
        if page > 800:
            break
    return deleted


def insert_month_from_parts(
    client: ZohoCreatorClient,
    repo_root: Path,
    yyyymm: str,
    parts_meta: List[Dict[str, Any]],
    ou_map: Dict[str, Dict[str, str]],
    *,
    batch_size: int = 200,
) -> Tuple[int, int]:
    """
    Insert all records from monthly/<yyyymm>/part-xxxx.ndjson in batches.
    Returns (batches_sent, records_total).
    """
    month_dir = repo_root / "docs" / "data" / "monthly" / yyyymm
    batches = 0
    total = 0

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

        total += len(zoho_recs)

        for batch in chunk_records(zoho_recs, batch_size):
            client.add_records(batch)
            batches += 1

    return batches, total


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo_root", default=".", help="Repository root (default: .)")
    ap.add_argument("--index", default="docs/data/index.json")
    ap.add_argument("--state", default="docs/data/zoho_sync_state.json")
    ap.add_argument("--refresh_last_n", type=int, default=3)
    ap.add_argument("--batch_size", type=int, default=200)
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

    index = load_index(index_path)
    months_sorted = sorted_months(index)
    if not months_sorted:
        print("No months found in index.json")
        return 0

    # ✅ load ou_map
    ou_map = load_ou_map(repo_root)
    print(f"OU_MAP loaded: level5={len(ou_map)}")

    last_months = last_n_months(months_sorted, args.refresh_last_n)
    months_obj = index.get("months") or {}

    state = load_state(state_path)
    done_months: Dict[str, Any] = state.get("done_months") or {}

    client = ZohoCreatorClient(cfg)

    print(f"Months in index: {months_sorted[0]} -> {months_sorted[-1]} (count={len(months_sorted)})")
    print(f"Refresh (delete+reinsert) months: {last_months}")
    print(f"Batch size: {args.batch_size}")

    for m in months_sorted:
        parts = (months_obj.get(m) or {}).get("parts") or []
        if not parts:
            print(f"[{m}] skip: no parts")
            continue

        if m in last_months:
            print(f"[{m}] refresh: deleting Zoho month records...")
            deleted = delete_month_records(client, m)
            print(f"[{m}] deleted={'bulk' if deleted == -1 else deleted}; inserting from parts...")
            batches, total = insert_month_from_parts(client, repo_root, m, parts, ou_map, batch_size=args.batch_size)
            print(f"[{m}] inserted_records={total} batches={batches}")
            done_months[m] = {"done": True, "refreshed": True, "last_sync": time.time()}
        else:
            if done_months.get(m, {}).get("done") is True:
                print(f"[{m}] skip: already done (historical)")
                continue
            print(f"[{m}] import historical month...")
            batches, total = insert_month_from_parts(client, repo_root, m, parts, ou_map, batch_size=args.batch_size)
            print(f"[{m}] inserted_records={total} batches={batches}")
            done_months[m] = {"done": True, "refreshed": False, "last_sync": time.time()}

    state["done_months"] = done_months
    state["last_run_at"] = time.time()
    save_state(state_path, state)

    print("OK: Zoho sync completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
