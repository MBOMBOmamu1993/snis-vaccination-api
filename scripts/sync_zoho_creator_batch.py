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
        # https://www.zohoapis.<dc>/creator/v2.1
        return f"https://{zoho_apis_domain(self.dc)}/creator/v2.1"

    @property
    def oauth_token_url(self) -> str:
        return f"https://{zoho_accounts_domain(self.dc)}/oauth/v2/token"


class ZohoCreatorClient:
    """
    IMPORTANT:
    - Certains tenants Zoho n'acceptent pas page/per_page.
    - Le cursor record_cursor peut être absent/inconstant selon DC.
    => On évite la pagination fragile pour la purge:
       on lit TOUJOURS les N premiers IDs puis on supprime, et on recommence.
       Comme on supprime, la "première page" change à chaque boucle => purge complète garantie.
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
    def _is_no_records_error(payload_text: str) -> bool:
        """
        Zoho renvoie parfois HTTP 400 + JSON:
          {"code":9280,"message":"No records found matching the given criteria..."}
        => on doit traiter ça comme "liste vide", pas une erreur fatale.
        """
        try:
            obj = json.loads(payload_text or "{}")
            if isinstance(obj, dict) and str(obj.get("code")) == "9280":
                return True
            msg = str(obj.get("message") or "").lower()
            if "no records found" in msg:
                return True
        except Exception:
            pass

        # fallback texte brut
        t = (payload_text or "").lower()
        return ("no records found" in t) or ('"code":9280' in t) or ("code\":9280" in t)

    def _req(
        self,
        method: str,
        url: str,
        *,
        params: Dict[str, Any] | None = None,
        json_body: Any | None = None,
        allow_no_records: bool = False,
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

            # retry on transient errors
            if r.status_code in (429, 500, 502, 503, 504):
                sleep_s = min(60.0, 3.0 * attempt)
                time.sleep(sleep_s)
                continue

            # ✅ considérer "no records found" comme vide (optionnel)
            if allow_no_records and r.status_code >= 400 and self._is_no_records_error(last_text):
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

    # ---- Records APIs

    def add_records(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not records:
            return {}
        url = f"{self.cfg.creator_base}/data/{self.cfg.owner}/{self.cfg.app_link_name}/form/{self.cfg.form_link_name}"
        body = {"data": records}
        return self._req("POST", url, json_body=body)

    def fetch_ids_batch(
        self,
        *,
        criteria: str = "",
        limit: int = 200,
    ) -> List[str]:
        """
        Récupère une liste d'IDs (max 'limit') du report, avec ou sans criteria.
        On utilise des params "compatibles" (pas de page/per_page).

        Tentatives:
          1) max_records=<limit> & fields=ID
          2) limit=<limit> & fields=ID (fallback)

        ✅ Si Zoho renvoie "No records found..." (code 9280), on retourne [].
        """
        url = f"{self.cfg.creator_base}/data/{self.cfg.owner}/{self.cfg.app_link_name}/report/{self.cfg.report_link_name}"

        # try 1: max_records
        params1: Dict[str, Any] = {"max_records": str(int(limit)), "fields": "ID"}
        if criteria:
            params1["criteria"] = criteria

        try:
            resp = self._req("GET", url, params=params1, allow_no_records=True)
            data = self._extract_data_list(resp)
            ids = self._extract_ids(data)
            return ids
        except Exception as e1:
            # try 2: limit
            params2: Dict[str, Any] = {"limit": str(int(limit)), "fields": "ID"}
            if criteria:
                params2["criteria"] = criteria
            try:
                resp = self._req("GET", url, params=params2, allow_no_records=True)
                data = self._extract_data_list(resp)
                ids = self._extract_ids(data)
                return ids
            except Exception as e2:
                raise RuntimeError(f"fetch_ids_batch failed with max_records and limit. e1={e1} e2={e2}")

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
    def _extract_ids(data: List[Dict[str, Any]]) -> List[str]:
        out: List[str] = []
        for r in data:
            rid = r.get("ID") or r.get("id")
            if rid is not None and str(rid).strip() != "":
                out.append(str(rid))
        return out

    def delete_record_by_id(self, record_id: str) -> Dict[str, Any]:
        url = f"{self.cfg.creator_base}/data/{self.cfg.owner}/{self.cfg.app_link_name}/report/{self.cfg.report_link_name}/{record_id}"
        return self._req("DELETE", url)

    def delete_records_by_criteria(self, *, criteria: str) -> Dict[str, Any]:
        """
        Si ton DC supporte DELETE en masse:
          DELETE .../report/<report>?criteria=...
        Sinon => exception => fallback delete par ID.
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
    """
    Bloque import si OrgUnit/Period absent => {}
    """
    ou = str(row.get("OrgUnit") or "").strip()
    period = str(row.get("Period") or "").strip()  # déjà dd-MMM-yyyy chez toi

    # ✅ BLOQUER lignes sans Key (donc sans OrgUnit/Period)
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
# PURGE helpers (safe)
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
    SAFE PURGE:
      - fetch first N IDs (criteria)
      - delete them
      - repeat until no IDs
    Works regardless of pagination quirks.
    """
    deleted = 0
    loops = 0
    while True:
        loops += 1
        if loops > hard_limit_loops:
            raise RuntimeError("purge_by_criteria_until_empty: too many loops, aborting for safety.")

        ids = client.fetch_ids_batch(criteria=criteria, limit=batch_limit)
        if not ids:
            break

        for rid in ids:
            # si l'ID n'existe déjà plus, Zoho peut répondre "No Data Available"
            # on laisse remonter si c'est une vraie erreur,
            # mais souvent ce cas n'arrive que si suppression manuelle en parallèle.
            try:
                client.delete_record_by_id(rid)
                deleted += 1
            except Exception as e:
                msg = str(e).lower()
                if "no data available" in msg or '"code":3100' in msg or "code\":3100" in msg:
                    # déjà supprimé => on ignore
                    pass
                else:
                    raise
            if throttle_s > 0:
                time.sleep(throttle_s)

        # tiny pause each loop to be kind to API
        if throttle_s > 0:
            time.sleep(min(0.5, throttle_s))

    return deleted


# ---------------------------
# PURGE ALL (manual one-shot)
# ---------------------------
def purge_all_records(client: ZohoCreatorClient, *, throttle_s: float, batch_limit: int) -> int:
    # criteria vide => tout
    return purge_by_criteria_until_empty(
        client,
        criteria="",
        throttle_s=throttle_s,
        batch_limit=batch_limit,
    )


# ---------------------------
# Month operations
# ---------------------------
def delete_month_records(client: ZohoCreatorClient, yyyymm: str, *, throttle_s: float, batch_limit: int) -> int:
    period = period_yyyymm_to_zoho(yyyymm)
    criteria = f'(Period == "{period}")'

    # Try bulk delete (fast if supported)
    try:
        client.delete_records_by_criteria(criteria=criteria)
        return -1
    except Exception as e:
        print(f"[{yyyymm}] bulk delete not available -> fallback safe purge. reason={e}", flush=True)

    deleted = purge_by_criteria_until_empty(
        client,
        criteria=criteria,
        throttle_s=throttle_s,
        batch_limit=batch_limit,
    )
    return deleted


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
    """
    Returns (batches_sent, records_total, skipped_no_key)
    """
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
    ap.add_argument("--refresh_last_n", type=int, default=3)
    ap.add_argument("--batch_size", type=int, default=200)
    ap.add_argument("--throttle_seconds", type=float, default=1.2, help="Sleep between API calls (rate limit safety)")

    # ✅ Manual one-shot purge
    ap.add_argument("--purge_all", action="store_true", help="DELETE ALL Zoho records in the report (manual one-shot).")
    ap.add_argument("--purge_only", action="store_true", help="If set with --purge_all, purge then exit.")

    # Batch size for purge deletes per loop (IDs per loop). Keep <=200 safe.
    ap.add_argument("--purge_batch_limit", type=int, default=200, help="How many IDs to delete per purge loop (<=200 recommended).")

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

    # ✅ PURGE MODE (manual unique)
    if args.purge_all:
        print("PURGE ALL: deleting all records in Zoho report until empty...", flush=True)
        deleted = purge_all_records(
            client,
            throttle_s=args.throttle_seconds,
            batch_limit=args.purge_batch_limit,
        )
        print(f"PURGE ALL done. deleted={deleted}", flush=True)

        # ✅ verify empty
        remaining_ids = client.fetch_ids_batch(criteria="", limit=1)
        if remaining_ids:
            raise RuntimeError(f"PURGE ALL verification failed: still found records (example ID={remaining_ids[0]}).")
        print("PURGE ALL verification OK: report is empty (0 records).", flush=True)

        # ✅ reset state so historical months can be re-imported from scratch
        print("Resetting zoho_sync_state.json ...", flush=True)
        reset_state_file(state_path)

        if args.purge_only:
            print("PURGE ONLY => exit.", flush=True)
            return 0
        # else continue (purge + import same run)

    # ---- Read index
    index = load_index(index_path)
    months_sorted = sorted_months(index)
    if not months_sorted:
        print("No months found in index.json")
        return 0

    # ✅ OU map
    ou_map = load_ou_map(repo_root)
    print(f"OU_MAP loaded: level5={len(ou_map)}", flush=True)

    last_months = last_n_months(months_sorted, args.refresh_last_n)
    months_obj = index.get("months") or {}

    state = load_state(state_path)
    done_months: Dict[str, Any] = state.get("done_months") or {}

    print(f"Months in index: {months_sorted[0]} -> {months_sorted[-1]} (count={len(months_sorted)})", flush=True)
    print(f"Refresh candidates (last_n={args.refresh_last_n}): {last_months}", flush=True)
    print(
        f"batch_size(add_records)={args.batch_size} | "
        f"purge_batch_limit={args.purge_batch_limit} | "
        f"throttle_seconds={args.throttle_seconds}",
        flush=True,
    )

    for m in months_sorted:
        parts = (months_obj.get(m) or {}).get("parts") or []
        if not parts:
            print(f"[{m}] skip: no parts", flush=True)
            continue

        is_refresh_candidate = (m in last_months)
        already_done = (done_months.get(m, {}).get("done") is True)

        # ✅ MODIF #1: refresh seulement si le mois existe déjà dans Zoho (done=True)
        if is_refresh_candidate and already_done:
            print(f"[{m}] refresh (already done): deleting Zoho month records...", flush=True)
            deleted = delete_month_records(
                client,
                m,
                throttle_s=args.throttle_seconds,
                batch_limit=args.purge_batch_limit,
            )
            print(f"[{m}] deleted={'bulk' if deleted == -1 else deleted}; inserting from parts...", flush=True)

            batches, total, skipped = insert_month_from_parts(
                client, repo_root, m, parts, ou_map,
                batch_size=args.batch_size,
                throttle_s=args.throttle_seconds,
            )
            print(f"[{m}] inserted_records={total} batches={batches} skipped_no_key={skipped}", flush=True)
            done_months[m] = {"done": True, "refreshed": True, "last_sync": time.time()}

        else:
            if already_done:
                # mois historique déjà chargé (et pas refresh) => skip
                print(f"[{m}] skip: already done (historical)", flush=True)
                continue

            # ✅ premier import: PAS DE DELETE (même si c'est dans last_months)
            print(f"[{m}] import month (first time, no delete)...", flush=True)
            batches, total, skipped = insert_month_from_parts(
                client, repo_root, m, parts, ou_map,
                batch_size=args.batch_size,
                throttle_s=args.throttle_seconds,
            )
            print(f"[{m}] inserted_records={total} batches={batches} skipped_no_key={skipped}", flush=True)
            done_months[m] = {"done": True, "refreshed": False, "last_sync": time.time()}

    state["done_months"] = done_months
    state["last_run_at"] = time.time()
    save_state(state_path, state)

    print("OK: Zoho sync completed.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
