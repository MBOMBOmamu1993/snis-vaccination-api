from __future__ import annotations

import gzip
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass
class Dhis2Client:
    base_url: str
    username: str
    password: str
    connect_timeout_s: int = 20
    read_timeout_s: int = 120
    retries_total: int = 4
    backoff_factor: float = 2.0

    def __post_init__(self) -> None:
        retry = Retry(
            total=self.retries_total,
            connect=self.retries_total,
            read=self.retries_total,
            status=self.retries_total,
            backoff_factor=self.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        self.session = requests.Session()
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _get(self, path: str, params: Dict[str, Any]) -> dict:
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        last_err = ""

        for attempt in range(1, self.retries_total + 2):
            try:
                r = self.session.get(
                    url,
                    params=params,
                    auth=(self.username, self.password),
                    headers={"Accept": "application/json"},
                    timeout=(self.connect_timeout_s, self.read_timeout_s),
                )

                if 200 <= r.status_code < 300:
                    return r.json()

                if r.status_code in (429, 500, 502, 503, 504):
                    last_err = f"HTTP {r.status_code}: {r.text[:200]}"
                    sleep_s = min(30.0, 2.0 * attempt)
                    print(
                        f"WARN: DHIS2 {r.status_code} attempt={attempt}/{self.retries_total + 1} "
                        f"sleep={sleep_s}s path={path}",
                        flush=True,
                    )
                    time.sleep(sleep_s)
                    continue

                r.raise_for_status()

            except requests.exceptions.RequestException as e:
                last_err = str(e)
                sleep_s = min(30.0, 2.0 * attempt)
                print(
                    f"WARN: request failed attempt={attempt}/{self.retries_total + 1} "
                    f"sleep={sleep_s}s path={path} err={e}",
                    flush=True,
                )
                time.sleep(sleep_s)

        raise RuntimeError(f"DHIS2 GET failed after retries: {path} | last_err={last_err}")

    def org_units_level(self, level: int) -> List[dict]:
        params = {
            "filter": f"level:eq:{level}",
            "paging": "false",
            "fields": "id,name,level,path",
        }
        data = self._get("api/organisationUnits.json", params)
        units = data.get("organisationUnits") or []
        return units if isinstance(units, list) else []


def build_all_units(client: Dhis2Client) -> Dict[str, dict]:
    all_units: Dict[str, dict] = {}

    for lvl in (2, 3, 4, 5):
        units = client.org_units_level(lvl)
        print(f"Loaded level {lvl}: {len(units)} units", flush=True)
        for ou in units:
            ou_id = str(ou.get("id") or "").strip()
            if ou_id:
                all_units[ou_id] = ou

    return all_units


def _name_for(path_ids: List[str], level: int, all_units: Dict[str, dict]) -> str:
    for pid in path_ids:
        u = all_units.get(pid)
        if u and u.get("level") == level:
            return str(u.get("name") or "").strip()
    return ""


def build_ou_map_fosa(all_units: Dict[str, dict]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}

    for ou_id, ou in all_units.items():
        if ou.get("level") != 5:
            continue

        path = str(ou.get("path") or "").strip()
        ids = [p for p in path.split("/") if p]

        out[ou_id] = {
            "Org2": _name_for(ids, 2, all_units),
            "Org3": _name_for(ids, 3, all_units),
            "Org4": _name_for(ids, 4, all_units),
            "Org5": str(ou.get("name") or "").strip(),
        }

    return out


def build_ou_map_as(all_units: Dict[str, dict]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}

    for ou_id, ou in all_units.items():
        if ou.get("level") != 4:
            continue

        path = str(ou.get("path") or "").strip()
        ids = [p for p in path.split("/") if p]

        out[ou_id] = {
            "Org2": _name_for(ids, 2, all_units),
            "Org3": _name_for(ids, 3, all_units),
            "Org4": str(ou.get("name") or "").strip(),
            "Org5": "",
        }

    return out


def write_gz_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    with gzip.open(path, "wb") as f:
        f.write(raw)


def load_existing_json_pair(json_path: Path, gz_path: Path) -> Optional[Dict[str, Dict[str, str]]]:
    try:
        if gz_path.exists():
            with gzip.open(gz_path, "rb") as f:
                return json.loads(f.read().decode("utf-8"))
        if json_path.exists():
            return json.loads(json_path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"WARN: existing map unreadable: {e}", flush=True)

    return None


def save_map_with_meta(
    *,
    out_dir: Path,
    json_name: str,
    gz_name: str,
    meta_name: str,
    payload: Dict[str, Dict[str, str]],
    kind: str,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / json_name
    gz_path = out_dir / gz_name
    meta_path = out_dir / meta_name

    json_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    write_gz_json(gz_path, payload)

    meta = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "kind": kind,
        "count": len(payload),
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")


def main() -> int:
    base_url = os.environ.get("DHIS2_BASE_URL")
    username = os.environ.get("DHIS2_USERNAME")
    password = os.environ.get("DHIS2_PASSWORD")

    fosa_dir = Path("docs/data")
    as_dir = Path("docs/data_as")

    fosa_json = fosa_dir / "ou_map.json"
    fosa_gz = fosa_dir / "ou_map.json.gz"

    as_json = as_dir / "ou_map_as.json"
    as_gz = as_dir / "ou_map_as.json.gz"

    if not (base_url and username and password):
        print("Missing secrets: DHIS2_BASE_URL, DHIS2_USERNAME, DHIS2_PASSWORD", file=sys.stderr)

        existing_fosa = load_existing_json_pair(fosa_json, fosa_gz)
        existing_as = load_existing_json_pair(as_json, as_gz)

        if existing_fosa or existing_as:
            print(
                "FALLBACK: using existing cached OU maps "
                f"(fosa={len(existing_fosa or {})}, as={len(existing_as or {})})",
                flush=True,
            )
            return 0

        return 2

    client = Dhis2Client(
        base_url=base_url,
        username=username,
        password=password,
        connect_timeout_s=20,
        read_timeout_s=120,
        retries_total=4,
        backoff_factor=2.0,
    )

    try:
        all_units = build_all_units(client)

        if not all_units:
            raise RuntimeError("No organisation units returned from DHIS2")

        fosa_map = build_ou_map_fosa(all_units)
        as_map = build_ou_map_as(all_units)

        if not fosa_map:
            raise RuntimeError("build_ou_map_fosa returned empty mapping")
        if not as_map:
            raise RuntimeError("build_ou_map_as returned empty mapping")

        save_map_with_meta(
            out_dir=fosa_dir,
            json_name="ou_map.json",
            gz_name="ou_map.json.gz",
            meta_name="ou_map.meta.json",
            payload=fosa_map,
            kind="fosa_org5",
        )

        save_map_with_meta(
            out_dir=as_dir,
            json_name="ou_map_as.json",
            gz_name="ou_map_as.json.gz",
            meta_name="ou_map_as.meta.json",
            payload=as_map,
            kind="as_org4",
        )

        print(
            f"OK: FOSA map generated for {len(fosa_map)} OU level 5 | "
            f"AS map generated for {len(as_map)} OU level 4",
            flush=True,
        )
        return 0

    except Exception as e:
        print(f"WARN: build_ou_map failed from DHIS2: {e}", flush=True)

        existing_fosa = load_existing_json_pair(fosa_json, fosa_gz)
        existing_as = load_existing_json_pair(as_json, as_gz)

        if existing_fosa or existing_as:
            print(
                "FALLBACK: using existing cached OU maps "
                f"(fosa={len(existing_fosa or {})}, as={len(existing_as or {})}) "
                "and continuing workflow.",
                flush=True,
            )
            return 0

        print(
            "ERROR: no cached docs/data/ou_map.json(.gz) or docs/data_as/ou_map_as.json(.gz) available.",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
