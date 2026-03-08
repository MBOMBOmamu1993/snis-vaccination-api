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


def build_ou_map(client: Dhis2Client) -> Dict[str, Dict[str, str]]:
    """
    Retourne: { ou5_id: {Org2, Org3, Org4, Org5} }
    """
    all_units: Dict[str, dict] = {}

    for lvl in (2, 3, 4, 5):
        units = client.org_units_level(lvl)
        print(f"Loaded level {lvl}: {len(units)} units", flush=True)
        for ou in units:
            ou_id = str(ou.get("id") or "").strip()
            if ou_id:
                all_units[ou_id] = ou

    out: Dict[str, Dict[str, str]] = {}

    for ou_id, ou in all_units.items():
        if ou.get("level") != 5:
            continue

        path = str(ou.get("path") or "").strip()
        ids = [p for p in path.split("/") if p]

        def name_for(level: int) -> str:
            for pid in ids:
                u = all_units.get(pid)
                if u and u.get("level") == level:
                    return str(u.get("name") or "").strip()
            return ""

        out[ou_id] = {
            "Org2": name_for(2),
            "Org3": name_for(3),
            "Org4": name_for(4),
            "Org5": str(ou.get("name") or "").strip(),
        }

    return out


def write_gz_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    with gzip.open(path, "wb") as f:
        f.write(raw)


def load_existing_ou_map(out_dir: Path) -> Optional[Dict[str, Dict[str, str]]]:
    gz_path = out_dir / "ou_map.json.gz"
    js_path = out_dir / "ou_map.json"

    try:
        if gz_path.exists():
            with gzip.open(gz_path, "rb") as f:
                return json.loads(f.read().decode("utf-8"))
        if js_path.exists():
            return json.loads(js_path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"WARN: existing ou_map unreadable: {e}", flush=True)

    return None


def save_ou_map(out_dir: Path, ou_map: Dict[str, Dict[str, str]]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    out_json = out_dir / "ou_map.json"
    out_json.write_text(json.dumps(ou_map, ensure_ascii=False), encoding="utf-8")

    write_gz_json(out_dir / "ou_map.json.gz", ou_map)

    meta = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "count_ou_level5": len(ou_map),
    }
    (out_dir / "ou_map.meta.json").write_text(
        json.dumps(meta, ensure_ascii=False),
        encoding="utf-8",
    )


def main() -> int:
    base_url = os.environ.get("DHIS2_BASE_URL")
    username = os.environ.get("DHIS2_USERNAME")
    password = os.environ.get("DHIS2_PASSWORD")

    out_dir = Path("docs/data")

    if not (base_url and username and password):
        print("Missing secrets: DHIS2_BASE_URL, DHIS2_USERNAME, DHIS2_PASSWORD", file=sys.stderr)
        existing = load_existing_ou_map(out_dir)
        if existing:
            print(f"FALLBACK: using existing cached ou_map (count={len(existing)})", flush=True)
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
        ou_map = build_ou_map(client)
        if not ou_map:
            raise RuntimeError("build_ou_map returned empty mapping")

        save_ou_map(out_dir, ou_map)
        print(f"OK: ou_map.json + ou_map.json.gz generated for {len(ou_map)} OU level 5", flush=True)
        return 0

    except Exception as e:
        print(f"WARN: build_ou_map failed from DHIS2: {e}", flush=True)

        existing = load_existing_ou_map(out_dir)
        if existing:
            print(
                f"FALLBACK: using existing cached docs/data/ou_map.* "
                f"(count={len(existing)}) and continuing workflow.",
                flush=True,
            )
            return 0

        print("ERROR: no cached docs/data/ou_map.json(.gz) available for fallback.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
