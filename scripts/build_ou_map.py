from __future__ import annotations

import gzip
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass
class Dhis2Client:
    base_url: str
    username: str
    password: str
    timeout_s: int = 600  # 10 minutes

    def __post_init__(self) -> None:
        retry = Retry(
            total=6,
            connect=6,
            read=6,
            status=6,
            backoff_factor=5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        self.session = requests.Session()
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _get(self, path: str, params: Dict[str, object]) -> dict:
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        r = self.session.get(
            url,
            params=params,
            auth=(self.username, self.password),
            headers={"Accept": "application/json"},
            timeout=self.timeout_s,
        )
        r.raise_for_status()
        return r.json()

    def org_units_level(self, level: int) -> List[dict]:
        params = {
            "filter": f"level:eq:{level}",
            "paging": "false",
            "fields": "id,name,level,path",
        }
        data = self._get("api/organisationUnits.json", params)
        return data.get("organisationUnits", [])


def build_ou_map(client: Dhis2Client) -> Dict[str, Dict[str, str]]:
    """
    Retourne: { ou5_id: {Org2, Org3, Org4, Org5} }
    """
    all_units: Dict[str, dict] = {}
    for lvl in (2, 3, 4, 5):
        units = client.org_units_level(lvl)
        for ou in units:
            all_units[ou["id"]] = ou

    out: Dict[str, Dict[str, str]] = {}

    for ou_id, ou in all_units.items():
        if ou.get("level") != 5:
            continue

        path = ou.get("path") or ""
        ids = [p for p in path.split("/") if p]

        def name_for(level: int) -> str:
            for pid in ids:
                u = all_units.get(pid)
                if u and u.get("level") == level:
                    return u.get("name") or ""
            return ""

        out[ou_id] = {
            "Org2": name_for(2),
            "Org3": name_for(3),
            "Org4": name_for(4),
            "Org5": ou.get("name") or "",
        }

    return out


def write_gz_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    with gzip.open(path, "wb") as f:
        f.write(raw)


def main() -> int:
    base_url = os.environ.get("DHIS2_BASE_URL")
    username = os.environ.get("DHIS2_USERNAME")
    password = os.environ.get("DHIS2_PASSWORD")
    if not (base_url and username and password):
        print("Missing secrets: DHIS2_BASE_URL, DHIS2_USERNAME, DHIS2_PASSWORD", file=sys.stderr)
        return 2

    client = Dhis2Client(base_url=base_url, username=username, password=password)
    ou_map = build_ou_map(client)

    out_dir = Path("docs/data")
    write_gz_json(out_dir / "ou_map.json.gz", ou_map)

    meta = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "count_ou_level5": len(ou_map),
    }
    (out_dir / "ou_map.meta.json").write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")

    print(f"OK: ou_map.json.gz generated for {len(ou_map)} OU level 5")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
