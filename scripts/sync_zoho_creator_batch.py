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


# ============================================================
# ✅ PAYLOAD CONTROL (IMPORTANT)
# ============================================================
# Objectif: réduire payload Zoho en N'ENVOYANT PAS les colonnes détaillées
# (fixe/avancé/mobile et leurs variantes 1/2) et ne garder que:
#   - Meta (Org2..Org5, Antenne, Key, Period, etc.)
#   - Champs "stables" utiles (populations, séances, complétude/promptitude, etc.)
#   - Calculs globaux 0-11 (et autres globaux) calculés ici
#
# IMPORTANT:
# - Les NDJSON contiennent des "link names" Zoho (underscores, pas d'accents),
#   pas les libellés DHIS2.
# - On charge docs/config/rename_map.json pour convertir label -> link name.
PASS_THROUGH_LABELS: set[str] = {
    # --- Reporting / qualité ---
    "Complétude",
    "Promptitude",
    "Rapports attendus",

    # --- Sessions / planification ---
    "Séances prévues",
    "Séances réalisées",
    "Séances fixes prévues",
    "Séances fixes réalisées",
    "Séances avancées prévues",
    "Séances avancées réalisées",
    "Séances mobiles prévues",
    "Séances mobiles réalisées",

    # --- Populations ---
    "Pop. totale",
    "Naissances vivantes",
    "Pop. par AS",
    "Pop. 0-11m (nv)",
    "Pop. 0-11m (survivants)",
    "Pop. 0-59m",
    "Pop. 12-59m",

    # --- Autres ---
    "ECV",
    "HPV",

    # --- PV / BCU-FAE (si tu veux garder) ---
    "Perdues de vue identifiés Penta1 0-11mois",
    "Perdues de vue identifiés Penta1 12-23mois",
    "Perdues de vue identifiés 24-59mois",
    "Perdues de vue récupérés Penta1 0-11mois",
    "Perdues de vue récupérés Penta1 12-23mois",
    "Perdues de vue récupérés Penta1 24-59mois",
    "Perdues de vue identifiés Penta3 0-11mois",
    "Perdues de vue identifiés Penta3 12-23mois",
    "Perdues de vue identifiés Penta3 24-59mois",
    "Perdues de vue récupérés Penta3 0-11mois",
    "Perdues de vue récupérés Penta3 12-23mois",
    "Perdues de vue récupérés Penta3 24-59mois",
    "Enfants récupérés Penta1 BCU-FAE",
    "Enfants récupérés Penta3 BCU-FAE",
    "Enfants récupérés VAR1 BCU-FAE",

    # --- Disagrégations "populations spéciales" (si tu veux garder) ---
    "AVS DTC1",
    "AVS DTC3",
    "AVS VAR1",
    "AVS VAR2",
    "OVM DTC1",
    "OVM DTC3",
    "OVM VAR1",
    "OVM VAR2",
    "Fluviale DTC1",
    "Fluviale DTC3",
    "Fluviale VAR1",
    "Fluviale VAR2",
    "IPVS DTC1",
    "IPVS DTC3",
    "IPVS VAR1",
    "IPVS VAR2",
    "Autochtones DTC1",
    "Autochtones DTC3",
    "Autochtones VAR1",
    "Autochtones VAR2",
    "Nomades DTC1",
    "Nomades DTC3",
    "Nomades VAR1",
    "Nomades VAR2",
    "Réfugiés/Déplacés DTC1",
    "Réfugiés/Déplacés DTC3",
    "Réfugiés/Déplacés VAR1",
    "Réfugiés/Déplacés VAR2",
    "Point de concentration DTC1",
    "Point de concentration DTC3",
    "Point de concentration VAR1",
    "Point de concentration VAR2",
    "Horaire adapté DTC1",
    "Horaire adapté DTC3",
    "Horaire adapté VAR1",
    "Horaire adapté VAR2",
    "Campements DTC1",
    "Campements DTC3",
    "Campements VAR1",
    "Campements VAR2",
    "Poches d'insécurité DTC1",
    "Poches d'insécurité DTC3",
    "Poches d'insécurité VAR1",
    "Poches d'insécurité VAR2",
}

# Champs calculés (toujours envoyés) - déjà en "link name" (underscores)
CALCULATED_FIELDS: set[str] = {
    "BCG_0_11",
    "DTC1_0_11",
    "DTC2_0_11",
    "DTC3_0_11",
    "VPO3_0_11",
    "VPI1_0_11",
    "VPI2_0_11",
    "ROTA3_0_11",
    "PCV13_3_0_11",
    "VAR1_0_11",
    "VAR2_total",
    "VAR2_0_11",
    "VPO0_0_11",
    "VPO1_0_11",
    "VPO2_0_11",
    "PCV13_1_0_11",
    "PCV13_2_0_11",
    "ROTA1_0_11",
    "ROTA2_0_11",
    "VAP1_0_11",
    "VAP2_0_11",
    "VAP3_0_11",
    "VAP4_12_23",
}


# ============================================================
# Zoho DC routing
# ============================================================
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
    - On évite DELETE bulk via API en routine (car DC parfois incompatible).
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
    def _try_parse_json(text: str) -> Optional[Dict[str, Any]]:
        try:
            return json.loads(text)
        except Exception:
            return None

    def _req(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Any = None,
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
        url = f"{self.cfg.creator_base}/data/{self.cfg.owner}/{self.cfg.app_link_name}/report/{self.cfg.report_link_name}"

        params: Dict[str, Any] = {"fields": fields}
        if criteria:
            params["criteria"] = criteria

        # Cursor mode
        if record_cursor is not None:
            params["max_records"] = str(int(per_page))
            params["record_cursor"] = record_cursor
            return self._req("GET", url, params=params)

        # First call attempt (some DC returns cursor without providing it)
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


# ============================================================
# Local repo data helpers
# ============================================================
def load_index(index_path: Path) -> Dict[str, Any]:
    return json.loads(index_path.read_text(encoding="utf-8"))


def sorted_months(index: Dict[str, Any]) -> List[str]:
    months = list((index.get("months") or {}).keys())
    months.sort()
    return months


def last_n_previous_months(months_sorted: List[str], n: int) -> List[str]:
    """
    ✅ "Deux derniers mois précédents" = on EXCLUT le dernier mois de l'index (souvent le mois courant),
    puis on prend les N derniers dans le reste.
    Ex: index [..., 202601, 202602, 202603] et n=2 => [202601, 202602]
    """
    if n <= 0:
        return []
    if len(months_sorted) <= 1:
        return []
    base = months_sorted[:-1]  # exclude latest
    if not base:
        return []
    return base[-n:] if len(base) >= n else base[:]


def load_ou_map(repo_root: Path) -> Dict[str, Dict[str, str]]:
    gz_path = repo_root / "docs" / "data" / "ou_map.json.gz"
    js_path = repo_root / "docs" / "data" / "ou_map.json"

    if gz_path.exists():
        raw = gzip.open(gz_path, "rb").read()
        return json.loads(raw.decode("utf-8"))

    if js_path.exists():
        return json.loads(js_path.read_text(encoding="utf-8"))

    raise FileNotFoundError("Missing docs/data/ou_map.json(.gz). Run build_ou_map.py first.")


def load_zoho_rename_map(repo_root: Path) -> Dict[str, str]:
    """
    docs/config/rename_map.json:
      { "BCG avancé1": "BCG_avanc_1", ... }
    """
    p = repo_root / "docs" / "config" / "rename_map.json"
    if not p.exists():
        return {}
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(obj, dict):
            return {str(k): str(v) for k, v in obj.items()}
        return {}
    except Exception:
        return {}


def load_antenne_rules(repo_root: Path) -> Dict[str, Dict[str, str]]:
    """
    docs/config/antenne_rules.json
    Format:
      { "Org2": { "Zone de Santé": "Antenne", ... }, ... }
    """
    p = repo_root / "docs" / "config" / "antenne_rules.json"
    if not p.exists():
        return {}
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
        out: Dict[str, Dict[str, str]] = {}
        if isinstance(obj, dict):
            for org2, m in obj.items():
                if isinstance(m, dict):
                    out[str(org2)] = {str(zs): str(a) for zs, a in m.items()}
        return out
    except Exception:
        return {}


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


# ============================================================
# Transform: NDJSON row -> Zoho record
# ============================================================
def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def _num(x: Any) -> float:
    if x is None:
        return 0.0
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        return float(x)
    s = str(x).strip()
    if s == "" or s.lower() == "null":
        return 0.0
    try:
        return float(s.replace(",", "."))
    except Exception:
        return 0.0


def _sum_by_link(row: Dict[str, Any], *link_fields: str) -> float:
    return sum(_num(row.get(f)) for f in link_fields)


def zone_de_sante_from_org3(org3_name: str) -> str:
    """
    équivalent PowerQuery:
      Text.BetweenDelimiters([Level 3 Org Unit], " ", " Zone")
    Ici Org3 ressemble à "xx <ZoneDeSante> Zone ..."
    """
    s = (org3_name or "").strip()
    if not s:
        return ""
    i = s.find(" ")
    j = s.find(" Zone")
    if i >= 0 and j > i:
        return s[i + 1: j].strip()
    return ""


def antenne_from(org2: str, org3: str, rules: Dict[str, Dict[str, str]]) -> str:
    zs = zone_de_sante_from_org3(org3)
    if not zs:
        return "Autre"
    m = rules.get((org2 or "").strip()) or {}
    return m.get(zs) or "Autre"


def build_zoho_record(
    row: Dict[str, Any],
    raw_line: str,
    ou_map: Dict[str, Dict[str, str]],
    antenne_rules: Dict[str, Dict[str, str]],
    zoho_rename_map: Dict[str, str],  # label -> link name
    pass_through_links: set[str],     # link names
) -> Dict[str, Any]:
    ou = str(row.get("OrgUnit") or "").strip()
    period = str(row.get("Period") or "").strip()

    if not ou or not period:
        return {}

    meta = ou_map.get(ou) or {}
    rec: Dict[str, Any] = {}

    # --- Meta
    rec["Org5_UID"] = ou
    rec["Period"] = period
    rec["Key"] = f"{ou}|{period}"

    rh = row.get("RowHash")
    rec["RowHash"] = str(rh).strip() if rh else md5_hex(raw_line)

    rec["Org2"] = meta.get("Org2", "")
    rec["Org3"] = meta.get("Org3", "")
    rec["Org4"] = meta.get("Org4", "")
    rec["Org5"] = meta.get("Org5", "")

    # ✅ Antenne (niveau intermédiaire entre Org2 et Org3)
    rec["Antenne"] = antenne_from(rec["Org2"], rec["Org3"], antenne_rules)

    # Helper label -> link
    def L(label: str) -> str:
        return zoho_rename_map.get(label, label)

    # ✅ PASS-THROUGH: uniquement les champs utiles (en link names)
    for k, v in row.items():
        if k in ("OrgUnit", "Period", "Key", "Org5_UID", "Org2", "Org3", "Org4", "Org5", "RowHash"):
            continue
        if k in pass_through_links:
            rec[k] = v

    # ✅ Calculs globaux 0-11 mois (en lisant les colonnes link names)
    rec["BCG_0_11"] = _sum_by_link(
        row,
        L("BCG fixe1"), L("BCG fixe2"),
        L("BCG avancé1"), L("BCG avancé2"),
        L("BCG mobile1"), L("BCG mobile2"),
    )

    rec["DTC1_0_11"] = _sum_by_link(
        row,
        L("Penta1 fixe1"), L("Penta1 fixe2"),
        L("Penta1 avancé1"), L("Penta1 avancé2"),
        L("Penta1 mobile1"), L("Penta1 mobile2"),
    )

    rec["DTC2_0_11"] = _sum_by_link(
        row,
        L("Penta2 fixe1"), L("Penta2 fixe2"),
        L("Penta2 avancé1"), L("Penta2 avancé2"),
        L("Penta2 mobile1"), L("Penta2 mobile2"),
    )

    rec["DTC3_0_11"] = _sum_by_link(
        row,
        L("Penta3 fixe1"), L("Penta3 fixe2"),
        L("Penta3 avancé1"), L("Penta3 avancé2"),
        L("Penta3 mobile1"), L("Penta3 mobile2"),
    )

    rec["VPO3_0_11"] = _sum_by_link(
        row,
        L("VPO3 fixe1"), L("VPO3 fixe2"),
        L("VPO3 avancé1"), L("VPO3 avancé2"),
        L("VPO3 mobile1"), L("VPO3 mobile2"),
    )

    rec["VPI1_0_11"] = _sum_by_link(
        row,
        L("VPI1 fixe1"), L("VPI1 fixe2"),
        L("VPI1 avancé1"), L("VPI1 avancé2"),
        L("VPI1 mobile1"), L("VPI1 mobile2"),
    )

    rec["VPI2_0_11"] = _sum_by_link(
        row,
        L("VPI2 fixe1"), L("VPI2 fixe2"),
        L("VPI2 avancé1"), L("VPI2 avancé2"),
        L("VPI2 mobile1"), L("VPI2 mobile2"),
    )

    rec["ROTA3_0_11"] = _sum_by_link(row, L("ROTA3 fixe"), L("ROTA3 avancé"), L("ROTA3 mobile"))

    rec["PCV13_3_0_11"] = _sum_by_link(
        row,
        L("PCV13 fixe1"), L("PCV13 fixe2"),
        L("PCV13 avancé1"), L("PCV13 avancé2"),
        L("PCV13 mobile1"), L("PCV13 mobile2"),
    )

    rec["VAR1_0_11"] = _sum_by_link(
        row,
        L("VAR1 fixe1"), L("VAR1 fixe2"),
        L("VAR1 avancé1"), L("VAR1 avancé2"),
        L("VAR1 mobile1"), L("VAR1 mobile2"),
    )

    rec["VAR2_total"] = _sum_by_link(
        row,
        L("VAR2 fixe1"), L("VAR2 fixe2"),
        L("VAR2 avancé"),
        L("VAR2 mobile1"), L("VAR2 mobile2"),
    )

    rec["VAR2_0_11"] = _sum_by_link(row, L("VAR2 0-11 mois fixe"), L("VAR2 0-11 mois avancée"), L("VAR2 0-11 mois mobile"))

    rec["VPO0_0_11"] = _sum_by_link(
        row,
        L("VPO0 0-11 mois fixe1"), L("VPO0 0-11 mois fixe2"),
        L("VPO0 0-11 mois avancée1"), L("VPO0 0-11 mois avancée2"),
        L("VPO0 0-11 mois mobile1"), L("VPO0 0-11 mois mobile2"),
    )

    rec["VPO1_0_11"] = _sum_by_link(
        row,
        L("VPO1 0-11 mois fixe1"), L("VPO1 0-11 mois fixe2"),
        L("VPO1 0-11 mois avancée1"), L("VPO1 0-11 mois avancée2"),
        L("VPO1 0-11 mois mobile1"), L("VPO1 0-11 mois mobile2"),
    )

    rec["VPO2_0_11"] = _sum_by_link(
        row,
        L("VPO2 0-11 mois fixe1"), L("VPO2 0-11 mois fixe2"),
        L("VPO2 0-11 mois avancée1"), L("VPO2 0-11 mois avancée2"),
        L("VPO2 0-11 mois mobile1"), L("VPO2 0-11 mois mobile2"),
    )

    rec["PCV13_1_0_11"] = _sum_by_link(
        row,
        L("PCV13(1) 0-11 mois fixe1"), L("PCV13(1) 0-11 mois fixe2"),
        L("PCV13(1) 0-11 mois avancée1"), L("PCV13(1) 0-11 mois avancée2"),
        L("PCV13(1) 0-11 mois mobile1"), L("PCV13(1) 0-11 mois mobile2"),
    )

    rec["PCV13_2_0_11"] = _sum_by_link(
        row,
        L("PCV13(2) 0-11 mois fixe1"), L("PCV13(2) 0-11 mois fixe2"),
        L("PCV13(2) 0-11 mois avancée1"), L("PCV13(2) 0-11 mois avancée2"),
        L("PCV13(2) 0-11 mois mobile1"), L("PCV13(2) 0-11 mois mobile2"),
    )

    rec["ROTA1_0_11"] = _sum_by_link(row, L("ROTA1 0-11 mois fixe"), L("ROTA1 0-11 mois avancée"), L("ROTA1 0-11 mois mobile"))
    rec["ROTA2_0_11"] = _sum_by_link(row, L("ROTA2 0-11 mois fixe"), L("ROTA2 0-11 mois avancée"), L("ROTA2 0-11 mois mobile"))

    rec["VAP1_0_11"] = _sum_by_link(row, L("VAP1 0-11 mois fixe"), L("VAP1 0-11 mois avancée"))
    rec["VAP2_0_11"] = _sum_by_link(row, L("VAP2 0-11 mois fixe"), L("VAP2 0-11 mois avancée"), L("VAP2 0-11 mois mobile"))
    rec["VAP3_0_11"] = _sum_by_link(row, L("VAP3 0-11 mois fixe"), L("VAP3 0-11 mois avancée"), L("VAP3 0-11 mois mobile"))
    rec["VAP4_12_23"] = _sum_by_link(row, L("VAP4 12-23 mois fixe"), L("VAP4 12-23 mois avancée"), L("VAP4 12-23 mois mobile"))

    return rec


# ============================================================
# PURGE helpers (manual only)
# ============================================================
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


# ============================================================
# ADD-only import for a month (batch) with safe fallback
# ============================================================
def _robust_add_records(
    client: ZohoCreatorClient,
    records: List[Dict[str, Any]],
    *,
    throttle_s: float,
    depth: int = 0,
    max_depth: int = 10,
) -> Tuple[int, int]:
    """
    Essaie d'ajouter en batch.
    Si Zoho rejette le batch (doublons Key, validation, etc.), on split pour isoler et continuer.
    Returns: (inserted_count_best_effort, failed_count)
    """
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
    antenne_rules: Dict[str, Dict[str, str]],
    zoho_rename_map: Dict[str, str],
    pass_through_links: set[str],
    *,
    batch_size: int,
    throttle_s: float,
) -> Dict[str, Any]:
    month_dir = repo_root / "docs" / "data" / "monthly" / yyyymm

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
            rec = build_zoho_record(obj, raw, ou_map, antenne_rules, zoho_rename_map, pass_through_links)
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


# ============================================================
# MAIN
# ============================================================
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo_root", default=".", help="Repository root (default: .)")
    ap.add_argument("--index", default="docs/data/index.json")
    ap.add_argument("--state", default="docs/data/zoho_sync_state.json")

    # Routine knobs
    ap.add_argument(
        "--refresh_last_n",
        type=int,
        default=2,
        help="ADD-ONLY for the last N PREVIOUS months (M-1..). Recommended=2.",
    )
    ap.add_argument("--batch_size", type=int, default=200)
    ap.add_argument("--throttle_seconds", type=float, default=1.5)

    # Manual purge
    ap.add_argument("--purge_all", action="store_true")
    ap.add_argument("--purge_only", action="store_true")
    ap.add_argument("--purge_batch_limit", type=int, default=200)

    # Full import mode (initialization)
    ap.add_argument("--import_historical", action="store_true", help="Import all historical months (add only).")

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

    # Load Antenne rules
    antenne_rules = load_antenne_rules(repo_root)
    print(f"ANTENNE_RULES loaded: org2={len(antenne_rules)}", flush=True)

    # Load Zoho rename map (label -> link name)
    zoho_rename_map = load_zoho_rename_map(repo_root)
    if not zoho_rename_map:
        print("WARN: rename_map.json missing/empty. Pass-through + sums may not match link names.", flush=True)

    # Build pass-through link names from labels
    pass_through_links: set[str] = set()
    for lbl in PASS_THROUGH_LABELS:
        pass_through_links.add(zoho_rename_map.get(lbl, lbl))

    # State
    state = load_state(state_path)
    done_months: Dict[str, Any] = state.get("done_months") or {}

    # ✅ Routine: only ADD the last N previous months (assumes Deluge deleted them at 00:00)
    refresh_months = last_n_previous_months(months_sorted, args.refresh_last_n)

    print(f"Months in index: {months_sorted[0]} -> {months_sorted[-1]} (count={len(months_sorted)})", flush=True)
    print(f"ADD-only months (previous N={args.refresh_last_n}) = {refresh_months}", flush=True)
    print(
        f"batch_size={args.batch_size} throttle_seconds={args.throttle_seconds} import_historical={args.import_historical}",
        flush=True,
    )
    print(
        f"PASS_THROUGH_LABELS={len(PASS_THROUGH_LABELS)} => PASS_THROUGH_LINKS={len(pass_through_links)} | "
        f"CALCULATED_FIELDS={len(CALCULATED_FIELDS)}",
        flush=True,
    )

    # 1) Daily add-only for previous months
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
            antenne_rules=antenne_rules,
            zoho_rename_map=zoho_rename_map,
            pass_through_links=pass_through_links,
            batch_size=args.batch_size,
            throttle_s=args.throttle_seconds,
        )
        print(
            f"[{m}] local={stats['local_rows']} batches={stats['batches']} "
            f"inserted={stats['inserted']} failed={stats['failed']} skipped_no_key={stats['skipped_no_key']}",
            flush=True,
        )
        done_months[m] = {"done": True, "refreshed": True, "last_sync": time.time(), "mode": "daily_add_only_previous"}

    # 2) Historical import (manual)
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
                antenne_rules=antenne_rules,
                zoho_rename_map=zoho_rename_map,
                pass_through_links=pass_through_links,
                batch_size=args.batch_size,
                throttle_s=args.throttle_seconds,
            )
            print(
                f"[{m}] local={stats['local_rows']} batches={stats['batches']} "
                f"inserted={stats['inserted']} failed={stats['failed']} skipped_no_key={stats['skipped_no_key']}",
                flush=True,
            )
            done_months[m] = {"done": True, "refreshed": False, "last_sync": time.time(), "mode": "historical_add_only"}

    state["done_months"] = done_months
    state["last_run_at"] = time.time()
    save_state(state_path, state)

    print("OK: Zoho sync completed.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
