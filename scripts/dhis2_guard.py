"""
Garde-fou de validation des données DHIS2 avant écriture.

Contexte (incident mars-juillet 2026) : l'API analytics du SNIS renvoie par
moments des valeurs transitoires fausses — combos « 12-23 mois » gonflés x2/x3,
ou mois entier à zéro — vraisemblablement pendant/juste après la reconstruction
nocturne des tables analytics. Les valeurs brutes saisies (/api/dataValues)
restent, elles, toujours correctes.

Ce module valide les enregistrements d'UN mois avant écriture, via trois
contrôles complémentaires :

  1. spot_check_raw  (niveau FOSA uniquement — les lignes sont au niveau 5,
     celui du stockage brut) : compare un échantillon de cellules aux valeurs
     brutes /api/dataValues, insensibles au rebuild analytics.
  2. ratio_check_vs_stored (tous niveaux, mois « mûrs » seulement) : compare
     les totaux par colonne aux fichiers déjà stockés pour le même mois.
     Un mois clôturé depuis >= 20 jours ne peut ni doubler ni s'effondrer
     entre deux runs quotidiens.
  3. cross_check_vs_reference (AS/ZS, mois mûrs) : compare les totaux
     nationaux par colonne aux fichiers FOSA du même mois (docs/data/monthly),
     eux-mêmes validés par le spot-check. Les totaux LEVEL-3/4/5 sont
     normalement identiques dans DHIS2.

En cas d'anomalie -> GuardError : le script appelant saute le mois (fichiers
existants conservés, mois remis en retry_queue) ; le run suivant réessaie.
Bypass volontaire : --skip_guard.
"""

from __future__ import annotations

import glob
import gzip
import json
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class GuardError(RuntimeError):
    """Les données fetchées sont jugées invalides : ne pas les écrire."""


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _num(v) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def _month_is_mature(pe: str, min_days: int = 20) -> bool:
    """Vrai si le mois pe (YYYYMM) est clôturé depuis au moins min_days jours."""
    try:
        y, m = int(pe[:4]), int(pe[4:6])
    except (ValueError, IndexError):
        return False
    if m == 12:
        first_next = datetime(y + 1, 1, 1)
    else:
        first_next = datetime(y, m + 1, 1)
    return datetime.utcnow() >= first_next + timedelta(days=min_days)


def _column_totals(records: List[dict]) -> Dict[str, float]:
    totals: Dict[str, float] = {}
    for rec in records:
        for k, v in rec.items():
            if k in ("OrgUnit", "Period"):
                continue
            f = _num(v)
            if f is not None:
                totals[k] = totals.get(k, 0.0) + f
    return totals


def _load_month_records(month_folder: Path) -> List[dict]:
    """Lit les part-*.ndjson (ou .ndjson.gz) d'un dossier mensuel."""
    if not month_folder.is_dir():
        return []
    records: List[dict] = []
    plain = sorted(glob.glob(str(month_folder / "part-*.ndjson")))
    if plain:
        for p in plain:
            with open(p, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))
        return records
    for p in sorted(glob.glob(str(month_folder / "part-*.ndjson.gz"))):
        with gzip.open(p, "rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    return records


def build_col_operands(
    rename_map_dx_to_label: Dict[str, str],
    zoho_map_label_to_link: Dict[str, str],
) -> Dict[str, Tuple[str, str]]:
    """colonne de sortie -> (dataElement, categoryOptionCombo).

    Ne garde que les opérandes purs DE.COC (11 caractères de part et d'autre),
    seuls vérifiables via /api/dataValues.
    """
    out: Dict[str, Tuple[str, str]] = {}
    for dx, label in rename_map_dx_to_label.items():
        parts = dx.split(".")
        if len(parts) != 2:
            continue
        de, co = parts
        if len(de) != 11 or len(co) != 11 or not de.isalnum() or not co.isalnum():
            continue
        col = zoho_map_label_to_link.get(label, label)
        out[col] = (de, co)
    return out


def _fetch_raw_value(client, de: str, ou: str, pe: str, co: str) -> Optional[float]:
    """Valeur brute saisie via /api/dataValues. 0.0 si absente, None si erreur technique."""
    url = client.base_url.rstrip("/") + "/api/dataValues"
    try:
        r = client.session.get(
            url,
            params={"de": de, "ou": ou, "pe": pe, "co": co},
            auth=(client.username, client.password),
            headers={"Accept": "application/json"},
            timeout=60,
        )
        if r.status_code in (404, 409):
            return 0.0
        if 200 <= r.status_code < 300:
            body = r.json()
            if isinstance(body, list) and body:
                f = _num(body[0])
                return f if f is not None else None
            return 0.0
        return None
    except Exception:
        return None


# ------------------------------------------------------------------
# Contrôle 1 : échantillon vs valeurs brutes (niveau FOSA)
# ------------------------------------------------------------------

def spot_check_raw(
    client,
    records: List[dict],
    col_operands: Dict[str, Tuple[str, str]],
    pe: str,
    n_priority: int = 8,
    n_other: int = 6,
    max_per_ou: int = 2,
) -> None:
    """Compare un échantillon de cellules aux valeurs brutes /api/dataValues.

    Échantillonne en priorité les colonnes « 12-23 mois » (mode de défaillance
    observé), complété par les plus grosses autres cellules.
    """
    cells: List[Tuple[float, str, str, str, str]] = []  # (val, ou, col, de, co)
    for rec in records:
        ou = rec.get("OrgUnit")
        if not ou:
            continue
        for col, (de, co) in col_operands.items():
            f = _num(rec.get(col))
            if f is not None and f >= 5:
                cells.append((f, ou, col, de, co))

    if not cells:
        print(f"[GUARD {pe}] spot-check: aucune cellule >= 5, contrôle sauté", flush=True)
        return

    def pick(pool, n):
        chosen, per_ou = [], {}
        for c in sorted(pool, key=lambda x: -x[0]):
            if per_ou.get(c[1], 0) >= max_per_ou:
                continue
            chosen.append(c)
            per_ou[c[1]] = per_ou.get(c[1], 0) + 1
            if len(chosen) >= n:
                break
        return chosen

    prio = [c for c in cells if "12_23" in c[2] or "12-23" in c[2]]
    rest = [c for c in cells if c not in prio]
    sample = pick(prio, n_priority) + pick(rest, n_other)

    compared = mismatches = unknown = 0
    details: List[str] = []
    for val, ou, col, de, co in sample:
        raw = _fetch_raw_value(client, de=de, ou=ou, pe=pe, co=co)
        if raw is None:
            unknown += 1
            continue
        compared += 1
        tol = max(2.0, 0.30 * max(val, raw))
        if abs(val - raw) > tol:
            mismatches += 1
            details.append(f"{col}@{ou}: analytics={val:g} brut={raw:g}")

    print(
        f"[GUARD {pe}] spot-check brut: {compared} comparées, "
        f"{mismatches} écarts, {unknown} indisponibles",
        flush=True,
    )
    if unknown > len(sample) / 2:
        print(f"[GUARD {pe}] WARN: majorité de valeurs brutes indisponibles, "
              f"spot-check non concluant", flush=True)
        return
    if mismatches >= 2 and compared > 0 and mismatches / compared >= 0.4:
        raise GuardError(
            f"spot-check brut échoué pour {pe}: {mismatches}/{compared} cellules "
            f"divergent des valeurs saisies (analytics transitoire probable). "
            f"Exemples: " + "; ".join(details[:5])
        )


# ------------------------------------------------------------------
# Contrôle 2 : ratio vs fichiers déjà stockés (même mois)
# ------------------------------------------------------------------

def ratio_check_vs_stored(
    month_folder: Path,
    new_records: List[dict],
    pe: str,
    min_col_total: float = 300.0,
    inflate: float = 1.8,
    collapse: float = 0.15,
    max_flags: int = 2,
) -> None:
    if not _month_is_mature(pe):
        return
    old_records = _load_month_records(month_folder)
    if not old_records:
        return
    old_t = _column_totals(old_records)
    new_t = _column_totals(new_records)

    flags: List[str] = []
    for col, old in old_t.items():
        if old < min_col_total:
            continue
        new = new_t.get(col, 0.0)
        ratio = new / old
        if ratio >= inflate or ratio <= collapse:
            flags.append(f"{col}: {old:g} -> {new:g} (x{ratio:.2f})")

    print(f"[GUARD {pe}] ratio vs stocké: {len(flags)} colonne(s) anormale(s)", flush=True)
    if len(flags) > max_flags:
        raise GuardError(
            f"ratio vs fichiers stockés échoué pour {pe} (mois clôturé, variation "
            f"anormale entre deux runs): " + "; ".join(flags[:6])
        )


# ------------------------------------------------------------------
# Contrôle 3 : totaux nationaux vs référence FOSA (pour AS/ZS)
# ------------------------------------------------------------------

def cross_check_vs_reference(
    reference_month_folder: Path,
    new_records: List[dict],
    pe: str,
    min_col_total: float = 1000.0,
    lo: float = 0.5,
    hi: float = 1.75,
    max_flags: int = 2,
) -> None:
    if not _month_is_mature(pe):
        return
    ref_records = _load_month_records(reference_month_folder)
    if not ref_records:
        print(f"[GUARD {pe}] cross-check: référence FOSA absente "
              f"({reference_month_folder}), contrôle sauté", flush=True)
        return
    ref_t = _column_totals(ref_records)
    new_t = _column_totals(new_records)

    flags: List[str] = []
    for col, ref in ref_t.items():
        if ref < min_col_total:
            continue
        if col not in new_t:
            continue
        ratio = new_t[col] / ref
        if ratio < lo or ratio > hi:
            flags.append(f"{col}: FOSA={ref:g} vs {new_t[col]:g} (x{ratio:.2f})")

    print(f"[GUARD {pe}] cross-check vs FOSA: {len(flags)} colonne(s) anormale(s)", flush=True)
    if len(flags) > max_flags:
        raise GuardError(
            f"cross-check vs données FOSA échoué pour {pe}: les totaux nationaux "
            f"divergent de la référence validée: " + "; ".join(flags[:6])
        )


# ------------------------------------------------------------------
# Point d'entrée unique
# ------------------------------------------------------------------

def check_month(
    client,
    records: List[dict],
    pe: str,
    month_folder: Path,
    rename_map_dx_to_label: Dict[str, str],
    zoho_map_label_to_link: Dict[str, str],
    level: str,
    reference_month_folder: Optional[Path] = None,
) -> None:
    """Valide les enregistrements d'un mois. Lève GuardError si suspect.

    level: "FOSA" (lignes niveau 5 -> spot-check brut), "AS" ou "ZS".
    reference_month_folder: dossier FOSA du même mois (AS/ZS uniquement).
    """
    if not records:
        print(f"[GUARD {pe}] aucun enregistrement: contrôle sauté "
              f"(le script gère déjà ce cas)", flush=True)
        return

    if level == "FOSA":
        col_operands = build_col_operands(rename_map_dx_to_label, zoho_map_label_to_link)
        spot_check_raw(client, records, col_operands, pe)

    ratio_check_vs_stored(month_folder, records, pe)

    if reference_month_folder is not None:
        cross_check_vs_reference(reference_month_folder, records, pe)

    print(f"[GUARD {pe}] OK — données validées", flush=True)
