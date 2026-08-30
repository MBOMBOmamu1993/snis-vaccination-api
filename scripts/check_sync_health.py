"""Sonde de santé post-synchronisation DHIS2.

Lancée par le workflow après le commit des données. Elle échoue (exit 1)
si la synchronisation laisse le niveau dans un état durablement désynchronisé
vis-à-vis de DHIS2 :

  - un mois clôturé depuis >= 25 jours traîne encore en retry_queue
    (le garde-fou ou l'API le rejette run après run — cause du gel
    ZS de juin/juillet 2026) ;
  - un mois clôturé est stocké avec 0 ligne (mois écrasé par du vide).

Le run GitHub Actions passe alors en rouge et le propriétaire du dépôt est
notifié, au lieu d'un échec silencieux découvert des semaines plus tard sur
le dashboard. Les mois récents (courant / fraîchement clôturés) ont le droit
d'échouer transitoirement : le retry quotidien les rattrape sans alerte.

Usage : python scripts/check_sync_health.py docs/data_zs/index.json
"""
from __future__ import annotations

import json
import sys
from datetime import date


def _month_is_settled(pe: str, today: date | None = None) -> bool:
    """Mois clôturé depuis >= 25 jours : il doit être stable et présent."""
    try:
        y, m = int(pe[:4]), int(pe[4:6])
    except (ValueError, IndexError):
        return False
    if not 1 <= m <= 12:
        return False
    end_y, end_m = (y, m + 1) if m < 12 else (y + 1, 1)
    closed = date(end_y, end_m, 1)
    return ((today or date.today()) - closed).days >= 25


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: check_sync_health.py <index.json>", file=sys.stderr)
        return 2

    with open(sys.argv[1], encoding="utf-8") as fh:
        idx = json.load(fh)

    rq = [m for m in (idx.get("retry_queue") or []) if isinstance(m, str)]
    months = idx.get("months") or {}

    stuck = sorted(m for m in rq if _month_is_settled(m))
    empty = sorted(
        m for m, v in months.items()
        if _month_is_settled(m) and not (v or {}).get("rows")
    )

    if stuck or empty:
        if stuck:
            print(f"ALERTE: mois clôturés toujours en retry_queue: {stuck}")
        if empty:
            print(f"ALERTE: mois clôturés stockés avec 0 ligne: {empty}")
        print("Le dashboard est désynchronisé de DHIS2 pour ces mois — "
              "voir les lignes [GUARD]/ERROR plus haut dans le log.")
        return 1

    print(f"Sonde OK — retry_queue transitoire: {rq or 'vide'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
