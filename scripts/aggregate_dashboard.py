from __future__ import annotations

import json
import gzip
import os
import re
import sys
from pathlib import Path
from collections import defaultdict
import shutil

DOCS = Path("docs")
DATA = DOCS / "data"
MONTHLY = DATA / "monthly"
DASH = DATA / "dashboard"

# Clean previous dashboard
if DASH.exists():
    shutil.rmtree(DASH)
DASH.mkdir(parents=True, exist_ok=True)

# Empêcher Jekyll d'ignorer les fichiers
(DOCS / ".nojekyll").touch()

# ── Load ou_map ──
ou_map_gz = DATA / "ou_map.json.gz"
ou_map_js = DATA / "ou_map.json"
if ou_map_gz.exists():
    with gzip.open(ou_map_gz, "rt", encoding="utf-8") as f:
        OU_MAP = json.load(f)
elif ou_map_js.exists():
    with open(ou_map_js, encoding="utf-8") as f:
        OU_MAP = json.load(f)
else:
    print("ERROR: ou_map.json(.gz) not found"); sys.exit(1)

# ── Load antenne_rules ──
ANT_RULES: dict = {}
ant_path = DOCS / "config" / "antenne_rules.json"
if ant_path.exists():
    with open(ant_path, encoding="utf-8") as f:
        ANT_RULES = json.load(f)

# ==========================================================
# SOMMATIONS PRIMAIRES (champs NDJSON renommés)
# ==========================================================
SUM_SPECS: dict[str, list[str]] = {
    # ── 0-11 MOIS ──
    "BCG_0_11": ["BCG_fixe1","BCG_fixe2","BCG_avanc_1","BCG_avanc_2","BCG_mobile1","BCG_mobile2"],
    "BCG_0_11_fixe": ["BCG_fixe1","BCG_fixe2"],
    "DTC1_0_11": ["Penta1_fixe1","Penta1_fixe2","Penta1_avanc_1","Penta1_avanc_2","Penta1_mobile1","Penta1_mobile2"],
    "DTC1_0_11_fixe": ["Penta1_fixe1","Penta1_fixe2"],
    "DTC2_0_11": ["Penta2_fixe1","Penta2_fixe2","Penta2_avanc_1","Penta2_avanc_2","Penta2_mobile1","Penta2_mobile2"],
    "DTC2_0_11_fixe": ["Penta2_fixe1","Penta2_fixe2"],
    "DTC3_0_11": ["Penta3_fixe1","Penta3_fixe2","Penta3_avanc_1","Penta3_avanc_2","Penta3_mobile1","Penta3_mobile2"],
    "DTC3_0_11_fixe": ["Penta3_fixe1","Penta3_fixe2"],
    "VPO0_0_11": ["VPO0_0_11_mois_fixe1","VPO0_0_11_mois_fixe2","VPO0_0_11_mois_avanc_e1","VPO0_0_11_mois_avanc_e2","VPO0_0_11_mois_mobile1","VPO0_0_11_mois_mobile2"],
    "VPO0_0_11_fixe": ["VPO0_0_11_mois_fixe1","VPO0_0_11_mois_fixe2"],
    "VPO1_0_11": ["VPO1_0_11_mois_fixe1","VPO1_0_11_mois_fixe2","VPO1_0_11_mois_avanc_e1","VPO1_0_11_mois_avanc_e2","VPO1_0_11_mois_mobile1","VPO1_0_11_mois_mobile2"],
    "VPO1_0_11_fixe": ["VPO1_0_11_mois_fixe1","VPO1_0_11_mois_fixe2"],
    "VPO2_0_11": ["VPO2_0_11_mois_fixe1","VPO2_0_11_mois_fixe2","VPO2_0_11_mois_avanc_e1","VPO2_0_11_mois_avanc_e2","VPO2_0_11_mois_mobile1","VPO2_0_11_mois_mobile2"],
    "VPO2_0_11_fixe": ["VPO2_0_11_mois_fixe1","VPO2_0_11_mois_fixe2"],
    "VPO3_0_11": ["VPO3_fixe1","VPO3_fixe2","VPO3_avanc_1","VPO3_avanc_2","VPO3_mobile1","VPO3_mobile2"],
    "VPO3_0_11_fixe": ["VPO3_fixe1","VPO3_fixe2"],
    "VPI1_0_11": ["VPI1_fixe1","VPI1_fixe2","VPI1_avanc_1","VPI1_avanc_2","VPI1_mobile1","VPI1_mobile2"],
    "VPI1_0_11_fixe": ["VPI1_fixe1","VPI1_fixe2"],
    "VPI2_0_11": ["VPI2_fixe1","VPI2_fixe2","VPI2_avanc_1","VPI2_avanc_2","VPI2_mobile1","VPI2_mobile2"],
    "VPI2_0_11_fixe": ["VPI2_fixe1","VPI2_fixe2"],
    "ROTA1_0_11": ["ROTA1_0_11_mois_fixe","ROTA1_0_11_mois_avanc_e","ROTA1_0_11_mois_mobile"],
    "ROTA1_0_11_fixe": ["ROTA1_0_11_mois_fixe"],
    "ROTA2_0_11": ["ROTA2_0_11_mois_fixe","ROTA2_0_11_mois_avanc_e","ROTA2_0_11_mois_mobile"],
    "ROTA2_0_11_fixe": ["ROTA2_0_11_mois_fixe"],
    "ROTA3_0_11": ["ROTA3_fixe","ROTA3_avanc_","ROTA3_mobile"],
    "ROTA3_0_11_fixe": ["ROTA3_fixe"],
    "PCV13_1_0_11": ["PCV13_1_0_11_mois_fixe1","PCV13_1_0_11_mois_fixe2","PCV13_1_0_11_mois_avanc_e1","PCV13_1_0_11_mois_avanc_e2","PCV13_1_0_11_mois_mobile1","PCV13_1_0_11_mois_mobile2"],
    "PCV13_1_0_11_fixe": ["PCV13_1_0_11_mois_fixe1","PCV13_1_0_11_mois_fixe2"],
    "PCV13_2_0_11": ["PCV13_2_0_11_mois_fixe1","PCV13_2_0_11_mois_fixe2","PCV13_2_0_11_mois_avanc_e1","PCV13_2_0_11_mois_avanc_e2","PCV13_2_0_11_mois_mobile1","PCV13_2_0_11_mois_mobile2"],
    "PCV13_2_0_11_fixe": ["PCV13_2_0_11_mois_fixe1","PCV13_2_0_11_mois_fixe2"],
    "PCV13_3_0_11": ["PCV13_fixe1","PCV13_fixe2","PCV13_avanc_1","PCV13_avanc_2","PCV13_mobile1","PCV13_mobile2"],
    "PCV13_3_0_11_fixe": ["PCV13_fixe1","PCV13_fixe2"],
    "VAR1_0_11": ["VAR1_fixe1","VAR1_fixe2","VAR1_avanc_1","VAR1_avanc_2","VAR1_mobile1","VAR1_mobile2"],
    "VAR1_0_11_fixe": ["VAR1_fixe1","VAR1_fixe2"],
    "VAR2_0_11": ["VAR2_0_11_mois_fixe","VAR2_0_11_mois_avanc_e","VAR2_0_11_mois_mobile"],
    "VAR2_0_11_fixe": ["VAR2_0_11_mois_fixe"],
    "VAR2_fixe": ["VAR2_fixe1","VAR2_fixe2"],
    "VAR2": ["VAR2_fixe1","VAR2_fixe2","VAR2_avanc_","VAR2_mobile1","VAR2_mobile2"],
    "VAA_0_11": ["VAA_fixe1","VAA_fixe2","VAA_avanc_1","VAA_avanc_2","VAA_mobile1","VAA_mobile2"],
    "VAA_0_11_fixe": ["VAA_fixe1","VAA_fixe2"],
    "VAP1_0_11": ["VAP1_0_11_mois_fixe","VAP1_0_11_mois_avanc_e","VAP1_0_11_mois_mobile"],
    "VAP1_0_11_fixe": ["VAP1_0_11_mois_fixe"],
    "VAP2_0_11": ["VAP2_0_11_mois_fixe","VAP2_0_11_mois_avanc_e","VAP2_0_11_mois_mobile"],
    "VAP2_0_11_fixe": ["VAP2_0_11_mois_fixe"],
    "VAP3_0_11": ["VAP3_0_11_mois_fixe","VAP3_0_11_mois_avanc_e","VAP3_0_11_mois_mobile"],
    "VAP3_0_11_fixe": ["VAP3_0_11_mois_fixe"],

    # ── 12-23 MOIS ──
    "BCG_12_23": ["BCG_12_23_mois_fixe1","BCG_12_23_mois_fixe2","BCG_12_23_mois_avanc_1","BCG_12_23_mois_avanc_2","BCG_12_23_mois_mobile1"],
    "BCG_12_23_fixe": ["BCG_12_23_mois_fixe1","BCG_12_23_mois_fixe2"],
    "DTC1_12_23": ["Penta1_12_23_mois_fixe1","Penta1_12_23_mois_fixe2","Penta1_12_23_mois_avanc_1","Penta1_12_23_mois_avanc_2","Penta1_12_23_mois_mobile1"],
    "DTC1_12_23_fixe": ["Penta1_12_23_mois_fixe1","Penta1_12_23_mois_fixe2"],
    "DTC2_12_23": ["Penta2_12_23_mois_fixe1","Penta2_12_23_mois_fixe2","Penta2_12_23_mois_avanc_1","Penta2_12_23_mois_avanc_2","Penta2_12_23_mois_mobile1"],
    "DTC2_12_23_fixe": ["Penta2_12_23_mois_fixe1","Penta2_12_23_mois_fixe2"],
    "DTC3_12_23": ["Penta3_12_23_mois_fixe1","Penta3_12_23_mois_fixe2","Penta3_12_23_mois_avanc_1","Penta3_12_23_mois_avanc_2","Penta3_12_23_mois_mobile1"],
    "DTC3_12_23_fixe": ["Penta3_12_23_mois_fixe1","Penta3_12_23_mois_fixe2"],
    "VPO0_12_23": ["VPO0_12_23_mois_fixe1","VPO0_12_23_mois_fixe2","VPO0_12_23_mois_mobile1"],
    "VPO0_12_23_fixe": ["VPO0_12_23_mois_fixe1","VPO0_12_23_mois_fixe2"],
    "VPO1_12_23": ["VPO1_12_23_mois_fixe1","VPO1_12_23_mois_fixe2","VPO1_12_23_mois_avanc_1","VPO1_12_23_mois_avanc_2","VPO1_12_23_mois_mobile1"],
    "VPO1_12_23_fixe": ["VPO1_12_23_mois_fixe1","VPO1_12_23_mois_fixe2"],
    "VPO2_12_23": ["VPO2_12_23_mois_fixe1","VPO2_23_mois_fixe2","VPO2_12_23_mois_avanc_1","VPO2_12_23_mois_avanc_2","VPO2_12_23_mois_mobile1"],
    "VPO2_12_23_fixe": ["VPO2_12_23_mois_fixe1","VPO2_12_23_mois_fixe2"],
    "VPO3_12_23": ["VPO3_12_23_mois_fixe1","VPO3_12_23_mois_fixe2","VPO3_12_23_mois_avanc_1","VPO3_12_23_mois_avanc_2","VPO3_12_23_mois_mobile1"],
    "VPO3_12_23_fixe": ["VPO3_12_23_mois_fixe1","VPO3_12_23_mois_fixe2"],
    "VPI1_12_23": ["VPI1_12_23_mois_fixe1","VPI1_12_23_mois_fixe2","VPI1_12_23_mois_avanc_1","VPI1_12_23_mois_avanc_2","VPI1_12_23_mois_mobile1"],
    "VPI1_12_23_fixe": ["VPI1_12_23_mois_fixe1","VPI1_12_23_mois_fixe2"],
    "VPI2_12_23": ["VPI2_12_23_mois_fixe1","VPI2_12_23_mois_fixe2","VPI2_12_23_mois_avanc_1","VPI2_12_23_mois_avanc_2","VPI2_12_23_mois_mobile1"],
    "VPI2_12_23_fixe": ["VPI2_12_23_mois_fixe1","VPI2_12_23_mois_fixe2"],
    "PCV13_1_12_23": ["PCV13_1_12_23_mois_fixe1","PCV13_1_12_23_mois_fixe2","PCV13_1_12_23_mois_avanc_1","PCV13_1_12_23_mois_avanc_2","PCV13_1_12_23_mois_mobile1"],
    "PCV13_1_12_23_fixe": ["PCV13_1_12_23_mois_fixe1","PCV13_1_12_23_mois_fixe2"],
    "PCV13_2_12_23": ["PCV13_2_12_23_mois_fixe1","PCV13_2_12_23_mois_fixe2","PCV13_2_12_23_mois_avanc_1","PCV13_2_12_23_mois_avanc_2","PCV13_2_12_23_mois_mobile1"],
    "PCV13_2_12_23_fixe": ["PCV13_2_12_23_mois_fixe1","PCV13_2_12_23_mois_fixe2"],
    "PCV13_3_12_23": ["PCV13_3_12_23_mois_fixe1","PCV13_3_12_23_mois_fixe2","PCV13_3_12_23_mois_avanc_1","PCV13_3_12_23_mois_avanc_2","PCV13_3_12_23_mois_mobile1"],
    "PCV13_3_12_23_fixe": ["PCV13_3_12_23_mois_fixe1","PCV13_3_12_23_mois_fixe2"],
    "ROTA1_12_23": ["ROTA1_12_23_mois_fixe1","ROTA1_12_23_mois_fixe2","ROTA1_12_23_mois_avanc_1","ROTA1_12_23_mois_avanc_2","ROTA1_12_23_mois_mobile1"],
    "ROTA1_12_23_fixe": ["ROTA1_12_23_mois_fixe1","ROTA1_12_23_mois_fixe2"],
    "ROTA2_12_23": ["ROTA2_12_23_mois_fixe1","ROTA2_12_23_mois_fixe2","ROTA2_12_23_mois_avanc_1","ROTA2_12_23_mois_avanc_2","ROTA2_12_23_mois_mobile1"],
    "ROTA2_12_23_fixe": ["ROTA2_12_23_mois_fixe1","ROTA2_12_23_mois_fixe2"],
    "ROTA3_12_23": ["ROTA3_12_23_mois_fixe1","ROTA3_12_23_mois_fixe2","ROTA3_12_23_mois_mobile1"],
    "ROTA3_12_23_fixe": ["ROTA3_12_23_mois_fixe1","ROTA3_12_23_mois_fixe2"],
    "VAR1_12_23": ["VAR1_12_23_mois_fixe1","VAR1_12_23_mois_avanc_1","VAR1_12_23_mois_mobile1"],
    "VAR1_12_23_fixe": ["VAR1_12_23_mois_fixe1"],
    "VAR2_12_23": ["VAR2_12_23_mois_fixe1","VAR2_12_23_mois_avanc_1","VAR2_12_23_mois_mobile1"],
    "VAR2_12_23_fixe": ["VAR2_12_23_mois_fixe1"],
    "VAA_12_23": ["VAA_12_23_mois_fixe1","VAA_12_23_mois_avanc_1","VAA_12_23_mois_mobile1"],
    "VAA_12_23_fixe": ["VAA_12_23_mois_fixe1"],
    "ECV_12_23": ["ECV_12_23_mois_fixe1","ECV_12_23_mois_avanc_1","ECV_12_23_mois_mobile1"],
    "ECV_12_23_fixe": ["ECV_12_23_mois_fixe1"],
    "HPV_12_23": ["HPV_12_23_mois_fixe1","HPV_12_23_mois_fixe2","HPV_12_23_mois_avanc_1","HPV_12_23_mois_avanc_2","HPV_12_23_mois_mobile1"],
    "HPV_12_23_fixe": ["HPV_12_23_mois_fixe1","HPV_12_23_mois_fixe2"],
    "VAP4_12_23": ["VAP4_12_23_mois_fixe","VAP4_12_23_mois_avanc_e","VAP4_12_23_mois_mobile"],
    "VAP4_12_23_fixe": ["VAP4_12_23_mois_fixe"],

    # ── 24-59 MOIS ──
    "BCG_24_59": ["BCG_24_59_mois_fixe","BCG_24_59_mois_avanc_","BCG_24_59_mois_mobile"],
    "BCG_24_59_fixe": ["BCG_24_59_mois_fixe"],
    "DTC1_24_59": ["Penta1_24_59_mois_fixe","Penta1_24_59_mois_avanc_","Penta1_24_59_mois_mobile"],
    "DTC1_24_59_fixe": ["Penta1_24_59_mois_fixe"],
    "DTC2_24_59": ["Penta2_24_59_mois_fixe","Penta2_24_59_mois_avanc_","Penta2_24_59_mois_mobile"],
    "DTC2_24_59_fixe": ["Penta2_24_59_mois_fixe"],
    "DTC3_24_59": ["Penta3_24_59_mois_fixe","Penta3_24_59_mois_avanc_","Penta3_24_59_mois_mobile"],
    "DTC3_24_59_fixe": ["Penta3_24_59_mois_fixe"],
    "VPO0_24_59": ["VPO0_24_59_mois_fixe","VPO0_24_59_mois_avanc_","VPO0_24_59_mois_mobile"],
    "VPO0_24_59_fixe": ["VPO0_24_59_mois_fixe"],
    "VPO1_24_59": ["VPO1_24_59_mois_fixe","VPO1_24_59_mois_avanc_","VPO1_24_59_mois_mobile"],
    "VPO1_24_59_fixe": ["VPO1_24_59_mois_fixe"],
    "VPO2_24_59": ["VPO2_24_59_mois_fixe","VPO2_24_59_mois_avanc_","VPO2_24_59_mois_mobile"],
    "VPO2_24_59_fixe": ["VPO2_24_59_mois_fixe"],
    "VPO3_24_59": ["VPO3_24_59_mois_fixe","VPO3_24_59_mois_avanc_","VPO3_24_59_mois_mobile"],
    "VPO3_24_59_fixe": ["VPO3_24_59_mois_fixe"],
    "VPI1_24_59": ["VPI1_24_59_mois_fixe","VPI1_24_59_mois_avanc_","VPI1_24_59_mois_mobile"],
    "VPI1_24_59_fixe": ["VPI1_24_59_mois_fixe"],
    "VPI2_24_59": ["VPI2_24_59_mois_fixe","VPI2_24_59_mois_avanc_","VPI2_24_59_mois_mobile"],
    "VPI2_24_59_fixe": ["VPI2_24_59_mois_fixe"],
    "PCV13_1_24_59": ["PCV13_1_24_59_mois_fixe","PCV13_1_24_59_mois_avanc_","PCV13_1_24_59_mois_mobile"],
    "PCV13_1_24_59_fixe": ["PCV13_1_24_59_mois_fixe"],
    "PCV13_2_24_59": ["PCV13_2_24_59_mois_fixe","PCV13_2_24_59_mois_avanc_","PCV13_2_24_59_mois_mobile"],
    "PCV13_2_24_59_fixe": ["PCV13_2_24_59_mois_fixe"],
    "PCV13_3_24_59": ["PCV13_3_24_59_mois_fixe","PCV13_3_24_59_mois_avanc_","PCV13_3_24_59_mois_mobile"],
    "PCV13_3_24_59_fixe": ["PCV13_3_24_59_mois_fixe"],
    "ROTA1_24_59": ["ROTA1_24_59_mois_fixe","ROTA1_24_59_mois_avanc_","ROTA1_24_59_mois_mobile"],
    "ROTA1_24_59_fixe": ["ROTA1_24_59_mois_fixe"],
    "ROTA2_24_59": ["ROTA2_24_59_mois_fixe","ROTA2_24_59_mois_avanc_","ROTA2_24_59_mois_mobile"],
    "ROTA2_24_59_fixe": ["ROTA2_24_59_mois_fixe"],
    "ROTA3_24_59": ["ROTA3_24_59_mois_fixe","ROTA3_24_59_mois_avanc_","ROTA3_24_59_mois_mobile"],
    "ROTA3_24_59_fixe": ["ROTA3_24_59_mois_fixe"],
    "VAR1_24_59": ["VAR1_24_59_mois_fixe","VAR1_24_59_mois_mobile"],
    "VAR1_24_59_fixe": ["VAR1_24_59_mois_fixe"],
    "VAR2_24_59": ["VAR2_24_59_mois_fixe","VAR2_24_59_mois_mobile"],
    "VAR2_24_59_fixe": ["VAR2_24_59_mois_fixe"],
    "VAA_24_59": ["VAA_24_59_mois_fixe","VAA_24_59_mois_mobile"],
    "VAA_24_59_fixe": ["VAA_24_59_mois_fixe"],
    "ECV_24_59": ["ECV_24_59_mois_fixe","ECV_24_59_mois_mobile"],
    "ECV_24_59_fixe": ["ECV_24_59_mois_fixe"],
    "VAP_24_59": ["VAP_24_59_mois_fixe","VAP_24_59_mois_avanc_","VAP_24_59_mois_mobile"],
    "VAP_24_59_fixe": ["VAP_24_59_mois_fixe"],
    "HPV_24_59": ["HPV_24_59_mois_fixe","HPV_24_59_mois_avanc_","HPV_24_59_mois_mobile"],
    "HPV_24_59_fixe": ["HPV_24_59_mois_fixe"],

    # ── Td ──
    "Td_2_plus": ["Td_2","Td_3","Td_4","Td_5"],
    "Td_total": ["Td_1","Td_2","Td_3","Td_4","Td_5"],
    # ── SÉANCES DE VACCINATION ──
    "seances_fixes_prevues": ["S_ances_fixes_pr_vues"],
    "seances_fixes_realisees": ["S_ances_fixes_r_alis_es"],
    "seances_avancees_prevues": ["S_ances_avanc_es_pr_vues"],
    "seances_avancees_realisees": ["S_ances_avanc_es_r_alis_es"],
    "seances_mobiles_prevues": ["S_ances_mobiles_pr_vues"],
    "seances_mobiles_realisees": ["S_ances_mobiles_r_alis_es"],

    # ── LOGISTIQUE VACCINS (rename_map) ──
    "BCG_utilis_es": ["BCG_utilis_es"], "BCG_pertes": ["BCG_pertes"],
    "BCG_jours_rupture": ["BCG_jours_rupture"], "BCG_stock_fin": ["BCG_stock_fin", "BCG dose-stock disponible utilisable"],
    "DTC_utilis_es": ["DTC_utilis_es"], "DTC_pertes": ["DTC_pertes"],
    "DTC_jours_rupture": ["DTC_jours_rupture"], "DTC_stock_fin": ["DTC_stock_fin", "DTC dose-stock disponible utilisable"],
    "VPO_utilis_es": ["VPO_utilis_es"], "VPO_pertes": ["VPO_pertes"],
    "VPO_jours_rupture": ["VPO_jours_rupture"], "VPO_stock_fin": ["VPO_stock_fin", "VPO dose-stock disponible utilisable"],
    "VPI_utilis_es": ["VPI_utilis_es"], "VPI_pertes": ["VPI_pertes"],
    "VPI_jours_rupture": ["VPI_jours_rupture"], "VPI_stock_fin": ["VPI_stock_fin", "VPI dose-stock disponible utilisable"],
    "VAR_utilis_es": ["VAR_utilis_es"], "VAR_pertes": ["VAR_pertes"],
    "VAR_jours_rupture": ["VAR_jours_rupture"], "VAR_stock_fin": ["VAR_stock_fin", "VAR dose-stock disponible utilisable"],
    "VAA_utilis_es": ["VAA_utilis_es"], "VAA_pertes": ["VAA_pertes"],
    "VAA_jours_rupture": ["VAA_jours_rupture"], "VAA_stock_fin": ["VAA_stock_fin", "VAA dose-stock disponible utilisable"],
    "Td_utilis_es": ["Td_utilis_es"], "Td_pertes": ["Td_pertes"],
    "Td_jours_rupture": ["Td_jours_rupture"], "Td_stock_fin": ["Td_stock_fin", "VAT dose-stock disponible utilisable"],
    "PCV13_utilis_es": ["PCV13_utilis_es","PCV13 dose-utilisee"], "PCV13_pertes": ["PCV13_pertes"],
    "PCV13_jours_rupture": ["PCV13_jours_rupture"], "PCV13_stock_fin": ["PCV13_stock_fin", "PCV13 dose-stock disponible utilisable"],
    "PCV13_administr_es": ["PCV13_administr_es","PCV13 dose-administree"],
    "ROTA_utilis_es": ["ROTA_utilis_es","ROTA dose-utilisee"], "ROTA_pertes": ["ROTA_pertes"],
    "ROTA_jours_rupture": ["ROTA_jours_rupture"], "ROTA_stock_fin": ["ROTA_stock_fin", "ROTA dose-stock disponible utilisable"],
    "ROTA_administr_es": ["ROTA_administr_es","ROTA dose-administree"],
    "VAP_utilis_es": ["VAP_utilis_es"], "VAP_pertes": ["VAP_pertes"],
    "VAP_jours_rupture": ["VAP_jours_rupture"], "VAP_stock_fin": ["VAP_stock_fin", "VAP dose-stock disponible utilisable"],
    "VAP_administr_es": ["VAP_administr_es","VAP dose-administree"],
    "HPV_utilis_es": ["HPV_utilis_es"], "HPV_pertes": ["HPV_pertes"],
    "HPV_jours_rupture": ["HPV_jours_rupture"], "HPV_stock_fin": ["HPV_stock_fin", "HPV dose-stock disponible utilisable"],
    "HPV_administr_es": ["HPV_administr_es","HPV dose-administree"],
    # ── LOGISTIQUE (Triangulation) stock_d_but + re_ues ──
    "BCG_re_ues": ["BCG_re_ues"],
    "BCG_stock_d_but": ["BCG_stock_d_but"],
    "DTC_re_ues": ["DTC_re_ues"],
    "DTC_stock_d_but": ["DTC_stock_d_but"],
    "PCV13_re_ues": ["PCV13_re_ues"],
    "PCV13_stock_d_but": ["PCV13_stock_d_but"],
    "VAR_re_ues": ["VAR_re_ues"],
    "VAR_stock_d_but": ["VAR_stock_d_but"],
    "Td_re_ues": ["Td_re_ues"],
    "Td_stock_d_but": ["Td_stock_d_but"],
    "VPO_re_ues": ["VPO_re_ues"],
    "VPO_stock_d_but": ["VPO_stock_d_but"],
    "VPI_re_ues": ["VPI_re_ues"],
    "VPI_stock_d_but": ["VPI_stock_d_but"],
    "ROTA_re_ues": ["ROTA_re_ues"],
    "ROTA_stock_d_but": ["ROTA_stock_d_but"],
    "VAA_re_ues": ["VAA_re_ues"],
    "VAA_stock_d_but": ["VAA_stock_d_but"],
    "VAP_re_ues": ["VAP_re_ues"],
    "VAP_stock_d_but": ["VAP_stock_d_but"],
    # ── DILUANTS (rename_map) ──
    "Diluant_BCG_stock_d_but": ["Diluant_BCG_stock_d_but"], "Diluant_BCG_re_ues": ["Diluant_BCG_re_ues"],
    "Diluant_BCG_utilis_es": ["Diluant_BCG_utilis_es"], "Diluant_BCG_pertes": ["Diluant_BCG_pertes"],
    "Diluant_BCG_stock_fin": ["Diluant_BCG_stock_fin", "Diluant_BCG dose-stock disponible utilisable"], "Diluant_BCG_jours_rupture": ["Diluant_BCG_jours_rupture"],
    "Diluant_BCG_administr_es": ["Diluant_BCG_administr_es","Diluant_BCG dose-administree"],
    "Diluant_VAR_stock_d_but": ["Diluant_VAR_stock_d_but"], "Diluant_VAR_re_ues": ["Diluant_VAR_re_ues"],
    "Diluant_VAR_utilis_es": ["Diluant_VAR_utilis_es"], "Diluant_VAR_pertes": ["Diluant_VAR_pertes"],
    "Diluant_VAR_stock_fin": ["Diluant_VAR_stock_fin", "Diluant_VAR dose-stock disponible utilisable"], "Diluant_VAR_jours_rupture": ["Diluant_VAR_jours_rupture"],
    "Diluant_VAR_administr_es": ["Diluant_VAR_administr_es","Diluant_VAR dose-administree"],
    "Diluant_VAA_stock_d_but": ["Diluant_VAA_stock_d_but"], "Diluant_VAA_re_ues": ["Diluant_VAA_re_ues"],
    "Diluant_VAA_utilis_es": ["Diluant_VAA_utilis_es"], "Diluant_VAA_pertes": ["Diluant_VAA_pertes"],
    "Diluant_VAA_stock_fin": ["Diluant_VAA_stock_fin", "Diluant_VAA dose-stock disponible utilisable"], "Diluant_VAA_jours_rupture": ["Diluant_VAA_jours_rupture"],
    "Diluant_VAA_administr_es": ["Diluant_VAA_administr_es","Diluant_VAA dose-administree"],
    "Diluant_VAP_stock_d_but": ["Diluant_VAP_stock_d_but"], "Diluant_VAP_re_ues": ["Diluant_VAP_re_ues"],
    "Diluant_VAP_utilis_es": ["Diluant_VAP_utilis_es"], "Diluant_VAP_pertes": ["Diluant_VAP_pertes"],
    "Diluant_VAP_stock_fin": ["Diluant_VAP_stock_fin", "Diluant_VAP dose-stock disponible utilisable"], "Diluant_VAP_jours_rupture": ["Diluant_VAP_jours_rupture"],
    "Diluant_VAP_administr_es": ["Diluant_VAP_administr_es","Diluant_VAP dose-administree"],
    # ── SAB (rename_map) ──
    "SAB_005ml_stock_d_but": ["SAB_005ml_stock_d_but"], "SAB_005ml_re_ues": ["SAB_005ml_re_ues"],
    "SAB_005ml_utilis_es": ["SAB_005ml_utilis_es"], "SAB_005ml_pertes": ["SAB_005ml_pertes"],
    "SAB_005ml_stock_fin": ["SAB_005ml_stock_fin", "SAB_005ml dose-stock disponible utilisable"], "SAB_005ml_jours_rupture": ["SAB_005ml_jours_rupture"],
    "SAB_005ml_administr_es": ["SAB_005ml_administr_es","SAB_005ml dose-administree"],
    "SAB_05ml_stock_d_but": ["SAB_05ml_stock_d_but"], "SAB_05ml_re_ues": ["SAB_05ml_re_ues"],
    "SAB_05ml_utilis_es": ["SAB_05ml_utilis_es"], "SAB_05ml_pertes": ["SAB_05ml_pertes"],
    "SAB_05ml_stock_fin": ["SAB_05ml_stock_fin", "SAB_05ml dose-stock disponible utilisable"], "SAB_05ml_jours_rupture": ["SAB_05ml_jours_rupture"],
    "SAB_05ml_administr_es": ["SAB_05ml_administr_es","SAB_05ml dose-administree"],
    "SAB_auto_bloquante_stock_d_but": ["SAB_auto_bloquante_stock_d_but"], "SAB_auto_bloquante_re_ues": ["SAB_auto_bloquante_re_ues"],
    "SAB_auto_bloquante_utilis_es": ["SAB_auto_bloquante_utilis_es"], "SAB_auto_bloquante_pertes": ["SAB_auto_bloquante_pertes"],
    "SAB_auto_bloquante_stock_fin": ["SAB_auto_bloquante_stock_fin", "SAB_auto_bloquante dose-stock disponible utilisable"], "SAB_auto_bloquante_jours_rupture": ["SAB_auto_bloquante_jours_rupture"],
    "SAB_auto_bloquante_administr_es": ["SAB_auto_bloquante_administr_es","SAB_auto_bloquante dose-administree"],
    # ── SERINGUES DILUTION (rename_map) ──
    "Ser_dilution_2ml_stock_d_but": ["Ser_dilution_2ml_stock_d_but"], "Ser_dilution_2ml_re_ues": ["Ser_dilution_2ml_re_ues"],
    "Ser_dilution_2ml_utilis_es": ["Ser_dilution_2ml_utilis_es"], "Ser_dilution_2ml_pertes": ["Ser_dilution_2ml_pertes"],
    "Ser_dilution_2ml_stock_fin": ["Ser_dilution_2ml_stock_fin", "Ser_dilution_2ml dose-stock disponible utilisable"], "Ser_dilution_2ml_jours_rupture": ["Ser_dilution_2ml_jours_rupture"],
    "Ser_dilution_2ml_administr_es": ["Ser_dilution_2ml_administr_es","Ser_dilution_2ml dose-administree"],
    "Ser_dilution_5ml_stock_d_but": ["Ser_dilution_5ml_stock_d_but"], "Ser_dilution_5ml_re_ues": ["Ser_dilution_5ml_re_ues"],
    "Ser_dilution_5ml_utilis_es": ["Ser_dilution_5ml_utilis_es"], "Ser_dilution_5ml_pertes": ["Ser_dilution_5ml_pertes"],
    "Ser_dilution_5ml_stock_fin": ["Ser_dilution_5ml_stock_fin", "Ser_dilution_5ml dose-stock disponible utilisable"], "Ser_dilution_5ml_jours_rupture": ["Ser_dilution_5ml_jours_rupture"],
    "Ser_dilution_5ml_administr_es": ["Ser_dilution_5ml_administr_es","Ser_dilution_5ml dose-administree"],
    "Ser_dilution_6ml_stock_d_but": ["Ser_dilution_6ml_stock_d_but"], "Ser_dilution_6ml_re_ues": ["Ser_dilution_6ml_re_ues"],
    "Ser_dilution_6ml_utilis_es": ["Ser_dilution_6ml_utilis_es"], "Ser_dilution_6ml_pertes": ["Ser_dilution_6ml_pertes"],
    "Ser_dilution_6ml_stock_fin": ["Ser_dilution_6ml_stock_fin", "Ser_dilution_6ml dose-stock disponible utilisable"], "Ser_dilution_6ml_jours_rupture": ["Ser_dilution_6ml_jours_rupture"],
    "Ser_dilution_6ml_administr_es": ["Ser_dilution_6ml_administr_es","Ser_dilution_6ml dose-administree"],
    # ── ADAPTATEURS / COMPTE-GOUTTE / RÉCEPTACLES (rename_map) ──
    "Adaptateurs_stock_d_but": ["Adaptateurs_stock_d_but"], "Adaptateurs_re_ues": ["Adaptateurs_re_ues"],
    "Adaptateurs_utilis_es": ["Adaptateurs_utilis_es"], "Adaptateurs_pertes": ["Adaptateurs_pertes"],
    "Adaptateurs_stock_fin": ["Adaptateurs_stock_fin", "Adaptateurs dose-stock disponible utilisable"], "Adaptateurs_jours_rupture": ["Adaptateurs_jours_rupture"],
    "Adaptateurs_administr_es": ["Adaptateurs_administr_es","Adaptateurs dose-administree"],
    "Compte_goutte_stock_d_but": ["Compte_goutte_stock_d_but"], "Compte_goutte_re_ues": ["Compte_goutte_re_ues"],
    "Compte_goutte_utilis_es": ["Compte_goutte_utilis_es"], "Compte_goutte_pertes": ["Compte_goutte_pertes"],
    "Compte_goutte_stock_fin": ["Compte_goutte_stock_fin", "Compte_goutte dose-stock disponible utilisable"], "Compte_goutte_jours_rupture": ["Compte_goutte_jours_rupture"],
    "Compte_goutte_administr_es": ["Compte_goutte_administr_es","Compte_goutte dose-administree"],
    "R_ceptacles_stock_d_but": ["R_ceptacles_stock_d_but"], "R_ceptacles_re_ues": ["R_ceptacles_re_ues"],
    "R_ceptacles_utilis_es": ["R_ceptacles_utilis_es"], "R_ceptacles_pertes": ["R_ceptacles_pertes"],
    "R_ceptacles_stock_fin": ["R_ceptacles_stock_fin", "R_ceptacles dose-stock disponible utilisable"], "R_ceptacles_jours_rupture": ["R_ceptacles_jours_rupture"],
    "R_ceptacles_administr_es": ["R_ceptacles_administr_es","R_ceptacles dose-administree"],
    # ── NOUVELLES VARIABLES DE GESTION LOGISTIQUE ── VACCINS & NON-VACCINS ──
    # Vaccins logistiques
    "BCG_stock_max": ["BCG_stock_max", "BCG Stock Max"],
    "BCG_qte_a_commander": ["BCG_qte_a_commander", "BCG Qté à commander"],
    "BCG_cmm": ["BCG_cmm", "BCG CMM"],
    "BCG_ajustement": ["BCG_ajustement", "BCG Adjustment"],
    "BCG_msd": ["BCG_msd", "BCG MSD"],
    "DTC_stock_max": ["DTC_stock_max", "DTC Stock Max"],
    "DTC_qte_a_commander": ["DTC_qte_a_commander", "DTC Qté à commander"],
    "DTC_cmm": ["DTC_cmm", "DTC CMM"],
    "DTC_ajustement": ["DTC_ajustement", "DTC Adjustment"],
    "DTC_msd": ["DTC_msd", "DTC MSD"],
    "VPO_stock_max": ["VPO_stock_max", "VPO Stock Max"],
    "VPO_qte_a_commander": ["VPO_qte_a_commander", "VPO Qté à commander"],
    "VPO_cmm": ["VPO_cmm", "VPO CMM"],
    "VPO_ajustement": ["VPO_ajustement", "VPO Adjustment"],
    "VPO_msd": ["VPO_msd", "VPO MSD"],
    "VPI_stock_max": ["VPI_stock_max", "VPI Stock Max"],
    "VPI_qte_a_commander": ["VPI_qte_a_commander", "VPI Qté à commander"],
    "VPI_cmm": ["VPI_cmm", "VPI CMM"],
    "VPI_ajustement": ["VPI_ajustement", "VPI Adjustment"],
    "VPI_msd": ["VPI_msd", "VPI MSD"],
    "VAR_stock_max": ["VAR_stock_max", "VAR Stock Max"],
    "VAR_qte_a_commander": ["VAR_qte_a_commander", "VAR Qté à commander"],
    "VAR_cmm": ["VAR_cmm", "VAR CMM"],
    "VAR_ajustement": ["VAR_ajustement", "VAR Adjustment"],
    "VAR_msd": ["VAR_msd", "VAR MSD"],
    "VAA_stock_max": ["VAA_stock_max", "VAA Stock Max"],
    "VAA_qte_a_commander": ["VAA_qte_a_commander", "VAA Qté à commander"],
    "VAA_cmm": ["VAA_cmm", "VAA CMM"],
    "VAA_ajustement": ["VAA_ajustement", "VAA Adjustment"],
    "VAA_msd": ["VAA_msd", "VAA MSD"],
    "PCV13_stock_max": ["PCV13_stock_max", "PCV13 Stock Max"],
    "PCV13_qte_a_commander": ["PCV13_qte_a_commander", "PCV13 Qté à commander"],
    "PCV13_cmm": ["PCV13_cmm", "PCV13 CMM"],
    "PCV13_ajustement": ["PCV13_ajustement", "PCV13 Adjustment"],
    "PCV13_msd": ["PCV13_msd", "PCV13 MSD"],
    "ROTA_stock_max": ["ROTA_stock_max", "ROTA Stock Max"],
    "ROTA_qte_a_commander": ["ROTA_qte_a_commander", "ROTA Qté à commander"],
    "ROTA_cmm": ["ROTA_cmm", "ROTA CMM"],
    "ROTA_ajustement": ["ROTA_ajustement", "ROTA Adjustment"],
    "ROTA_msd": ["ROTA_msd", "ROTA MSD"],
    "VAP_stock_max": ["VAP_stock_max", "VAP Stock Max"],
    "VAP_qte_a_commander": ["VAP_qte_a_commander", "VAP Qté à commander"],
    "VAP_cmm": ["VAP_cmm", "VAP CMM"],
    "VAP_ajustement": ["VAP_ajustement", "VAP Adjustment"],
    "VAP_msd": ["VAP_msd", "VAP MSD"],
    "HPV_stock_max": ["HPV_stock_max", "HPV Stock Max"],
    "HPV_qte_a_commander": ["HPV_qte_a_commander", "HPV Qté à commander"],
    "HPV_cmm": ["HPV_cmm", "HPV CMM"],
    "HPV_ajustement": ["HPV_ajustement", "HPV Adjustment"],
    "HPV_msd": ["HPV_msd", "HPV MSD"],
    "Td_stock_max": ["Td_stock_max", "VAT Stock Max"],
    "Td_qte_a_commander": ["Td_qte_a_commander", "VAT Qté à commander"],
    "Td_cmm": ["Td_cmm", "VAT CMM"],
    "Td_ajustement": ["Td_ajustement", "VAT Adjustment"],
    "Td_msd": ["Td_msd", "VAT MSD"],
    # Non-vaccins: Diluants & SAB
    "Diluant_BCG_stock_max": ["Diluant_BCG Stock Max"],
    "Diluant_BCG_qte_a_commander": ["Diluant_BCG Qté à commander"],
    "Diluant_BCG_cmm": ["Diluant_BCG CMM"],
    "Diluant_BCG_ajustement": ["Diluant_BCG Adjustment"],
    "Diluant_BCG_msd": ["Diluant_BCG MSD"],
    "Diluant_VAR_stock_max": ["Diluant_VAR Stock Max"],
    "Diluant_VAR_qte_a_commander": ["Diluant_VAR Qté à commander"],
    "Diluant_VAR_cmm": ["Diluant_VAR CMM"],
    "Diluant_VAR_ajustement": ["Diluant_VAR Adjustment"],
    "Diluant_VAR_msd": ["Diluant_VAR MSD"],
    "Diluant_VAA_stock_max": ["Diluant_VAA Stock Max"],
    "Diluant_VAA_qte_a_commander": ["Diluant_VAA Qté à commander"],
    "Diluant_VAA_cmm": ["Diluant_VAA CMM"],
    "Diluant_VAA_ajustement": ["Diluant_VAA Adjustment"],
    "Diluant_VAA_msd": ["Diluant_VAA MSD"],
    "SAB_005ml_stock_max": ["SAB_005ml Stock Max"],
    "SAB_005ml_qte_a_commander": ["SAB_005ml Qté à commander"],
    "SAB_005ml_cmm": ["SAB_005ml CMM"],
    "SAB_005ml_ajustement": ["SAB_005ml Adjustment"],
    "SAB_005ml_msd": ["SAB_005ml MSD"],
    "SAB_05ml_stock_max": ["SAB_05ml Stock Max"],
    "SAB_05ml_qte_a_commander": ["SAB_05ml Qté à commander"],
    "SAB_05ml_cmm": ["SAB_05ml CMM"],
    "SAB_05ml_ajustement": ["SAB_05ml Adjustment"],
    "SAB_05ml_msd": ["SAB_05ml MSD"],
    "SAB_auto_bloquante_stock_max": ["SAB_auto_bloquante Stock Max"],
    "SAB_auto_bloquante_qte_a_commander": ["SAB_auto_bloquante Qté à commander"],
    "SAB_auto_bloquante_cmm": ["SAB_auto_bloquante CMM"],
    "SAB_auto_bloquante_ajustement": ["SAB_auto_bloquante Adjustment"],
    "SAB_auto_bloquante_msd": ["SAB_auto_bloquante MSD"],
    # Instruments et consommables non vaccins
    "Ser_dilution_2ml_stock_max": ["Ser_dilution_2ml Stock Max"],
    "Ser_dilution_2ml_qte_a_commander": ["Ser_dilution_2ml Qté à commander"],
    "Ser_dilution_2ml_cmm": ["Ser_dilution_2ml CMM"],
    "Ser_dilution_2ml_ajustement": ["Ser_dilution_2ml Adjustment"],
    "Ser_dilution_2ml_msd": ["Ser_dilution_2ml MSD"],
    "Ser_dilution_5ml_stock_max": ["Ser_dilution_5ml Stock Max"],
    "Ser_dilution_5ml_qte_a_commander": ["Ser_dilution_5ml Qté à commander"],
    "Ser_dilution_5ml_cmm": ["Ser_dilution_5ml CMM"],
    "Ser_dilution_5ml_ajustement": ["Ser_dilution_5ml Adjustment"],
    "Ser_dilution_5ml_msd": ["Ser_dilution_5ml MSD"],
    "Ser_dilution_6ml_stock_max": ["Ser_dilution_6ml Stock Max"],
    "Ser_dilution_6ml_qte_a_commander": ["Ser_dilution_6ml Qté à commander"],
    "Ser_dilution_6ml_cmm": ["Ser_dilution_6ml CMM"],
    "Ser_dilution_6ml_ajustement": ["Ser_dilution_6ml Adjustment"],
    "Ser_dilution_6ml_msd": ["Ser_dilution_6ml MSD"],
    "Adaptateurs_stock_max": ["Adaptateurs Stock Max"],
    "Adaptateurs_qte_a_commander": ["Adaptateurs Qté à commander"],
    "Adaptateurs_cmm": ["Adaptateurs CMM"],
    "Adaptateurs_ajustement": ["Adaptateurs Adjustment"],
    "Adaptateurs_msd": ["Adaptateurs MSD"],
    "Compte_goutte_stock_max": ["Compte_goutte Stock Max"],
    "Compte_goutte_qte_a_commander": ["Compte_goutte Qté à commander"],
    "Compte_goutte_cmm": ["Compte_goutte CMM"],
    "Compte_goutte_ajustement": ["Compte_goutte Adjustment"],
    "Compte_goutte_msd": ["Compte_goutte MSD"],
    "R_ceptacles_stock_max": ["R_ceptacles Stock Max"],
    "R_ceptacles_qte_a_commander": ["R_ceptacles Qté à commander"],
    "R_ceptacles_cmm": ["R_ceptacles CMM"],
    "R_ceptacles_ajustement": ["R_ceptacles Adjustment"],
    "R_ceptacles_msd": ["R_ceptacles MSD"],
}

# ==========================================================
# SOMMATIONS DÉRIVÉES (calculées après les primaires)
# ==========================================================
DERIVED_SUM_SPECS: dict[str, list[str]] = {
    # 12-59m = 12-23m + 24-59m
    "BCG_12_59": ["BCG_12_23","BCG_24_59"],
    "DTC1_12_59": ["DTC1_12_23","DTC1_24_59"],
    "DTC2_12_59": ["DTC2_12_23","DTC2_24_59"],
    "DTC3_12_59": ["DTC3_12_23","DTC3_24_59"],
    "VPO0_12_59": ["VPO0_12_23","VPO0_24_59"],
    "VPO1_12_59": ["VPO1_12_23","VPO1_24_59"],
    "VPO2_12_59": ["VPO2_12_23","VPO2_24_59"],
    "VPO3_12_59": ["VPO3_12_23","VPO3_24_59"],
    "VPI1_12_59": ["VPI1_12_23","VPI1_24_59"],
    "VPI2_12_59": ["VPI2_12_23","VPI2_24_59"],
    "PCV13_1_12_59": ["PCV13_1_12_23","PCV13_1_24_59"],
    "PCV13_2_12_59": ["PCV13_2_12_23","PCV13_2_24_59"],
    "PCV13_3_12_59": ["PCV13_3_12_23","PCV13_3_24_59"],
    "ROTA1_12_59": ["ROTA1_12_23","ROTA1_24_59"],
    "ROTA2_12_59": ["ROTA2_12_23","ROTA2_24_59"],
    "ROTA3_12_59": ["ROTA3_12_23","ROTA3_24_59"],
    "VAR1_12_59": ["VAR1_12_23","VAR1_24_59"],
    "VAR2_12_59": ["VAR2_12_23","VAR2_24_59"],
    "VAA_12_59": ["VAA_12_23","VAA_24_59"],
    "ECV_12_59": ["ECV_12_23","ECV_24_59"],
    "VAP_12_59": ["VAP4_12_23","VAP_24_59"],
    "HPV_12_59": ["HPV_12_23","HPV_24_59"],
    # All ages = 0-11m + 12-59m
    "BCG_all": ["BCG_0_11","BCG_12_59"],
    "DTC1_all": ["DTC1_0_11","DTC1_12_59"],
    "DTC2_all": ["DTC2_0_11","DTC2_12_59"],
    "DTC3_all": ["DTC3_0_11","DTC3_12_59"],
    "VPO0_all": ["VPO0_0_11","VPO0_12_59"],
    "VPO1_all": ["VPO1_0_11","VPO1_12_59"],
    "VPO2_all": ["VPO2_0_11","VPO2_12_59"],
    "VPO3_all": ["VPO3_0_11","VPO3_12_59"],
    "VPI1_all": ["VPI1_0_11","VPI1_12_59"],
    "VPI2_all": ["VPI2_0_11","VPI2_12_59"],
    "PCV13_1_all": ["PCV13_1_0_11","PCV13_1_12_59"],
    "PCV13_2_all": ["PCV13_2_0_11","PCV13_2_12_59"],
    "PCV13_3_all": ["PCV13_3_0_11","PCV13_3_12_59"],
    "ROTA1_all": ["ROTA1_0_11","ROTA1_12_59"],
    "ROTA2_all": ["ROTA2_0_11","ROTA2_12_59"],
    "ROTA3_all": ["ROTA3_0_11","ROTA3_12_59"],
    "VAR1_all": ["VAR1_0_11","VAR1_12_59"],
    "VAR2_all": ["VAR2_0_11","VAR2_12_59"],
    "VAA_all": ["VAA_0_11","VAA_12_59"],
}
ALL_AGG_KEYS = list(SUM_SPECS.keys()) + list(DERIVED_SUM_SPECS.keys())

def nv(row: dict, field: str) -> float:
    v = row.get(field)
    if v is None or v == "":
        return 0.0
    try:
        return float(v)
    except Exception:
        return 0.0

def normalize_org3(org3: str) -> str:
    s = (org3 or "").strip()
    if len(s) > 3 and s[2] == " ":
        s = s[3:].strip()
    for suf in [" Zone de Santé", " Zone de Sante"]:
        if s.endswith(suf):
            s = s[: -len(suf)].strip()
            break
    return s

def resolve_antenne(province: str, zs: str) -> str:
    rules = ANT_RULES.get(province, {})
    norm = normalize_org3(zs)
    return rules.get(norm, rules.get(zs, ""))

def period_to_ym(p: str) -> str:
    p = (p or "").strip()
    if len(p) >= 7 and p[4] == "-":
        return p[:4] + p[5:7]
    if len(p) >= 6 and p.isdigit():
        return p[:6]
    mmm = {
        "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
        "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
        "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
    }
    parts = p.split("-")
    if len(parts) == 3 and parts[1] in mmm:
        return parts[2] + mmm[parts[1]]
    return p

def slug(name: str) -> str:
    s = re.sub(r"[^\w\s-]", "", (name or "unknown").strip())
    s = re.sub(r"[\s-]+", "_", s)
    return s.lower() or "unknown"

# ── Aggregation Storage ──
# key: (Prov, ZS, Ant, AS, FOSA, YM)
agg_fosa = {}

# Metadata sets
all_provinces = set()
all_antennes = set()
all_zs = set()
all_as_list = set()
all_fosa_names = set()
all_months = set()
total_records = 0

# ── Load and Aggregate ──
print("Loading and aggregating NDJSON files (FOSA) on-the-fly...")
with open(DATA / "index.json", encoding="utf-8") as f:
    index = json.load(f)

months_list = sorted(index.get("months", {}).keys())

for month in months_list:
    parts = index["months"][month].get("parts", [])
    for part in parts:
        fname = part.get("plain") or part.get("file", "")
        if not fname:
            continue
        fpath = MONTHLY / month / fname
        gz_path = MONTHLY / month / part.get("file", "")

        f_obj = None
        if fpath.exists() and not fname.endswith(".gz"):
            f_obj = open(fpath, encoding="utf-8")
        elif gz_path.exists() and str(gz_path).endswith(".gz"):
            f_obj = gzip.open(gz_path, "rt", encoding="utf-8")
        elif fpath.exists():
            try:
                with gzip.open(fpath, "rt", encoding="utf-8") as temp_f:
                    temp_f.read(1)
                f_obj = gzip.open(fpath, "rt", encoding="utf-8")
            except Exception:
                f_obj = open(fpath, encoding="utf-8")

        if not f_obj:
            continue

        with f_obj as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    ou = row.get("OrgUnit", "")
                    meta = OU_MAP.get(ou, {})
                    
                    prov = meta.get("Org2", "")
                    zs = meta.get("Org3", "")
                    as_ = meta.get("Org4", "")
                    fosa = meta.get("Org5", "")
                    ant = resolve_antenne(prov, zs)
                    ym = period_to_ym(row.get("Period", ""))
                    
                    # Calculate sums (always include, even 0)
                    for sf, sources in SUM_SPECS.items():
                        val = sum(nv(row, s) for s in sources)
                        row[sf] = val

                    for sf, sources in DERIVED_SUM_SPECS.items():
                        val = sum(nv(row, s) for s in sources)
                        row[sf] = val
                    
                    total_records += 1
                    if prov: all_provinces.add(prov)
                    if ant: all_antennes.add(ant)
                    if zs: all_zs.add(zs)
                    if as_: all_as_list.add(as_)
                    if fosa: all_fosa_names.add(fosa)
                    if ym: all_months.add(ym)

                    key = (prov, zs, ant, as_, fosa, ym)
                    if key not in agg_fosa:
                        agg_fosa[key] = {"n": 0, "rap": 0, "sum_comp": 0.0, "sum_prompt": 0.0}
                    
                    g = agg_fosa[key]
                    g["n"] += 1
                    comp = nv(row, "Compl_tude")
                    if comp > 0: g["rap"] += 1
                    g["sum_comp"] += comp
                    g["sum_prompt"] += nv(row, "Promptitude")
                    
                    for k in ALL_AGG_KEYS:
                        val = row.get(k, 0.0)
                        g[k] = g.get(k, 0.0) + val
                except Exception:
                    pass

    print(f"  {month}: {total_records} records processed so far")

print(f"Total: {total_records} records, {len(agg_fosa)} FOSA-month entries")

# ── Hierarchical Aggregation ──
def update_agg_from_agg(target_agg, target_key, source_g):
    if target_key not in target_agg:
        target_agg[target_key] = {"n": 0, "rap": 0, "sum_comp": 0.0, "sum_prompt": 0.0}
    tg = target_agg[target_key]
    tg["n"] += source_g["n"]
    tg["rap"] += source_g["rap"]
    tg["sum_comp"] += source_g["sum_comp"]
    tg["sum_prompt"] += source_g["sum_prompt"]
    for k in ALL_AGG_KEYS:
        if k in source_g:
            tg[k] = tg.get(k, 0.0) + source_g[k]

print("Performing hierarchical aggregation...")
agg_as = {}
agg_zs = {}
agg_prov = {}

for key, g in agg_fosa.items():
    prov, zs, ant, as_, fosa, ym = key
    
    # AS
    as_key = (prov, zs, ant, as_, ym)
    update_agg_from_agg(agg_as, as_key, g)
    
    # ZS
    zs_key = (prov, zs, ant, ym)
    update_agg_from_agg(agg_zs, zs_key, g)
    
    # Prov
    prov_key = (prov, ym)
    update_agg_from_agg(agg_prov, prov_key, g)

# ── Finalize Helpers ──
def finalize(agg_dict, fields):
    res = []
    for key, g in agg_dict.items():
        d = {f: key[i] for i, f in enumerate(fields)}
        n = g["n"]
        d["n"] = n
        d["rap"] = g["rap"]
        d["comp"] = round(g["sum_comp"] / n, 2) if n > 0 else 0
        d["prompt"] = round(g["sum_prompt"] / n, 2) if n > 0 else 0
        for k in ALL_AGG_KEYS:
            if k in g:
                d[k] = round(g[k], 2)
        res.append(d)
    return res

fosa_month = finalize(agg_fosa, ["_Province", "_ZS", "_Antenne", "_AS", "_FOSA", "_YM"])
as_month = finalize(agg_as, ["_Province", "_ZS", "_Antenne", "_AS", "_YM"])
zs_month = finalize(agg_zs, ["_Province", "_ZS", "_Antenne", "_YM"])
prov_month = finalize(agg_prov, ["_Province", "_YM"])

# ── Write helpers ──
def write_json(path: Path, data: any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"), ensure_ascii=False)
    size = os.path.getsize(path)
    print(f"  Written {path} ({size / 1024:.0f} KB)")

def write_split(base_dir: Path, records: list[dict], key_field: str = "_Province") -> dict:
    base_dir.mkdir(parents=True, exist_ok=True)
    by_key: dict[str, list] = defaultdict(list)
    for r in records:
        by_key[r.get(key_field, "") or "unknown"].append(r)

    manifest = {}
    for k, rows in sorted(by_key.items()):
        fname = slug(k) + ".json"
        write_json(base_dir / fname, rows)
        manifest[k] = fname

    write_json(base_dir / "manifest.json", manifest)
    return manifest

# ── Generate Metadata ──
prov_slugs = {p: slug(p) for p in sorted(all_provinces)}

meta_data = {
    "generated_at": index.get("generated_at", ""),
    "total_records": total_records,
    "provinces": sorted(all_provinces),
    "province_slugs": prov_slugs,
    "antennes": sorted(all_antennes),
    "zs": sorted(all_zs),
    "as": sorted(all_as_list),
    "months": sorted(all_months),
    "nb_fosa": len(all_fosa_names),
}

print("\nWriting output files...")
write_json(DASH / "meta.json", meta_data)
write_json(DASH / "by_province.json", prov_month)
write_json(DASH / "by_zs.json", zs_month)

print("\nSplitting by_as by province...")
write_split(DASH / "by_as", as_month)

print("\nSplitting by_fosa by province...")
write_split(DASH / "by_fosa", fosa_month)

print("\nBuilding heatmap by province...")
hm_dir = DASH / "heatmap"
hm_dir.mkdir(parents=True, exist_ok=True)
hm_by_prov: dict[str, dict] = defaultdict(dict)
for r in fosa_month:
    prov = r.get("_Province", "") or "unknown"
    fosa = r.get("_FOSA", "")
    ym = r.get("_YM", "")
    if fosa and ym:
        if fosa not in hm_by_prov[prov]:
            hm_by_prov[prov][fosa] = {}
        hm_by_prov[prov][fosa][ym] = 1 if r["rap"] > 0 else 0

hm_manifest = {}
for prov, data in sorted(hm_by_prov.items()):
    fname = slug(prov) + ".json"
    write_json(hm_dir / fname, data)
    hm_manifest[prov] = fname

write_json(hm_dir / "manifest.json", hm_manifest)

print("\n✅ Dashboard aggregation complete! (FOSA)")
total_size = 0
file_count = 0
for p in DASH.rglob("*.json"):
    total_size += os.path.getsize(p)
    file_count += 1
print(f"  {file_count} files, total {total_size / 1024 / 1024:.1f} MB")

for p in DASH.rglob("*.json"):
    sz = os.path.getsize(p)
    if sz > 90_000_000:
        print(f"  ⚠️  WARNING: {p} is {sz / 1024 / 1024:.0f} MB (near GitHub limit)")
