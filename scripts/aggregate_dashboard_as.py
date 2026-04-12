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
DATA = DOCS / "data_as"
MONTHLY = DATA / "monthly"
DASH = DATA / "dashboard"


# Clean previous dashboard
if DASH.exists():
    shutil.rmtree(DASH)
DASH.mkdir(parents=True, exist_ok=True)


# Empêcher Jekyll d'ignorer les fichiers
(DOCS / ".nojekyll").touch()


# ── Load ou_map ──
ou_map_gz = DATA / "ou_map_as.json.gz"
ou_map_js = DATA / "ou_map_as.json"
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
    "VPO2_12_23": ["VPO2_12_23_mois_fixe1","VPO2_12_23_mois_fixe2","VPO2_12_23_mois_avanc_1","VPO2_12_23_mois_avanc_2","VPO2_12_23_mois_mobile1"],
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
    # ── LOGISTIQUE ÉTENDUE ──
    "BCG_utilis_es": ["BCG_utilis_es"],
    "BCG_jours_rupture": ["BCG_jours_rupture"],
    "BCG_stock_fin": ["BCG_stock_fin"],
    "DTC_utilis_es": ["DTC_utilis_es"],
    "DTC_jours_rupture": ["DTC_jours_rupture"],
    "DTC_stock_fin": ["DTC_stock_fin"],
    "VPO_utilis_es": ["VPO_utilis_es"],
    "VPO_jours_rupture": ["VPO_jours_rupture"],
    "VPO_stock_fin": ["VPO_stock_fin"],
    "VPI_utilis_es": ["VPI_utilis_es"],
    "VPI_jours_rupture": ["VPI_jours_rupture"],
    "VPI_stock_fin": ["VPI_stock_fin"],
    "VAR_utilis_es": ["VAR_utilis_es"],
    "VAR_jours_rupture": ["VAR_jours_rupture"],
    "VAR_stock_fin": ["VAR_stock_fin"],
    "VAA_utilis_es": ["VAA_utilis_es"],
    "VAA_jours_rupture": ["VAA_jours_rupture"],
    "VAA_stock_fin": ["VAA_stock_fin"],
    "Td_utilis_es": ["VAT_utilis_es"],
    "Td_jours_rupture": ["VAT_jours_rupture"],
    "Td_stock_fin": ["VAT_stock_fin"],
    "PCV13_utilis_es": ["PCV13_utilis_es"],
    "PCV13_jours_rupture": ["PCV13_jours_rupture"],
    "PCV13_stock_fin": ["PCV13_stock_fin"],
    "ROTA_utilis_es": ["ROTA_utilis_es"],
    "ROTA_jours_rupture": ["ROTA_jours_rupture"],
    "ROTA_stock_fin": ["ROTA_stock_fin"],
    "VAP_utilis_es": ["VAP_utilis_es"],
    "VAP_jours_rupture": ["VAP_jours_rupture"],
    "VAP_stock_fin": ["VAP_stock_fin"],
    # ── LOGISTIQUE (Triangulation) ──
    "BCG_re_ues": ["BCG dose-recue mois"],
    "BCG_stock_d_but": ["BCG dose-stock debut mois"],
    "DTC_re_ues": ["DTC dose-recue mois"],
    "DTC_stock_d_but": ["DTC dose-stock debut mois"],
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




# ── Load all records ──
print("Loading all NDJSON files (AS)...")
with open(DATA / "index.json", encoding="utf-8") as f:
    index = json.load(f)


all_records: list[dict] = []
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

        if not f_obj: continue

        with f_obj as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try:
                    row = json.loads(line)
                    ou = row.get("OrgUnit", "")
                    meta = OU_MAP.get(ou, {})
                    row["_Province"] = meta.get("Org2", "")
                    row["_ZS"] = meta.get("Org3", "")
                    row["_AS"] = meta.get("Org4", "")
                    row["_Antenne"] = resolve_antenne(row["_Province"], row["_ZS"])
                    row["_YM"] = period_to_ym(row.get("Period", ""))
                    for sf, sources in SUM_SPECS.items():
                        row[sf] = sum(nv(row, s) for s in sources)
                    for sf, sources in DERIVED_SUM_SPECS.items():
                        row[sf] = sum(nv(row, s) for s in sources)
                    minimized_row = {k: row[k] for k in KEEP_KEYS_LIST if k in row}
                    all_records.append(minimized_row)
                except Exception: pass


    print(f"  {month}: {len(all_records)} total records")


print(f"Total: {len(all_records)} records")




# ── Aggregation helper ──
def agg_group(records: list[dict], group_fields: list[str]) -> list[dict]:
    groups: dict[tuple, dict] = defaultdict(lambda: {
        "n": 0, "rap": 0,
        "sum_comp": 0.0, "sum_prompt": 0.0,
        **{k: 0.0 for k in ALL_AGG_KEYS},
    })


    for r in records:
        key = tuple(r.get(f, "") for f in group_fields)
        g = groups[key]
        g["n"] += 1
        comp = nv(r, "Compl_tude")
        if comp > 0:
            g["rap"] += 1
        g["sum_comp"] += comp
        g["sum_prompt"] += nv(r, "Promptitude")
        for k in ALL_AGG_KEYS:
            g[k] += nv(r, k)


    result = []
    for key, g in groups.items():
        entry: dict = {}
        for i, f in enumerate(group_fields):
            entry[f] = key[i]
        entry["n"] = g["n"]
        entry["rap"] = g["rap"]
        entry["comp"] = round(g["sum_comp"] / g["n"], 2) if g["n"] > 0 else 0
        entry["prompt"] = round(g["sum_prompt"] / g["n"], 2) if g["n"] > 0 else 0
        for k in ALL_AGG_KEYS:
            if g[k] > 0:
                entry[k] = g[k]
        result.append(entry)


    return result




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




# ── Generate ──
print("Aggregating...")


prov_month = agg_group(all_records, ["_Province", "_YM"])
print(f"  Province×Month: {len(prov_month)} rows")


zs_month = agg_group(all_records, ["_Province", "_ZS", "_Antenne", "_YM"])
print(f"  ZS×Month: {len(zs_month)} rows")


as_month = agg_group(all_records, ["_Province", "_ZS", "_Antenne", "_AS", "_YM"])
print(f"  AS×Month: {len(as_month)} rows")


# No FOSA level in AS data (data is already at AS level)



all_provinces = sorted({r["_Province"] for r in all_records if r.get("_Province")})
all_antennes = sorted({r["_Antenne"] for r in all_records if r.get("_Antenne")})
all_zs = sorted({r["_ZS"] for r in all_records if r.get("_ZS")})
all_as_list = sorted({r["_AS"] for r in all_records if r.get("_AS")})

all_months = sorted({r["_YM"] for r in all_records if r.get("_YM")})
prov_slugs = {p: slug(p) for p in all_provinces}


meta_data = {
    "generated_at": index.get("generated_at", ""),
    "total_records": len(all_records),
    "provinces": all_provinces,
    "province_slugs": prov_slugs,
    "antennes": all_antennes,
    "zs": all_zs,
    "as": all_as_list,
    "months": all_months,
    
}


print("\nWriting output files...")
write_json(DASH / "meta.json", meta_data)
write_json(DASH / "by_province.json", prov_month)
write_json(DASH / "by_zs.json", zs_month)


print("\nSplitting by_as by province...")
write_split(DASH / "by_as", as_month)





print("\nBuilding heatmap by province...")
hm_dir = DASH / "heatmap"
hm_dir.mkdir(parents=True, exist_ok=True)
hm_by_prov: dict[str, dict] = defaultdict(dict)
for r in as_month:
    prov = r.get("_Province", "") or "unknown"
    entity = r.get("_AS", "")
    ym = r.get("_YM", "")
    if entity and ym:
        if entity not in hm_by_prov[prov]:
            hm_by_prov[prov][entity] = {}
        hm_by_prov[prov][entity][ym] = 1 if r["rap"] > 0 else 0


hm_manifest = {}
for prov, data in sorted(hm_by_prov.items()):
    fname = slug(prov) + ".json"
    write_json(hm_dir / fname, data)
    hm_manifest[prov] = fname


write_json(hm_dir / "manifest.json", hm_manifest)


print("\n✅ Dashboard aggregation complete! (AS)")
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
