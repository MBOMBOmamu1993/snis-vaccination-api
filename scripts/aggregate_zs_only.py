from __future__ import annotations

import json
import gzip
import os
import re
import sys
from pathlib import Path
from collections import defaultdict
import shutil
import time
from datetime import datetime

# ── Configuration ──
DOCS = Path("docs")
CONFIG = DOCS / "config"
ANT_RULES_PATH = CONFIG / "antenne_rules.json"

POP_KEYS = [
    "Pop_totale", "Naissances_vivantes", "Pop_par_AS", 
    "Pop_0_11m_nv", "Pop_0_11m_survivants", "Pop_0_59m", "Pop_12_59m"
]

# Sources and destinations
LEVEL_CONFIGS = [
    {
        "name": "ZS",
        "data_dir": DOCS / "data_zs",
        "ou_map_path": DOCS / "data_zs" / "ou_map_zs.json.gz",
        "dash_dir": DOCS / "data_zs" / "dashboard",
        "fields": ["_Province", "_ZS", "_Antenne", "_YM"],
        "split_by": None,
        "has_heatmap": True,
        "heatmap_entity": "_ZS",
        "include_pop": True
    }
]

# ── Shared Specs ──
BASE_SUM_SPECS = {
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
    "VAR2_12_23": ["VAR2_12_23_mois_fixe1","VAR2_12_23_mois_fixe2","VAR2_12_23_mois_avanc_1","VAR2_12_23_mois_mobile1","VAR2_12_23_mois_mobile2"],
    "VAR2_12_23_fixe": ["VAR2_12_23_mois_fixe1","VAR2_12_23_mois_fixe2"],
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

    "Td_2_plus": ["Td_2","Td_3","Td_4","Td_5"],
    "Td_total": ["Td_1","Td_2","Td_3","Td_4","Td_5"],
    "seances_fixes_prevues": ["S_ances_fixes_pr_vues"],
    "seances_fixes_realisees": ["S_ances_fixes_r_alis_es"],
    "seances_avancees_prevues": ["S_ances_avanc_es_pr_vues"],
    "seances_avancees_realisees": ["S_ances_avanc_es_r_alis_es"],
    "seances_mobiles_prevues": ["S_ances_mobiles_pr_vues"],
    "seances_mobiles_realisees": ["S_ances_mobiles_r_alis_es"],

    # ── LOGISTIQUE VACCINS ──
    "BCG_stock_d_but": ["BCG_stock_d_but"], "BCG_re_ues": ["BCG_re_ues"], "BCG_utilis_es": ["BCG_utilis_es"], "BCG_pertes": ["BCG_pertes"], "BCG_stock_fin": ["BCG_stock_fin"], "BCG_jours_rupture": ["BCG_jours_rupture"], "BCG_administr_es": ["BCG_administr_es"],
    "BCG_stock_max": ["BCG_stock_max"], "BCG_qte_a_commander": ["BCG_qte_a_commander"], "BCG_cmm": ["BCG_cmm"], "BCG_ajustement": ["BCG_ajustement"], "BCG_msd": ["BCG_msd"],

    "DTC_stock_d_but": ["DTC_stock_d_but"], "DTC_re_ues": ["DTC_re_ues"], "DTC_utilis_es": ["DTC_utilis_es"], "DTC_pertes": ["DTC_pertes"], "DTC_stock_fin": ["DTC_stock_fin"], "DTC_jours_rupture": ["DTC_jours_rupture"], "DTC_administr_es": ["DTC_administr_es"],
    "DTC_stock_max": ["DTC_stock_max"], "DTC_qte_a_commander": ["DTC_qte_a_commander"], "DTC_cmm": ["DTC_cmm"], "DTC_ajustement": ["DTC_ajustement"], "DTC_msd": ["DTC_msd"],

    "VPO_stock_d_but": ["VPO_stock_d_but"], "VPO_re_ues": ["VPO_re_ues"], "VPO_utilis_es": ["VPO_utilis_es"], "VPO_pertes": ["VPO_pertes"], "VPO_stock_fin": ["VPO_stock_fin"], "VPO_jours_rupture": ["VPO_jours_rupture"], "VPO_administr_es": ["VPO_administr_es"],
    "VPO_stock_max": ["VPO_stock_max"], "VPO_qte_a_commander": ["VPO_qte_a_commander"], "VPO_cmm": ["VPO_cmm"], "VPO_ajustement": ["VPO_ajustement"], "VPO_msd": ["VPO_msd"],

    "VPI_stock_d_but": ["VPI_stock_d_but"], "VPI_re_ues": ["VPI_re_ues"], "VPI_utilis_es": ["VPI_utilis_es"], "VPI_pertes": ["VPI_pertes"], "VPI_stock_fin": ["VPI_stock_fin"], "VPI_jours_rupture": ["VPI_jours_rupture"], "VPI_administr_es": ["VPI_administr_es"],
    "VPI_stock_max": ["VPI_stock_max"], "VPI_qte_a_commander": ["VPI_qte_a_commander"], "VPI_cmm": ["VPI_cmm"], "VPI_ajustement": ["VPI_ajustement"], "VPI_msd": ["VPI_msd"],

    "VAR_stock_d_but": ["VAR_stock_d_but"], "VAR_re_ues": ["VAR_re_ues"], "VAR_utilis_es": ["VAR_utilis_es"], "VAR_pertes": ["VAR_pertes"], "VAR_stock_fin": ["VAR_stock_fin"], "VAR_jours_rupture": ["VAR_jours_rupture"], "VAR_administr_es": ["VAR_administr_es"],
    "VAR_stock_max": ["VAR_stock_max"], "VAR_qte_a_commander": ["VAR_qte_a_commander"], "VAR_cmm": ["VAR_cmm"], "VAR_ajustement": ["VAR_ajustement"], "VAR_msd": ["VAR_msd"],

    "VAA_stock_d_but": ["VAA_stock_d_but"], "VAA_re_ues": ["VAA_re_ues"], "VAA_utilis_es": ["VAA_utilis_es"], "VAA_pertes": ["VAA_pertes"], "VAA_stock_fin": ["VAA_stock_fin"], "VAA_jours_rupture": ["VAA_jours_rupture"], "VAA_administr_es": ["VAA_administr_es"],
    "VAA_stock_max": ["VAA_stock_max"], "VAA_qte_a_commander": ["VAA_qte_a_commander"], "VAA_cmm": ["VAA_cmm"], "VAA_ajustement": ["VAA_ajustement"], "VAA_msd": ["VAA_msd"],

    "Td_stock_d_but": ["Td_stock_d_but"], "Td_re_ues": ["Td_re_ues"], "Td_utilis_es": ["Td_utilis_es"], "Td_pertes": ["Td_pertes"], "Td_stock_fin": ["Td_stock_fin"], "Td_jours_rupture": ["Td_jours_rupture"], "Td_administr_es": ["Td_administr_es"],
    "Td_stock_max": ["Td_stock_max"], "Td_qte_a_commander": ["Td_qte_a_commander"], "Td_cmm": ["Td_cmm"], "Td_ajustement": ["Td_ajustement"], "Td_msd": ["Td_msd"],

    "PCV13_stock_d_but": ["PCV13_stock_d_but"], "PCV13_re_ues": ["PCV13_re_ues"], "PCV13_utilis_es": ["PCV13_utilis_es"], "PCV13_pertes": ["PCV13_pertes"], "PCV13_stock_fin": ["PCV13_stock_fin"], "PCV13_jours_rupture": ["PCV13_jours_rupture"], "PCV13_administr_es": ["PCV13_administr_es"],
    "PCV13_stock_max": ["PCV13_stock_max"], "PCV13_qte_a_commander": ["PCV13_qte_a_commander"], "PCV13_cmm": ["PCV13_cmm"], "PCV13_ajustement": ["PCV13_ajustement"], "PCV13_msd": ["PCV13_msd"],

    "ROTA_stock_d_but": ["ROTA_stock_d_but"], "ROTA_re_ues": ["ROTA_re_ues"], "ROTA_utilis_es": ["ROTA_utilis_es"], "ROTA_pertes": ["ROTA_pertes"], "ROTA_stock_fin": ["ROTA_stock_fin"], "ROTA_jours_rupture": ["ROTA_jours_rupture"], "ROTA_administr_es": ["ROTA_administr_es"],
    "ROTA_stock_max": ["ROTA_stock_max"], "ROTA_qte_a_commander": ["ROTA_qte_a_commander"], "ROTA_cmm": ["ROTA_cmm"], "ROTA_ajustement": ["ROTA_ajustement"], "ROTA_msd": ["ROTA_msd"],

    "VAP_stock_d_but": ["VAP_stock_d_but"], "VAP_re_ues": ["VAP_re_ues"], "VAP_utilis_es": ["VAP_utilis_es"], "VAP_pertes": ["VAP_pertes"], "VAP_stock_fin": ["VAP_stock_fin"], "VAP_jours_rupture": ["VAP_jours_rupture"], "VAP_administr_es": ["VAP_administr_es"],
    "VAP_stock_max": ["VAP_stock_max"], "VAP_qte_a_commander": ["VAP_qte_a_commander"], "VAP_cmm": ["VAP_cmm"], "VAP_ajustement": ["VAP_ajustement"], "VAP_msd": ["VAP_msd"],

    "HPV_stock_d_but": ["HPV_stock_d_but"], "HPV_re_ues": ["HPV_re_ues"], "HPV_utilis_es": ["HPV_utilis_es"], "HPV_pertes": ["HPV_pertes"], "HPV_stock_fin": ["HPV_stock_fin"], "HPV_jours_rupture": ["HPV_jours_rupture"], "HPV_administr_es": ["HPV_administr_es"],
    "HPV_stock_max": ["HPV_stock_max"], "HPV_qte_a_commander": ["HPV_qte_a_commander"], "HPV_cmm": ["HPV_cmm"], "HPV_ajustement": ["HPV_ajustement"], "HPV_msd": ["HPV_msd"],

    # ── LOGISTIQUE NON-VACCINS ──
    "Diluant_BCG_stock_d_but": ["Diluant_BCG_stock_d_but"], "Diluant_BCG_re_ues": ["Diluant_BCG_re_ues"], "Diluant_BCG_utilis_es": ["Diluant_BCG_utilis_es"], "Diluant_BCG_pertes": ["Diluant_BCG_pertes"], "Diluant_BCG_stock_fin": ["Diluant_BCG_stock_fin", "Diluant_BCG dose-stock disponible utilisable"], "Diluant_BCG_jours_rupture": ["Diluant_BCG_jours_rupture"], "Diluant_BCG_administr_es": ["Diluant_BCG_administr_es"],
    "Diluant_BCG_stock_max": ["Diluant_BCG_stock_max"], "Diluant_BCG_qte_a_commander": ["Diluant_BCG_qte_a_commander"], "Diluant_BCG_cmm": ["Diluant_BCG_cmm"], "Diluant_BCG_ajustement": ["Diluant_BCG_ajustement"], "Diluant_BCG_msd": ["Diluant_BCG_msd"],

    "Diluant_VAR_stock_d_but": ["Diluant_VAR_stock_d_but"], "Diluant_VAR_re_ues": ["Diluant_VAR_re_ues"], "Diluant_VAR_utilis_es": ["Diluant_VAR_utilis_es"], "Diluant_VAR_pertes": ["Diluant_VAR_pertes"], "Diluant_VAR_stock_fin": ["Diluant_VAR_stock_fin", "Diluant_VAR dose-stock disponible utilisable"], "Diluant_VAR_jours_rupture": ["Diluant_VAR_jours_rupture"], "Diluant_VAR_administr_es": ["Diluant_VAR_administr_es"],
    "Diluant_VAR_stock_max": ["Diluant_VAR_stock_max"], "Diluant_VAR_qte_a_commander": ["Diluant_VAR_qte_a_commander"], "Diluant_VAR_cmm": ["Diluant_VAR_cmm"], "Diluant_VAR_ajustement": ["Diluant_VAR_ajustement"], "Diluant_VAR_msd": ["Diluant_VAR_msd"],

    "Diluant_VAA_stock_d_but": ["Diluant_VAA_stock_d_but"], "Diluant_VAA_re_ues": ["Diluant_VAA_re_ues"], "Diluant_VAA_utilis_es": ["Diluant_VAA_utilis_es"], "Diluant_VAA_pertes": ["Diluant_VAA_pertes"], "Diluant_VAA_stock_fin": ["Diluant_VAA_stock_fin", "Diluant_VAA dose-stock disponible utilisable"], "Diluant_VAA_jours_rupture": ["Diluant_VAA_jours_rupture"], "Diluant_VAA_administr_es": ["Diluant_VAA_administr_es"],
    "Diluant_VAA_stock_max": ["Diluant_VAA_stock_max"], "Diluant_VAA_qte_a_commander": ["Diluant_VAA_qte_a_commander"], "Diluant_VAA_cmm": ["Diluant_VAA_cmm"], "Diluant_VAA_ajustement": ["Diluant_VAA_ajustement"], "Diluant_VAA_msd": ["Diluant_VAA_msd"],

    "SAB_005ml_stock_d_but": ["SAB_005ml_stock_d_but"],
 "SAB_005ml_re_ues": ["SAB_005ml_re_ues"], "SAB_005ml_utilis_es": ["SAB_005ml_utilis_es"], "SAB_005ml_pertes": ["SAB_005ml_pertes"], "SAB_005ml_stock_fin": ["SAB_005ml_stock_fin", "SAB_005ml dose-stock disponible utilisable"], "SAB_005ml_jours_rupture": ["SAB_005ml_jours_rupture"], "SAB_005ml_administr_es": ["SAB_005ml_administr_es"],
    "SAB_005ml_stock_max": ["SAB_005ml_stock_max"], "SAB_005ml_qte_a_commander": ["SAB_005ml_qte_a_commander"], "SAB_005ml_cmm": ["SAB_005ml_cmm"], "SAB_005ml_ajustement": ["SAB_005ml_ajustement"], "SAB_005ml_msd": ["SAB_005ml_msd"],

    "SAB_05ml_stock_d_but": ["SAB_05ml_stock_d_but"], "SAB_05ml_re_ues": ["SAB_05ml_re_ues"], "SAB_05ml_utilis_es": ["SAB_05ml_utilis_es"], "SAB_05ml_pertes": ["SAB_05ml_pertes"], "SAB_05ml_stock_fin": ["SAB_05ml_stock_fin", "SAB_05ml dose-stock disponible utilisable"], "SAB_05ml_jours_rupture": ["SAB_05ml_jours_rupture"], "SAB_05ml_administr_es": ["SAB_05ml_administr_es"],
    "SAB_05ml_stock_max": ["SAB_05ml_stock_max"], "SAB_05ml_qte_a_commander": ["SAB_05ml_qte_a_commander"], "SAB_05ml_cmm": ["SAB_05ml_cmm"], "SAB_05ml_ajustement": ["SAB_05ml_ajustement"], "SAB_05ml_msd": ["SAB_05ml_msd"],

    "SAB_auto_bloquante_stock_d_but": ["SAB_auto_bloquante_stock_d_but"], "SAB_auto_bloquante_re_ues": ["SAB_auto_bloquante_re_ues"], "SAB_auto_bloquante_utilis_es": ["SAB_auto_bloquante_utilis_es"], "SAB_auto_bloquante_pertes": ["SAB_auto_bloquante_pertes"], "SAB_auto_bloquante_stock_fin": ["SAB_auto_bloquante_stock_fin", "SAB_auto_bloquante dose-stock disponible utilisable"], "SAB_auto_bloquante_jours_rupture": ["SAB_auto_bloquante_jours_rupture"], "SAB_auto_bloquante_administr_es": ["SAB_auto_bloquante_administr_es"],
    "SAB_auto_bloquante_stock_max": ["SAB_auto_bloquante_stock_max"], "SAB_auto_bloquante_qte_a_commander": ["SAB_auto_bloquante_qte_a_commander"], "SAB_auto_bloquante_cmm": ["SAB_auto_bloquante_cmm"], "SAB_auto_bloquante_ajustement": ["SAB_auto_bloquante_ajustement"], "SAB_auto_bloquante_msd": ["SAB_auto_bloquante_msd"],

    "Ser_dilution_2ml_stock_d_but": ["Ser_dilution_2ml_stock_d_but"], "Ser_dilution_2ml_re_ues": ["Ser_dilution_2ml_re_ues"], "Ser_dilution_2ml_utilis_es": ["Ser_dilution_2ml_utilis_es"], "Ser_dilution_2ml_pertes": ["Ser_dilution_2ml_pertes"], "Ser_dilution_2ml_stock_fin": ["Ser_dilution_2ml_stock_fin", "Ser_dilution_2ml dose-stock disponible utilisable"], "Ser_dilution_2ml_jours_rupture": ["Ser_dilution_2ml_jours_rupture"], "Ser_dilution_2ml_administr_es": ["Ser_dilution_2ml_administr_es"],
    "Ser_dilution_2ml_stock_max": ["Ser_dilution_2ml_stock_max"], "Ser_dilution_2ml_qte_a_commander": ["Ser_dilution_2ml_qte_a_commander"], "Ser_dilution_2ml_cmm": ["Ser_dilution_2ml_cmm"], "Ser_dilution_2ml_ajustement": ["Ser_dilution_2ml_ajustement"], "Ser_dilution_2ml_msd": ["Ser_dilution_2ml_msd"],

    "Ser_dilution_5ml_stock_d_but": ["Ser_dilution_5ml_stock_d_but"], "Ser_dilution_5ml_re_ues": ["Ser_dilution_5ml_re_ues"], "Ser_dilution_5ml_utilis_es": ["Ser_dilution_5ml_utilis_es"], "Ser_dilution_5ml_pertes": ["Ser_dilution_5ml_pertes"], "Ser_dilution_5ml_stock_fin": ["Ser_dilution_5ml_stock_fin", "Ser_dilution_5ml dose-stock disponible utilisable"], "Ser_dilution_5ml_jours_rupture": ["Ser_dilution_5ml_jours_rupture"], "Ser_dilution_5ml_administr_es": ["Ser_dilution_5ml_administr_es"],
    "Ser_dilution_5ml_stock_max": ["Ser_dilution_5ml_stock_max"], "Ser_dilution_5ml_qte_a_commander": ["Ser_dilution_5ml_qte_a_commander"], "Ser_dilution_5ml_cmm": ["Ser_dilution_5ml_cmm"], "Ser_dilution_5ml_ajustement": ["Ser_dilution_5ml_ajustement"], "Ser_dilution_5ml_msd": ["Ser_dilution_5ml_msd"],

    "Ser_dilution_6ml_stock_d_but": ["Ser_dilution_6ml_stock_d_but"], "Ser_dilution_6ml_re_ues": ["Ser_dilution_6ml_re_ues"], "Ser_dilution_6ml_utilis_es": ["Ser_dilution_6ml_utilis_es"], "Ser_dilution_6ml_pertes": ["Ser_dilution_6ml_pertes"], "Ser_dilution_6ml_stock_fin": ["Ser_dilution_6ml_stock_fin", "Ser_dilution_6ml dose-stock disponible utilisable"], "Ser_dilution_6ml_jours_rupture": ["Ser_dilution_6ml_jours_rupture"], "Ser_dilution_6ml_administr_es": ["Ser_dilution_6ml_administr_es"],
    "Ser_dilution_6ml_stock_max": ["Ser_dilution_6ml_stock_max"], "Ser_dilution_6ml_qte_a_commander": ["Ser_dilution_6ml_qte_a_commander"], "Ser_dilution_6ml_cmm": ["Ser_dilution_6ml_cmm"], "Ser_dilution_6ml_ajustement": ["Ser_dilution_6ml_ajustement"], "Ser_dilution_6ml_msd": ["Ser_dilution_6ml_msd"],

    "Adaptateurs_stock_d_but": ["Adaptateurs_stock_d_but"], "Adaptateurs_re_ues": ["Adaptateurs_re_ues"], "Adaptateurs_utilis_es": ["Adaptateurs_utilis_es"], "Adaptateurs_pertes": ["Adaptateurs_pertes"], "Adaptateurs_stock_fin": ["Adaptateurs_stock_fin", "Adaptateurs dose-stock disponible utilisable"], "Adaptateurs_jours_rupture": ["Adaptateurs_jours_rupture"], "Adaptateurs_administr_es": ["Adaptateurs_administr_es"],
    "Adaptateurs_stock_max": ["Adaptateurs_stock_max"], "Adaptateurs_qte_a_commander": ["Adaptateurs_qte_a_commander"], "Adaptateurs_cmm": ["Adaptateurs_cmm"], "Adaptateurs_ajustement": ["Adaptateurs_ajustement"], "Adaptateurs_msd": ["Adaptateurs_msd"],

    "Compte_goutte_stock_d_but": ["Compte_goutte_stock_d_but"], "Compte_goutte_re_ues": ["Compte_goutte_re_ues"], "Compte_goutte_utilis_es": ["Compte_goutte_utilis_es"], "Compte_goutte_pertes": ["Compte_goutte_pertes"], "Compte_goutte_stock_fin": ["Compte_goutte_stock_fin", "Compte_goutte dose-stock disponible utilisable"], "Compte_goutte_jours_rupture": ["Compte_goutte_jours_rupture"], "Compte_goutte_administr_es": ["Compte_goutte_administr_es"],
    "Compte_goutte_stock_max": ["Compte_goutte_stock_max"], "Compte_goutte_qte_a_commander": ["Compte_goutte_qte_a_commander"], "Compte_goutte_cmm": ["Compte_goutte_cmm"], "Compte_goutte_ajustement": ["Compte_goutte_ajustement"], "Compte_goutte_msd": ["Compte_goutte_msd"],

    "R_ceptacles_stock_d_but": ["R_ceptacles_stock_d_but"], "R_ceptacles_re_ues": ["R_ceptacles_re_ues"], "R_ceptacles_utilis_es": ["R_ceptacles_utilis_es"], "R_ceptacles_pertes": ["R_ceptacles_pertes"], "R_ceptacles_stock_fin": ["R_ceptacles_stock_fin"], "R_ceptacles_jours_rupture": ["R_ceptacles_jours_rupture"], "R_ceptacles_administr_es": ["R_ceptacles_administr_es"],
    "R_ceptacles_stock_max": ["R_ceptacles_stock_max"], "R_ceptacles_qte_a_commander": ["R_ceptacles_qte_a_commander"], "R_ceptacles_cmm": ["R_ceptacles_cmm"], "R_ceptacles_ajustement": ["R_ceptacles_ajustement"], "R_ceptacles_msd": ["R_ceptacles_msd"],

    # ── RECO : enfants perdus de vue identifiés / récupérés par les RECO ──
    #   (Penta1 ppMFLxX6NvU + Penta3 DZ2xt2mgVzQ, tous âges ; VAR non collectée) ──
    "RECO_identifies": ["Perdues_de_vue_identifi_s_Penta1_0_11mois", "Perdues_de_vue_identifi_s_Penta1_12_23mois", "Perdues_de_vue_identifi_s_24_59mois", "Perdues_de_vue_identifi_s_Penta3_0_11mois", "Perdues_de_vue_identifi_s_Penta3_12_23mois", "Perdues_de_vue_identifi_s_Penta3_24_59mois"],
    "RECO_recuperes": ["Perdues_de_vue_r_cup_r_s_Penta1_0_11mois", "Perdues_de_vue_r_cup_r_s_Penta1_12_23mois", "Perdues_de_vue_r_cup_r_s_Penta1_24_59mois", "Perdues_de_vue_r_cup_r_s_Penta3_0_11mois", "Perdues_de_vue_r_cup_r_s_Penta3_12_23mois", "Perdues_de_vue_r_cup_r_s_Penta3_24_59mois"],

}

DERIVED_SUM_SPECS = {
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

# ── Helpers ──
def nv(row, field):
    v = row.get(field)
    if v is None or v == "": return 0.0
    try: return float(v)
    except: return 0.0

def normalize_org3(org3):
    s = (org3 or "").strip()
    if len(s) > 3 and s[2] == " ": s = s[3:].strip()
    for suf in [" Zone de Santé", " Zone de Sante"]:
        if s.endswith(suf): s = s[: -len(suf)].strip(); break
    return s

def resolve_antenne(province, zs, ant_rules):
    rules = ant_rules.get(province, {})
    norm = normalize_org3(zs)
    return rules.get(norm, rules.get(zs, ""))

def period_to_ym(p):
    p = (p or "").strip()
    if len(p) >= 7 and p[4] == "-": return p[:4] + p[5:7]
    if len(p) >= 6 and p.isdigit(): return p[:6]
    mmm = {"Jan":"01","Feb":"02","Mar":"03","Apr":"04","May":"05","Jun":"06","Jul":"07","Aug":"08","Sep":"09","Oct":"10","Nov":"11","Dec":"12"}
    parts = p.split("-")
    if len(parts) == 3 and parts[1] in mmm: return parts[2] + mmm[parts[1]]
    return p

def slug(name):
    s = re.sub(r"[^\w\s-]", "", (name or "unknown").strip())
    s = re.sub(r"[\s-]+", "_", s)
    return s.lower() or "unknown"

def write_json_gz(path, data):
    # Ensure path ends with .gz
    if not str(path).endswith(".gz"):
        path = Path(str(path) + ".gz")
    with gzip.open(path, "wt", encoding="utf-8", compresslevel=9) as f:
        json.dump(data, f, separators=(",", ":"), ensure_ascii=False)

def finalize_row(key, g, fields, agg_keys):
    d = {f: key[i] for i, f in enumerate(fields)}
    n = g["n"]
    d["n"] = n
    d["rap"] = g["rap"]
    d["comp"] = round(g["sum_comp"] / n, 2) if n > 0 else 0
    d["prompt"] = round(g["sum_prompt"] / n, 2) if n > 0 else 0
    for k in agg_keys:
        if k in g: d[k] = round(g[k], 2)
    return d

def update_totals(agg_dict, key, g, agg_keys):
    if key not in agg_dict:
        agg_dict[key] = {"n": 0, "rap": 0, "sum_comp": 0.0, "sum_prompt": 0.0}
    t = agg_dict[key]
    t["n"] += g["n"]
    t["rap"] += g["rap"]
    t["sum_comp"] += g["sum_comp"]
    t["sum_prompt"] += g["sum_prompt"]
    for k in agg_keys:
        t[k] = t.get(k, 0.0) + g.get(k, 0.0)

# ── Main Aggregation Function ──
def process_level(level_cfg, ant_rules):
    name = level_cfg["name"]
    data_dir = level_cfg["data_dir"]
    ou_map_path = level_cfg["ou_map_path"]
    dash_dir = level_cfg["dash_dir"]
    fields = level_cfg["fields"]
    include_pop = level_cfg["include_pop"]
    
    print(f"\n>> Level: {name} (Include Population: {include_pop})")
    
    # Clean and create dashboard dir
    if dash_dir.exists(): shutil.rmtree(dash_dir)
    dash_dir.mkdir(parents=True, exist_ok=True)
    
    # Define agg keys for this level
    current_agg_keys = list(BASE_SUM_SPECS.keys()) + list(DERIVED_SUM_SPECS.keys())
    if include_pop:
        current_agg_keys += POP_KEYS
    
    # Load OU MAP
    if ou_map_path.suffix == ".gz":
        with gzip.open(ou_map_path, "rt", encoding="utf-8") as f:
            ou_map = json.load(f)
    else:
        with open(ou_map_path, encoding="utf-8") as f:
            ou_map = json.load(f)
    
    # Load Index
    with open(data_dir / "index.json", encoding="utf-8") as f:
        index = json.load(f)
    
    months_list = sorted(index.get("months", {}).keys())
    
    # We use a nested dict: agg_by_prov[prov][tuple_key] = totals
    # This allows us to finalize and free memory province by province.
    agg_by_prov = defaultdict(dict)
    agg_zs = {}
    agg_prov = {}
    
    all_provinces = set()
    all_antennes = set()
    all_months = set()
    total_records = 0
    
    print(f"  Streaming NDJSON files...")
    for month in months_list:
        parts = index["months"][month].get("parts", [])
        for part in parts:
            fname = part.get("plain") or part.get("file", "")
            gz_path = data_dir / "monthly" / month / part.get("file", "")
            fpath = data_dir / "monthly" / month / fname
            
            f_obj = None
            if gz_path.exists() and str(gz_path).endswith(".gz"):
                f_obj = gzip.open(gz_path, "rt", encoding="utf-8")
            elif fpath.exists():
                f_obj = open(fpath, encoding="utf-8")
            
            if not f_obj: continue
            
            with f_obj as f:
                for line in f:
                    line = line.strip()
                    if not line: continue
                    try:
                        row = json.loads(line)
                        ou = row.get("OrgUnit", "")
                        meta = ou_map.get(ou, {})
                        
                        p_name = meta.get("Org2", "")
                        zs_name = meta.get("Org3", "")
                        ant = resolve_antenne(p_name, zs_name, ant_rules)
                        ym = period_to_ym(row.get("Period", ""))
                        
                        # Primary sums
                        sums = {}
                        for sf, sources in BASE_SUM_SPECS.items():
                            sums[sf] = sum(nv(row, s) for s in sources)
                        
                        # Population sums (if enabled)
                        if include_pop:
                            for pk in POP_KEYS:
                                sums[pk] = nv(row, pk)
                        
                        # Derived sums
                        for sf, sources in DERIVED_SUM_SPECS.items():
                            val = 0.0
                            for s in sources:
                                if s in sums: val += sums[s]
                                else: val += nv(row, s)
                            sums[sf] = val

                        # Build key
                        key_vals = []
                        for fld in fields:
                            if fld == "_Province": key_vals.append(p_name)
                            elif fld == "_ZS": key_vals.append(zs_name)
                            elif fld == "_Antenne": key_vals.append(ant)
                            elif fld == "_AS": key_vals.append(meta.get("Org4", ""))
                            elif fld == "_FOSA": key_vals.append(meta.get("Org5", ""))
                            elif fld == "_YM": key_vals.append(ym)
                        key = tuple(key_vals)
                        
                        # Update provincial agg
                        prov_agg = agg_by_prov[p_name]
                        if key not in prov_agg:
                            prov_agg[key] = {"n": 0, "rap": 0, "sum_comp": 0.0, "sum_prompt": 0.0}
                        g = prov_agg[key]
                        g["n"] += 1
                        comp = nv(row, "Compl_tude")
                        if comp > 0: g["rap"] += 1
                        g["sum_comp"] += comp
                        g["sum_prompt"] += nv(row, "Promptitude")
                        for k, val in sums.items():
                            g[k] = g.get(k, 0.0) + val
                        
                        total_records += 1
                        if p_name: all_provinces.add(p_name)
                        if ant: all_antennes.add(ant)
                        if ym: all_months.add(ym)
                    except: continue
        print(f"    {month}: {total_records} records processed")

    # Global aggregation for ZS and Prov
    print(f"  Aggregating to ZS and Province levels...")
    for p_name, prov_agg in agg_by_prov.items():
        for key, g in prov_agg.items():
            # Identify Prov, ZS, Ant, YM for hierarchy
            # fields example: ["_Province", "_ZS", "_Antenne", "_AS", "_FOSA", "_YM"]
            d_key = {f: key[i] for i, f in enumerate(fields)}
            prov = d_key.get("_Province", "")
            zs = d_key.get("_ZS", "")
            ant = d_key.get("_Antenne", "")
            ym = d_key.get("_YM", "")
            
            # Update ZS
            update_totals(agg_zs, (prov, zs, ant, ym), g, current_agg_keys)
            # Update Prov
            update_totals(agg_prov, (prov, ym), g, current_agg_keys)

    # Finalize and write Province-by-Province
    print(f"  Writing split files and heatmaps...")
    split_dir = dash_dir / ("by_" + name.lower())
    if level_cfg["split_by"]: split_dir.mkdir(parents=True, exist_ok=True)
    
    hm_dir = dash_dir / "heatmap"
    if level_cfg["has_heatmap"]: hm_dir.mkdir(parents=True, exist_ok=True)
    
    manifest = {}
    hm_manifest = {}
    
    sorted_provs = sorted(list(agg_by_prov.keys()))
    for p_name in sorted_provs:
        prov_agg = agg_by_prov[p_name]
        
        # Split File
        if level_cfg["split_by"]:
            rows = [finalize_row(k, v, fields, current_agg_keys) for k, v in prov_agg.items()]
            fname = slug(p_name) + ".json.gz"
            write_json_gz(split_dir / fname, rows)
            manifest[p_name] = fname
            rows = None # Free
            
        # Heatmap
        if level_cfg["has_heatmap"]:
            hm_data = defaultdict(dict)
            for key, g in prov_agg.items():
                d_key = {f: key[i] for i, f in enumerate(fields)}
                entity = d_key.get(level_cfg["heatmap_entity"], "")
                ym = d_key.get("_YM", "")
                if entity and ym:
                    hm_data[entity][ym] = 1 if g["rap"] > 0 else 0
            fname = slug(p_name) + ".json.gz"
            write_json_gz(hm_dir / fname, hm_data)
            hm_manifest[p_name] = fname
            hm_data = None # Free
            
        # CLEAR Provincial Memory
        del agg_by_prov[p_name]
    
    if level_cfg["split_by"]: write_json_gz(split_dir / "manifest.json", manifest)
    if level_cfg["has_heatmap"]: write_json_gz(hm_dir / "manifest.json", hm_manifest)
    
    # Finalize ZS and Prov global files
    print(f"  Writing global files...")
    zs_final = [finalize_row(k, v, ["_Province", "_ZS", "_Antenne", "_YM"], current_agg_keys) for k, v in agg_zs.items()]
    prov_final = [finalize_row(k, v, ["_Province", "_YM"], current_agg_keys) for k, v in agg_prov.items()]
    write_json_gz(dash_dir / "by_province.json", prov_final)
    write_json_gz(dash_dir / "by_zs.json", zs_final)
    
    # Write Meta (keep meta.json plain as it's very small and used for initial load)
    def write_json_plain(path, data):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, separators=(",", ":"), ensure_ascii=False)
    
    meta = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "total_records": total_records,
        "provinces": sorted(list(all_provinces)),
        "province_slugs": {p: slug(p) for p in sorted(list(all_provinces))},
        "antennes": sorted(list(all_antennes)),
        "months": sorted(list(all_months)),
        "include_pop": include_pop
    }
    write_json_plain(dash_dir / "meta.json", meta)
    print(f"✅ Level {name} complete.")

# ── Entry Point ──
if __name__ == "__main__":
    t_start = time.time()
    
    # Load Antenne Rules once
    ANT_RULES = {}
    if ANT_RULES_PATH.exists():
        with open(ANT_RULES_PATH, encoding="utf-8") as f:
            ANT_RULES = json.load(f)
            
    # Process each level
    for lvl in LEVEL_CONFIGS:
        process_level(lvl, ANT_RULES)
        
    # Touch .nojekyll
    (DOCS / ".nojekyll").touch()
    
    print(f"\n✨ ALL Dashboard aggregations complete in {time.time() - t_start:.1f}s")
