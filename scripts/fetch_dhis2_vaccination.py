from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests


# =========================
# 1) CONFIG: COLLER ICI
# =========================

# 1A) Colle ici ton dxList (séparé par ;)
DX_LIST =  "W25tOXS0rxS.dqydGQFHahb;W25tOXS0rxS.NOHlOxLczjc;W25tOXS0rxS.vbx8t4WAbR8;" &
          "W25tOXS0rxS.KPDzIsWq7JK;W25tOXS0rxS.oSYTyzezWif;W25tOXS0rxS.Dr4rWTqepnP;" &
          "uNdFg1eymsa.dqydGQFHahb;uNdFg1eymsa.NOHlOxLczjc;uNdFg1eymsa.vbx8t4WAbR8;" &
          "uNdFg1eymsa.KPDzIsWq7JK;uNdFg1eymsa.oSYTyzezWif;uNdFg1eymsa.Dr4rWTqepnP;" &
          "YYKXOc7xBUi.dqydGQFHahb;YYKXOc7xBUi.NOHlOxLczjc;YYKXOc7xBUi.vbx8t4WAbR8;" &
          "YYKXOc7xBUi.KPDzIsWq7JK;YYKXOc7xBUi.oSYTyzezWif;YYKXOc7xBUi.Dr4rWTqepnP;" &
          "WWxqaHSeiwd.dqydGQFHahb;WWxqaHSeiwd.NOHlOxLczjc;WWxqaHSeiwd.vbx8t4WAbR8;" &
          "WWxqaHSeiwd.KPDzIsWq7JK;WWxqaHSeiwd.oSYTyzezWif;WWxqaHSeiwd.Dr4rWTqepnP;" &
          "SPGaDDw2JzG.dqydGQFHahb;SPGaDDw2JzG.NOHlOxLczjc;SPGaDDw2JzG.vbx8t4WAbR8;" &
          "SPGaDDw2JzG.KPDzIsWq7JK;SPGaDDw2JzG.oSYTyzezWif;SPGaDDw2JzG.Dr4rWTqepnP;" &
          "I6tLH3nk9tw.dqydGQFHahb;I6tLH3nk9tw.NOHlOxLczjc;I6tLH3nk9tw.vbx8t4WAbR8;" &
          "I6tLH3nk9tw.KPDzIsWq7JK;I6tLH3nk9tw.oSYTyzezWif;I6tLH3nk9tw.Dr4rWTqepnP;" &
          "wwde2QtlIRN.dqydGQFHahb;wwde2QtlIRN.NOHlOxLczjc;wwde2QtlIRN.vbx8t4WAbR8;" &
          "wwde2QtlIRN.KPDzIsWq7JK;wwde2QtlIRN.oSYTyzezWif;wwde2QtlIRN.Dr4rWTqepnP;" &
          "c4VvzI5zTep.dqydGQFHahb;c4VvzI5zTep.vbx8t4WAbR8;c4VvzI5zTep.oSYTyzezWif;" &
          "J8R3WFpMAZI.dqydGQFHahb;J8R3WFpMAZI.NOHlOxLczjc;J8R3WFpMAZI.vbx8t4WAbR8;" &
          "J8R3WFpMAZI.KPDzIsWq7JK;J8R3WFpMAZI.oSYTyzezWif;J8R3WFpMAZI.Dr4rWTqepnP;" &
          "pak21wvkWJC.dqydGQFHahb;cTLKwfG8pSv.NOHlOxLczjc;pak21wvkWJC.vbx8t4WAbR8;" &
          "cTLKwfG8pSv.KPDzIsWq7JK;pak21wvkWJC.oSYTyzezWif;cTLKwfG8pSv.Dr4rWTqepnP;" &
          "i5zmivDIHN8.g6mIyKoGIh2;i5zmivDIHN8.QRyK6yxKBU3;i5zmivDIHN8.Rby9Jdri29F;" &
          "i5zmivDIHN8.FCXzheCQXtr;i5zmivDIHN8.VrEj0UVVGr4;M2JQW0H44dI;" &
          "s5EUr8GznWY;lHB57kjRtUz;h7bxqdKWYCa;" &
          "gDnbubwzuts;WLSKVyA8LoY;IRuHNExvC4m;" &
          "pVOrmIskonM;nGZz4qgNal7;YvO03GnK1oQ;" &
          "E4BX1ea2iDJ.REPORTING_RATE;E4BX1ea2iDJ.REPORTING_RATE_ON_TIME;s5EUr8GznWY.HVViCmJi85w;" &
          "lHB57kjRtUz.HVViCmJi85w;s5EUr8GznWY.TG2PxzbbIjT;lHB57kjRtUz.TG2PxzbbIjT;" &
          "s5EUr8GznWY.aE55ruo9YeU;lHB57kjRtUz.aE55ruo9YeU;ppMFLxX6NvU.FGpuKKTof4X;" &
          "ppMFLxX6NvU.RD9Iut5kjja;ppMFLxX6NvU.LnwvJBIlzxk;ppMFLxX6NvU.RMMpx7TBreP;" &
          "ppMFLxX6NvU.ZYkCdGglDmr;ppMFLxX6NvU.aZmhNMRCrmZ;DZ2xt2mgVzQ.FGpuKKTof4X;" &
          "DZ2xt2mgVzQ.RD9Iut5kjja;DZ2xt2mgVzQ.LnwvJBIlzxk;DZ2xt2mgVzQ.RMMpx7TBreP;" &
          "DZ2xt2mgVzQ.ZYkCdGglDmr;DZ2xt2mgVzQ.aZmhNMRCrmZ;SnhwwhcaDRS;fycXkvxPUjH;" &
          "Nyke8sWJbn9.oSYTyzezWif;Nyke8sWJbn9.vbx8t4WAbR8;slj1Yy0YgAc.dqydGQFHahb;" &
          "slj1Yy0YgAc.vbx8t4WAbR8;slj1Yy0YgAc.oSYTyzezWif;OnKbxNJzsCw.dqydGQFHahb;" &
          "OnKbxNJzsCw.vbx8t4WAbR8;OnKbxNJzsCw.oSYTyzezWif;KVjtJyNKcYQ.g6mIyKoGIh2;" &
          "KVjtJyNKcYQ.Rby9Jdri29F;KVjtJyNKcYQ.FCXzheCQXtr;WzOgzB6fLCW;" &
          "E4BX1ea2iDJ;i5zmivDIHN8.dqydGQFHahb;i5zmivDIHN8.vbx8t4WAbR8;" &
          "i5zmivDIHN8.oSYTyzezWif;SnhwwhcaDRS.M7I3dsr9L5Z;fycXkvxPUjH.M7I3dsr9L5Z;" &
          "CKhvpizB7wo.M7I3dsr9L5Z;OkgUUlDsGAN.M7I3dsr9L5Z;SnhwwhcaDRS.eX294IHIRVM;" &
          "fycXkvxPUjH.eX294IHIRVM;CKhvpizB7wo.eX294IHIRVM;OkgUUlDsGAN.eX294IHIRVM;" &
          "SnhwwhcaDRS.QeTSJLfbwEw;fycXkvxPUjH.QeTSJLfbwEw;CKhvpizB7wo.QeTSJLfbwEw;" &
          "OkgUUlDsGAN.QeTSJLfbwEw;SnhwwhcaDRS.rJgTQZ63hnO;fycXkvxPUjH.rJgTQZ63hnO;" &
          "CKhvpizB7wo.rJgTQZ63hnO;OkgUUlDsGAN.rJgTQZ63hnO;SnhwwhcaDRS.YWikHnhDAXE;" &
          "fycXkvxPUjH.YWikHnhDAXE;CKhvpizB7wo.YWikHnhDAXE;OkgUUlDsGAN.YWikHnhDAXE;" &
          "SnhwwhcaDRS.lvDrwyxXMq2;fycXkvxPUjH.lvDrwyxXMq2;CKhvpizB7wo.lvDrwyxXMq2;" &
          "OkgUUlDsGAN.lvDrwyxXMq2;SnhwwhcaDRS.QOXbI3fshPV;fycXkvxPUjH.QOXbI3fshPV;" &
          "CKhvpizB7wo.QOXbI3fshPV;OkgUUlDsGAN.QOXbI3fshPV;SnhwwhcaDRS.IdjExsz3izf;" &
          "fycXkvxPUjH.IdjExsz3izf;CKhvpizB7wo.IdjExsz3izf;OkgUUlDsGAN.IdjExsz3izf;" &
          "SnhwwhcaDRS.OdTBbi5jfhb;fycXkvxPUjH.OdTBbi5jfhb;CKhvpizB7wo.OdTBbi5jfhb;" &
          "OkgUUlDsGAN.OdTBbi5jfhb;SnhwwhcaDRS.WbtfH2Deu6j;fycXkvxPUjH.WbtfH2Deu6j;" &
          "CKhvpizB7wo.WbtfH2Deu6j;OkgUUlDsGAN.WbtfH2Deu6j;SnhwwhcaDRS.hiiCMvb0tTp;" &
          "fycXkvxPUjH.hiiCMvb0tTp;CKhvpizB7wo.hiiCMvb0tTp;OkgUUlDsGAN.hiiCMvb0tTp;" &
          "CKhvpizB7wo;f1MiAAIu366.dqydGQFHahb;f1MiAAIu366.NOHlOxLczjc;" &
          "f1MiAAIu366.vbx8t4WAbR8;f1MiAAIu366.KPDzIsWq7JK;f1MiAAIu366.oSYTyzezWif;" &
          "f1MiAAIu366.Dr4rWTqepnP;j3P9bSnwF12.dqydGQFHahb;j3P9bSnwF12.NOHlOxLczjc;" &
          "j3P9bSnwF12.vbx8t4WAbR8;j3P9bSnwF12.KPDzIsWq7JK;j3P9bSnwF12.oSYTyzezWif;" &
          "j3P9bSnwF12.Dr4rWTqepnP;ozVdkud4F03.dqydGQFHahb;ozVdkud4F03.NOHlOxLczjc;" &
          "ozVdkud4F03.vbx8t4WAbR8;ozVdkud4F03.KPDzIsWq7JK;ozVdkud4F03.oSYTyzezWif;" &
          "ozVdkud4F03.Dr4rWTqepnP;CgR8kWNdK3W.dqydGQFHahb;CgR8kWNdK3W.NOHlOxLczjc;" &
          "CgR8kWNdK3W.vbx8t4WAbR8;CgR8kWNdK3W.KPDzIsWq7JK;CgR8kWNdK3W.oSYTyzezWif;" &
          "CgR8kWNdK3W.Dr4rWTqepnP;WGu60o5mePq.dqydGQFHahb;WGu60o5mePq.NOHlOxLczjc;" &
          "WGu60o5mePq.vbx8t4WAbR8;WGu60o5mePq.KPDzIsWq7JK;WGu60o5mePq.oSYTyzezWif;" &
          "WGu60o5mePq.Dr4rWTqepnP;TLKusYaKY5A.dqydGQFHahb;TLKusYaKY5A.vbx8t4WAbR8;" &
          "TLKusYaKY5A.oSYTyzezWif;GzSBTZkxSZf.dqydGQFHahb;GzSBTZkxSZf.vbx8t4WAbR8;" &
          "GzSBTZkxSZf.oSYTyzezWif".strip()

# 1B) Colle ici toutes tes paires RenamePairs converties en dict Python
RENAME_MAP: Dict[str, str] = {
    {"W25tOXS0rxS.dqydGQFHahb","BCG fixe1"},
                                {"W25tOXS0rxS.NOHlOxLczjc","BCG fixe2"},
                                {"W25tOXS0rxS.vbx8t4WAbR8","BCG avancé1"},
                                {"W25tOXS0rxS.KPDzIsWq7JK","BCG avancé2"},
                                {"W25tOXS0rxS.oSYTyzezWif","BCG mobile1"},
                                {"W25tOXS0rxS.Dr4rWTqepnP","BCG mobile2"},
                                {"uNdFg1eymsa.dqydGQFHahb","Penta1 fixe1"},
                                {"uNdFg1eymsa.NOHlOxLczjc","Penta1 fixe2"},
                                {"uNdFg1eymsa.vbx8t4WAbR8","Penta1 avancé1"},
                                {"uNdFg1eymsa.KPDzIsWq7JK","Penta1 avancé2"},
                                {"uNdFg1eymsa.oSYTyzezWif","Penta1 mobile1"},
                                {"uNdFg1eymsa.Dr4rWTqepnP","Penta1 mobile2"},
                                {"YYKXOc7xBUi.dqydGQFHahb","Penta2 fixe1"},
                                {"YYKXOc7xBUi.NOHlOxLczjc","Penta2 fixe2"},
                                {"YYKXOc7xBUi.vbx8t4WAbR8","Penta2 avancé1"},
                                {"YYKXOc7xBUi.KPDzIsWq7JK","Penta2 avancé2"},
                                {"YYKXOc7xBUi.oSYTyzezWif","Penta2 mobile1"},
                                {"YYKXOc7xBUi.Dr4rWTqepnP","Penta2 mobile2"},
                                {"WWxqaHSeiwd.dqydGQFHahb","Penta3 fixe1"},
                                {"WWxqaHSeiwd.NOHlOxLczjc","Penta3 fixe2"},
                                {"WWxqaHSeiwd.vbx8t4WAbR8","Penta3 avancé1"},
                                {"WWxqaHSeiwd.KPDzIsWq7JK","Penta3 avancé2"},
                                {"WWxqaHSeiwd.oSYTyzezWif","Penta3 mobile1"},
                                {"WWxqaHSeiwd.Dr4rWTqepnP","Penta3 mobile2"},
                                {"SPGaDDw2JzG.dqydGQFHahb","VPO3 fixe1"},
                                {"SPGaDDw2JzG.NOHlOxLczjc","VPO3 fixe2"},
                                {"SPGaDDw2JzG.vbx8t4WAbR8","VPO3 avancé1"},
                                {"SPGaDDw2JzG.KPDzIsWq7JK","VPO3 avancé2"},
                                {"SPGaDDw2JzG.oSYTyzezWif","VPO3 mobile1"},
                                {"SPGaDDw2JzG.Dr4rWTqepnP","VPO3 mobile2"},
                                {"I6tLH3nk9tw.dqydGQFHahb","VPI1 fixe1"},
                                {"I6tLH3nk9tw.NOHlOxLczjc","VPI1 fixe2"},
                                {"I6tLH3nk9tw.vbx8t4WAbR8","VPI1 avancé1"},
                                {"I6tLH3nk9tw.KPDzIsWq7JK","VPI1 avancé2"},
                                {"I6tLH3nk9tw.oSYTyzezWif","VPI1 mobile1"},
                                {"I6tLH3nk9tw.Dr4rWTqepnP","VPI1 mobile2"},
                                {"wwde2QtlIRN.dqydGQFHahb","VPI2 fixe1"},
                                {"wwde2QtlIRN.NOHlOxLczjc","VPI2 fixe2"},
                                {"wwde2QtlIRN.vbx8t4WAbR8","VPI2 avancé1"},
                                {"wwde2QtlIRN.KPDzIsWq7JK","VPI2 avancé2"},
                                {"wwde2QtlIRN.oSYTyzezWif","VPI2 mobile1"},
                                {"wwde2QtlIRN.Dr4rWTqepnP","VPI2 mobile2"},
                                {"c4VvzI5zTep.dqydGQFHahb","ROTA3 fixe"},
                                {"c4VvzI5zTep.vbx8t4WAbR8","ROTA3 avancé"},                                                                                    
                                {"c4VvzI5zTep.oSYTyzezWif","ROTA3 mobile"},
                                {"J8R3WFpMAZI.dqydGQFHahb","PCV13 fixe1"},
                                {"J8R3WFpMAZI.NOHlOxLczjc","PCV13 fixe2"},
                                {"J8R3WFpMAZI.vbx8t4WAbR8","PCV13 avancé1"},
                                {"J8R3WFpMAZI.KPDzIsWq7JK","PCV13 avancé2"},
                                {"J8R3WFpMAZI.oSYTyzezWif","PCV13 mobile1"},
                                {"J8R3WFpMAZI.Dr4rWTqepnP","PCV13 mobile2"},
                                {"pak21wvkWJC.dqydGQFHahb","VAR1 fixe1"},
                                {"cTLKwfG8pSv.NOHlOxLczjc","VAR1 fixe2"},
                                {"pak21wvkWJC.vbx8t4WAbR8","VAR1 avancé1"},
                                {"cTLKwfG8pSv.KPDzIsWq7JK","VAR1 avancé2"},
                                {"pak21wvkWJC.oSYTyzezWif","VAR1 mobile1"},
                                {"cTLKwfG8pSv.Dr4rWTqepnP","VAR1 mobile2"},
                                {"i5zmivDIHN8.g6mIyKoGIh2","VAR2 fixe1"},
                                {"i5zmivDIHN8.QRyK6yxKBU3","VAR2 fixe2"},
                                {"i5zmivDIHN8.Rby9Jdri29F","VAR2 avancé"},
                                {"i5zmivDIHN8.FCXzheCQXtr","VAR2 mobile1"},
                                {"i5zmivDIHN8.VrEj0UVVGr4","VAR2 mobile2"},
                                {"M2JQW0H44dI","ECV"},
                                {"s5EUr8GznWY","Séances prévues"},
                                {"lHB57kjRtUz","Séances réalisées"},
                                {"h7bxqdKWYCa","Pop. totale"},
                                {"gDnbubwzuts","Naissances vivantes"},
                                {"WLSKVyA8LoY","Pop. par AS"},
                                {"IRuHNExvC4m","Pop. 0-11m (nv)"},
                                {"pVOrmIskonM","Pop. 0-11m (survivants)"},
                                {"nGZz4qgNal7","Pop. 0-59m"},
                                {"YvO03GnK1oQ","Pop. 12-59m"},
                                {"E4BX1ea2iDJ.REPORTING_RATE","Complétude"},
                                {"E4BX1ea2iDJ.REPORTING_RATE_ON_TIME","Promptitude"},
                                {"s5EUr8GznWY.HVViCmJi85w","Séances fixes prévues"},
                                {"lHB57kjRtUz.HVViCmJi85w","Séances fixes réalisées"},
                                {"s5EUr8GznWY.TG2PxzbbIjT","Séances avancées prévues"},
                                {"lHB57kjRtUz.TG2PxzbbIjT","Séances avancées réalisées"},
                                {"s5EUr8GznWY.aE55ruo9YeU","Séances mobiles prévues"},
                                {"lHB57kjRtUz.aE55ruo9YeU","Séances mobiles réalisées"},
                                {"ppMFLxX6NvU.FGpuKKTof4X","Perdues de vue identifiés Penta1 0-11mois"},
                                {"ppMFLxX6NvU.RD9Iut5kjja","Perdues de vue identifiés Penta1 12-23mois"},
                                {"ppMFLxX6NvU.LnwvJBIlzxk","Perdues de vue identifiés 24-59mois"},
                                {"ppMFLxX6NvU.RMMpx7TBreP","Perdues de vue récupérés Penta1 0-11mois"},
                                {"ppMFLxX6NvU.ZYkCdGglDmr","Perdues de vue récupérés Penta1 12-23mois"},
                                {"ppMFLxX6NvU.aZmhNMRCrmZ","Perdues de vue récupérés Penta1 24-59mois"},
                                {"DZ2xt2mgVzQ.FGpuKKTof4X","Perdues de vue identifiés Penta3 0-11mois"},
                                {"DZ2xt2mgVzQ.RD9Iut5kjja","Perdues de vue identifiés Penta3 12-23mois"},
                                {"DZ2xt2mgVzQ.LnwvJBIlzxk","Perdues de vue identifiés Penta3 24-59mois"},
                                {"DZ2xt2mgVzQ.RMMpx7TBreP","Perdues de vue récupérés Penta3 0-11mois"},
                                {"DZ2xt2mgVzQ.ZYkCdGglDmr","Perdues de vue récupérés Penta3 12-23mois"},
                                {"DZ2xt2mgVzQ.aZmhNMRCrmZ","Perdues de vue récupérés Penta3 24-59mois"},
                                {"fycXkvxPUjH","Enfants récupérés Penta3 BCU-FAE"},
                                {"Nyke8sWJbn9.oSYTyzezWif","VAP1 0-11 mois fixe"},
                                {"Nyke8sWJbn9.vbx8t4WAbR8","VAP1 0-11 mois avancée"},
                                {"slj1Yy0YgAc.dqydGQFHahb","VAP2 0-11 mois fixe"},
                                {"slj1Yy0YgAc.vbx8t4WAbR8","VAP2 0-11 mois avancée"},
                                {"slj1Yy0YgAc.oSYTyzezWif","VAP2 0-11 mois mobile"},
                                {"OnKbxNJzsCw.dqydGQFHahb","VAP3 0-11 mois fixe"},
                                {"OnKbxNJzsCw.vbx8t4WAbR8","VAP3 0-11 mois avancée"},
                                {"OnKbxNJzsCw.oSYTyzezWif","VAP3 0-11 mois mobile"},
                                {"KVjtJyNKcYQ.g6mIyKoGIh2","VAP4 12-23 mois fixe"},
                                {"KVjtJyNKcYQ.Rby9Jdri29F","VAP4 12-23 mois avancée"},
                                {"KVjtJyNKcYQ.FCXzheCQXtr","VAP4 12-23 mois mobile"},
                                {"WzOgzB6fLCW","HPV"},
                                {"E4BX1ea2iDJ","Rapports attendus"},
                                {"i5zmivDIHN8.dqydGQFHahb","VAR2 0-11 mois fixe"},
                                {"i5zmivDIHN8.vbx8t4WAbR8","VAR2 0-11 mois avancée"},
                                {"i5zmivDIHN8.oSYTyzezWif","VAR2 0-11 mois mobile"},
                                {"SnhwwhcaDRS.M7I3dsr9L5Z","AVS DTC1"},
                                {"fycXkvxPUjH.M7I3dsr9L5Z","AVS DTC3"},
                                {"CKhvpizB7wo.M7I3dsr9L5Z","AVS VAR1"},
                                {"OkgUUlDsGAN.M7I3dsr9L5Z","AVS VAR2"},
                                {"SnhwwhcaDRS.eX294IHIRVM","OVM DTC1"},
                                {"fycXkvxPUjH.eX294IHIRVM","OVM DTC3"},
                                {"CKhvpizB7wo.eX294IHIRVM","OVM VAR1"},

                                {"OkgUUlDsGAN.eX294IHIRVM","OVM VAR2"},
                                {"SnhwwhcaDRS.QeTSJLfbwEw","Fluviale DTC1"},
                                {"fycXkvxPUjH.QeTSJLfbwEw","Fluviale DTC3"},
                                {"CKhvpizB7wo.QeTSJLfbwEw","Fluviale VAR1"},
                                {"OkgUUlDsGAN.QeTSJLfbwEw","Fluviale VAR2"},
                                {"SnhwwhcaDRS.rJgTQZ63hnO","IPVS DTC1"},
                                {"fycXkvxPUjH.rJgTQZ63hnO","IPVS DTC3"},
                                {"CKhvpizB7wo.rJgTQZ63hnO","IPVS VAR1"},
                                {"OkgUUlDsGAN.rJgTQZ63hnO","IPVS VAR2"},
                                {"SnhwwhcaDRS.YWikHnhDAXE","Autochtones DTC1"},
                            {"fycXkvxPUjH.YWikHnhDAXE","Autochtones DTC3"},
                            {"CKhvpizB7wo.YWikHnhDAXE","Autochtones VAR1"},
                            {"OkgUUlDsGAN.YWikHnhDAXE","Autochtones VAR2"},
                            {"SnhwwhcaDRS.lvDrwyxXMq2","Nomades DTC1"},
                            {"fycXkvxPUjH.lvDrwyxXMq2","Nomades DTC3"},
                            {"CKhvpizB7wo.lvDrwyxXMq2","Nomades VAR1"},
                            {"OkgUUlDsGAN.lvDrwyxXMq2","Nomades VAR2"},
                            {"SnhwwhcaDRS.QOXbI3fshPV","Réfugiés/Déplacés DTC1"},
                            {"fycXkvxPUjH.QOXbI3fshPV","Réfugiés/Déplacés DTC3"},
                            {"CKhvpizB7wo.QOXbI3fshPV","Réfugiés/Déplacés VAR1"},
                            {"OkgUUlDsGAN.QOXbI3fshPV","Réfugiés/Déplacés VAR2"},
                            {"SnhwwhcaDRS.IdjExsz3izf","Point de concentration DTC1"},
                            {"fycXkvxPUjH.IdjExsz3izf","Point de concentration DTC3"},
                            {"CKhvpizB7wo.IdjExsz3izf","Point de concentration VAR1"},
                            {"OkgUUlDsGAN.IdjExsz3izf","Point de concentration VAR2"},
                            {"SnhwwhcaDRS.OdTBbi5jfhb","Horaire adapté DTC1"},
                            {"fycXkvxPUjH.OdTBbi5jfhb","Horaire adapté DTC3"},
                            {"CKhvpizB7wo.OdTBbi5jfhb","Horaire adapté VAR1"},
                            {"OkgUUlDsGAN.OdTBbi5jfhb","Horaire adapté VAR2"},
                            {"SnhwwhcaDRS.WbtfH2Deu6j","Campements DTC1"},
                            {"fycXkvxPUjH.WbtfH2Deu6j","Campements DTC3"},
                            {"CKhvpizB7wo.WbtfH2Deu6j","Campements VAR1"},
                            {"OkgUUlDsGAN.WbtfH2Deu6j","Campements VAR2"},
                            {"SnhwwhcaDRS.hiiCMvb0tTp","Poches d'insécurité DTC1"},
                            {"fycXkvxPUjH.hiiCMvb0tTp","Poches d'insécurité DTC3"},
                            {"CKhvpizB7wo.hiiCMvb0tTp","Poches d'insécurité VAR1"},
                            {"OkgUUlDsGAN.hiiCMvb0tTp","Poches d'insécurité VAR2"},
                                {"CKhvpizB7wo","Enfants récupérés VAR1 BCU-FAE"},
                                {"f1MiAAIu366.dqydGQFHahb","VPO0 0-11 mois fixe1"},
                                {"f1MiAAIu366.NOHlOxLczjc","VPO0 0-11 mois fixe2"},
                                {"f1MiAAIu366.vbx8t4WAbR8","VPO0 0-11 mois avancée1"},
                                {"f1MiAAIu366.KPDzIsWq7JK","VPO0 0-11 mois avancée2"},
                                {"f1MiAAIu366.oSYTyzezWif","VPO0 0-11 mois mobile1"},
                                {"f1MiAAIu366.Dr4rWTqepnP","VPO0 0-11 mois mobile2"},
                                {"j3P9bSnwF12.dqydGQFHahb","VPO1 0-11 mois fixe1"},
                                {"j3P9bSnwF12.NOHlOxLczjc","VPO1 0-11 mois fixe2"},
                                {"j3P9bSnwF12.vbx8t4WAbR8","VPO1 0-11 mois avancée1"},
                                {"j3P9bSnwF12.KPDzIsWq7JK","VPO1 0-11 mois avancée2"},
                                {"j3P9bSnwF12.oSYTyzezWif","VPO1 0-11 mois mobile1"},
                                {"j3P9bSnwF12.Dr4rWTqepnP","VPO1 0-11 mois mobile2"},
                                {"ozVdkud4F03.dqydGQFHahb","VPO2 0-11 mois fixe1"},
                                {"ozVdkud4F03.NOHlOxLczjc","VPO2 0-11 mois fixe2"},
                                {"ozVdkud4F03.vbx8t4WAbR8","VPO2 0-11 mois avancée1"},
                                {"ozVdkud4F03.KPDzIsWq7JK","VPO2 0-11 mois avancée2"},
                                {"ozVdkud4F03.oSYTyzezWif","VPO2 0-11 mois mobile1"},
                                {"ozVdkud4F03.Dr4rWTqepnP","VPO2 0-11 mois mobile2"},
                                {"CgR8kWNdK3W.dqydGQFHahb","PCV13(1) 0-11 mois fixe1"},
                                {"CgR8kWNdK3W.NOHlOxLczjc","PCV13(1) 0-11 mois fixe2"},
                                {"CgR8kWNdK3W.vbx8t4WAbR8","PCV13(1) 0-11 mois avancée1"},
                                {"CgR8kWNdK3W.KPDzIsWq7JK","PCV13(1) 0-11 mois avancée2"},
                                {"CgR8kWNdK3W.oSYTyzezWif","PCV13(1) 0-11 mois mobile1"},
                                {"CgR8kWNdK3W.Dr4rWTqepnP","PCV13(1) 0-11 mois mobile2"},
                                {"WGu60o5mePq.dqydGQFHahb","PCV13(2) 0-11 mois fixe1"},
                                {"WGu60o5mePq.NOHlOxLczjc","PCV13(2) 0-11 mois fixe2"},
                                {"WGu60o5mePq.vbx8t4WAbR8","PCV13(2) 0-11 mois avancée1"},
                                {"WGu60o5mePq.KPDzIsWq7JK","PCV13(2) 0-11 mois avancée2"},
                                {"WGu60o5mePq.oSYTyzezWif","PCV13(2) 0-11 mois mobile1"},
                                {"WGu60o5mePq.Dr4rWTqepnP","PCV13(2) 0-11 mois mobile2"},
                                {"TLKusYaKY5A.dqydGQFHahb","ROTA1 0-11 mois fixe"},
                                {"TLKusYaKY5A.vbx8t4WAbR8","ROTA1 0-11 mois avancée"},
                                {"TLKusYaKY5A.oSYTyzezWif","ROTA1 0-11 mois mobile"},
                                {"GzSBTZkxSZf.dqydGQFHahb","ROTA2 0-11 mois fixe"},
                                {"GzSBTZkxSZf.vbx8t4WAbR8","ROTA2 0-11 mois avancée"},
                                {"GzSBTZkxSZf.oSYTyzezWif","ROTA2 0-11 mois mobile"}
}


# =========================
# 2) HELPERS
# =========================

def current_yyyymm(today: Optional[date] = None) -> str:
    d = today or date.today()
    return f"{d.year:04d}{d.month:02d}"


def month_range(start_yyyymm: str, end_yyyymm: str) -> List[str]:
    sy, sm = int(start_yyyymm[:4]), int(start_yyyymm[4:6])
    ey, em = int(end_yyyymm[:4]), int(end_yyyymm[4:6])
    months: List[str] = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}{m:02d}")
        m += 1
        if m == 13:
            y += 1
            m = 1
    return months


def chunk_list(items: List[str], max_chars: int = 6500) -> List[List[str]]:
    chunks: List[List[str]] = []
    cur: List[str] = []
    cur_len = 0
    for it in items:
        add = len(it) + (1 if cur else 0)
        if cur and cur_len + add > max_chars:
            chunks.append(cur)
            cur = [it]
            cur_len = len(it)
        else:
            cur.append(it)
            cur_len += add
    if cur:
        chunks.append(cur)
    return chunks


@dataclass
class Dhis2Client:
    base_url: str
    username: str
    password: str
    timeout_s: int = 120

    def _get(self, path: str, params: Dict[str, object]) -> dict:
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        r = requests.get(
            url,
            params=params,
            auth=(self.username, self.password),
            headers={"Accept": "application/json"},
            timeout=self.timeout_s,
        )
        r.raise_for_status()
        return r.json()

    def analytics(self, dx_items: List[str], pe: str, ou: str = "LEVEL-5") -> dict:
        params = {
            "dimension": [f"dx:{';'.join(dx_items)}", f"pe:{pe}", f"ou:{ou}"],
            "displayProperty": "NAME",
            "outputIdScheme": "UID",
            "skipMeta": "true",
            "paging": "false",
        }
        return self._get("api/analytics.json", params)

    def org_units_level(self, level: int) -> List[dict]:
        params = {
            "filter": f"level:eq:{level}",
            "paging": "false",
            "fields": "id,name,level,path",
        }
        data = self._get("api/organisationUnits.json", params)
        return data.get("organisationUnits", [])


def build_ou_hierarchy(client: Dhis2Client) -> Dict[str, Dict[str, Optional[str]]]:
    all_units: Dict[str, dict] = {}
    for lvl in (2, 3, 4, 5):
        for ou in client.org_units_level(lvl):
            all_units[ou["id"]] = ou

    out: Dict[str, Dict[str, Optional[str]]] = {}
    for ou_id, ou in all_units.items():
        if ou.get("level") != 5:
            continue
        path = ou.get("path") or ""
        ids = [p for p in path.split("/") if p]

        def name_for(level: int) -> Optional[str]:
            for pid in ids:
                u = all_units.get(pid)
                if u and u.get("level") == level:
                    return u.get("name")
            return None

        out[ou_id] = {
            "Level 2 Org Unit": name_for(2),
            "Level 3 Org Unit": name_for(3),
            "Level 4 Org Unit": name_for(4),
            "Level 5 Org Unit": ou.get("name"),
        }
    return out


def rows_to_records(analytics_json: dict) -> List[dict]:
    rows = analytics_json.get("rows") or []
    recs: List[dict] = []
    for r in rows:
        try:
            dx, pe, ou, val = r[0], r[1], r[2], r[3]
        except Exception:
            continue
        try:
            v = float(val)
        except Exception:
            v = None
        recs.append({"dx": dx, "pe": pe, "ou": ou, "value": v})
    return recs


def pivot_records(
    long_recs: List[dict],
    dx_expected: List[str],
    ou_hier: Dict[str, Dict[str, Optional[str]]],
    rename_map: Dict[str, str],
) -> List[dict]:
    idx: Dict[Tuple[str, str], dict] = {}
    for r in long_recs:
        key = (r["ou"], r["pe"])
        row = idx.get(key)
        if row is None:
            row = {"ou": r["ou"], "pe": r["pe"]}
            row.update(ou_hier.get(r["ou"], {}))
            idx[key] = row
        # En cas de doublons, on additionne
        old = row.get(r["dx"])
        val = r["value"]
        if old is None:
            row[r["dx"]] = val
        else:
            row[r["dx"]] = (old or 0) + (val or 0)

    for row in idx.values():
        for dx in dx_expected:
            row.setdefault(dx, None)

    out: List[dict] = []
    for row in idx.values():
        out_row: dict = {
            "Level 2 Org Unit": row.get("Level 2 Org Unit"),
            "Level 3 Org Unit": row.get("Level 3 Org Unit"),
            "Level 4 Org Unit": row.get("Level 4 Org Unit"),
            "Level 5 Org Unit": row.get("Level 5 Org Unit"),
            "Period": f"{row['pe'][:4]}-{row['pe'][4:6]}-01",
        }
        for dx in dx_expected:
            col = rename_map.get(dx, dx)
            out_row[col] = row.get(dx)
        out.append(out_row)

    out.sort(key=lambda x: (
        x.get("Level 2 Org Unit") or "",
        x.get("Level 3 Org Unit") or "",
        x.get("Level 4 Org Unit") or "",
        x.get("Level 5 Org Unit") or "",
        x.get("Period") or "",
    ))
    return out


def fetch_period(
    client: Dhis2Client,
    pe: str,
    dx_expected: List[str],
    ou_hier: Dict[str, Dict[str, Optional[str]]],
    rename_map: Dict[str, str],
    dx_chunk_chars: int,
    sleep_s: float,
) -> List[dict]:
    chunks = chunk_list(dx_expected, max_chars=dx_chunk_chars)
    long_all: List[dict] = []
    for ch in chunks:
        data = client.analytics(dx_items=ch, pe=pe, ou="LEVEL-5")
        long_all.extend(rows_to_records(data))
        if sleep_s:
            time.sleep(sleep_s)
    return pivot_records(long_all, dx_expected, ou_hier, rename_map)


def write_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")


def write_csv_gz(path: Path, records: List[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fixed = ["Level 2 Org Unit", "Level 3 Org Unit", "Level 4 Org Unit", "Level 5 Org Unit", "Period"]
    rest = []
    if records:
        rest = sorted([k for k in records[0].keys() if k not in fixed])
    headers = fixed + rest

    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in records:
            w.writerow(r)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="202501", help="YYYYMM")
    ap.add_argument("--months", type=int, default=3, help="Refresh last N months (scheduled runs)")
    ap.add_argument("--backfill", action="store_true", help="Fetch ALL months from --start to current")
    ap.add_argument("--out", default="docs/data", help="Output folder (docs/ for GitHub Pages)")
    ap.add_argument("--dx_chunk_chars", type=int, default=6500)
    ap.add_argument("--sleep", type=float, default=0.2)
    args = ap.parse_args()

    base_url = os.environ.get("DHIS2_BASE_URL")
    username = os.environ.get("DHIS2_USERNAME")
    password = os.environ.get("DHIS2_PASSWORD")
    if not (base_url and username and password):
        print("Missing secrets: DHIS2_BASE_URL, DHIS2_USERNAME, DHIS2_PASSWORD", file=sys.stderr)
        return 2

    if not DX_LIST or DX_LIST == "PASTE_YOUR_DX_LIST_HERE":
        print("You must paste DX_LIST into the script.", file=sys.stderr)
        return 2

    dx_expected = [x.strip() for x in DX_LIST.split(";") if x.strip()]
    client = Dhis2Client(base_url=base_url, username=username, password=password)

    # Hiérarchie OU (L2–L5)
    ou_hier = build_ou_hierarchy(client)

    end = current_yyyymm()
    all_months = month_range(args.start, end)

    if args.backfill:
        periods = all_months  # 202501 -> mois courant
    else:
        # ✅ Les 3 derniers mois, incluant le mois courant
        periods = all_months[-max(1, args.months):]

    out_dir = Path(args.out)
    monthly_dir = out_dir / "monthly"

    combined: List[dict] = []
    combined_path = out_dir / "combined.json"

    # Sur les runs quotidiens, on recharge l'historique et on remplace seulement les mois refresh
    if combined_path.exists() and not args.backfill:
        try:
            combined = json.loads(combined_path.read_text(encoding="utf-8"))
        except Exception:
            combined = []

    refresh_prefixes = {f"{p[:4]}-{p[4:6]}" for p in periods}
    combined = [r for r in combined if str(r.get("Period", ""))[:7] not in refresh_prefixes]

    for pe in periods:
        records = fetch_period(
            client=client,
            pe=pe,
            dx_expected=dx_expected,
            ou_hier=ou_hier,
            rename_map=RENAME_MAP,
            dx_chunk_chars=args.dx_chunk_chars,
            sleep_s=args.sleep,
        )
        write_json(monthly_dir / f"{pe}.json", records)
        combined.extend(records)

    write_json(combined_path, combined)
    write_csv_gz(out_dir / "combined.csv.gz", combined)

    meta = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "periods_refreshed": periods,
        "records": len(combined),
    }
    write_json(out_dir / "combined.meta.json", meta)

    print(f"OK: {len(combined)} records; refreshed {periods}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
