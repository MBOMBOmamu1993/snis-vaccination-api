from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import dhis2_guard

# ============================================================
# 1) CONFIG: AJOUTE TES INDICATEURS ICI
# ============================================================

# (A) AJOUTE ICI TON DX_LIST COMPLET
# Exemple:
# DX_LIST = "dx1;dx2;dx3"
DX_LIST = """
          "W25tOXS0rxS.dqydGQFHahb;W25tOXS0rxS.NOHlOxLczjc;W25tOXS0rxS.vbx8t4WAbR8;"
          "W25tOXS0rxS.KPDzIsWq7JK;W25tOXS0rxS.oSYTyzezWif;W25tOXS0rxS.Dr4rWTqepnP;"
          "uNdFg1eymsa.dqydGQFHahb;uNdFg1eymsa.NOHlOxLczjc;uNdFg1eymsa.vbx8t4WAbR8;"
          "uNdFg1eymsa.KPDzIsWq7JK;uNdFg1eymsa.oSYTyzezWif;uNdFg1eymsa.Dr4rWTqepnP;"
          "YYKXOc7xBUi.dqydGQFHahb;YYKXOc7xBUi.NOHlOxLczjc;YYKXOc7xBUi.vbx8t4WAbR8;"
          "YYKXOc7xBUi.KPDzIsWq7JK;YYKXOc7xBUi.oSYTyzezWif;YYKXOc7xBUi.Dr4rWTqepnP;"
          "WWxqaHSeiwd.dqydGQFHahb;WWxqaHSeiwd.NOHlOxLczjc;WWxqaHSeiwd.vbx8t4WAbR8;"
          "WWxqaHSeiwd.KPDzIsWq7JK;WWxqaHSeiwd.oSYTyzezWif;WWxqaHSeiwd.Dr4rWTqepnP;"
          "SPGaDDw2JzG.dqydGQFHahb;SPGaDDw2JzG.NOHlOxLczjc;SPGaDDw2JzG.vbx8t4WAbR8;"
          "SPGaDDw2JzG.KPDzIsWq7JK;SPGaDDw2JzG.oSYTyzezWif;SPGaDDw2JzG.Dr4rWTqepnP;"
          "I6tLH3nk9tw.dqydGQFHahb;I6tLH3nk9tw.NOHlOxLczjc;I6tLH3nk9tw.vbx8t4WAbR8;"
          "I6tLH3nk9tw.KPDzIsWq7JK;I6tLH3nk9tw.oSYTyzezWif;I6tLH3nk9tw.Dr4rWTqepnP;"
          "wwde2QtlIRN.dqydGQFHahb;wwde2QtlIRN.NOHlOxLczjc;wwde2QtlIRN.vbx8t4WAbR8;"
          "wwde2QtlIRN.KPDzIsWq7JK;wwde2QtlIRN.oSYTyzezWif;wwde2QtlIRN.Dr4rWTqepnP;"
          "c4VvzI5zTep.dqydGQFHahb;c4VvzI5zTep.vbx8t4WAbR8;c4VvzI5zTep.oSYTyzezWif;"
          "J8R3WFpMAZI.dqydGQFHahb;J8R3WFpMAZI.NOHlOxLczjc;J8R3WFpMAZI.vbx8t4WAbR8;"
          "J8R3WFpMAZI.KPDzIsWq7JK;J8R3WFpMAZI.oSYTyzezWif;J8R3WFpMAZI.Dr4rWTqepnP;"
          "pak21wvkWJC.dqydGQFHahb;cTLKwfG8pSv.NOHlOxLczjc;pak21wvkWJC.vbx8t4WAbR8;"
          "cTLKwfG8pSv.KPDzIsWq7JK;pak21wvkWJC.oSYTyzezWif;cTLKwfG8pSv.Dr4rWTqepnP;"
          "i5zmivDIHN8.g6mIyKoGIh2;i5zmivDIHN8.QRyK6yxKBU3;i5zmivDIHN8.Rby9Jdri29F;"
          "i5zmivDIHN8.FCXzheCQXtr;i5zmivDIHN8.VrEj0UVVGr4;M2JQW0H44dI;"
          "s5EUr8GznWY;lHB57kjRtUz;h7bxqdKWYCa;"
          "gDnbubwzuts;WLSKVyA8LoY;IRuHNExvC4m;"
          "pVOrmIskonM;nGZz4qgNal7;YvO03GnK1oQ;"
          "E4BX1ea2iDJ.REPORTING_RATE;E4BX1ea2iDJ.REPORTING_RATE_ON_TIME;s5EUr8GznWY.HVViCmJi85w;"
          "lHB57kjRtUz.HVViCmJi85w;s5EUr8GznWY.TG2PxzbbIjT;lHB57kjRtUz.TG2PxzbbIjT;"
          "s5EUr8GznWY.aE55ruo9YeU;lHB57kjRtUz.aE55ruo9YeU;ppMFLxX6NvU.FGpuKKTof4X;"
          "ppMFLxX6NvU.RD9Iut5kjja;ppMFLxX6NvU.LnwvJBIlzxk;ppMFLxX6NvU.RMMpx7TBreP;"
          "ppMFLxX6NvU.ZYkCdGglDmr;ppMFLxX6NvU.aZmhNMRCrmZ;DZ2xt2mgVzQ.FGpuKKTof4X;"
          "DZ2xt2mgVzQ.RD9Iut5kjja;DZ2xt2mgVzQ.LnwvJBIlzxk;DZ2xt2mgVzQ.RMMpx7TBreP;"
          "DZ2xt2mgVzQ.ZYkCdGglDmr;DZ2xt2mgVzQ.aZmhNMRCrmZ;SnhwwhcaDRS;fycXkvxPUjH;"
          "Nyke8sWJbn9.oSYTyzezWif;Nyke8sWJbn9.vbx8t4WAbR8;slj1Yy0YgAc.dqydGQFHahb;"
          "slj1Yy0YgAc.vbx8t4WAbR8;slj1Yy0YgAc.oSYTyzezWif;OnKbxNJzsCw.dqydGQFHahb;"
          "OnKbxNJzsCw.vbx8t4WAbR8;OnKbxNJzsCw.oSYTyzezWif;KVjtJyNKcYQ.g6mIyKoGIh2;"
          "KVjtJyNKcYQ.Rby9Jdri29F;KVjtJyNKcYQ.FCXzheCQXtr;WzOgzB6fLCW;"
          "E4BX1ea2iDJ;i5zmivDIHN8.dqydGQFHahb;i5zmivDIHN8.vbx8t4WAbR8;"
          "i5zmivDIHN8.oSYTyzezWif;SnhwwhcaDRS.M7I3dsr9L5Z;fycXkvxPUjH.M7I3dsr9L5Z;"
          "CKhvpizB7wo.M7I3dsr9L5Z;OkgUUlDsGAN.M7I3dsr9L5Z;SnhwwhcaDRS.eX294IHIRVM;"
          "fycXkvxPUjH.eX294IHIRVM;CKhvpizB7wo.eX294IHIRVM;OkgUUlDsGAN.eX294IHIRVM;"
          "SnhwwhcaDRS.QeTSJLfbwEw;fycXkvxPUjH.QeTSJLfbwEw;CKhvpizB7wo.QeTSJLfbwEw;"
          "OkgUUlDsGAN.QeTSJLfbwEw;SnhwwhcaDRS.rJgTQZ63hnO;fycXkvxPUjH.rJgTQZ63hnO;"
          "CKhvpizB7wo.rJgTQZ63hnO;OkgUUlDsGAN.rJgTQZ63hnO;SnhwwhcaDRS.YWikHnhDAXE;"
          "fycXkvxPUjH.YWikHnhDAXE;CKhvpizB7wo.YWikHnhDAXE;OkgUUlDsGAN.YWikHnhDAXE;"
          "SnhwwhcaDRS.lvDrwyxXMq2;fycXkvxPUjH.lvDrwyxXMq2;CKhvpizB7wo.lvDrwyxXMq2;"
          "OkgUUlDsGAN.lvDrwyxXMq2;SnhwwhcaDRS.QOXbI3fshPV;fycXkvxPUjH.QOXbI3fshPV;"
          "CKhvpizB7wo.QOXbI3fshPV;OkgUUlDsGAN.QOXbI3fshPV;SnhwwhcaDRS.IdjExsz3izf;"
          "fycXkvxPUjH.IdjExsz3izf;CKhvpizB7wo.IdjExsz3izf;OkgUUlDsGAN.IdjExsz3izf;"
          "SnhwwhcaDRS.OdTBbi5jfhb;fycXkvxPUjH.OdTBbi5jfhb;CKhvpizB7wo.OdTBbi5jfhb;"
          "OkgUUlDsGAN.OdTBbi5jfhb;SnhwwhcaDRS.WbtfH2Deu6j;fycXkvxPUjH.WbtfH2Deu6j;"
          "CKhvpizB7wo.WbtfH2Deu6j;OkgUUlDsGAN.WbtfH2Deu6j;SnhwwhcaDRS.hiiCMvb0tTp;"
          "fycXkvxPUjH.hiiCMvb0tTp;CKhvpizB7wo.hiiCMvb0tTp;OkgUUlDsGAN.hiiCMvb0tTp;"
          "CKhvpizB7wo;f1MiAAIu366.dqydGQFHahb;f1MiAAIu366.NOHlOxLczjc;"
          "f1MiAAIu366.vbx8t4WAbR8;f1MiAAIu366.KPDzIsWq7JK;f1MiAAIu366.oSYTyzezWif;"
          "f1MiAAIu366.Dr4rWTqepnP;j3P9bSnwF12.dqydGQFHahb;j3P9bSnwF12.NOHlOxLczjc;"
          "j3P9bSnwF12.vbx8t4WAbR8;j3P9bSnwF12.KPDzIsWq7JK;j3P9bSnwF12.oSYTyzezWif;"
          "j3P9bSnwF12.Dr4rWTqepnP;ozVdkud4F03.dqydGQFHahb;ozVdkud4F03.NOHlOxLczjc;"
          "ozVdkud4F03.vbx8t4WAbR8;ozVdkud4F03.KPDzIsWq7JK;ozVdkud4F03.oSYTyzezWif;"
          "ozVdkud4F03.Dr4rWTqepnP;CgR8kWNdK3W.dqydGQFHahb;CgR8kWNdK3W.NOHlOxLczjc;"
          "CgR8kWNdK3W.vbx8t4WAbR8;CgR8kWNdK3W.KPDzIsWq7JK;CgR8kWNdK3W.oSYTyzezWif;"
          "CgR8kWNdK3W.Dr4rWTqepnP;WGu60o5mePq.dqydGQFHahb;WGu60o5mePq.NOHlOxLczjc;"
          "WGu60o5mePq.vbx8t4WAbR8;WGu60o5mePq.KPDzIsWq7JK;WGu60o5mePq.oSYTyzezWif;"
          "WGu60o5mePq.Dr4rWTqepnP;TLKusYaKY5A.dqydGQFHahb;TLKusYaKY5A.vbx8t4WAbR8;"
          "TLKusYaKY5A.oSYTyzezWif;GzSBTZkxSZf.dqydGQFHahb;GzSBTZkxSZf.vbx8t4WAbR8;"
          "GzSBTZkxSZf.oSYTyzezWif;"
          "Nyke8sWJbn9.dqydGQFHahb;"
          "bzD4QxaNkJm.dqydGQFHahb;bzD4QxaNkJm.NOHlOxLczjc;bzD4QxaNkJm.vbx8t4WAbR8;"
          "bzD4QxaNkJm.KPDzIsWq7JK;bzD4QxaNkJm.oSYTyzezWif;bzD4QxaNkJm.Dr4rWTqepnP;"
          "RGW6llyyusM;hGBtbI7kjvb;bZDbSvdUcPC;lXstTq1MDSv;"
          "uAWIVDnGPGH.dxDQKDcTn6Z;uAWIVDnGPGH.XEN3ucCGa07;uAWIVDnGPGH.J8mw9rFkY4v;"
          "uAWIVDnGPGH.WZwmzIuRvwV;uAWIVDnGPGH.GSCwhT2SCCr;uAWIVDnGPGH.pdKyvaYRqCj;"
          "uAWIVDnGPGH.kOWsLrtvrhn;uAWIVDnGPGH.mZLRF4eSPIk;"
          "l1bhrYUPsde.dxDQKDcTn6Z;l1bhrYUPsde.XEN3ucCGa07;l1bhrYUPsde.J8mw9rFkY4v;"
          "l1bhrYUPsde.WZwmzIuRvwV;l1bhrYUPsde.GSCwhT2SCCr;l1bhrYUPsde.pdKyvaYRqCj;"
          "l1bhrYUPsde.kOWsLrtvrhn;l1bhrYUPsde.mZLRF4eSPIk;"
          "Hjlw6DpooIo.dxDQKDcTn6Z;Hjlw6DpooIo.XEN3ucCGa07;Hjlw6DpooIo.J8mw9rFkY4v;"
          "Hjlw6DpooIo.WZwmzIuRvwV;Hjlw6DpooIo.GSCwhT2SCCr;Hjlw6DpooIo.pdKyvaYRqCj;"
          "Hjlw6DpooIo.kOWsLrtvrhn;Hjlw6DpooIo.mZLRF4eSPIk;"
          "BCXcLiQNI8M.dxDQKDcTn6Z;BCXcLiQNI8M.XEN3ucCGa07;BCXcLiQNI8M.J8mw9rFkY4v;"
          "BCXcLiQNI8M.WZwmzIuRvwV;BCXcLiQNI8M.GSCwhT2SCCr;BCXcLiQNI8M.pdKyvaYRqCj;"
          "BCXcLiQNI8M.kOWsLrtvrhn;BCXcLiQNI8M.mZLRF4eSPIk;"
          "IisI6JudbS8.dxDQKDcTn6Z;IisI6JudbS8.XEN3ucCGa07;IisI6JudbS8.J8mw9rFkY4v;"
          "IisI6JudbS8.WZwmzIuRvwV;IisI6JudbS8.GSCwhT2SCCr;IisI6JudbS8.pdKyvaYRqCj;"
          "IisI6JudbS8.mZLRF4eSPIk;IisI6JudbS8.kOWsLrtvrhn;"
          "vrTdwvAQSUc.dxDQKDcTn6Z;vrTdwvAQSUc.XEN3ucCGa07;vrTdwvAQSUc.J8mw9rFkY4v;"
          "vrTdwvAQSUc.WZwmzIuRvwV;vrTdwvAQSUc.GSCwhT2SCCr;vrTdwvAQSUc.pdKyvaYRqCj;"
          "vrTdwvAQSUc.kOWsLrtvrhn;vrTdwvAQSUc.mZLRF4eSPIk;"
          "iFMn6ceqdIN.dxDQKDcTn6Z;iFMn6ceqdIN.XEN3ucCGa07;iFMn6ceqdIN.J8mw9rFkY4v;"
          "iFMn6ceqdIN.WZwmzIuRvwV;iFMn6ceqdIN.GSCwhT2SCCr;iFMn6ceqdIN.pdKyvaYRqCj;"
          "iFMn6ceqdIN.kOWsLrtvrhn;iFMn6ceqdIN.mZLRF4eSPIk;"
          "hqqlXMmY95c.dxDQKDcTn6Z;hqqlXMmY95c.XEN3ucCGa07;hqqlXMmY95c.J8mw9rFkY4v;"
          "hqqlXMmY95c.WZwmzIuRvwV;hqqlXMmY95c.GSCwhT2SCCr;hqqlXMmY95c.pdKyvaYRqCj;hqqlXMmY95c.kOWsLrtvrhn;"
          "MJrLCya7qzt.dxDQKDcTn6Z;MJrLCya7qzt.XEN3ucCGa07;MJrLCya7qzt.J8mw9rFkY4v;"
          "MJrLCya7qzt.WZwmzIuRvwV;MJrLCya7qzt.GSCwhT2SCCr;MJrLCya7qzt.pdKyvaYRqCj;MJrLCya7qzt.kOWsLrtvrhn;"
          "RYaAzE1eqya.dxDQKDcTn6Z;RYaAzE1eqya.XEN3ucCGa07;RYaAzE1eqya.J8mw9rFkY4v;"
          "RYaAzE1eqya.WZwmzIuRvwV;RYaAzE1eqya.GSCwhT2SCCr;RYaAzE1eqya.pdKyvaYRqCj;RYaAzE1eqya.kOWsLrtvrhn;"
          "BfoCv6bUeBk.dxDQKDcTn6Z;BfoCv6bUeBk.XEN3ucCGa07;BfoCv6bUeBk.J8mw9rFkY4v;"
          "BfoCv6bUeBk.WZwmzIuRvwV;BfoCv6bUeBk.GSCwhT2SCCr;BfoCv6bUeBk.pdKyvaYRqCj;BfoCv6bUeBk.kOWsLrtvrhn;"
          "laQa8YfpVrp.dxDQKDcTn6Z;laQa8YfpVrp.XEN3ucCGa07;laQa8YfpVrp.J8mw9rFkY4v;"
          "laQa8YfpVrp.WZwmzIuRvwV;laQa8YfpVrp.GSCwhT2SCCr;laQa8YfpVrp.pdKyvaYRqCj;"
          "nnSKbBscmxH.dxDQKDcTn6Z;nnSKbBscmxH.XEN3ucCGa07;nnSKbBscmxH.J8mw9rFkY4v;"
          "nnSKbBscmxH.WZwmzIuRvwV;nnSKbBscmxH.GSCwhT2SCCr;nnSKbBscmxH.pdKyvaYRqCj;"
          "fQfJhi742vt.dxDQKDcTn6Z;fQfJhi742vt.XEN3ucCGa07;fQfJhi742vt.J8mw9rFkY4v;"
          "fQfJhi742vt.WZwmzIuRvwV;fQfJhi742vt.GSCwhT2SCCr;fQfJhi742vt.pdKyvaYRqCj;"
          "bwUoJJgDst9.dxDQKDcTn6Z;bwUoJJgDst9.XEN3ucCGa07;bwUoJJgDst9.J8mw9rFkY4v;"
          "bwUoJJgDst9.WZwmzIuRvwV;bwUoJJgDst9.GSCwhT2SCCr;bwUoJJgDst9.pdKyvaYRqCj;"
          "blfdHcia9nP.dxDQKDcTn6Z;blfdHcia9nP.XEN3ucCGa07;blfdHcia9nP.J8mw9rFkY4v;"
          "blfdHcia9nP.WZwmzIuRvwV;blfdHcia9nP.GSCwhT2SCCr;blfdHcia9nP.pdKyvaYRqCj;"
          "bqJvWJtJwtK.dxDQKDcTn6Z;bqJvWJtJwtK.XEN3ucCGa07;bqJvWJtJwtK.J8mw9rFkY4v;"
          "bqJvWJtJwtK.WZwmzIuRvwV;bqJvWJtJwtK.GSCwhT2SCCr;bqJvWJtJwtK.pdKyvaYRqCj;"
          "DNZEo533IMh.dxDQKDcTn6Z;DNZEo533IMh.XEN3ucCGa07;DNZEo533IMh.J8mw9rFkY4v;"
          "DNZEo533IMh.WZwmzIuRvwV;DNZEo533IMh.GSCwhT2SCCr;DNZEo533IMh.pdKyvaYRqCj;"
          "ZMcyV45XAkW.dxDQKDcTn6Z;ZMcyV45XAkW.XEN3ucCGa07;ZMcyV45XAkW.J8mw9rFkY4v;"
          "ZMcyV45XAkW.WZwmzIuRvwV;ZMcyV45XAkW.GSCwhT2SCCr;ZMcyV45XAkW.pdKyvaYRqCj;"
          "gzPW3f2ijo5.dxDQKDcTn6Z;gzPW3f2ijo5.XEN3ucCGa07;gzPW3f2ijo5.J8mw9rFkY4v;"
          "gzPW3f2ijo5.WZwmzIuRvwV;gzPW3f2ijo5.GSCwhT2SCCr;gzPW3f2ijo5.pdKyvaYRqCj;"
          "tmp1zLSXw0Q.dxDQKDcTn6Z;tmp1zLSXw0Q.XEN3ucCGa07;tmp1zLSXw0Q.J8mw9rFkY4v;"
          "tmp1zLSXw0Q.WZwmzIuRvwV;tmp1zLSXw0Q.GSCwhT2SCCr;tmp1zLSXw0Q.pdKyvaYRqCj;"
          "jJUIodWQH0T.dxDQKDcTn6Z;jJUIodWQH0T.XEN3ucCGa07;jJUIodWQH0T.J8mw9rFkY4v;"
          "jJUIodWQH0T.WZwmzIuRvwV;jJUIodWQH0T.GSCwhT2SCCr;jJUIodWQH0T.pdKyvaYRqCj;"
          "K8Yt0p1z21M.dxDQKDcTn6Z;K8Yt0p1z21M.XEN3ucCGa07;K8Yt0p1z21M.J8mw9rFkY4v;"
          "K8Yt0p1z21M.WZwmzIuRvwV;K8Yt0p1z21M.GSCwhT2SCCr;K8Yt0p1z21M.pdKyvaYRqCj;"
          "uAWIVDnGPGH.Lpu56KTyGvy;uAWIVDnGPGH.ddJmZUacsvQ;uAWIVDnGPGH.t5L9ODSuYOG;kd0elH7N75k.pGSfKOKL9s0;kpvY4GzEsOv.jpmZl19SSta;"
          "l1bhrYUPsde.Lpu56KTyGvy;l1bhrYUPsde.ddJmZUacsvQ;l1bhrYUPsde.t5L9ODSuYOG;VcI6UxYSqb7.pGSfKOKL9s0;bJD8kwm6xz8.jpmZl19SSta;"
          "Hjlw6DpooIo.Lpu56KTyGvy;Hjlw6DpooIo.ddJmZUacsvQ;Hjlw6DpooIo.t5L9ODSuYOG;DYoKTCw2Oqi.pGSfKOKL9s0;fJvcsDrhScZ.jpmZl19SSta;"
          "BCXcLiQNI8M.Lpu56KTyGvy;BCXcLiQNI8M.ddJmZUacsvQ;BCXcLiQNI8M.t5L9ODSuYOG;KCfG0FCgGWU.pGSfKOKL9s0;iEbT3SCQStG.jpmZl19SSta;"
          "IisI6JudbS8.Lpu56KTyGvy;IisI6JudbS8.ddJmZUacsvQ;IisI6JudbS8.t5L9ODSuYOG;EhguMRWyJjV.pGSfKOKL9s0;UgMNX4AWqj1.jpmZl19SSta;"
          "vrTdwvAQSUc.Lpu56KTyGvy;vrTdwvAQSUc.ddJmZUacsvQ;vrTdwvAQSUc.t5L9ODSuYOG;pho7jci1XYc.pGSfKOKL9s0;AVhKGbmEA9G.jpmZl19SSta;"
          "iFMn6ceqdIN.Lpu56KTyGvy;iFMn6ceqdIN.ddJmZUacsvQ;iFMn6ceqdIN.t5L9ODSuYOG;BhgOrHZVgdT.pGSfKOKL9s0;aRkYMm4tHvX.jpmZl19SSta;"
          "hqqlXMmY95c.Lpu56KTyGvy;hqqlXMmY95c.ddJmZUacsvQ;hqqlXMmY95c.t5L9ODSuYOG;llBBZ1VxhaY.pGSfKOKL9s0;CDc5q7ornpu.jpmZl19SSta;"
          "MJrLCya7qzt.Lpu56KTyGvy;MJrLCya7qzt.ddJmZUacsvQ;MJrLCya7qzt.t5L9ODSuYOG;qr4nc6wQ0nK.pGSfKOKL9s0;JrGE0PSspn3.jpmZl19SSta;"
          "RYaAzE1eqya.Lpu56KTyGvy;RYaAzE1eqya.ddJmZUacsvQ;RYaAzE1eqya.t5L9ODSuYOG;ADvotRbZQxB.pGSfKOKL9s0;kYGW0DcBwWe.jpmZl19SSta;"
          "BfoCv6bUeBk.Lpu56KTyGvy;BfoCv6bUeBk.ddJmZUacsvQ;BfoCv6bUeBk.t5L9ODSuYOG;VsoGWlC8T9Q.pGSfKOKL9s0;JdrnZD4dPKl.jpmZl19SSta;"
          "laQa8YfpVrp.kOWsLrtvrhn;"
          "laQa8YfpVrp.Lpu56KTyGvy;laQa8YfpVrp.ddJmZUacsvQ;laQa8YfpVrp.t5L9ODSuYOG;ln32rKUhVog.pGSfKOKL9s0;x2wBx3s2SLQ.jpmZl19SSta;"
          "nnSKbBscmxH.kOWsLrtvrhn;"
          "nnSKbBscmxH.Lpu56KTyGvy;nnSKbBscmxH.ddJmZUacsvQ;nnSKbBscmxH.t5L9ODSuYOG;XQLOXoInd3R.pGSfKOKL9s0;UX65tnTXUBr.jpmZl19SSta;"
          "fQfJhi742vt.kOWsLrtvrhn;"
          "fQfJhi742vt.Lpu56KTyGvy;fQfJhi742vt.ddJmZUacsvQ;fQfJhi742vt.t5L9ODSuYOG;GqgVvEI7JaC.pGSfKOKL9s0;DZZLXF2N7Kw.jpmZl19SSta;"
          "bwUoJJgDst9.kOWsLrtvrhn;"
          "bwUoJJgDst9.Lpu56KTyGvy;bwUoJJgDst9.ddJmZUacsvQ;bwUoJJgDst9.t5L9ODSuYOG;esgalSEQ9hA.pGSfKOKL9s0;SJ1EAubhzIR.jpmZl19SSta;"
          "blfdHcia9nP.kOWsLrtvrhn;"
          "blfdHcia9nP.Lpu56KTyGvy;blfdHcia9nP.ddJmZUacsvQ;blfdHcia9nP.t5L9ODSuYOG;e3k4D2zCCgo.pGSfKOKL9s0;Qstq45EG8DV.jpmZl19SSta;"
          "bqJvWJtJwtK.kOWsLrtvrhn;"
          "bqJvWJtJwtK.Lpu56KTyGvy;bqJvWJtJwtK.ddJmZUacsvQ;bqJvWJtJwtK.t5L9ODSuYOG;JRwL4KKN7Nk.pGSfKOKL9s0;AdtKSkta4bh.jpmZl19SSta;"
          "DNZEo533IMh.kOWsLrtvrhn;"
          "DNZEo533IMh.Lpu56KTyGvy;DNZEo533IMh.ddJmZUacsvQ;DNZEo533IMh.t5L9ODSuYOG;HrYG9MrHUdg.pGSfKOKL9s0;cmjf7VquiXH.jpmZl19SSta;"
          "ZMcyV45XAkW.kOWsLrtvrhn;"
          "ZMcyV45XAkW.Lpu56KTyGvy;ZMcyV45XAkW.ddJmZUacsvQ;ZMcyV45XAkW.t5L9ODSuYOG;NesfIHNd9xd.pGSfKOKL9s0;rJgmuGc4ynC.jpmZl19SSta;"
          "gzPW3f2ijo5.kOWsLrtvrhn;"
          "gzPW3f2ijo5.Lpu56KTyGvy;gzPW3f2ijo5.ddJmZUacsvQ;gzPW3f2ijo5.t5L9ODSuYOG;gN8mcPpAiiI.pGSfKOKL9s0;pZdQli2zn4y.jpmZl19SSta;"
          "tmp1zLSXw0Q.kOWsLrtvrhn;"
          "tmp1zLSXw0Q.Lpu56KTyGvy;tmp1zLSXw0Q.ddJmZUacsvQ;tmp1zLSXw0Q.t5L9ODSuYOG;OoDet9UWItm.pGSfKOKL9s0;cOhDoR0ddSp.jpmZl19SSta;"
          "jJUIodWQH0T.kOWsLrtvrhn;"
          "jJUIodWQH0T.Lpu56KTyGvy;jJUIodWQH0T.ddJmZUacsvQ;jJUIodWQH0T.t5L9ODSuYOG;MAyhe30b20R.pGSfKOKL9s0;WYiF7sbZ5cV.jpmZl19SSta;"
          "K8Yt0p1z21M.kOWsLrtvrhn;"
          "K8Yt0p1z21M.Lpu56KTyGvy;K8Yt0p1z21M.ddJmZUacsvQ;K8Yt0p1z21M.t5L9ODSuYOG;hllO2kXSuwM.pGSfKOKL9s0;xOlmaXSPviM.jpmZl19SSta;"
          "W25tOXS0rxS.g6mIyKoGIh2;W25tOXS0rxS.QRyK6yxKBU3;W25tOXS0rxS.Rby9Jdri29F;"
          "W25tOXS0rxS.FCXzheCQXtr;W25tOXS0rxS.VrEj0UVVGr4;"
          "uNdFg1eymsa.g6mIyKoGIh2;uNdFg1eymsa.QRyK6yxKBU3;uNdFg1eymsa.Rby9Jdri29F;"
          "uNdFg1eymsa.FCXzheCQXtr;uNdFg1eymsa.VrEj0UVVGr4;"
          "YYKXOc7xBUi.g6mIyKoGIh2;YYKXOc7xBUi.QRyK6yxKBU3;YYKXOc7xBUi.Rby9Jdri29F;"
          "YYKXOc7xBUi.FCXzheCQXtr;YYKXOc7xBUi.VrEj0UVVGr4;"
          "WWxqaHSeiwd.g6mIyKoGIh2;WWxqaHSeiwd.QRyK6yxKBU3;WWxqaHSeiwd.Rby9Jdri29F;"
          "WWxqaHSeiwd.FCXzheCQXtr;WWxqaHSeiwd.VrEj0UVVGr4;"
          "f1MiAAIu366.g6mIyKoGIh2;f1MiAAIu366.QRyK6yxKBU3;f1MiAAIu366.Rby9Jdri29F;"
          "f1MiAAIu366.FCXzheCQXtr;f1MiAAIu366.VrEj0UVVGr4;"
          "j3P9bSnwF12.g6mIyKoGIh2;j3P9bSnwF12.QRyK6yxKBU3;j3P9bSnwF12.Rby9Jdri29F;"
          "j3P9bSnwF12.FCXzheCQXtr;j3P9bSnwF12.VrEj0UVVGr4;"
          "ozVdkud4F03.g6mIyKoGIh2;ozVdkud4F03.QRyK6yxKBU3;ozVdkud4F03.Rby9Jdri29F;"
          "ozVdkud4F03.FCXzheCQXtr;ozVdkud4F03.VrEj0UVVGr4;"
          "SPGaDDw2JzG.g6mIyKoGIh2;SPGaDDw2JzG.QRyK6yxKBU3;SPGaDDw2JzG.Rby9Jdri29F;"
          "SPGaDDw2JzG.FCXzheCQXtr;SPGaDDw2JzG.VrEj0UVVGr4;"
          "I6tLH3nk9tw.g6mIyKoGIh2;I6tLH3nk9tw.QRyK6yxKBU3;I6tLH3nk9tw.Rby9Jdri29F;"
          "I6tLH3nk9tw.FCXzheCQXtr;I6tLH3nk9tw.VrEj0UVVGr4;"
          "wwde2QtlIRN.g6mIyKoGIh2;wwde2QtlIRN.QRyK6yxKBU3;wwde2QtlIRN.Rby9Jdri29F;"
          "wwde2QtlIRN.FCXzheCQXtr;wwde2QtlIRN.VrEj0UVVGr4;"
          "CgR8kWNdK3W.g6mIyKoGIh2;CgR8kWNdK3W.QRyK6yxKBU3;CgR8kWNdK3W.Rby9Jdri29F;"
          "CgR8kWNdK3W.FCXzheCQXtr;CgR8kWNdK3W.VrEj0UVVGr4;"
          "WGu60o5mePq.g6mIyKoGIh2;WGu60o5mePq.QRyK6yxKBU3;WGu60o5mePq.Rby9Jdri29F;"
          "WGu60o5mePq.FCXzheCQXtr;WGu60o5mePq.VrEj0UVVGr4;"
          "J8R3WFpMAZI.g6mIyKoGIh2;J8R3WFpMAZI.QRyK6yxKBU3;J8R3WFpMAZI.Rby9Jdri29F;"
          "J8R3WFpMAZI.FCXzheCQXtr;J8R3WFpMAZI.VrEj0UVVGr4;"
          "TLKusYaKY5A.g6mIyKoGIh2;TLKusYaKY5A.Rby9Jdri29F;TLKusYaKY5A.FCXzheCQXtr;"
          "GzSBTZkxSZf.g6mIyKoGIh2;GzSBTZkxSZf.Rby9Jdri29F;GzSBTZkxSZf.FCXzheCQXtr;"
          "c4VvzI5zTep.g6mIyKoGIh2;c4VvzI5zTep.Rby9Jdri29F;c4VvzI5zTep.FCXzheCQXtr;"
          "pak21wvkWJC.g6mIyKoGIh2;cTLKwfG8pSv.QRyK6yxKBU3;pak21wvkWJC.Rby9Jdri29F;"
          "cTLKwfG8pSv.Rby9Jdri29F;pak21wvkWJC.FCXzheCQXtr;cTLKwfG8pSv.VrEj0UVVGr4;"
          "bzD4QxaNkJm.g6mIyKoGIh2;bzD4QxaNkJm.QRyK6yxKBU3;bzD4QxaNkJm.Rby9Jdri29F;"
          "bzD4QxaNkJm.FCXzheCQXtr;bzD4QxaNkJm.VrEj0UVVGr4;"
          "M2JQW0H44dI.QRyK6yxKBU3;M2JQW0H44dI.VrEj0UVVGr4;"
          "W25tOXS0rxS.tWyeOXJgU3A;W25tOXS0rxS.cI92X1TYlnD;W25tOXS0rxS.BisCHS3wArx;"
          "uNdFg1eymsa.tWyeOXJgU3A;uNdFg1eymsa.cI92X1TYlnD;uNdFg1eymsa.BisCHS3wArx;"
          "YYKXOc7xBUi.tWyeOXJgU3A;YYKXOc7xBUi.cI92X1TYlnD;YYKXOc7xBUi.BisCHS3wArx;"
          "WWxqaHSeiwd.tWyeOXJgU3A;WWxqaHSeiwd.cI92X1TYlnD;WWxqaHSeiwd.BisCHS3wArx;"
          "f1MiAAIu366.tWyeOXJgU3A;f1MiAAIu366.cI92X1TYlnD;f1MiAAIu366.BisCHS3wArx;"
          "j3P9bSnwF12.tWyeOXJgU3A;j3P9bSnwF12.cI92X1TYlnD;j3P9bSnwF12.BisCHS3wArx;"
          "ozVdkud4F03.tWyeOXJgU3A;ozVdkud4F03.cI92X1TYlnD;ozVdkud4F03.BisCHS3wArx;"
          "SPGaDDw2JzG.tWyeOXJgU3A;SPGaDDw2JzG.cI92X1TYlnD;SPGaDDw2JzG.BisCHS3wArx;"
          "I6tLH3nk9tw.tWyeOXJgU3A;I6tLH3nk9tw.cI92X1TYlnD;I6tLH3nk9tw.BisCHS3wArx;"
          "wwde2QtlIRN.tWyeOXJgU3A;wwde2QtlIRN.cI92X1TYlnD;wwde2QtlIRN.BisCHS3wArx;"
          "CgR8kWNdK3W.tWyeOXJgU3A;CgR8kWNdK3W.cI92X1TYlnD;CgR8kWNdK3W.BisCHS3wArx;"
          "WGu60o5mePq.tWyeOXJgU3A;WGu60o5mePq.cI92X1TYlnD;WGu60o5mePq.BisCHS3wArx;"
          "J8R3WFpMAZI.tWyeOXJgU3A;J8R3WFpMAZI.cI92X1TYlnD;J8R3WFpMAZI.BisCHS3wArx;"
          "TLKusYaKY5A.tWyeOXJgU3A;TLKusYaKY5A.cI92X1TYlnD;TLKusYaKY5A.BisCHS3wArx;"
          "GzSBTZkxSZf.tWyeOXJgU3A;GzSBTZkxSZf.cI92X1TYlnD;GzSBTZkxSZf.BisCHS3wArx;"
          "c4VvzI5zTep.tWyeOXJgU3A;c4VvzI5zTep.cI92X1TYlnD;c4VvzI5zTep.BisCHS3wArx;"
          "pak21wvkWJC.tWyeOXJgU3A;cTLKwfG8pSv.tWyeOXJgU3A;pak21wvkWJC.cI92X1TYlnD;"
          "cTLKwfG8pSv.cI92X1TYlnD;pak21wvkWJC.BisCHS3wArx;cTLKwfG8pSv.BisCHS3wArx;"
          "i5zmivDIHN8.tWyeOXJgU3A;i5zmivDIHN8.cI92X1TYlnD;i5zmivDIHN8.BisCHS3wArx;"
          "bzD4QxaNkJm.tWyeOXJgU3A;bzD4QxaNkJm.cI92X1TYlnD;bzD4QxaNkJm.BisCHS3wArx;"
          "tjcH6RS9mXd"
""".strip().replace("\n", "").replace(" ", "").replace('"', "")

# (B) AJOUTE ICI TON RENAME_MAP COMPLET
# IMPORTANT: dx -> "Label DHIS2" (exactement les labels utilisés dans rename_map.json)
RENAME_MAP: Dict[str, str] = {
    "W25tOXS0rxS.dqydGQFHahb": "BCG fixe1",
    "W25tOXS0rxS.NOHlOxLczjc": "BCG fixe2",
    "W25tOXS0rxS.vbx8t4WAbR8": "BCG avancé1",
    "W25tOXS0rxS.KPDzIsWq7JK": "BCG avancé2",
    "W25tOXS0rxS.oSYTyzezWif": "BCG mobile1",
    "W25tOXS0rxS.Dr4rWTqepnP": "BCG mobile2",
    
    "uNdFg1eymsa.dqydGQFHahb": "Penta1 fixe1",
    "uNdFg1eymsa.NOHlOxLczjc": "Penta1 fixe2",
    "uNdFg1eymsa.vbx8t4WAbR8": "Penta1 avancé1",
    "uNdFg1eymsa.KPDzIsWq7JK": "Penta1 avancé2",
    "uNdFg1eymsa.oSYTyzezWif": "Penta1 mobile1",
    "uNdFg1eymsa.Dr4rWTqepnP": "Penta1 mobile2",

    "YYKXOc7xBUi.dqydGQFHahb": "Penta2 fixe1",
    "YYKXOc7xBUi.NOHlOxLczjc": "Penta2 fixe2",
    "YYKXOc7xBUi.vbx8t4WAbR8": "Penta2 avancé1",
    "YYKXOc7xBUi.KPDzIsWq7JK": "Penta2 avancé2",
    "YYKXOc7xBUi.oSYTyzezWif": "Penta2 mobile1",
    "YYKXOc7xBUi.Dr4rWTqepnP": "Penta2 mobile2",

    "WWxqaHSeiwd.dqydGQFHahb": "Penta3 fixe1",
    "WWxqaHSeiwd.NOHlOxLczjc": "Penta3 fixe2",
    "WWxqaHSeiwd.vbx8t4WAbR8": "Penta3 avancé1",
    "WWxqaHSeiwd.KPDzIsWq7JK": "Penta3 avancé2",
    "WWxqaHSeiwd.oSYTyzezWif": "Penta3 mobile1",
    "WWxqaHSeiwd.Dr4rWTqepnP": "Penta3 mobile2",

    "SPGaDDw2JzG.dqydGQFHahb": "VPO3 fixe1",
    "SPGaDDw2JzG.NOHlOxLczjc": "VPO3 fixe2",
    "SPGaDDw2JzG.vbx8t4WAbR8": "VPO3 avancé1",
    "SPGaDDw2JzG.KPDzIsWq7JK": "VPO3 avancé2",
    "SPGaDDw2JzG.oSYTyzezWif": "VPO3 mobile1",
    "SPGaDDw2JzG.Dr4rWTqepnP": "VPO3 mobile2",

    "I6tLH3nk9tw.dqydGQFHahb": "VPI1 fixe1",
    "I6tLH3nk9tw.NOHlOxLczjc": "VPI1 fixe2",
    "I6tLH3nk9tw.vbx8t4WAbR8": "VPI1 avancé1",
    "I6tLH3nk9tw.KPDzIsWq7JK": "VPI1 avancé2",
    "I6tLH3nk9tw.oSYTyzezWif": "VPI1 mobile1",
    "I6tLH3nk9tw.Dr4rWTqepnP": "VPI1 mobile2",

    "wwde2QtlIRN.dqydGQFHahb": "VPI2 fixe1",
    "wwde2QtlIRN.NOHlOxLczjc": "VPI2 fixe2",
    "wwde2QtlIRN.vbx8t4WAbR8": "VPI2 avancé1",
    "wwde2QtlIRN.KPDzIsWq7JK": "VPI2 avancé2",
    "wwde2QtlIRN.oSYTyzezWif": "VPI2 mobile1",
    "wwde2QtlIRN.Dr4rWTqepnP": "VPI2 mobile2",

    "c4VvzI5zTep.dqydGQFHahb": "ROTA3 fixe",
    "c4VvzI5zTep.vbx8t4WAbR8": "ROTA3 avancé",
    "c4VvzI5zTep.oSYTyzezWif": "ROTA3 mobile",

    "J8R3WFpMAZI.dqydGQFHahb": "PCV13 fixe1",
    "J8R3WFpMAZI.NOHlOxLczjc": "PCV13 fixe2",
    "J8R3WFpMAZI.vbx8t4WAbR8": "PCV13 avancé1",
    "J8R3WFpMAZI.KPDzIsWq7JK": "PCV13 avancé2",
    "J8R3WFpMAZI.oSYTyzezWif": "PCV13 mobile1",
    "J8R3WFpMAZI.Dr4rWTqepnP": "PCV13 mobile2",

    "pak21wvkWJC.dqydGQFHahb": "VAR1 fixe1",
    "cTLKwfG8pSv.NOHlOxLczjc": "VAR1 fixe2",
    "pak21wvkWJC.vbx8t4WAbR8": "VAR1 avancé1",
    "cTLKwfG8pSv.KPDzIsWq7JK": "VAR1 avancé2",
    "pak21wvkWJC.oSYTyzezWif": "VAR1 mobile1",
    "cTLKwfG8pSv.Dr4rWTqepnP": "VAR1 mobile2",

    "M2JQW0H44dI": "ECV",
    "s5EUr8GznWY": "Séances prévues",
    "lHB57kjRtUz": "Séances réalisées",
    "h7bxqdKWYCa": "Pop. totale",
    "gDnbubwzuts": "Naissances vivantes",
    "WLSKVyA8LoY": "Pop. par AS",
    "IRuHNExvC4m": "Pop. 0-11m (nv)",
    "pVOrmIskonM": "Pop. 0-11m (survivants)",
    "nGZz4qgNal7": "Pop. 0-59m",
    "YvO03GnK1oQ": "Pop. 12-59m",

    "E4BX1ea2iDJ.REPORTING_RATE": "Complétude",
    "E4BX1ea2iDJ.REPORTING_RATE_ON_TIME": "Promptitude",

    "s5EUr8GznWY.HVViCmJi85w": "Séances fixes prévues",
    "lHB57kjRtUz.HVViCmJi85w": "Séances fixes réalisées",
    "s5EUr8GznWY.TG2PxzbbIjT": "Séances avancées prévues",
    "lHB57kjRtUz.TG2PxzbbIjT": "Séances avancées réalisées",
    "s5EUr8GznWY.aE55ruo9YeU": "Séances mobiles prévues",
    "lHB57kjRtUz.aE55ruo9YeU": "Séances mobiles réalisées",

    "ppMFLxX6NvU.FGpuKKTof4X": "Perdues de vue identifiés Penta1 0-11mois",
    "ppMFLxX6NvU.RD9Iut5kjja": "Perdues de vue identifiés Penta1 12-23mois",
    "ppMFLxX6NvU.LnwvJBIlzxk": "Perdues de vue identifiés 24-59mois",
    "ppMFLxX6NvU.RMMpx7TBreP": "Perdues de vue récupérés Penta1 0-11mois",
    "ppMFLxX6NvU.ZYkCdGglDmr": "Perdues de vue récupérés Penta1 12-23mois",
    "ppMFLxX6NvU.aZmhNMRCrmZ": "Perdues de vue récupérés Penta1 24-59mois",

    "DZ2xt2mgVzQ.FGpuKKTof4X": "Perdues de vue identifiés Penta3 0-11mois",
    "DZ2xt2mgVzQ.RD9Iut5kjja": "Perdues de vue identifiés Penta3 12-23mois",
    "DZ2xt2mgVzQ.LnwvJBIlzxk": "Perdues de vue identifiés Penta3 24-59mois",
    "DZ2xt2mgVzQ.RMMpx7TBreP": "Perdues de vue récupérés Penta3 0-11mois",
    "DZ2xt2mgVzQ.ZYkCdGglDmr": "Perdues de vue récupérés Penta3 12-23mois",
    "DZ2xt2mgVzQ.aZmhNMRCrmZ": "Perdues de vue récupérés Penta3 24-59mois",

    "SnhwwhcaDRS": "Enfants récupérés Penta1 BCU-FAE",
    "fycXkvxPUjH": "Enfants récupérés Penta3 BCU-FAE",
 
    "Nyke8sWJbn9.dqydGQFHahb": "VAP1 0-11 mois fixe",
    "Nyke8sWJbn9.vbx8t4WAbR8": "VAP1 0-11 mois avancée",
    "Nyke8sWJbn9.oSYTyzezWif": "VAP1 0-11 mois mobile",

    "slj1Yy0YgAc.dqydGQFHahb": "VAP2 0-11 mois fixe",
    "slj1Yy0YgAc.vbx8t4WAbR8": "VAP2 0-11 mois avancée",
    "slj1Yy0YgAc.oSYTyzezWif": "VAP2 0-11 mois mobile",

    "OnKbxNJzsCw.dqydGQFHahb": "VAP3 0-11 mois fixe",
    "OnKbxNJzsCw.vbx8t4WAbR8": "VAP3 0-11 mois avancée",
    "OnKbxNJzsCw.oSYTyzezWif": "VAP3 0-11 mois mobile",

    "KVjtJyNKcYQ.g6mIyKoGIh2": "VAP4 12-23 mois fixe",
    "KVjtJyNKcYQ.Rby9Jdri29F": "VAP4 12-23 mois avancée",
    "KVjtJyNKcYQ.FCXzheCQXtr": "VAP4 12-23 mois mobile",

    "WzOgzB6fLCW": "HPV",
    "E4BX1ea2iDJ": "Rapports attendus",

    "i5zmivDIHN8.dqydGQFHahb": "VAR2 0-11 mois fixe",
    "i5zmivDIHN8.vbx8t4WAbR8": "VAR2 0-11 mois avancée",
    "i5zmivDIHN8.oSYTyzezWif": "VAR2 0-11 mois mobile",

    "SnhwwhcaDRS.M7I3dsr9L5Z": "AVS DTC1",
    "fycXkvxPUjH.M7I3dsr9L5Z": "AVS DTC3",
    "CKhvpizB7wo.M7I3dsr9L5Z": "AVS VAR1",
    "OkgUUlDsGAN.M7I3dsr9L5Z": "AVS VAR2",

    "SnhwwhcaDRS.eX294IHIRVM": "OVM DTC1",
    "fycXkvxPUjH.eX294IHIRVM": "OVM DTC3",
    "CKhvpizB7wo.eX294IHIRVM": "OVM VAR1",
    "OkgUUlDsGAN.eX294IHIRVM": "OVM VAR2",

    "SnhwwhcaDRS.QeTSJLfbwEw": "Fluviale DTC1",
    "fycXkvxPUjH.QeTSJLfbwEw": "Fluviale DTC3",
    "CKhvpizB7wo.QeTSJLfbwEw": "Fluviale VAR1",
    "OkgUUlDsGAN.QeTSJLfbwEw": "Fluviale VAR2",

    "SnhwwhcaDRS.rJgTQZ63hnO": "IPVS DTC1",
    "fycXkvxPUjH.rJgTQZ63hnO": "IPVS DTC3",
    "CKhvpizB7wo.rJgTQZ63hnO": "IPVS VAR1",
    "OkgUUlDsGAN.rJgTQZ63hnO": "IPVS VAR2",

    "SnhwwhcaDRS.YWikHnhDAXE": "Autochtones DTC1",
    "fycXkvxPUjH.YWikHnhDAXE": "Autochtones DTC3",
    "CKhvpizB7wo.YWikHnhDAXE": "Autochtones VAR1",
    "OkgUUlDsGAN.YWikHnhDAXE": "Autochtones VAR2",

    "SnhwwhcaDRS.lvDrwyxXMq2": "Nomades DTC1",
    "fycXkvxPUjH.lvDrwyxXMq2": "Nomades DTC3",
    "CKhvpizB7wo.lvDrwyxXMq2": "Nomades VAR1",
    "OkgUUlDsGAN.lvDrwyxXMq2": "Nomades VAR2",

    "SnhwwhcaDRS.QOXbI3fshPV": "Réfugiés/Déplacés DTC1",
    "fycXkvxPUjH.QOXbI3fshPV": "Réfugiés/Déplacés DTC3",
    "CKhvpizB7wo.QOXbI3fshPV": "Réfugiés/Déplacés VAR1",
    "OkgUUlDsGAN.QOXbI3fshPV": "Réfugiés/Déplacés VAR2",

    "SnhwwhcaDRS.IdjExsz3izf": "Point de concentration DTC1",
    "fycXkvxPUjH.IdjExsz3izf": "Point de concentration DTC3",
    "CKhvpizB7wo.IdjExsz3izf": "Point de concentration VAR1",
    "OkgUUlDsGAN.IdjExsz3izf": "Point de concentration VAR2",

    "SnhwwhcaDRS.OdTBbi5jfhb": "Horaire adapté DTC1",
    "fycXkvxPUjH.OdTBbi5jfhb": "Horaire adapté DTC3",
    "CKhvpizB7wo.OdTBbi5jfhb": "Horaire adapté VAR1",
    "OkgUUlDsGAN.OdTBbi5jfhb": "Horaire adapté VAR2",

    "SnhwwhcaDRS.WbtfH2Deu6j": "Campements DTC1",
    "fycXkvxPUjH.WbtfH2Deu6j": "Campements DTC3",
    "CKhvpizB7wo.WbtfH2Deu6j": "Campements VAR1",
    "OkgUUlDsGAN.WbtfH2Deu6j": "Campements VAR2",

    "SnhwwhcaDRS.hiiCMvb0tTp": "Poches d'insécurité DTC1",
    "fycXkvxPUjH.hiiCMvb0tTp": "Poches d'insécurité DTC3",
    "CKhvpizB7wo.hiiCMvb0tTp": "Poches d'insécurité VAR1",
    "OkgUUlDsGAN.hiiCMvb0tTp": "Poches d'insécurité VAR2",

    "CKhvpizB7wo": "Enfants récupérés VAR1 BCU-FAE",

    "f1MiAAIu366.dqydGQFHahb": "VPO0 0-11 mois fixe1",
    "f1MiAAIu366.NOHlOxLczjc": "VPO0 0-11 mois fixe2",
    "f1MiAAIu366.vbx8t4WAbR8": "VPO0 0-11 mois avancée1",
    "f1MiAAIu366.KPDzIsWq7JK": "VPO0 0-11 mois avancée2",
    "f1MiAAIu366.oSYTyzezWif": "VPO0 0-11 mois mobile1",
    "f1MiAAIu366.Dr4rWTqepnP": "VPO0 0-11 mois mobile2",

    "j3P9bSnwF12.dqydGQFHahb": "VPO1 0-11 mois fixe1",
    "j3P9bSnwF12.NOHlOxLczjc": "VPO1 0-11 mois fixe2",
    "j3P9bSnwF12.vbx8t4WAbR8": "VPO1 0-11 mois avancée1",
    "j3P9bSnwF12.KPDzIsWq7JK": "VPO1 0-11 mois avancée2",
    "j3P9bSnwF12.oSYTyzezWif": "VPO1 0-11 mois mobile1",
    "j3P9bSnwF12.Dr4rWTqepnP": "VPO1 0-11 mois mobile2",

    "ozVdkud4F03.dqydGQFHahb": "VPO2 0-11 mois fixe1",
    "ozVdkud4F03.NOHlOxLczjc": "VPO2 0-11 mois fixe2",
    "ozVdkud4F03.vbx8t4WAbR8": "VPO2 0-11 mois avancée1",
    "ozVdkud4F03.KPDzIsWq7JK": "VPO2 0-11 mois avancée2",
    "ozVdkud4F03.oSYTyzezWif": "VPO2 0-11 mois mobile1",
    "ozVdkud4F03.Dr4rWTqepnP": "VPO2 0-11 mois mobile2",

    "CgR8kWNdK3W.dqydGQFHahb": "PCV13(1) 0-11 mois fixe1",
    "CgR8kWNdK3W.NOHlOxLczjc": "PCV13(1) 0-11 mois fixe2",
    "CgR8kWNdK3W.vbx8t4WAbR8": "PCV13(1) 0-11 mois avancée1",
    "CgR8kWNdK3W.KPDzIsWq7JK": "PCV13(1) 0-11 mois avancée2",
    "CgR8kWNdK3W.oSYTyzezWif": "PCV13(1) 0-11 mois mobile1",
    "CgR8kWNdK3W.Dr4rWTqepnP": "PCV13(1) 0-11 mois mobile2",

    "WGu60o5mePq.dqydGQFHahb": "PCV13(2) 0-11 mois fixe1",
    "WGu60o5mePq.NOHlOxLczjc": "PCV13(2) 0-11 mois fixe2",
    "WGu60o5mePq.vbx8t4WAbR8": "PCV13(2) 0-11 mois avancée1",
    "WGu60o5mePq.KPDzIsWq7JK": "PCV13(2) 0-11 mois avancée2",
    "WGu60o5mePq.oSYTyzezWif": "PCV13(2) 0-11 mois mobile1",
    "WGu60o5mePq.Dr4rWTqepnP": "PCV13(2) 0-11 mois mobile2",

    "TLKusYaKY5A.dqydGQFHahb": "ROTA1 0-11 mois fixe",
    "TLKusYaKY5A.vbx8t4WAbR8": "ROTA1 0-11 mois avancée",
    "TLKusYaKY5A.oSYTyzezWif": "ROTA1 0-11 mois mobile",

    "GzSBTZkxSZf.dqydGQFHahb": "ROTA2 0-11 mois fixe",
    "GzSBTZkxSZf.vbx8t4WAbR8": "ROTA2 0-11 mois avancée",
    "GzSBTZkxSZf.oSYTyzezWif": "ROTA2 0-11 mois mobile",

    "bzD4QxaNkJm.dqydGQFHahb": "VAA fixe1",
    "bzD4QxaNkJm.NOHlOxLczjc": "VAA fixe2",
    "bzD4QxaNkJm.vbx8t4WAbR8": "VAA avancé1",
    "bzD4QxaNkJm.KPDzIsWq7JK": "VAA avancé2",
    "bzD4QxaNkJm.oSYTyzezWif": "VAA mobile1",
    "bzD4QxaNkJm.Dr4rWTqepnP": "VAA mobile2",

    "RGW6llyyusM": "Td 2",
    "hGBtbI7kjvb": "Td 3",
    "bZDbSvdUcPC": "Td 4",
    "lXstTq1MDSv": "Td 5",

    # ============================================
    # LOGISTIQUES - RENAME MAP
    # ============================================
    "uAWIVDnGPGH.dxDQKDcTn6Z": "DTC dose-administree",
    "uAWIVDnGPGH.XEN3ucCGa07": "DTC dose-jour rupture stock",
    "uAWIVDnGPGH.J8mw9rFkY4v": "DTC dose-perdue",
    "uAWIVDnGPGH.WZwmzIuRvwV": "DTC dose-recue mois",
    "uAWIVDnGPGH.GSCwhT2SCCr": "DTC dose-stock debut mois",
    "uAWIVDnGPGH.pdKyvaYRqCj": "DTC dose-utilisee",
    "uAWIVDnGPGH.kOWsLrtvrhn": "DTC dose-stock disponible utilisable",
    "uAWIVDnGPGH.mZLRF4eSPIk": "DTC dose-sortie",
    "uAWIVDnGPGH.Lpu56KTyGvy": "DTC Stock Max",
    "uAWIVDnGPGH.ddJmZUacsvQ": "DTC Qté à commander",
    "uAWIVDnGPGH.t5L9ODSuYOG": "DTC CMM",
    "kd0elH7N75k.pGSfKOKL9s0": "DTC Adjustment",
    "kpvY4GzEsOv.jpmZl19SSta": "DTC MSD",

    "l1bhrYUPsde.dxDQKDcTn6Z": "BCG dose-administree",
    "l1bhrYUPsde.XEN3ucCGa07": "BCG dose-jour rupture stock",
    "l1bhrYUPsde.J8mw9rFkY4v": "BCG dose-perdue",
    "l1bhrYUPsde.WZwmzIuRvwV": "BCG dose-recue mois",
    "l1bhrYUPsde.GSCwhT2SCCr": "BCG dose-stock debut mois",
    "l1bhrYUPsde.pdKyvaYRqCj": "BCG dose-utilisee",
    "l1bhrYUPsde.kOWsLrtvrhn": "BCG dose-stock disponible utilisable",
    "l1bhrYUPsde.mZLRF4eSPIk": "BCG dose-sortie",
    "l1bhrYUPsde.Lpu56KTyGvy": "BCG Stock Max",
    "l1bhrYUPsde.ddJmZUacsvQ": "BCG Qté à commander",
    "l1bhrYUPsde.t5L9ODSuYOG": "BCG CMM",
    "VcI6UxYSqb7.pGSfKOKL9s0": "BCG Adjustment",
    "bJD8kwm6xz8.jpmZl19SSta": "BCG MSD",

    "Hjlw6DpooIo.dxDQKDcTn6Z": "VAR dose-administree",
    "Hjlw6DpooIo.XEN3ucCGa07": "VAR dose-jour rupture stock",
    "Hjlw6DpooIo.J8mw9rFkY4v": "VAR dose-perdue",
    "Hjlw6DpooIo.WZwmzIuRvwV": "VAR dose-recue mois",
    "Hjlw6DpooIo.GSCwhT2SCCr": "VAR dose-stock debut mois",
    "Hjlw6DpooIo.pdKyvaYRqCj": "VAR dose-utilisee",
    "Hjlw6DpooIo.kOWsLrtvrhn": "VAR dose-stock disponible utilisable",
    "Hjlw6DpooIo.mZLRF4eSPIk": "VAR dose-sortie",
    "Hjlw6DpooIo.Lpu56KTyGvy": "VAR Stock Max",
    "Hjlw6DpooIo.ddJmZUacsvQ": "VAR Qté à commander",
    "Hjlw6DpooIo.t5L9ODSuYOG": "VAR CMM",
    "DYoKTCw2Oqi.pGSfKOKL9s0": "VAR Adjustment",
    "fJvcsDrhScZ.jpmZl19SSta": "VAR MSD",

    "BCXcLiQNI8M.dxDQKDcTn6Z": "VPO dose-administree",
    "BCXcLiQNI8M.XEN3ucCGa07": "VPO dose-jour rupture stock",
    "BCXcLiQNI8M.J8mw9rFkY4v": "VPO dose-perdue",
    "BCXcLiQNI8M.WZwmzIuRvwV": "VPO dose-recue mois",
    "BCXcLiQNI8M.GSCwhT2SCCr": "VPO dose-stock debut mois",
    "BCXcLiQNI8M.pdKyvaYRqCj": "VPO dose-utilisee",
    "BCXcLiQNI8M.kOWsLrtvrhn": "VPO dose-stock disponible utilisable",
    "BCXcLiQNI8M.mZLRF4eSPIk": "VPO dose-sortie",
    "BCXcLiQNI8M.Lpu56KTyGvy": "VPO Stock Max",
    "BCXcLiQNI8M.ddJmZUacsvQ": "VPO Qté à commander",
    "BCXcLiQNI8M.t5L9ODSuYOG": "VPO CMM",
    "KCfG0FCgGWU.pGSfKOKL9s0": "VPO Adjustment",
    "iEbT3SCQStG.jpmZl19SSta": "VPO MSD",

    "IisI6JudbS8.dxDQKDcTn6Z": "VPI dose-administree",
    "IisI6JudbS8.XEN3ucCGa07": "VPI dose-jour rupture stock",
    "IisI6JudbS8.J8mw9rFkY4v": "VPI dose-perdue",
    "IisI6JudbS8.WZwmzIuRvwV": "VPI dose-recue mois",
    "IisI6JudbS8.GSCwhT2SCCr": "VPI dose-stock debut mois",
    "IisI6JudbS8.pdKyvaYRqCj": "VPI dose-utilisee",
    "IisI6JudbS8.mZLRF4eSPIk": "VPI dose-sortie",
    "IisI6JudbS8.kOWsLrtvrhn": "VPI dose-stock disponible utilisable",
    "IisI6JudbS8.Lpu56KTyGvy": "VPI Stock Max",
    "IisI6JudbS8.ddJmZUacsvQ": "VPI Qté à commander",
    "IisI6JudbS8.t5L9ODSuYOG": "VPI CMM",
    "EhguMRWyJjV.pGSfKOKL9s0": "VPI Adjustment",
    "UgMNX4AWqj1.jpmZl19SSta": "VPI MSD",

    "vrTdwvAQSUc.dxDQKDcTn6Z": "VAA dose-administree",
    "vrTdwvAQSUc.XEN3ucCGa07": "VAA dose-jour rupture stock",
    "vrTdwvAQSUc.J8mw9rFkY4v": "VAA dose-perdue",
    "vrTdwvAQSUc.WZwmzIuRvwV": "VAA dose-recue mois",
    "vrTdwvAQSUc.GSCwhT2SCCr": "VAA dose-stock debut mois",
    "vrTdwvAQSUc.pdKyvaYRqCj": "VAA dose-utilisee",
    "vrTdwvAQSUc.kOWsLrtvrhn": "VAA dose-stock disponible utilisable",
    "vrTdwvAQSUc.mZLRF4eSPIk": "VAA dose-sortie",
    "vrTdwvAQSUc.Lpu56KTyGvy": "VAA Stock Max",
    "vrTdwvAQSUc.ddJmZUacsvQ": "VAA Qté à commander",
    "vrTdwvAQSUc.t5L9ODSuYOG": "VAA CMM",
    "pho7jci1XYc.pGSfKOKL9s0": "VAA Adjustment",
    "AVhKGbmEA9G.jpmZl19SSta": "VAA MSD",

    "iFMn6ceqdIN.dxDQKDcTn6Z": "VAT dose-administree",
    "iFMn6ceqdIN.XEN3ucCGa07": "VAT dose-jour rupture stock",
    "iFMn6ceqdIN.J8mw9rFkY4v": "VAT dose-perdue",
    "iFMn6ceqdIN.WZwmzIuRvwV": "VAT dose-recue mois",
    "iFMn6ceqdIN.GSCwhT2SCCr": "VAT dose-stock debut mois",
    "iFMn6ceqdIN.pdKyvaYRqCj": "VAT dose-utilisee",
    "iFMn6ceqdIN.kOWsLrtvrhn": "VAT dose-stock disponible utilisable",
    "iFMn6ceqdIN.mZLRF4eSPIk": "VAT dose-sortie",
    "iFMn6ceqdIN.Lpu56KTyGvy": "VAT Stock Max",
    "iFMn6ceqdIN.ddJmZUacsvQ": "VAT Qté à commander",
    "iFMn6ceqdIN.t5L9ODSuYOG": "VAT CMM",
    "BhgOrHZVgdT.pGSfKOKL9s0": "VAT Adjustment",
    "aRkYMm4tHvX.jpmZl19SSta": "VAT MSD",

    "hqqlXMmY95c.dxDQKDcTn6Z": "PCV13 dose-administree",
    "hqqlXMmY95c.XEN3ucCGa07": "PCV13 dose-jour rupture stock",
    "hqqlXMmY95c.J8mw9rFkY4v": "PCV13 dose-perdue",
    "hqqlXMmY95c.WZwmzIuRvwV": "PCV13 dose-recue mois",
    "hqqlXMmY95c.GSCwhT2SCCr": "PCV13 dose-stock debut mois",
    "hqqlXMmY95c.pdKyvaYRqCj": "PCV13 dose-utilisee",
    "hqqlXMmY95c.kOWsLrtvrhn": "PCV13 dose-stock disponible utilisable",
    "hqqlXMmY95c.Lpu56KTyGvy": "PCV13 Stock Max",
    "hqqlXMmY95c.ddJmZUacsvQ": "PCV13 Qté à commander",
    "hqqlXMmY95c.t5L9ODSuYOG": "PCV13 CMM",
    "llBBZ1VxhaY.pGSfKOKL9s0": "PCV13 Adjustment",
    "CDc5q7ornpu.jpmZl19SSta": "PCV13 MSD",

    "MJrLCya7qzt.dxDQKDcTn6Z": "ROTA dose-administree",
    "MJrLCya7qzt.XEN3ucCGa07": "ROTA dose-jour rupture stock",
    "MJrLCya7qzt.J8mw9rFkY4v": "ROTA dose-perdue",
    "MJrLCya7qzt.WZwmzIuRvwV": "ROTA dose-recue mois",
    "MJrLCya7qzt.GSCwhT2SCCr": "ROTA dose-stock debut mois",
    "MJrLCya7qzt.pdKyvaYRqCj": "ROTA dose-utilisee",
    "MJrLCya7qzt.kOWsLrtvrhn": "ROTA dose-stock disponible utilisable",
    "MJrLCya7qzt.Lpu56KTyGvy": "ROTA Stock Max",
    "MJrLCya7qzt.ddJmZUacsvQ": "ROTA Qté à commander",
    "MJrLCya7qzt.t5L9ODSuYOG": "ROTA CMM",
    "qr4nc6wQ0nK.pGSfKOKL9s0": "ROTA Adjustment",
    "JrGE0PSspn3.jpmZl19SSta": "ROTA MSD",

    "RYaAzE1eqya.dxDQKDcTn6Z": "VAP dose-administree",
    "RYaAzE1eqya.XEN3ucCGa07": "VAP dose-jour rupture stock",
    "RYaAzE1eqya.J8mw9rFkY4v": "VAP dose-perdue",
    "RYaAzE1eqya.WZwmzIuRvwV": "VAP dose-recue mois",
    "RYaAzE1eqya.GSCwhT2SCCr": "VAP dose-stock debut mois",
    "RYaAzE1eqya.pdKyvaYRqCj": "VAP dose-utilisee",
    "RYaAzE1eqya.kOWsLrtvrhn": "VAP dose-stock disponible utilisable",
    "RYaAzE1eqya.Lpu56KTyGvy": "VAP Stock Max",
    "RYaAzE1eqya.ddJmZUacsvQ": "VAP Qté à commander",
    "RYaAzE1eqya.t5L9ODSuYOG": "VAP CMM",
    "ADvotRbZQxB.pGSfKOKL9s0": "VAP Adjustment",
    "kYGW0DcBwWe.jpmZl19SSta": "VAP MSD",

    "BfoCv6bUeBk.dxDQKDcTn6Z": "HPV dose-administree",
    "BfoCv6bUeBk.XEN3ucCGa07": "HPV dose-jour rupture stock",
    "BfoCv6bUeBk.J8mw9rFkY4v": "HPV dose-perdue",
    "BfoCv6bUeBk.WZwmzIuRvwV": "HPV dose-recue mois",
    "BfoCv6bUeBk.GSCwhT2SCCr": "HPV dose-stock debut mois",
    "BfoCv6bUeBk.pdKyvaYRqCj": "HPV dose-utilisee",
    "BfoCv6bUeBk.kOWsLrtvrhn": "HPV dose-stock disponible utilisable",
    "BfoCv6bUeBk.Lpu56KTyGvy": "HPV Stock Max",
    "BfoCv6bUeBk.ddJmZUacsvQ": "HPV Qté à commander",
    "BfoCv6bUeBk.t5L9ODSuYOG": "HPV CMM",
    "VsoGWlC8T9Q.pGSfKOKL9s0": "HPV Adjustment",
    "JdrnZD4dPKl.jpmZl19SSta": "HPV MSD",

    "laQa8YfpVrp.dxDQKDcTn6Z": "Diluant_BCG dose-administree",
    "laQa8YfpVrp.XEN3ucCGa07": "Diluant_BCG dose-jour rupture stock",
    "laQa8YfpVrp.J8mw9rFkY4v": "Diluant_BCG dose-perdue",
    "laQa8YfpVrp.WZwmzIuRvwV": "Diluant_BCG dose-recue mois",
    "laQa8YfpVrp.GSCwhT2SCCr": "Diluant_BCG dose-stock debut mois",
    "laQa8YfpVrp.pdKyvaYRqCj": "Diluant_BCG dose-utilisee",
    "laQa8YfpVrp.kOWsLrtvrhn": "Diluant_BCG dose-stock disponible utilisable",
    "laQa8YfpVrp.Lpu56KTyGvy": "Diluant_BCG Stock Max",
    "laQa8YfpVrp.ddJmZUacsvQ": "Diluant_BCG qte a commander",
    "laQa8YfpVrp.t5L9ODSuYOG": "Diluant_BCG cmm",
    "ln32rKUhVog.pGSfKOKL9s0": "Diluant_BCG ajustement",
    "x2wBx3s2SLQ.jpmZl19SSta": "Diluant_BCG msd",

    "nnSKbBscmxH.dxDQKDcTn6Z": "Diluant_VAR dose-administree",
    "nnSKbBscmxH.XEN3ucCGa07": "Diluant_VAR dose-jour rupture stock",
    "nnSKbBscmxH.J8mw9rFkY4v": "Diluant_VAR dose-perdue",
    "nnSKbBscmxH.WZwmzIuRvwV": "Diluant_VAR dose-recue mois",
    "nnSKbBscmxH.GSCwhT2SCCr": "Diluant_VAR dose-stock debut mois",
    "nnSKbBscmxH.pdKyvaYRqCj": "Diluant_VAR dose-utilisee",
    "nnSKbBscmxH.kOWsLrtvrhn": "Diluant_VAR dose-stock disponible utilisable",
    "nnSKbBscmxH.Lpu56KTyGvy": "Diluant_VAR Stock Max",
    "nnSKbBscmxH.ddJmZUacsvQ": "Diluant_VAR qte a commander",
    "nnSKbBscmxH.t5L9ODSuYOG": "Diluant_VAR cmm",
    "XQLOXoInd3R.pGSfKOKL9s0": "Diluant_VAR ajustement",
    "UX65tnTXUBr.jpmZl19SSta": "Diluant_VAR msd",

    "fQfJhi742vt.dxDQKDcTn6Z": "Diluant_VAA dose-administree",
    "fQfJhi742vt.XEN3ucCGa07": "Diluant_VAA dose-jour rupture stock",
    "fQfJhi742vt.J8mw9rFkY4v": "Diluant_VAA dose-perdue",
    "fQfJhi742vt.WZwmzIuRvwV": "Diluant_VAA dose-recue mois",
    "fQfJhi742vt.GSCwhT2SCCr": "Diluant_VAA dose-stock debut mois",
    "fQfJhi742vt.pdKyvaYRqCj": "Diluant_VAA dose-utilisee",
    "fQfJhi742vt.kOWsLrtvrhn": "Diluant_VAA dose-stock disponible utilisable",
    "fQfJhi742vt.Lpu56KTyGvy": "Diluant_VAA Stock Max",
    "fQfJhi742vt.ddJmZUacsvQ": "Diluant_VAA qte a commander",
    "fQfJhi742vt.t5L9ODSuYOG": "Diluant_VAA cmm",
    "GqgVvEI7JaC.pGSfKOKL9s0": "Diluant_VAA ajustement",
    "DZZLXF2N7Kw.jpmZl19SSta": "Diluant_VAA msd",

    "bwUoJJgDst9.dxDQKDcTn6Z": "SAB_005ml dose-administree",
    "bwUoJJgDst9.XEN3ucCGa07": "SAB_005ml dose-jour rupture stock",
    "bwUoJJgDst9.J8mw9rFkY4v": "SAB_005ml dose-perdue",
    "bwUoJJgDst9.WZwmzIuRvwV": "SAB_005ml dose-recue mois",
    "bwUoJJgDst9.GSCwhT2SCCr": "SAB_005ml dose-stock debut mois",
    "bwUoJJgDst9.pdKyvaYRqCj": "SAB_005ml dose-utilisee",
    "bwUoJJgDst9.kOWsLrtvrhn": "SAB_005ml dose-stock disponible utilisable",
    "bwUoJJgDst9.Lpu56KTyGvy": "SAB_005ml Stock Max",
    "bwUoJJgDst9.ddJmZUacsvQ": "SAB_005ml qte a commander",
    "bwUoJJgDst9.t5L9ODSuYOG": "SAB_005ml cmm",
    "esgalSEQ9hA.pGSfKOKL9s0": "SAB_005ml ajustement",
    "SJ1EAubhzIR.jpmZl19SSta": "SAB_005ml msd",

    "blfdHcia9nP.dxDQKDcTn6Z": "SAB_05ml dose-administree",
    "blfdHcia9nP.XEN3ucCGa07": "SAB_05ml dose-jour rupture stock",
    "blfdHcia9nP.J8mw9rFkY4v": "SAB_05ml dose-perdue",
    "blfdHcia9nP.WZwmzIuRvwV": "SAB_05ml dose-recue mois",
    "blfdHcia9nP.GSCwhT2SCCr": "SAB_05ml dose-stock debut mois",
    "blfdHcia9nP.pdKyvaYRqCj": "SAB_05ml dose-utilisee",
    "blfdHcia9nP.kOWsLrtvrhn": "SAB_05ml dose-stock disponible utilisable",
    "blfdHcia9nP.Lpu56KTyGvy": "SAB_05ml Stock Max",
    "blfdHcia9nP.ddJmZUacsvQ": "SAB_05ml qte a commander",
    "blfdHcia9nP.t5L9ODSuYOG": "SAB_05ml cmm",
    "e3k4D2zCCgo.pGSfKOKL9s0": "SAB_05ml ajustement",
    "Qstq45EG8DV.jpmZl19SSta": "SAB_05ml msd",

    "bqJvWJtJwtK.dxDQKDcTn6Z": "Ser_dilution_2ml dose-administree",
    "bqJvWJtJwtK.XEN3ucCGa07": "Ser_dilution_2ml dose-jour rupture stock",
    "bqJvWJtJwtK.J8mw9rFkY4v": "Ser_dilution_2ml dose-perdue",
    "bqJvWJtJwtK.WZwmzIuRvwV": "Ser_dilution_2ml dose-recue mois",
    "bqJvWJtJwtK.GSCwhT2SCCr": "Ser_dilution_2ml dose-stock debut mois",
    "bqJvWJtJwtK.pdKyvaYRqCj": "Ser_dilution_2ml dose-utilisee",
    "bqJvWJtJwtK.kOWsLrtvrhn": "Ser_dilution_2ml dose-stock disponible utilisable",
    "bqJvWJtJwtK.Lpu56KTyGvy": "Ser_dilution_2ml Stock Max",
    "bqJvWJtJwtK.ddJmZUacsvQ": "Ser_dilution_2ml qte a commander",
    "bqJvWJtJwtK.t5L9ODSuYOG": "Ser_dilution_2ml cmm",
    "JRwL4KKN7Nk.pGSfKOKL9s0": "Ser_dilution_2ml ajustement",
    "AdtKSkta4bh.jpmZl19SSta": "Ser_dilution_2ml msd",

    "DNZEo533IMh.dxDQKDcTn6Z": "Ser_dilution_5ml dose-administree",
    "DNZEo533IMh.XEN3ucCGa07": "Ser_dilution_5ml dose-jour rupture stock",
    "DNZEo533IMh.J8mw9rFkY4v": "Ser_dilution_5ml dose-perdue",
    "DNZEo533IMh.WZwmzIuRvwV": "Ser_dilution_5ml dose-recue mois",
    "DNZEo533IMh.GSCwhT2SCCr": "Ser_dilution_5ml dose-stock debut mois",
    "DNZEo533IMh.pdKyvaYRqCj": "Ser_dilution_5ml dose-utilisee",
    "DNZEo533IMh.kOWsLrtvrhn": "Ser_dilution_5ml dose-stock disponible utilisable",
    "DNZEo533IMh.Lpu56KTyGvy": "Ser_dilution_5ml Stock Max",
    "DNZEo533IMh.ddJmZUacsvQ": "Ser_dilution_5ml qte a commander",
    "DNZEo533IMh.t5L9ODSuYOG": "Ser_dilution_5ml cmm",
    "HrYG9MrHUdg.pGSfKOKL9s0": "Ser_dilution_5ml ajustement",
    "cmjf7VquiXH.jpmZl19SSta": "Ser_dilution_5ml msd",

    "ZMcyV45XAkW.dxDQKDcTn6Z": "SAB_auto_bloquante dose-administree",
    "ZMcyV45XAkW.XEN3ucCGa07": "SAB_auto_bloquante dose-jour rupture stock",
    "ZMcyV45XAkW.J8mw9rFkY4v": "SAB_auto_bloquante dose-perdue",
    "ZMcyV45XAkW.WZwmzIuRvwV": "SAB_auto_bloquante dose-recue mois",
    "ZMcyV45XAkW.GSCwhT2SCCr": "SAB_auto_bloquante dose-stock debut mois",
    "ZMcyV45XAkW.pdKyvaYRqCj": "SAB_auto_bloquante dose-utilisee",
    "ZMcyV45XAkW.kOWsLrtvrhn": "SAB_auto_bloquante dose-stock disponible utilisable",
    "ZMcyV45XAkW.Lpu56KTyGvy": "SAB_auto_bloquante Stock Max",
    "ZMcyV45XAkW.ddJmZUacsvQ": "SAB_auto_bloquante qte a commander",
    "ZMcyV45XAkW.t5L9ODSuYOG": "SAB_auto_bloquante cmm",
    "NesfIHNd9xd.pGSfKOKL9s0": "SAB_auto_bloquante ajustement",
    "rJgmuGc4ynC.jpmZl19SSta": "SAB_auto_bloquante msd",

    "gzPW3f2ijo5.dxDQKDcTn6Z": "Ser_dilution_6ml dose-administree",
    "gzPW3f2ijo5.XEN3ucCGa07": "Ser_dilution_6ml dose-jour rupture stock",
    "gzPW3f2ijo5.J8mw9rFkY4v": "Ser_dilution_6ml dose-perdue",
    "gzPW3f2ijo5.WZwmzIuRvwV": "Ser_dilution_6ml dose-recue mois",
    "gzPW3f2ijo5.GSCwhT2SCCr": "Ser_dilution_6ml dose-stock debut mois",
    "gzPW3f2ijo5.pdKyvaYRqCj": "Ser_dilution_6ml dose-utilisee",
    "gzPW3f2ijo5.kOWsLrtvrhn": "Ser_dilution_6ml dose-stock disponible utilisable",
    "gzPW3f2ijo5.Lpu56KTyGvy": "Ser_dilution_6ml Stock Max",
    "gzPW3f2ijo5.ddJmZUacsvQ": "Ser_dilution_6ml qte a commander",
    "gzPW3f2ijo5.t5L9ODSuYOG": "Ser_dilution_6ml cmm",
    "gN8mcPpAiiI.pGSfKOKL9s0": "Ser_dilution_6ml ajustement",
    "pZdQli2zn4y.jpmZl19SSta": "Ser_dilution_6ml msd",

    "tmp1zLSXw0Q.dxDQKDcTn6Z": "Adaptateurs dose-administree",
    "tmp1zLSXw0Q.XEN3ucCGa07": "Adaptateurs dose-jour rupture stock",
    "tmp1zLSXw0Q.J8mw9rFkY4v": "Adaptateurs dose-perdue",
    "tmp1zLSXw0Q.WZwmzIuRvwV": "Adaptateurs dose-recue mois",
    "tmp1zLSXw0Q.GSCwhT2SCCr": "Adaptateurs dose-stock debut mois",
    "tmp1zLSXw0Q.pdKyvaYRqCj": "Adaptateurs dose-utilisee",
    "tmp1zLSXw0Q.kOWsLrtvrhn": "Adaptateurs dose-stock disponible utilisable",
    "tmp1zLSXw0Q.Lpu56KTyGvy": "Adaptateurs Stock Max",
    "tmp1zLSXw0Q.ddJmZUacsvQ": "Adaptateurs qte a commander",
    "tmp1zLSXw0Q.t5L9ODSuYOG": "Adaptateurs cmm",
    "OoDet9UWItm.pGSfKOKL9s0": "Adaptateurs ajustement",
    "cOhDoR0ddSp.jpmZl19SSta": "Adaptateurs msd",

    "jJUIodWQH0T.dxDQKDcTn6Z": "Compte_goutte dose-administree",
    "jJUIodWQH0T.XEN3ucCGa07": "Compte_goutte dose-jour rupture stock",
    "jJUIodWQH0T.J8mw9rFkY4v": "Compte_goutte dose-perdue",
    "jJUIodWQH0T.WZwmzIuRvwV": "Compte_goutte dose-recue mois",
    "jJUIodWQH0T.GSCwhT2SCCr": "Compte_goutte dose-stock debut mois",
    "jJUIodWQH0T.pdKyvaYRqCj": "Compte_goutte dose-utilisee",
    "jJUIodWQH0T.kOWsLrtvrhn": "Compte_goutte dose-stock disponible utilisable",
    "jJUIodWQH0T.Lpu56KTyGvy": "Compte_goutte Stock Max",
    "jJUIodWQH0T.ddJmZUacsvQ": "Compte_goutte qte a commander",
    "jJUIodWQH0T.t5L9ODSuYOG": "Compte_goutte cmm",
    "MAyhe30b20R.pGSfKOKL9s0": "Compte_goutte ajustement",
    "WYiF7sbZ5cV.jpmZl19SSta": "Compte_goutte msd",

    "K8Yt0p1z21M.dxDQKDcTn6Z": "R_ceptacles dose-administree",
    "K8Yt0p1z21M.XEN3ucCGa07": "R_ceptacles dose-jour rupture stock",
    "K8Yt0p1z21M.J8mw9rFkY4v": "R_ceptacles dose-perdue",
    "K8Yt0p1z21M.WZwmzIuRvwV": "R_ceptacles dose-recue mois",
    "K8Yt0p1z21M.GSCwhT2SCCr": "R_ceptacles dose-stock debut mois",
    "K8Yt0p1z21M.pdKyvaYRqCj": "R_ceptacles dose-utilisee",
    "K8Yt0p1z21M.kOWsLrtvrhn": "R_ceptacles dose-stock disponible utilisable",
    "K8Yt0p1z21M.Lpu56KTyGvy": "R_ceptacles Stock Max",
    "K8Yt0p1z21M.ddJmZUacsvQ": "R_ceptacles qte a commander",
    "K8Yt0p1z21M.t5L9ODSuYOG": "R_ceptacles cmm",
    "hllO2kXSuwM.pGSfKOKL9s0": "R_ceptacles ajustement",
    "xOlmaXSPviM.jpmZl19SSta": "R_ceptacles msd",
    "W25tOXS0rxS.g6mIyKoGIh2": "BCG 12-23 mois fixe1",
    "W25tOXS0rxS.QRyK6yxKBU3": "BCG 12-23 mois fixe2",
    "W25tOXS0rxS.Rby9Jdri29F": "BCG 12-23 mois avance",
    "W25tOXS0rxS.FCXzheCQXtr": "BCG 12-23 mois mobile1",
    "W25tOXS0rxS.VrEj0UVVGr4": "BCG 12-23 mois mobile2",

    "uNdFg1eymsa.g6mIyKoGIh2": "Penta1 12-23 mois fixe1",
    "uNdFg1eymsa.QRyK6yxKBU3": "Penta1 12-23 mois fixe2",
    "uNdFg1eymsa.Rby9Jdri29F": "Penta1 12-23 mois avance",
    "uNdFg1eymsa.FCXzheCQXtr": "Penta1 12-23 mois mobile1",
    "uNdFg1eymsa.VrEj0UVVGr4": "Penta1 12-23 mois mobile2",

    "YYKXOc7xBUi.g6mIyKoGIh2": "Penta2 12-23 mois fixe1",
    "YYKXOc7xBUi.QRyK6yxKBU3": "Penta2 12-23 mois fixe2",
    "YYKXOc7xBUi.Rby9Jdri29F": "Penta2 12-23 mois avance",
    "YYKXOc7xBUi.FCXzheCQXtr": "Penta2 12-23 mois mobile1",
    "YYKXOc7xBUi.VrEj0UVVGr4": "Penta2 12-23 mois mobile2",

    "WWxqaHSeiwd.g6mIyKoGIh2": "Penta3 12-23 mois fixe1",
    "WWxqaHSeiwd.QRyK6yxKBU3": "Penta3 12-23 mois fixe2",
    "WWxqaHSeiwd.Rby9Jdri29F": "Penta3 12-23 mois avance",
    "WWxqaHSeiwd.FCXzheCQXtr": "Penta3 12-23 mois mobile1",
    "WWxqaHSeiwd.VrEj0UVVGr4": "Penta3 12-23 mois mobile2",

    "f1MiAAIu366.g6mIyKoGIh2": "VPO0 12-23 mois fixe1",
    "f1MiAAIu366.QRyK6yxKBU3": "VPO0 12-23 mois fixe2",
    "f1MiAAIu366.Rby9Jdri29F": "VPO0 12-23 mois avance",
    "f1MiAAIu366.FCXzheCQXtr": "VPO0 12-23 mois mobile1",
    "f1MiAAIu366.VrEj0UVVGr4": "VPO0 12-23 mois mobile2",

    "j3P9bSnwF12.g6mIyKoGIh2": "VPO1 12-23 mois fixe1",
    "j3P9bSnwF12.QRyK6yxKBU3": "VPO1 12-23 mois fixe2",
    "j3P9bSnwF12.Rby9Jdri29F": "VPO1 12-23 mois avance",
    "j3P9bSnwF12.FCXzheCQXtr": "VPO1 12-23 mois mobile1",
    "j3P9bSnwF12.VrEj0UVVGr4": "VPO1 12-23 mois mobile2",

    "ozVdkud4F03.g6mIyKoGIh2": "VPO2 12-23 mois fixe1",
    "ozVdkud4F03.QRyK6yxKBU3": "VPO2 12-23 mois fixe2",
    "ozVdkud4F03.Rby9Jdri29F": "VPO2 12-23 mois avance",
    "ozVdkud4F03.FCXzheCQXtr": "VPO2 12-23 mois mobile1",
    "ozVdkud4F03.VrEj0UVVGr4": "VPO2 12-23 mois mobile2",

    "SPGaDDw2JzG.g6mIyKoGIh2": "VPO3 12-23 mois fixe1",
    "SPGaDDw2JzG.QRyK6yxKBU3": "VPO3 12-23 mois fixe2",
    "SPGaDDw2JzG.Rby9Jdri29F": "SPGaDDw2JzG.Rby9Jdri29F", # FIXED mapping
    "SPGaDDw2JzG.FCXzheCQXtr": "VPO3 12-23 mois mobile1",
    "SPGaDDw2JzG.VrEj0UVVGr4": "VPO3 12-23 mois mobile2",

    "I6tLH3nk9tw.g6mIyKoGIh2": "VPI1 12-23 mois fixe1",
    "I6tLH3nk9tw.QRyK6yxKBU3": "VPI1 12-23 mois fixe2",
    "I6tLH3nk9tw.Rby9Jdri29F": "VPI1 12-23 mois avance",
    "I6tLH3nk9tw.FCXzheCQXtr": "VPI1 12-23 mois mobile1",
    "I6tLH3nk9tw.VrEj0UVVGr4": "VPI1 12-23 mois mobile2",

    "wwde2QtlIRN.g6mIyKoGIh2": "VPI2 12-23 mois fixe1",
    "wwde2QtlIRN.QRyK6yxKBU3": "VPI2 12-23 mois fixe2",
    "wwde2QtlIRN.Rby9Jdri29F": "VPI2 12-23 mois avance",
    "wwde2QtlIRN.FCXzheCQXtr": "VPI2 12-23 mois mobile1",
    "wwde2QtlIRN.VrEj0UVVGr4": "VPI2 12-23 mois mobile2",

    "CgR8kWNdK3W.g6mIyKoGIh2": "PCV13_1 12-23 mois fixe1",
    "CgR8kWNdK3W.QRyK6yxKBU3": "PCV13_1 12-23 mois fixe2",
    "CgR8kWNdK3W.Rby9Jdri29F": "PCV13_1 12-23 mois avance",
    "CgR8kWNdK3W.FCXzheCQXtr": "PCV13_1 12-23 mois mobile1",
    "CgR8kWNdK3W.VrEj0UVVGr4": "PCV13_1 12-23 mois mobile2",

    "WGu60o5mePq.g6mIyKoGIh2": "PCV13_2 12-23 mois fixe1",
    "WGu60o5mePq.QRyK6yxKBU3": "PCV13_2 12-23 mois fixe2",
    "WGu60o5mePq.Rby9Jdri29F": "PCV13_2 12-23 mois avance",
    "WGu60o5mePq.FCXzheCQXtr": "PCV13_2 12-23 mois mobile1",
    "WGu60o5mePq.VrEj0UVVGr4": "PCV13_2 12-23 mois mobile2",

    "J8R3WFpMAZI.g6mIyKoGIh2": "PCV13_3 12-23 mois fixe1",
    "J8R3WFpMAZI.QRyK6yxKBU3": "PCV13_3 12-23 mois fixe2",
    "J8R3WFpMAZI.Rby9Jdri29F": "PCV13_3 12-23 mois avance",
    "J8R3WFpMAZI.FCXzheCQXtr": "PCV13_3 12-23 mois mobile1",
    "J8R3WFpMAZI.VrEj0UVVGr4": "PCV13_3 12-23 mois mobile2",

    "TLKusYaKY5A.g6mIyKoGIh2": "ROTA1 12-23 mois fixe",
    "TLKusYaKY5A.Rby9Jdri29F": "ROTA1 12-23 mois avance",
    "TLKusYaKY5A.FCXzheCQXtr": "ROTA1 12-23 mois mobile",

    "GzSBTZkxSZf.g6mIyKoGIh2": "ROTA2 12-23 mois fixe",
    "GzSBTZkxSZf.Rby9Jdri29F": "ROTA2 12-23 mois avance",
    "GzSBTZkxSZf.FCXzheCQXtr": "ROTA2 12-23 mois mobile",

    "c4VvzI5zTep.g6mIyKoGIh2": "ROTA3 12-23 mois fixe",
    "c4VvzI5zTep.Rby9Jdri29F": "ROTA3 12-23 mois avance",
    "c4VvzI5zTep.FCXzheCQXtr": "ROTA3 12-23 mois mobile",

    "pak21wvkWJC.g6mIyKoGIh2": "VAR1 12-23 mois fixe1",
    "cTLKwfG8pSv.QRyK6yxKBU3": "VAR1 12-23 mois fixe2",
    "pak21wvkWJC.Rby9Jdri29F": "VAR1 12-23 mois avance1",
    "cTLKwfG8pSv.Rby9Jdri29F": "VAR1 12-23 mois avance2",
    "pak21wvkWJC.FCXzheCQXtr": "VAR1 12-23 mois mobile1",
    "cTLKwfG8pSv.VrEj0UVVGr4": "VAR1 12-23 mois mobile2",

    "i5zmivDIHN8.g6mIyKoGIh2": "VAR2 12-23 mois fixe1",
    "i5zmivDIHN8.QRyK6yxKBU3": "VAR2 12-23 mois fixe2",
    "i5zmivDIHN8.Rby9Jdri29F": "VAR2 12-23 mois avance",
    "i5zmivDIHN8.FCXzheCQXtr": "VAR2 12-23 mois mobile1",
    "i5zmivDIHN8.VrEj0UVVGr4": "VAR2 12-23 mois mobile2",

    "bzD4QxaNkJm.g6mIyKoGIh2": "VAA 12-23 mois fixe1",
    "bzD4QxaNkJm.QRyK6yxKBU3": "VAA 12-23 mois fixe2",
    "bzD4QxaNkJm.Rby9Jdri29F": "VAA 12-23 mois avance",
    "bzD4QxaNkJm.FCXzheCQXtr": "VAA 12-23 mois mobile1",
    "bzD4QxaNkJm.VrEj0UVVGr4": "VAA 12-23 mois mobile2",

    "M2JQW0H44dI.QRyK6yxKBU3": "ECV 12-23 mois fixe",
    "M2JQW0H44dI.VrEj0UVVGr4": "ECV 12-23 mois mobile",

    "W25tOXS0rxS.tWyeOXJgU3A": "BCG 24-59 mois fixe",
    "W25tOXS0rxS.cI92X1TYlnD": "BCG 24-59 mois avance",
    "W25tOXS0rxS.BisCHS3wArx": "BCG 24-59 mois mobile",

    "uNdFg1eymsa.tWyeOXJgU3A": "Penta1 24-59 mois fixe",
    "uNdFg1eymsa.cI92X1TYlnD": "Penta1 24-59 mois avance",
    "uNdFg1eymsa.BisCHS3wArx": "Penta1 24-59 mois mobile",

    "YYKXOc7xBUi.tWyeOXJgU3A": "Penta2 24-59 mois fixe",
    "YYKXOc7xBUi.cI92X1TYlnD": "Penta2 24-59 mois avance",
    "YYKXOc7xBUi.BisCHS3wArx": "Penta2 24-59 mois mobile",

    "WWxqaHSeiwd.tWyeOXJgU3A": "Penta3 24-59 mois fixe",
    "WWxqaHSeiwd.cI92X1TYlnD": "Penta3 24-59 mois avance",
    "WWxqaHSeiwd.BisCHS3wArx": "Penta3 24-59 mois mobile",

    "f1MiAAIu366.tWyeOXJgU3A": "VPO0 24-59 mois fixe",
    "f1MiAAIu366.cI92X1TYlnD": "VPO0 24-59 mois avance",
    "f1MiAAIu366.BisCHS3wArx": "VPO0 24-59 mois mobile",

    "j3P9bSnwF12.tWyeOXJgU3A": "VPO1 24-59 mois fixe",
    "j3P9bSnwF12.cI92X1TYlnD": "VPO1 24-59 mois avance",
    "j3P9bSnwF12.BisCHS3wArx": "VPO1 24-59 mois mobile",

    "ozVdkud4F03.tWyeOXJgU3A": "VPO2 24-59 mois fixe",
    "ozVdkud4F03.cI92X1TYlnD": "VPO2 24-59 mois avance",
    "ozVdkud4F03.BisCHS3wArx": "VPO2 24-59 mois mobile",

    "SPGaDDw2JzG.tWyeOXJgU3A": "VPO3 24-59 mois fixe",
    "SPGaDDw2JzG.cI92X1TYlnD": "VPO3 24-59 mois avance",
    "SPGaDDw2JzG.BisCHS3wArx": "VPO3 24-59 mois mobile",

    "I6tLH3nk9tw.tWyeOXJgU3A": "VPI1 24-59 mois fixe",
    "I6tLH3nk9tw.cI92X1TYlnD": "VPI1 24-59 mois avance",
    "I6tLH3nk9tw.BisCHS3wArx": "VPI1 24-59 mois mobile",

    "wwde2QtlIRN.tWyeOXJgU3A": "VPI2 24-59 mois fixe",
    "wwde2QtlIRN.cI92X1TYlnD": "VPI2 24-59 mois avance",
    "wwde2QtlIRN.BisCHS3wArx": "VPI2 24-59 mois mobile",

    "CgR8kWNdK3W.tWyeOXJgU3A": "PCV13_1 24-59 mois fixe",
    "CgR8kWNdK3W.cI92X1TYlnD": "PCV13_1 24-59 mois avance",
    "CgR8kWNdK3W.BisCHS3wArx": "PCV13_1 24-59 mois mobile",

    "WGu60o5mePq.tWyeOXJgU3A": "PCV13_2 24-59 mois fixe",
    "WGu60o5mePq.cI92X1TYlnD": "PCV13_2 24-59 mois avance",
    "WGu60o5mePq.BisCHS3wArx": "PCV13_2 24-59 mois mobile",

    "J8R3WFpMAZI.tWyeOXJgU3A": "PCV13_3 24-59 mois fixe",
    "J8R3WFpMAZI.cI92X1TYlnD": "PCV13_3 24-59 mois avance",
    "J8R3WFpMAZI.BisCHS3wArx": "PCV13_3 24-59 mois mobile",

    "TLKusYaKY5A.tWyeOXJgU3A": "ROTA1 24-59 mois fixe",
    "TLKusYaKY5A.cI92X1TYlnD": "ROTA1 24-59 mois avance",
    "TLKusYaKY5A.BisCHS3wArx": "ROTA1 24-59 mois mobile",

    "GzSBTZkxSZf.tWyeOXJgU3A": "ROTA2 24-59 mois fixe",
    "GzSBTZkxSZf.cI92X1TYlnD": "ROTA2 24-59 mois avance",
    "GzSBTZkxSZf.BisCHS3wArx": "ROTA2 24-59 mois mobile",

    "c4VvzI5zTep.tWyeOXJgU3A": "ROTA3 24-59 mois fixe",
    "c4VvzI5zTep.cI92X1TYlnD": "ROTA3 24-59 mois avance",
    "c4VvzI5zTep.BisCHS3wArx": "ROTA3 24-59 mois mobile",

    "pak21wvkWJC.tWyeOXJgU3A": "VAR1 24-59 mois fixe1",
    "cTLKwfG8pSv.tWyeOXJgU3A": "VAR1 24-59 mois fixe2",
    "pak21wvkWJC.cI92X1TYlnD": "VAR1 24-59 mois avance1",
    "cTLKwfG8pSv.cI92X1TYlnD": "VAR1 24-59 mois avance2",
    "pak21wvkWJC.BisCHS3wArx": "VAR1 24-59 mois mobile1",
    "cTLKwfG8pSv.BisCHS3wArx": "VAR1 24-59 mois mobile2",

    "i5zmivDIHN8.tWyeOXJgU3A": "VAR2 24-59 mois fixe",
    "i5zmivDIHN8.cI92X1TYlnD": "VAR2 24-59 mois avance",
    "i5zmivDIHN8.BisCHS3wArx": "VAR2 24-59 mois mobile",

    "bzD4QxaNkJm.tWyeOXJgU3A": "VAA 24-59 mois fixe",
    "bzD4QxaNkJm.cI92X1TYlnD": "VAA 24-59 mois avance",
    "bzD4QxaNkJm.BisCHS3wArx": "VAA 24-59 mois mobile",

    "tjcH6RS9mXd": "Td 1",
}

# ============================================================
# 2) HELPERS
# ============================================================

MMM = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
       "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


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


def load_zoho_rename_map(path: Path) -> Dict[str, str]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def period_iso_to_zoho(period_iso: str) -> str:
    s = (period_iso or "").strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        y = int(s[0:4])
        m = int(s[5:7])
        d = int(s[8:10])
        if 1 <= m <= 12:
            return f"{d:02d}-{MMM[m - 1]}-{y:04d}"
    return s


def normalize_number(v: Any) -> Any:
    if v is None:
        return 0

    if isinstance(v, bool):
        return int(v)

    if isinstance(v, int):
        return v

    if isinstance(v, float):
        return int(v) if v.is_integer() else v

    if isinstance(v, str):
        s = v.strip()
        if s == "" or s.lower() == "null":
            return 0
        s2 = s.replace(",", ".")
        try:
            f = float(s2)
            return int(f) if f.is_integer() else f
        except Exception:
            return v

    return v


# ============================================================
# 3) DHIS2 CLIENT (FOSA = ORG LEVEL 5)
# ============================================================

@dataclass
class Dhis2Client:
    base_url: str
    username: str
    password: str
    timeout_s: int = 1200

    def __post_init__(self) -> None:
        retry = Retry(
            total=6,
            connect=6,
            read=6,
            status=6,
            backoff_factor=3,
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
        last_err = ""

        for attempt in range(1, 8):
            r = self.session.get(
                url,
                params=params,
                auth=(self.username, self.password),
                headers={"Accept": "application/json"},
                timeout=self.timeout_s,
            )

            if 200 <= r.status_code < 300:
                return r.json()

            if r.status_code in (429, 500, 502, 503, 504):
                last_err = f"{r.status_code} {r.text[:200]}"
                sleep_s = min(90.0, 5.0 * attempt)
                print(
                    f"WARN: DHIS2 {r.status_code} attempt={attempt}/7 "
                    f"sleep={sleep_s}s url={path}",
                    flush=True,
                )
                time.sleep(sleep_s)
                continue

            r.raise_for_status()

        raise requests.exceptions.HTTPError(
            f"DHIS2 API failed after retries: {url} last={last_err}"
        )

    def analytics(self, dx_items: List[str], pe: str, ou: str = "LEVEL-5") -> dict:
        params = {
            "dimension": [f"dx:{';'.join(dx_items)}", f"pe:{pe}", f"ou:{ou}"],
            "displayProperty": "NAME",
            "outputIdScheme": "UID",
            "skipMeta": "true",
            "paging": "false",
        }
        return self._get("api/analytics.json", params)


def _analytics_with_split(
    client: Dhis2Client,
    pe: str,
    dx_items: List[str],
    *,
    max_split_depth: int = 4,
    depth: int = 0,
) -> dict:
    try:
        return client.analytics(dx_items=dx_items, pe=pe, ou="LEVEL-5")
    except Exception as e:
        msg = str(e)
        is_timeoutish = (
            "504" in msg
            or "Gateway" in msg
            or "Read timed out" in msg
            or "timeout" in msg.lower()
            or "Max retries exceeded" in msg
        )

        if (not is_timeoutish) or (depth >= max_split_depth) or (len(dx_items) <= 20):
            raise

        mid = len(dx_items) // 2
        left = dx_items[:mid]
        right = dx_items[mid:]

        print(
            f"WARN: analytics failed depth={depth} dx={len(dx_items)} "
            f"-> split {len(left)}+{len(right)} reason={msg[:140]}",
            flush=True,
        )

        out = {"rows": []}
        a = _analytics_with_split(
            client,
            pe,
            left,
            max_split_depth=max_split_depth,
            depth=depth + 1,
        )
        b = _analytics_with_split(
            client,
            pe,
            right,
            max_split_depth=max_split_depth,
            depth=depth + 1,
        )
        out["rows"].extend(a.get("rows") or [])
        out["rows"].extend(b.get("rows") or [])
        return out


# ============================================================
# 4) TRANSFORM
# ============================================================

def rows_to_records(analytics_json: dict, allowed_dx: Optional[set] = None) -> List[dict]:
    """Lignes analytics -> enregistrements longs.

    allowed_dx : opérandes réellement demandés dans CE chunk. Au-delà d'un certain
    nombre d'opérandes d'un même dataElement dans une requête, l'analytics du SNIS
    cesse de filtrer par categoryOptionCombo et renvoie TOUTES les lignes du
    dataElement — y compris des COC non demandés (« lignes fantômes »). Ces lignes
    réapparaissent alors dans le chunk qui les demande vraiment, et pivot_records
    les additionnait : d'où des valeurs ×2/×3 sur les colonnes 12-23 mois
    (surestimation de la CV VAR2). On ne garde donc que ce qui a été demandé.
    """
    rows = analytics_json.get("rows") or []
    recs: List[dict] = []
    for r in rows:
        try:
            dx, pe, ou, val = r[0], r[1], r[2], r[3]
        except Exception:
            continue
        if allowed_dx is not None and dx not in allowed_dx:
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
    rename_map_dx_to_label: Dict[str, str],
    zoho_map_label_to_link: Dict[str, str],
) -> List[dict]:
    idx: Dict[Tuple[str, str], dict] = {}

    for r in long_recs:
        key = (r["ou"], r["pe"])
        row = idx.get(key)
        if row is None:
            row = {"ou": r["ou"], "pe": r["pe"]}
            idx[key] = row

        # Une clé (ou, pe, dx) n'a qu'UNE valeur vraie : ne jamais additionner.
        # Un même opérande peut revenir dans plusieurs chunks (lignes fantômes,
        # cf. rows_to_records) avec la même valeur — l'additionner la doublait.
        if r["dx"] not in row:
            row[r["dx"]] = r["value"]

    for row in idx.values():
        for dx in dx_expected:
            row.setdefault(dx, None)

    out: List[dict] = []
    for row in idx.values():
        pe = str(row["pe"])
        period_iso = f"{pe[:4]}-{pe[4:6]}-01"
        period_zoho = period_iso_to_zoho(period_iso)

        out_row: dict = {
            "OrgUnit": row.get("ou"),
            "Period": period_zoho,
        }

        for dx in dx_expected:
            label = rename_map_dx_to_label.get(dx, dx)
            zoho_link = zoho_map_label_to_link.get(label, label)
            out_row[zoho_link] = normalize_number(row.get(dx))

        out.append(out_row)

    out.sort(key=lambda x: (x.get("OrgUnit") or "", x.get("Period") or ""))
    return out


def fetch_period(
    client: Dhis2Client,
    pe: str,
    dx_expected: List[str],
    rename_map_dx_to_label: Dict[str, str],
    zoho_map_label_to_link: Dict[str, str],
    dx_chunk_chars: int,
    sleep_s: float,
) -> List[dict]:
    chunks = chunk_list(dx_expected, max_chars=dx_chunk_chars)
    long_all: List[dict] = []

    for i, ch in enumerate(chunks, start=1):
        print(f"[{pe}] chunk {i}/{len(chunks)} dx_items={len(ch)}", flush=True)
        data = _analytics_with_split(client, pe, ch)
        long_all.extend(rows_to_records(data, allowed_dx=set(ch)))
        if sleep_s and sleep_s > 0:
            time.sleep(sleep_s)

    return pivot_records(long_all, dx_expected, rename_map_dx_to_label, zoho_map_label_to_link)


# ============================================================
# 5) OUTPUT
# ============================================================

def write_ndjson_parts_dual(
    folder: Path,
    records: List[dict],
    max_plain_bytes: int = 800_000,
) -> List[dict]:
    folder.mkdir(parents=True, exist_ok=True)

    parts_meta: List[dict] = []
    part_idx = 1
    rows_in_part = 0
    plain_bytes_in_part = 0

    def paths(i: int) -> Tuple[Path, Path]:
        plain = folder / f"part-{i:04d}.ndjson"
        gz = folder / f"part-{i:04d}.ndjson.gz"
        return plain, gz

    plain_path, gz_path = paths(part_idx)
    plain_f = plain_path.open("w", encoding="utf-8", newline="\n")
    gz_f = gzip.open(gz_path, "wb")

    def close_part() -> None:
        nonlocal plain_f, gz_f, rows_in_part, plain_path, gz_path
        plain_f.close()
        gz_f.close()
        parts_meta.append(
            {
                "file": gz_path.name,
                "plain": plain_path.name,
                "rows": rows_in_part,
                "bytes_plain": plain_path.stat().st_size if plain_path.exists() else 0,
                "bytes_gz": gz_path.stat().st_size if gz_path.exists() else 0,
            }
        )

    for rec in records:
        line_str = json.dumps(rec, ensure_ascii=False) + "\n"
        line_b = line_str.encode("utf-8")
        line_len = len(line_b)

        if rows_in_part > 0 and (plain_bytes_in_part + line_len) > max_plain_bytes:
            close_part()
            part_idx += 1
            rows_in_part = 0
            plain_bytes_in_part = 0
            plain_path, gz_path = paths(part_idx)
            plain_f = plain_path.open("w", encoding="utf-8", newline="\n")
            gz_f = gzip.open(gz_path, "wb")

        plain_f.write(line_str)
        gz_f.write(line_b)
        rows_in_part += 1
        plain_bytes_in_part += line_len

    close_part()

    if not records and not parts_meta:
        plain_path, gz_path = paths(1)
        plain_path.write_text("", encoding="utf-8")
        with gzip.open(gz_path, "wb") as f:
            f.write(b"")
        parts_meta.append(
            {
                "file": gz_path.name,
                "plain": plain_path.name,
                "rows": 0,
                "bytes_plain": plain_path.stat().st_size,
                "bytes_gz": gz_path.stat().st_size,
            }
        )

    return parts_meta


# ============================================================
# 6) MAIN
# ============================================================

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="202501", help="YYYYMM")
    ap.add_argument("--end", default=None, help="YYYYMM (optional)")
    ap.add_argument("--months", type=int, default=3, help="Refresh last N months")
    ap.add_argument("--backfill", action="store_true", help="Fetch ALL months from --start to --end/current")
    ap.add_argument("--out", default="docs/data", help="Output folder")
    ap.add_argument("--dx_chunk_chars", type=int, default=6500)
    ap.add_argument("--sleep", type=float, default=0.2, help="Sleep between DHIS2 calls")
    ap.add_argument("--max_plain_bytes", type=int, default=800_000)
    ap.add_argument("--retry_failed", action="store_true")
    ap.add_argument("--retry_limit", type=int, default=2)
    ap.add_argument("--skip_guard", action="store_true",
                    help="Désactive le garde-fou de validation des données (à utiliser en connaissance de cause)")

    args = ap.parse_args()

    base_url = os.environ.get("DHIS2_BASE_URL")
    username = os.environ.get("DHIS2_USERNAME")
    password = os.environ.get("DHIS2_PASSWORD")
    if not (base_url and username and password):
        print("Missing secrets: DHIS2_BASE_URL, DHIS2_USERNAME, DHIS2_PASSWORD", file=sys.stderr)
        return 2

    if not DX_LIST.strip():
        print("DX_LIST is empty. Paste your dx list in section (A).", file=sys.stderr)
        return 2

    if not RENAME_MAP:
        print("RENAME_MAP is empty. Paste your dx->label map in section (B).", file=sys.stderr)
        return 2

    zoho_rename_path = Path("docs/config/rename_map.json")
    zoho_map = load_zoho_rename_map(zoho_rename_path)
    if not zoho_map:
        print(f"Missing/invalid docs/config/rename_map.json at {zoho_rename_path}", file=sys.stderr)
        return 2

    # dédupliquer en conservant l'ordre : un même dx.coc présent 2x dans DX_LIST
    # serait sinon requêté dans 2 chunks puis additionné par pivot_records (valeur doublée)
    dx_expected = list(dict.fromkeys(x.strip() for x in DX_LIST.split(";") if x.strip()))
    client = Dhis2Client(base_url=base_url, username=username, password=password)

    end = args.end or current_yyyymm()
    all_months = month_range(args.start, end)

    out_dir = Path(args.out)
    monthly_root = out_dir / "monthly"
    index_path = out_dir / "index.json"

    index: Dict[str, Any] = {"generated_at": None, "months": {}, "retry_queue": []}
    if index_path.exists():
        try:
            index = json.loads(index_path.read_text(encoding="utf-8"))
            if "months" not in index or index["months"] is None:
                index["months"] = {}
            if "retry_queue" not in index or index["retry_queue"] is None:
                index["retry_queue"] = []
        except Exception:
            index = {"generated_at": None, "months": {}, "retry_queue": []}

    if args.backfill:
        periods = all_months
    else:
        periods = all_months[-max(1, args.months):]
        if args.retry_failed:
            rq = [m for m in (index.get("retry_queue") or []) if isinstance(m, str)]
            extra = [m for m in rq if m not in periods]
            periods = periods + extra[:max(0, int(args.retry_limit))]

    ok_months: List[str] = []
    failed_months: List[str] = []

    seen = set()
    periods = [m for m in periods if not (m in seen or seen.add(m))]

    print(f"Planned periods: {periods}", flush=True)

    for pe in periods:
        try:
            records = fetch_period(
                client=client,
                pe=pe,
                dx_expected=dx_expected,
                rename_map_dx_to_label=RENAME_MAP,
                zoho_map_label_to_link=zoho_map,
                dx_chunk_chars=args.dx_chunk_chars,
                sleep_s=args.sleep,
            )
            # Un mois sans AUCUN enregistrement = analytics transitoire
            # (« mois à zéro » pendant le rebuild nocturne) : ne jamais
            # écraser les fichiers existants avec du vide — cause du trou
            # ZS 202606 d'août 2026. Le mois part en retry_queue.
            if not records:
                raise RuntimeError("analytics a renvoyé 0 enregistrement")
            # Garde-fou : rejette le mois si analytics renvoie des valeurs
            # transitoires (x2/x3 sur les combos 12-23, mois à zéro, …)
            if not args.skip_guard:
                dhis2_guard.check_month(
                    client=client,
                    records=records,
                    pe=pe,
                    month_folder=monthly_root / pe,
                    rename_map_dx_to_label=RENAME_MAP,
                    zoho_map_label_to_link=zoho_map,
                    level="FOSA",
                )
        except Exception as e:
            print(f"ERROR: fetch_period failed for {pe}: {e}", flush=True)
            print(f"SKIP month {pe}: keeping existing files/index for this month if any", flush=True)
            failed_months.append(pe)

            rq = index.get("retry_queue") or []
            if pe not in rq:
                rq.append(pe)
            index["retry_queue"] = rq
            continue

        month_folder = monthly_root / pe
        month_folder.mkdir(parents=True, exist_ok=True)

        for p in month_folder.glob("*"):
            try:
                p.unlink()
            except Exception:
                pass

        parts = write_ndjson_parts_dual(
            month_folder,
            records,
            max_plain_bytes=args.max_plain_bytes,
        )

        index["months"][pe] = {"parts": parts, "rows": len(records)}
        ok_months.append(pe)

        rq = index.get("retry_queue") or []
        if pe in rq:
            rq = [m for m in rq if m != pe]
        index["retry_queue"] = rq

    index["generated_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text(json.dumps(index, ensure_ascii=False), encoding="utf-8")

    print(f"OK months: {ok_months}", flush=True)
    if failed_months:
        print(f"FAILED months (added to retry_queue): {failed_months}", flush=True)
    print(f"Index months={len(index.get('months') or {})} | retry_queue={index.get('retry_queue')}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
