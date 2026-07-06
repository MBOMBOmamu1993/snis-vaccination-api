"use strict";
var __dsPreview = (() => {
  var __create = Object.create;
  var __defProp = Object.defineProperty;
  var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
  var __getOwnPropNames = Object.getOwnPropertyNames;
  var __getProtoOf = Object.getPrototypeOf;
  var __hasOwnProp = Object.prototype.hasOwnProperty;
  var __esm = (fn, res, err) => function __init() {
    if (err) throw err[0];
    try {
      return fn && (res = (0, fn[__getOwnPropNames(fn)[0]])(fn = 0)), res;
    } catch (e) {
      throw err = [e], e;
    }
  };
  var __commonJS = (cb, mod) => function __require() {
    try {
      return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
    } catch (e) {
      throw mod = 0, e;
    }
  };
  var __export = (target, all) => {
    for (var name in all)
      __defProp(target, name, { get: all[name], enumerable: true });
  };
  var __copyProps = (to, from, except, desc) => {
    if (from && typeof from === "object" || typeof from === "function") {
      for (let key of __getOwnPropNames(from))
        if (!__hasOwnProp.call(to, key) && key !== except)
          __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
    }
    return to;
  };
  var __reExport = (target, mod, secondTarget) => (__copyProps(target, mod, "default"), secondTarget && __copyProps(secondTarget, mod, "default"));
  var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
    // If the importer is in node compatibility mode or this is not an ESM
    // file that has been converted to a CommonJS file using a Babel-
    // compatible transform (i.e. "__esModule" has not been set), then set
    // "default" to the CommonJS "module.exports" for node compatibility.
    isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
    mod
  ));
  var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

  // <define:import.meta.env>
  var init_define_import_meta_env = __esm({
    "<define:import.meta.env>"() {
    }
  });

  // ds-raw:__ds_raw__
  var require_ds_raw = __commonJS({
    "ds-raw:__ds_raw__"(exports, module) {
      init_define_import_meta_env();
      module.exports = window.SnisPev;
    }
  });

  // shim:react-shim
  var require_react_shim = __commonJS({
    "shim:react-shim"(exports, module) {
      init_define_import_meta_env();
      var R = window.React;
      function np(p, k) {
        var o = {};
        for (var x in p) if (x !== "children") o[x] = p[x];
        if (k !== void 0) o.key = k;
        return o;
      }
      function jsx2(t, p, k) {
        var c = p && p.children;
        return c === void 0 ? R.createElement(t, np(p, k)) : R.createElement(t, np(p, k), c);
      }
      function jsxs2(t, p, k) {
        return R.createElement.apply(R, [t, np(p, k)].concat(p.children));
      }
      module.exports = R;
      module.exports.jsx = jsx2;
      module.exports.jsxs = jsxs2;
      module.exports.jsxDEV = function(t, p, k, s) {
        return (s ? jsxs2 : jsx2)(t, p, k);
      };
      module.exports.Fragment = R.Fragment;
    }
  });

  // .design-sync/previews/EChart.tsx
  var EChart_exports = {};
  __export(EChart_exports, {
    BarresEmpilees: () => BarresEmpilees,
    Histogramme: () => Histogramme,
    JaugeSansMenu: () => JaugeSansMenu
  });
  init_define_import_meta_env();

  // .design-sync/previews/_liveClock.ts
  init_define_import_meta_env();
  function clockFrozen() {
    const d0 = Date.now();
    const p0 = performance.now();
    while (performance.now() - p0 < 12) {
    }
    return Date.now() - d0 < 4;
  }
  if (typeof window !== "undefined" && clockFrozen()) {
    const RealDate = Date;
    const base = RealDate.now();
    const perf0 = performance.now();
    const liveNow = () => base + (performance.now() - perf0) * 50;
    class LiveDate extends RealDate {
      constructor(...args) {
        if (args.length === 0) super(liveNow());
        else super(...args);
      }
      static now() {
        return liveNow();
      }
    }
    globalThis.Date = LiveDate;
  }

  // ds-shim:ds
  var ds_exports = {};
  __export(ds_exports, {
    default: () => ds_default
  });
  init_define_import_meta_env();
  __reExport(ds_exports, __toESM(require_ds_raw()));
  var g = window.SnisPev;
  var ds_default = "default" in g ? g.default : g;

  // .design-sync/previews/EChart.tsx
  var import_jsx_runtime = __toESM(require_react_shim());
  var Histogramme = () => /* @__PURE__ */ (0, import_jsx_runtime.jsxs)(ds_exports.Card, { children: [
    /* @__PURE__ */ (0, import_jsx_runtime.jsx)(ds_exports.CardHeader, { title: "Doses administrées par antigène", subtitle: "Juin 2026 · Toutes provinces", icon: "syringe", iconTone: "blue" }),
    /* @__PURE__ */ (0, import_jsx_runtime.jsx)(
      ds_exports.EChart,
      {
        height: 260,
        exportTitle: "Doses par antigène",
        option: {
          grid: { left: 8, right: 8, top: 24, bottom: 4, containLabel: true },
          tooltip: { trigger: "axis" },
          xAxis: { type: "category", data: ["BCG", "Penta1", "Penta2", "Penta3", "VPO3", "VAR1", "PCV13"] },
          yAxis: { type: "value" },
          series: [{ type: "bar", data: [118204, 132880, 130215, 128450, 121772, 96204, 125661] }]
        }
      }
    )
  ] });
  var BarresEmpilees = () => /* @__PURE__ */ (0, import_jsx_runtime.jsx)(
    ds_exports.EChart,
    {
      height: 240,
      exportTitle: "Doses par stratégie",
      option: {
        grid: { left: 8, right: 8, top: 30, bottom: 4, containLabel: true },
        tooltip: { trigger: "axis" },
        legend: { top: 0 },
        xAxis: { type: "category", data: ["Kinshasa", "Kongo Central", "Tshopo", "Tshuapa", "Nord-Kivu"] },
        yAxis: { type: "value" },
        series: [
          { name: "Stratégie fixe", type: "bar", stack: "s", data: [42100, 28400, 15800, 9600, 24800] },
          { name: "Stratégie avancée", type: "bar", stack: "s", data: [8200, 9100, 7400, 6100, 8900] }
        ]
      }
    }
  );
  var JaugeSansMenu = () => /* @__PURE__ */ (0, import_jsx_runtime.jsx)(
    ds_exports.EChart,
    {
      height: 200,
      menu: false,
      option: {
        series: [{
          type: "gauge",
          min: 0,
          max: 100,
          progress: { show: true, width: 10 },
          axisLine: { lineStyle: { width: 10 } },
          axisTick: { show: false },
          splitLine: { show: false },
          axisLabel: { show: false },
          pointer: { show: false },
          detail: { formatter: "{value} %", fontSize: 22, offsetCenter: [0, 0], color: ds_exports.PEV_PALETTE[0] },
          data: [{ value: 87.3, name: "Couverture Penta3" }]
        }]
      }
    }
  );
  return __toCommonJS(EChart_exports);
})();
