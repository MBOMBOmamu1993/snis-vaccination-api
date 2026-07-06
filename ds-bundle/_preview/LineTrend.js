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
      function jsxs(t, p, k) {
        return R.createElement.apply(R, [t, np(p, k)].concat(p.children));
      }
      module.exports = R;
      module.exports.jsx = jsx2;
      module.exports.jsxs = jsxs;
      module.exports.jsxDEV = function(t, p, k, s) {
        return (s ? jsxs : jsx2)(t, p, k);
      };
      module.exports.Fragment = R.Fragment;
    }
  });

  // .design-sync/previews/LineTrend.tsx
  var LineTrend_exports = {};
  __export(LineTrend_exports, {
    AvecTrous: () => AvecTrous,
    MultiSeries: () => MultiSeries,
    SerieUnique: () => SerieUnique
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

  // .design-sync/previews/LineTrend.tsx
  var import_jsx_runtime = __toESM(require_react_shim());
  var months = ["2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06"];
  var SerieUnique = () => /* @__PURE__ */ (0, import_jsx_runtime.jsx)(
    ds_exports.LineTrend,
    {
      months,
      exportTitle: "Complétude mensuelle",
      series: [{ name: "Complétude", data: [88, 90, 91, 93, 92, 94], color: "#0093d5" }]
    }
  );
  var MultiSeries = () => /* @__PURE__ */ (0, import_jsx_runtime.jsx)(
    ds_exports.LineTrend,
    {
      height: 260,
      months,
      exportTitle: "Suivi des indicateurs qualité",
      series: [
        { name: "Complétude", data: [88, 90, 91, 93, 92, 94], color: "#0093d5" },
        { name: "Promptitude", data: [64, 69, 71, 75, 74, 79], color: "#f59e0b" },
        { name: "Couverture Penta3", data: [78, 80, 83, 85, 84, 87], color: "#1f9d57" }
      ]
    }
  );
  var AvecTrous = () => /* @__PURE__ */ (0, import_jsx_runtime.jsx)(
    ds_exports.LineTrend,
    {
      months,
      exportTitle: "Promptitude Tshuapa",
      series: [{ name: "Promptitude", data: [58, null, 62, null, 66, 61], color: "#7c3aed" }]
    }
  );
  return __toCommonJS(LineTrend_exports);
})();
