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

  // .design-sync/previews/DataTable.tsx
  var DataTable_exports = {};
  __export(DataTable_exports, {
    AvecExport: () => AvecExport,
    AvecHeatmap: () => AvecHeatmap,
    CouvertureParProvince: () => CouvertureParProvince,
    SansDonnees: () => SansDonnees
  });
  init_define_import_meta_env();

  // ds-shim:ds
  var ds_exports = {};
  __export(ds_exports, {
    default: () => ds_default
  });
  init_define_import_meta_env();
  __reExport(ds_exports, __toESM(require_ds_raw()));
  var g = window.SnisPev;
  var ds_default = "default" in g ? g.default : g;

  // .design-sync/previews/DataTable.tsx
  var import_jsx_runtime = __toESM(require_react_shim());
  var columns = ["Province", "Complétude (%)", "Promptitude (%)", "Penta3 (%)", "VAR1 (%)"];
  var rows = [
    { "Province": "Kinshasa", "Complétude (%)": 96.4, "Promptitude (%)": 88.1, "Penta3 (%)": 92.7, "VAR1 (%)": 89.3 },
    { "Province": "Kongo Central", "Complétude (%)": 93.2, "Promptitude (%)": 76.5, "Penta3 (%)": 85.1, "VAR1 (%)": 81.9 },
    { "Province": "Tshopo", "Complétude (%)": 88.7, "Promptitude (%)": 64.2, "Penta3 (%)": 71.4, "VAR1 (%)": 68 },
    { "Province": "Tshuapa", "Complétude (%)": 84.5, "Promptitude (%)": 58.9, "Penta3 (%)": 66.2, "VAR1 (%)": 61.7 },
    { "Province": "Nord-Kivu", "Complétude (%)": 91.8, "Promptitude (%)": 72.3, "Penta3 (%)": 78.6, "VAR1 (%)": 74.4 },
    { "Province": "Haut-Katanga", "Complétude (%)": 95.1, "Promptitude (%)": 83.7, "Penta3 (%)": 90.2, "VAR1 (%)": 86.5 }
  ];
  var heat = (col, v) => col === "Province" || typeof v !== "number" ? null : v >= 90 ? "good" : v >= 70 ? "warn" : "bad";
  var CouvertureParProvince = () => /* @__PURE__ */ (0, import_jsx_runtime.jsx)(ds_exports.DataTable, { columns, rows });
  var AvecHeatmap = () => /* @__PURE__ */ (0, import_jsx_runtime.jsx)(ds_exports.DataTable, { columns, rows, heat });
  var AvecExport = () => /* @__PURE__ */ (0, import_jsx_runtime.jsx)(ds_exports.DataTable, { columns, rows, exportFilename: "couverture_par_province", heat });
  var SansDonnees = () => /* @__PURE__ */ (0, import_jsx_runtime.jsx)(ds_exports.DataTable, { columns, rows: [] });
  return __toCommonJS(DataTable_exports);
})();
