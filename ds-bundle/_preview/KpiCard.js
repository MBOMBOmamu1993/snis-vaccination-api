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

  // .design-sync/previews/KpiCard.tsx
  var KpiCard_exports = {};
  __export(KpiCard_exports, {
    AvecRealisation: () => AvecRealisation,
    IndicateursQualite: () => IndicateursQualite,
    Tons: () => Tons
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

  // .design-sync/previews/KpiCard.tsx
  var import_jsx_runtime = __toESM(require_react_shim());
  var IndicateursQualite = () => /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", { style: { display: "grid", gridTemplateColumns: "repeat(2, minmax(200px, 1fr))", gap: 12 }, children: [
    /* @__PURE__ */ (0, import_jsx_runtime.jsx)(ds_exports.KpiCard, { label: "Complétude", value: "94,2 %", tone: "good", icon: "clipboard", sub: "1 842 / 1 955 rapports attendus" }),
    /* @__PURE__ */ (0, import_jsx_runtime.jsx)(ds_exports.KpiCard, { label: "Promptitude", value: "78,6 %", tone: "warn", icon: "time", sub: "Rapports transmis avant le 5 du mois" }),
    /* @__PURE__ */ (0, import_jsx_runtime.jsx)(ds_exports.KpiCard, { label: "Abandon Penta1 → Penta3", value: "12,4 %", tone: "bad", icon: "alert", sub: "Seuil OMS : < 10 %" }),
    /* @__PURE__ */ (0, import_jsx_runtime.jsx)(ds_exports.KpiCard, { label: "Enfants vaccinés Penta3", value: "128 450", tone: "navy", icon: "syringe", pct: 87.3 })
  ] });
  var Tons = () => /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", { style: { display: "grid", gridTemplateColumns: "repeat(4, minmax(150px, 1fr))", gap: 10 }, children: [
    /* @__PURE__ */ (0, import_jsx_runtime.jsx)(ds_exports.KpiCard, { label: "Neutral", value: "1 955", tone: "neutral", icon: "doc", sub: "Aires de santé" }),
    /* @__PURE__ */ (0, import_jsx_runtime.jsx)(ds_exports.KpiCard, { label: "Navy", value: "26", tone: "navy", icon: "map", sub: "Provinces DPS" }),
    /* @__PURE__ */ (0, import_jsx_runtime.jsx)(ds_exports.KpiCard, { label: "Good", value: "91 %", tone: "good", icon: "check", sub: "Objectif atteint" }),
    /* @__PURE__ */ (0, import_jsx_runtime.jsx)(ds_exports.KpiCard, { label: "Warn", value: "74 %", tone: "warn", icon: "alert", sub: "Sous le seuil" }),
    /* @__PURE__ */ (0, import_jsx_runtime.jsx)(ds_exports.KpiCard, { label: "Bad", value: "52 %", tone: "bad", icon: "down", sub: "Action requise" }),
    /* @__PURE__ */ (0, import_jsx_runtime.jsx)(ds_exports.KpiCard, { label: "Brand", value: "87 %", tone: "brand", icon: "shield", sub: "Couverture VAR" }),
    /* @__PURE__ */ (0, import_jsx_runtime.jsx)(ds_exports.KpiCard, { label: "Violet", value: "63 410", tone: "violet", icon: "child", sub: "Doses BCG" }),
    /* @__PURE__ */ (0, import_jsx_runtime.jsx)(ds_exports.KpiCard, { label: "Teal", value: "4 812", tone: "teal", icon: "truck", sub: "Stratégie avancée" })
  ] });
  var AvecRealisation = () => /* @__PURE__ */ (0, import_jsx_runtime.jsxs)("div", { style: { display: "grid", gridTemplateColumns: "repeat(2, minmax(200px, 1fr))", gap: 12 }, children: [
    /* @__PURE__ */ (0, import_jsx_runtime.jsx)(ds_exports.KpiCard, { label: "Doses Penta3 administrées", value: "128 450", tone: "brand", icon: "syringe", pct: 87.3 }),
    /* @__PURE__ */ (0, import_jsx_runtime.jsx)(ds_exports.KpiCard, { label: "Doses VAR administrées", value: "96 204", tone: "violet", icon: "shield", pct: null })
  ] });
  return __toCommonJS(KpiCard_exports);
})();
