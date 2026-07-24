#!/usr/bin/env node
/**
 * Synchronisation Tableau Cloud → dashboard SNIS (onglet « Plan Mashako 3.0 »).
 *
 * Se connecte à Tableau Cloud (site axdata) avec un Personal Access Token,
 * localise le classeur « Mashako3_0RapportdelAntenne », puis exporte pour
 * chaque vue les données résumées (CSV → JSON) et une image PNG de la vue
 * principale. Les fichiers sont écrits dans docs/data/mashako/ — publiés
 * ensuite sur GitHub Pages, d'où le dashboard (Vercel/Pages) les lit.
 *
 * Effet secondaire voulu : chaque exécution compte comme une connexion du
 * compte Tableau → la licence « Viewer » (révoquée après 1 mois d'inactivité)
 * reste active tant que ce job tourne.
 *
 * Env requis : TABLEAU_PAT_NAME, TABLEAU_PAT_SECRET
 * Env optionnels : TABLEAU_SERVER (défaut eu-west-1a.online.tableau.com),
 *                  TABLEAU_SITE (défaut axdata),
 *                  TABLEAU_WORKBOOK (défaut Mashako3_0RapportdelAntenne),
 *                  TABLEAU_MAIN_VIEW (défaut HZScores_ANT)
 */
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

const SERVER = process.env.TABLEAU_SERVER || "eu-west-1a.online.tableau.com";
const SITE = process.env.TABLEAU_SITE || "axdata";
const WORKBOOK = process.env.TABLEAU_WORKBOOK || "Mashako3_0RapportdelAntenne";
const MAIN_VIEW = process.env.TABLEAU_MAIN_VIEW || "HZScores_ANT";
const PAT_NAME = process.env.TABLEAU_PAT_NAME;
const PAT_SECRET = process.env.TABLEAU_PAT_SECRET;
const API = `https://${SERVER}/api/3.22`;
const OUT = path.join(process.cwd(), "docs", "data", "mashako");

if (!PAT_NAME || !PAT_SECRET) {
  console.error("✖ TABLEAU_PAT_NAME / TABLEAU_PAT_SECRET manquants.");
  process.exit(1);
}

async function api(pathname, opts = {}, token) {
  const res = await fetch(`${API}${pathname}`, {
    ...opts,
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      ...(token ? { "X-Tableau-Auth": token } : {}),
      ...(opts.headers || {}),
    },
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`HTTP ${res.status} sur ${pathname} : ${body.slice(0, 400)}`);
  }
  return res;
}

/* Parse CSV (guillemets, virgules et sauts de ligne dans les champs). */
function parseCsv(text) {
  const rows = [];
  let row = [], field = "", inQ = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQ) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++; }
        else inQ = false;
      } else field += c;
    } else if (c === '"') inQ = true;
    else if (c === ",") { row.push(field); field = ""; }
    else if (c === "\n" || c === "\r") {
      if (c === "\r" && text[i + 1] === "\n") i++;
      row.push(field); field = "";
      if (row.length > 1 || row[0] !== "") rows.push(row);
      row = [];
    } else field += c;
  }
  if (field !== "" || row.length) { row.push(field); rows.push(row); }
  return rows;
}

function csvToRecords(text) {
  const rows = parseCsv(text);
  if (!rows.length) return { columns: [], rows: [] };
  const columns = rows[0];
  const records = rows.slice(1).map((r) => {
    const o = {};
    columns.forEach((c, i) => { o[c] = r[i] ?? ""; });
    return o;
  });
  return { columns, rows: records };
}

async function main() {
  console.log(`→ Connexion à ${SERVER} (site ${SITE})…`);
  const signin = await api("/auth/signin", {
    method: "POST",
    body: JSON.stringify({
      credentials: {
        personalAccessTokenName: PAT_NAME,
        personalAccessTokenSecret: PAT_SECRET,
        site: { contentUrl: SITE },
      },
    }),
  });
  const cred = (await signin.json()).credentials;
  const token = cred.token;
  const siteId = cred.site.id;
  console.log(`✓ Connecté (site id ${siteId}). Cette connexion maintient la licence active.`);

  // Classeur
  const wbRes = await api(
    `/sites/${siteId}/workbooks?filter=contentUrl:eq:${encodeURIComponent(WORKBOOK)}`,
    {}, token
  );
  const wbs = (await wbRes.json()).workbooks?.workbook || [];
  if (!wbs.length) throw new Error(`Classeur « ${WORKBOOK} » introuvable sur le site ${SITE}.`);
  const wb = wbs[0];
  console.log(`✓ Classeur : ${wb.name} (${wb.id})`);

  // Vues du classeur
  const vRes = await api(`/sites/${siteId}/workbooks/${wb.id}/views`, {}, token);
  const views = (await vRes.json()).views?.view || [];
  if (!views.length) throw new Error("Aucune vue dans le classeur.");
  console.log(`✓ ${views.length} vue(s) : ${views.map((v) => v.viewUrlName).join(", ")}`);

  await mkdir(path.join(OUT, "views"), { recursive: true });

  const metaViews = [];
  for (const v of views) {
    try {
      const dRes = await api(
        `/sites/${siteId}/views/${v.id}/data?maxAge=1`,
        { headers: { Accept: "text/csv" } }, token
      );
      const csv = await dRes.text();
      const data = csvToRecords(csv);
      const file = `views/${v.viewUrlName}.json`;
      await writeFile(
        path.join(OUT, file),
        JSON.stringify({ name: v.name, urlName: v.viewUrlName, ...data })
      );
      metaViews.push({ name: v.name, urlName: v.viewUrlName, rows: data.rows.length, file });
      console.log(`  ✓ ${v.viewUrlName} : ${data.rows.length} lignes`);
    } catch (e) {
      console.warn(`  ⚠ ${v.viewUrlName} : export données impossible (${e.message})`);
      metaViews.push({ name: v.name, urlName: v.viewUrlName, rows: 0, file: null });
    }
  }

  // Image PNG de la vue principale (aperçu fidèle du dashboard original)
  const main = views.find((v) => v.viewUrlName === MAIN_VIEW) || views[0];
  try {
    const iRes = await api(
      `/sites/${siteId}/views/${main.id}/image?resolution=high&maxAge=1`,
      { headers: { Accept: "image/png" } }, token
    );
    await writeFile(path.join(OUT, "snapshot.png"), Buffer.from(await iRes.arrayBuffer()));
    console.log(`✓ Aperçu PNG : ${main.viewUrlName}`);
  } catch (e) {
    console.warn(`⚠ Aperçu PNG impossible : ${e.message}`);
  }

  await writeFile(
    path.join(OUT, "meta.json"),
    JSON.stringify({
      generated_at: new Date().toISOString(),
      server: SERVER, site: SITE,
      workbook: { name: wb.name, contentUrl: WORKBOOK },
      main_view: main.viewUrlName,
      original_url: `https://${SERVER}/#/site/${SITE}/views/${WORKBOOK}/${MAIN_VIEW}`,
      views: metaViews,
    }, null, 2)
  );

  await api("/auth/signout", { method: "POST" }, token).catch(() => {});
  console.log(`✓ Terminé — données dans ${OUT}`);
}

main().catch((e) => { console.error(`✖ ${e.message}`); process.exit(1); });
