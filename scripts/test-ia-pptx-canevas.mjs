#!/usr/bin/env node
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import JSZip from 'jszip';

const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/(.:)/, '$1')), '..');
const HTML_PATH = path.join(ROOT, 'docs', 'index.html');
const SOURCE = process.argv[2] || 'C:\\Users\\felly\\Downloads\\CANEVAS DE LA REVUE SEMESTRIELLE S1 2026 ZS.pptx';
const OUT_DIR = path.join(os.tmpdir(), 'assistant-ia-pptx-pretest');
const OUT = path.join(OUT_DIR, 'CANEVAS_S1_2026_PRETEST_SCORE_QUALITE.pptx');
const EDGE = 'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe';

if (!fs.existsSync(SOURCE)) throw new Error('Canevas introuvable : ' + SOURCE);
if (!fs.existsSync(EDGE)) throw new Error('Microsoft Edge introuvable : ' + EDGE);
fs.mkdirSync(OUT_DIR, { recursive: true });

const sourceHtml = fs.readFileSync(HTML_PATH, 'utf8');
if (/IA_AGENT_(?:MAX|STANDARD|DOCUMENT)_MS/.test(sourceHtml) || /Analyse interrompue après \d+ minutes/.test(sourceHtml)) {
  throw new Error('Un délai global interdit est encore actif dans la boucle Assistant IA');
}
if (/IA_MAX_(?:KIMI_)?TOOL_TURNS/.test(sourceHtml) || !/while \(true\) \{\s*turns\+\+;/.test(sourceHtml)) {
  throw new Error('La boucle Assistant IA doit rester sans limite de durée ni de nombre d’étapes');
}
if (/Analyse interrompue : limite de/.test(sourceHtml) || /\+ turns \+ '\/' \+/.test(sourceHtml)) {
  throw new Error('L’interface Assistant IA expose encore une limite d’étapes');
}
if (/CANEVAS \/ MODÈLE \.pptx joint[^\n]+calculer toutes les valeurs DHIS2 avant écriture/.test(sourceHtml)) {
  throw new Error('Le prompt reporte encore toute l’écriture du PPTX après toutes les extractions');
}
function between(start, end) {
  const a = sourceHtml.indexOf(start);
  const b = sourceHtml.indexOf(end, a + start.length);
  if (a < 0 || b < 0) throw new Error('Bloc source introuvable : ' + start);
  return sourceHtml.slice(a, b);
}
const parseMaybe = between('function iaParseMaybe(v)', 'function iaFixFigure(fig)');
const cellHelpers = between('var IA_FEUX =', 'function iaCellText(cell)');
const scoreHelper = between('function iaScoreQualiteDps(o)', 'async function iaRunTool(name, input, msgsEl)');
const pptxBlock = between("var IA_P_NS = 'http://schemas.openxmlformats.org/presentationml/2006/main';", 'function iaScroll(el)');

const sampleRows = [
  ['Bita', 55, 'Faible', 70, 'Moyenne', '+15'],
  ['Bu', 60, 'Moyenne', 75, 'Moyenne', '+15'],
  ['Dumi', 45, 'Très faible', 65, 'Moyenne', '+20'],
  ['Kimpoko', 50, 'Faible', 60, 'Moyenne', '+10'],
  ['Kingankati', 70, 'Moyenne', 80, 'Performante', '+10'],
  ['Kingankati 2', 40, 'Très faible', 55, 'Faible', '+15'],
  ['Maluku', 65, 'Moyenne', 75, 'Moyenne', '+10'],
  ['Mangegenge', 45, 'Très faible', 60, 'Moyenne', '+15'],
  ['Mayindombe 1', 35, 'Très faible', 55, 'Faible', '+20'],
  ['Mayindombe 2', 40, 'Très faible', 50, 'Faible', '+10'],
  ['Menkao', 60, 'Moyenne', 70, 'Moyenne', '+10'],
  ['Mongata', 55, 'Faible', 65, 'Moyenne', '+10'],
  ['Nguma 1', 45, 'Très faible', 55, 'Faible', '+10'],
  ['Nguma 2', 40, 'Très faible', 50, 'Faible', '+10'],
  ['Tandala', 65, 'Moyenne', 75, 'Moyenne', '+10'],
  ['Yuo', 60, 'Moyenne', 70, 'Moyenne', '+10']
].map(r => r.map((v, i) => i === 1 || i === 3 ? { t: v, bg: v >= 80 ? 'vert' : v >= 60 ? 'jaune' : v >= 50 ? 'orange' : 'rouge' } : v));

let resolveResult, rejectResult;
const resultPromise = new Promise((res, rej) => { resolveResult = res; rejectResult = rej; });
const jszipBrowser = fs.readFileSync(path.join(ROOT, 'node_modules', 'jszip', 'dist', 'jszip.min.js'));
const sourceBytes = fs.readFileSync(SOURCE);
const page = `<!doctype html><meta charset="utf-8"><title>Prétest PPTX</title><pre id="status">Prétest…</pre><script src="/jszip.js"></script><script>
var IA={docs:{},pptxBaseline:{}}, captured=null;
var IA_A_NS='http://schemas.openxmlformats.org/drawingml/2006/main';
var IA_R_NS='http://schemas.openxmlformats.org/officeDocument/2006/relationships';
function iaEnsureZip(){return Promise.resolve();}
function iaDocxImages(doc){var out=[],bs=doc.getElementsByTagNameNS(IA_A_NS,'blip');for(var i=0;i<bs.length;i++){var id=bs[i].getAttributeNS(IA_R_NS,'embed');if(id)out.push({el:bs[i],qname:'r:embed',rid:id});}return out;}
function iaDocxRenderPng(){throw new Error('rendu image inattendu dans ce test');}
function iaPngDims(){return null;} function iaStripData(v){return v;}
function E(){return null;} function esc(v){return String(v);} function iaScroll(){}
function iaDlBlob(name,mime,blob){captured={name:name,mime:mime,blob:blob};}
${parseMaybe}
${cellHelpers}
${scoreHelper}
${pptxBlock}
(async function(){
 try{
  var ab=await (await fetch('/source.pptx')).arrayBuffer();
  var name='CANEVAS DE LA REVUE SEMESTRIELLE S1 2026 ZS.pptx'; IA.docs[name]=ab;
  var s1=iaScoreQualiteDps({completude:96,promptitude:88,datasets_atteints:8,datasets_applicables:10,rvv:1,rapports_recus:100,rvv_corrigees:true});
  var s2=iaScoreQualiteDps({completude:96,promptitude:88,datasets_atteints:8,datasets_applicables:10,rvv:1,rapports_recus:100});
  if(s1.total!==85||s2.total!==null)throw new Error('calculateur score qualité invalide');
  var refused=await iaPptxModify({fichier:name,structure_stricte:true,autoriser_nouvelles_diapos:false,operations:[{op:'nouvelle_diapositive',apres:35,titre:'INTERDITE'}]},null);
  if(refused.indexOf('ÉCHEC TRANSACTIONNEL')!==0)throw new Error('le verrou structurel n’a pas refusé la nouvelle diapositive : '+refused);
  var msg=await iaPptxModify({fichier:name,nom_sortie:'CANEVAS_S1_2026_PRETEST_SCORE_QUALITE',structure_stricte:true,autoriser_nouvelles_diapos:false,autoriser_ajout_visuel:false,operations:[{op:'remplacer_forme_par_tableau',d:35,f:5,colonnes:['Aire de santé','Score S1 2025','Catégorie 2025','Score S1 2026','Catégorie 2026','Écart'],largeurs:[2.1,1,1.25,1,1.25,.75],lignes:${JSON.stringify(sampleRows)}}],controles:[{d:35,min_tableaux:1,max_images:0,max_graphiques:0,texte_absent:'Mettre un tableau comparatif'}]},null);
  if(msg.indexOf('OK :')!==0||!captured)throw new Error('livraison refusée : '+msg);
  var out=await captured.blob.arrayBuffer();
  var r=await fetch('/result',{method:'POST',headers:{'x-result-message':encodeURIComponent(msg),'content-type':'application/octet-stream'},body:out});
  if(!r.ok)throw new Error('envoi résultat HTTP '+r.status);
  document.getElementById('status').textContent=msg;
 }catch(e){document.getElementById('status').textContent='FAIL '+(e&&e.stack||e);fetch('/error',{method:'POST',body:String(e&&e.stack||e)});}
})();
</script>`;

const server = http.createServer((req, res) => {
  if (req.url === '/test') { res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' }); return res.end(page); }
  if (req.url === '/jszip.js') { res.writeHead(200, { 'content-type': 'application/javascript' }); return res.end(jszipBrowser); }
  if (req.url === '/source.pptx') { res.writeHead(200, { 'content-type': 'application/vnd.openxmlformats-officedocument.presentationml.presentation' }); return res.end(sourceBytes); }
  if (req.url === '/result' && req.method === 'POST') {
    const chunks=[]; req.on('data', c=>chunks.push(c)); req.on('end',()=>{const b=Buffer.concat(chunks);fs.writeFileSync(OUT,b);res.end('ok');resolveResult({bytes:b,message:decodeURIComponent(req.headers['x-result-message']||'')});}); return;
  }
  if (req.url === '/error' && req.method === 'POST') { const chunks=[];req.on('data',c=>chunks.push(c));req.on('end',()=>{res.end('ok');rejectResult(new Error(Buffer.concat(chunks).toString('utf8')));});return; }
  res.writeHead(404); res.end('not found');
});

await new Promise(res => server.listen(0, '127.0.0.1', res));
const port = server.address().port;
const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'ia-pptx-edge-'));
const edge = spawn(EDGE, ['--headless=new','--disable-gpu','--no-first-run','--no-default-browser-check','--user-data-dir='+profile,'http://127.0.0.1:'+port+'/test'], { stdio: 'ignore' });
const timer = setTimeout(() => rejectResult(new Error('Prétest navigateur expiré')), 60000);
let result;
try { result = await resultPromise; } finally { clearTimeout(timer); edge.kill(); server.close(); }

const original = await JSZip.loadAsync(sourceBytes);
const produced = await JSZip.loadAsync(result.bytes);
function slideParts(zip) { return Object.keys(zip.files).filter(n => /^ppt\/slides\/slide\d+\.xml$/.test(n)).sort((a,b)=>+a.match(/\d+/)[0]-+b.match(/\d+/)[0]); }
const beforeParts=slideParts(original), afterParts=slideParts(produced);
if(beforeParts.length!==49||afterParts.length!==49)throw new Error('Nombre de diapositives incorrect : '+beforeParts.length+' -> '+afterParts.length);
const changed=[];
for(const p of beforeParts){const a=await original.file(p).async('nodebuffer'),b=await produced.file(p).async('nodebuffer');if(!a.equals(b))changed.push(p);}
if(changed.length!==1||changed[0]!=='ppt/slides/slide35.xml')throw new Error('Diapositives hors périmètre modifiées : '+changed.join(', '));
const scoreXml=await produced.file('ppt/slides/slide35.xml').async('string');
if(!/<a:tbl>/.test(scoreXml)||/Mettre un tableau comparatif/i.test(scoreXml))throw new Error('Le tableau score qualité n’a pas remplacé la consigne');
const ptfBefore=await original.file('ppt/slides/slide6.xml').async('nodebuffer'),ptfAfter=await produced.file('ppt/slides/slide6.xml').async('nodebuffer');
if(!ptfBefore.equals(ptfAfter))throw new Error('La diapositive PTF a été modifiée');
const hash=b=>crypto.createHash('sha256').update(b).digest('hex').slice(0,12);
console.log('PASS — aucune limite globale de durée ou d’étapes, verrou nouvelle diapositive, score DPS, insertion tableau et contrôles finaux');
console.log('slides=49 -> 49; seule slide35.xml modifiée; PTF identique; sha256='+hash(result.bytes));
console.log('output='+OUT);
