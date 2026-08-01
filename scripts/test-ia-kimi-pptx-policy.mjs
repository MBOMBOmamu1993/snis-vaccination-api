#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
const PROXY = 'https://pev-ia-proxy.pev-rdc.workers.dev';
const html = fs.readFileSync(path.join(ROOT, 'docs', 'index.html'), 'utf8');
const grab = re => { const m=html.match(re); if(!m)throw new Error('bloc introuvable: '+re); return m[1]||m[0]; };
const sandbox={}; globalThis.sandbox=sandbox; globalThis.S={data:[],vals:{},filters:{}};globalThis.AG=[];globalThis.IA_TOOL_RESULT_MAX=8000;
(0,eval)(grab(/function iaSystem\(\) \{[\s\S]*?\n {12}\}/).replace('function iaSystem()','sandbox.iaSystem = function ()'));
(0,eval)(grab(/var IA_TOOLS = \[[\s\S]*?\n {12}\];/).replace('var IA_TOOLS','sandbox.IA_TOOLS'));

const trial=await fetch(PROXY+'/essai',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({deviceId:'codex-pptx-policy-pretest-20260801'})});
if(!trial.ok)throw new Error('Essai HTTP '+trial.status+' : '+(await trial.text()).slice(0,300));
const trialData=await trial.json(); const code=trialData.code;
if(!code)throw new Error('Code de prétest absent');

const attachment=`<fichier nom="CANEVAS DE LA REVUE SEMESTRIELLE S1 2026 ZS.pptx" note="Canevas strict : 49 diapositives, même ordre obligatoire, parties hors DHIS2 inchangées.">
=== [D6] — 1 tableau(x)
[D6:F3] 1. 2. CARTOGRAPHIE DE PTF EN 2026
[D6:T1] tableau PTF volontairement vide — HORS DHIS2, ne pas toucher
=== [D35]
[D35:F3] III. 3. Score qualité des données
[D35:F5] Mettre un tableau comparatif du score qualité S1 2025 – S1 2026 par AS
=== [D49]
[D49:F2] Merci
</fichier>`;
const prompt=`À partir de la présentation PowerPoint ci-jointe, produire une version actualisée S1 2026 pour la Zone de Santé de Maluku I, comparée au S1 2025, exclusivement avec DHIS2. Respecter strictement la structure et la mise en page originales. Ne rien modifier hors DHIS2. La rubrique PTF vide doit rester telle quelle. Ne pas ajouter de graphiques ni de diapositives qui ne sont pas prévus. La diapositive Score qualité doit contenir le tableau comparatif par AS demandé dans le canevas. Voici l'extraction du fichier :\n${attachment}`;
const messages=[{role:'system',content:sandbox.iaSystem()},{role:'user',content:prompt}];
let sawModify=false, sawForbidden=false, sawCorrectTable=false, sawControls=false, calls=[];
const fixture={source:'PRETEST_FIXTURE_DHIS2',zone:'Maluku I',periodes:['2025S1','2026S1'],aires:[
 {as:'Bita',s2025:55,c2025:'Faible',s2026:70,c2026:'Moyenne',ecart:15},
 {as:'Bu',s2025:60,c2025:'Moyenne',s2026:75,c2026:'Moyenne',ecart:15},
 {as:'Dumi',s2025:45,c2025:'Très faible',s2026:65,c2026:'Moyenne',ecart:20}
],note:'Fixture compacte du prétest de politique. Les 5 critères ont déjà été calculés avec ctx.scoreQualiteDps.'};

for(let turn=1;turn<=6;turn++){
 const r=await fetch(PROXY+'/kimi/v1/chat/completions',{method:'POST',headers:{'content-type':'application/json','x-access-code':code},body:JSON.stringify({model:'kimi-k3',stream:false,temperature:1,reasoning_effort:'low',max_tokens:6000,messages,tools:sandbox.IA_TOOLS})});
 if(!r.ok)throw new Error('Kimi HTTP '+r.status+' : '+(await r.text()).slice(0,500));
 const d=await r.json(); const m=d.choices?.[0]?.message||d.message||{}; const tcs=m.tool_calls||[];
 const assistant={role:'assistant',content:m.content||''}; if(m.reasoning_content)assistant.reasoning_content=m.reasoning_content;if(tcs.length)assistant.tool_calls=tcs;messages.push(assistant);
 if(!tcs.length)break;
 for(const tc of tcs){
  const name=tc.function?.name||'';let a=tc.function?.arguments||{};if(typeof a==='string'){try{a=JSON.parse(a)}catch{a={}}}calls.push(name);
  let out;
  if(name==='generer_rapport'){sawForbidden=true;out='REFUSÉ : un PPTX est joint.';}
  else if(name==='modifier_presentation'){
   sawModify=true;const ops=Array.isArray(a.operations)?a.operations:[];
   if(a.structure_stricte!==true||a.autoriser_nouvelles_diapos!==false||a.autoriser_ajout_visuel!==false)sawForbidden=true;
   if(ops.some(o=>['nouvelle_diapositive','dupliquer_diapositive','supprimer_diapositive','paginer_tableau','inserer_image'].includes(o.op)))sawForbidden=true;
   sawCorrectTable ||= ops.some(o=>o.op==='remplacer_forme_par_tableau'&&o.d===35&&o.f===5&&Array.isArray(o.lignes));
   sawControls ||= Array.isArray(a.controles)&&a.controles.some(c=>c.d===35&&c.min_tableaux>=1&&/Mettre un tableau/i.test(c.texte_absent||''));
   out='OK : lot de prétest accepté, présentation téléchargée chez l’utilisateur — structure d’origine contrôlée, contrôles finaux réussis.';
  } else if(name==='executer_js'||name==='requete_dhis2') out=JSON.stringify(fixture);
  else if(name==='afficher_resultat') out='OK : résultat de prétest affiché.';
  else out='Prétest : outil non nécessaire pour ce scénario.';
  messages.push({role:'tool',tool_call_id:tc.id,name,content:out});
 }
 if(sawModify&&sawCorrectTable&&sawControls)break;
}
if(!sawModify||!sawCorrectTable||!sawControls||sawForbidden)throw new Error('FAIL politique Kimi : '+JSON.stringify({sawModify,sawCorrectTable,sawControls,sawForbidden,calls}));
console.log('PASS — Kimi K3 choisit modifier_presentation, conserve 49 diapositives et insère le tableau score qualité dans D35:F5 avec contrôles bloquants.');
console.log('tool_calls='+calls.join(' -> '));
