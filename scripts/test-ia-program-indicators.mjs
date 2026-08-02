import fs from 'node:fs/promises';
import assert from 'node:assert/strict';

const html = await fs.readFile(new URL('../docs/index.html', import.meta.url), 'utf8');

assert.match(html, /async function iaProgramIndicatorDirect\(input\)/, 'helper programIndicatorDirect absent');
assert.match(html, /analytics\/events\/aggregate\//, 'endpoint événementiel absent');
assert.match(html, /name: 'calculer_indicateur_programme_direct'/, 'tool programIndicator absent');
assert.match(html, /programIndicatorDirect: iaProgramIndicatorDirect/, 'helper non exposé dans ctx');
assert.match(html, /ils sont ACTIFS et doivent être contrôlés/, 'consigne active absente');
assert.doesNotMatch(html, /programmes à événements ABANDONNÉS, sans données/, 'ancienne consigne erronée encore présente');

console.log('OK — indicateurs de programme EVENT : endpoint, tool, ctx et consigne vérifiés.');
