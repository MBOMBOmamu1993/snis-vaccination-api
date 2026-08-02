import fs from 'node:fs/promises';
import assert from 'node:assert/strict';

const worker = await fs.readFile(new URL('../cloudflare-worker/worker.js', import.meta.url), 'utf8');
const offersBlock = worker.match(/const OFFERS = \[([\s\S]*?)\n\];/)?.[1] || '';
const amounts = [...offersBlock.matchAll(/amount:\s*(\d+)/g)].map((m) => Number(m[1]));
const requests = [...offersBlock.matchAll(/requests:\s*(\d+)/g)].map((m) => Number(m[1]));

assert.deepEqual(amounts, [20, 30, 40, 50, 100]);
assert.deepEqual(requests, [200, 300, 400, 500, 1000]);
assert.match(worker, /const CUSTOM_MIN_USD = 10;/);
assert.match(worker, /min="\$\{CUSTOM_MIN_USD\}"/);
assert.match(worker, /if \(!\(usd >= CUSTOM_MIN_USD && usd <= CUSTOM_MAX_USD\)\)/);
assert.doesNotMatch(offersBlock, /amount:\s*(?:5|10)\b/);

console.log('OK — achat client : offres 20/30/40/50/100 USD et montant libre minimum 10 USD.');
