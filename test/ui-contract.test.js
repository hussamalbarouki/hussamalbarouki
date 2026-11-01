'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const app = fs.readFileSync(path.join(__dirname, '..', 'public', 'app.js'), 'utf8');
const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'index.html'), 'utf8');

test('all statically referenced dashboard element IDs exist in HTML', () => {
 const usedIds = new Set([...app.matchAll(/\$\('([^']+)'\)/g)].map(m => m[1]));
 const availableIds = new Set([...html.matchAll(/\bid=["']([^"']+)["']/g)].map(m => m[1]));
 // closeDetail is generated dynamically inside the event-detail template.
 const dynamicIds = new Set(['closeDetail']);
 const missing = [...usedIds].filter(id => !availableIds.has(id) && !dynamicIds.has(id));
 assert.deepEqual(missing, [], 'Missing static IDs: ' + missing.join(', '));
 assert.ok(availableIds.has('shFeed'));
});

test('data-fetch and rendering failures use distinct diagnostics', () => {
 assert.match(app, /Status API failure/);
 assert.match(app, /Dashboard render failure/);
});
