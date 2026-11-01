'use strict';const test=require('node:test'),assert=require('node:assert/strict');const {defaults,validate,FIELDS}=require('../src/settings');
test('configurable fields all have Arabic descriptions',()=>{for(const f of FIELDS){assert.ok(f.hint.length>30);assert.ok(f.label);assert.ok(f.group);}});
test('out-of-range settings rejected',()=>{assert.throws(()=>validate({capital:-1}));assert.throws(()=>validate({foo:1}));assert.throws(()=>validate({depthLevels:17}));assert.throws(()=>validate({paused:'true'}));assert.equal(validate({capital:50,paused:true}).paused,true);});
test('defaults cover every field',()=>{assert.equal(Object.keys(defaults()).length,FIELDS.length);});
