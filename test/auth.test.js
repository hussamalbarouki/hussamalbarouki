'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const a=require('../src/auth');
test('TOTP generation and verify near clock drift',()=>{const secret='JBSWY3DPEHPK3PXP',t=1700000000000,code=a.totp(secret,t);assert.equal(a.checkTotp(secret,code,-1,t),Math.floor(t/30000));assert.equal(a.checkTotp(secret,code,Math.floor(t/30000),t),-1);assert.equal(a.checkTotp(secret,'99999',-1,t),-1);});
test('password salted scrypt hash',()=>{const h=a.passwordHash('some-very-long-password');assert.ok(a.passwordMatches('some-very-long-password',h));assert.equal(a.passwordMatches('wrong-password',h),false);assert.notEqual(h.hash,a.passwordHash('some-very-long-password').hash);});
test('session token hashes do not reveal raw tokens',()=>{const t=a.newToken();assert.equal(t.length,64);assert.notEqual(t,a.hashToken(t));assert.ok(a.cookie(t,true).includes('HttpOnly; SameSite=Strict'));assert.ok(a.cookie(t,true).includes('; Secure'));});
