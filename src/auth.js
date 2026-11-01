'use strict';
const crypto=require('node:crypto');
const ALPHABET='ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
function randomSecret(bytes=20){const buf=crypto.randomBytes(bytes);let out='',acc=0,bits=0;for(const b of buf){acc=(acc<<8)|b;bits+=8;while(bits>=5){out+=ALPHABET[(acc>>>(bits-=5))&31];}}if(bits>0)out+=ALPHABET[(acc<<(5-bits))&31];return out;}
function decode32(s){s=String(s).toUpperCase().replace(/\s|=|-/g,'');let acc=0,bits=0,res=[];for(const c of s){const n=ALPHABET.indexOf(c);if(n<0)throw Error('Invalid TOTP secret');acc=(acc<<5)|n;bits+=5;if(bits>=8){res.push((acc>>>(bits-=8))&255);}}return Buffer.from(res);}
function totp(secret,epoch=Date.now()){const counter=Buffer.alloc(8);counter.writeBigUInt64BE(BigInt(Math.floor(epoch/30000)));const digest=crypto.createHmac('sha1',decode32(secret)).update(counter).digest();const off=digest[digest.length-1]&15;return String((digest.readUInt32BE(off)&0x7fffffff)%1000000).padStart(6,'0');}
function checkTotp(secret,code,lastStep=-1,now=Date.now()){if(!/^\d{6}$/.test(String(code)))return -1;const step=Math.floor(now/30000);for(let n=-1;n<=1;n++){const idx=step+n;if(idx>lastStep&&crypto.timingSafeEqual(Buffer.from(totp(secret,idx*30000)),Buffer.from(String(code))))return idx;}return -1;}
function passwordHash(password,salt=crypto.randomBytes(16).toString('hex')){return {salt,hash:crypto.scryptSync(String(password),Buffer.from(salt,'hex'),64).toString('hex')};}
function passwordMatches(password,rec){if(!rec||!rec.salt||!rec.hash)return false;const calculated=passwordHash(password,rec.salt).hash;return crypto.timingSafeEqual(Buffer.from(calculated,'hex'),Buffer.from(rec.hash,'hex'));}
function newToken(){return crypto.randomBytes(32).toString('hex');}
function hashToken(token){return crypto.createHash('sha256').update(String(token)).digest('hex');}
function cookie(token,secure){return `hussam_session=${token}; Path=/; HttpOnly; SameSite=Strict; Max-Age=43200${secure?'; Secure':''}`;}
module.exports={randomSecret,totp,checkTotp,passwordHash,passwordMatches,newToken,hashToken,cookie};
