'use strict';
const path=require('node:path'),fs=require('node:fs');const {DatabaseSync}=require('node:sqlite');
class Store{
 constructor(dir){fs.mkdirSync(dir,{recursive:true,mode:0o700});this.db=new DatabaseSync(path.join(dir,'hussam-monitor.sqlite'));this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;
 CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY,v TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS sessions(token_hash TEXT PRIMARY KEY,csrf TEXT NOT NULL, expires INTEGER NOT NULL);
 CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT,at INTEGER NOT NULL,route TEXT NOT NULL,outcome TEXT NOT NULL,preview REAL,first REAL,second REAL,net_pct REAL,elapsed INTEGER,reason TEXT,detail TEXT);
 CREATE INDEX IF NOT EXISTS event_by_date ON events(at DESC);
 CREATE INDEX IF NOT EXISTS event_by_outcome ON events(outcome,at DESC);`);
 }
 get(k){const r=this.db.prepare('SELECT v FROM kv WHERE k=?').get(k);return r?JSON.parse(r.v):null;}
 set(k,value){this.db.prepare('INSERT INTO kv(k,v) VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v').run(k,JSON.stringify(value));}
 addSession(hash,csrf,expires){this.db.prepare('INSERT INTO sessions(token_hash,csrf,expires) VALUES (?,?,?)').run(hash,csrf,expires);}
 session(hash){const r=this.db.prepare('SELECT * FROM sessions WHERE token_hash=? AND expires>?').get(hash,Date.now());return r||null;}
 revoke(hash){this.db.prepare('DELETE FROM sessions WHERE token_hash=?').run(hash);}
 revokeAll(){this.db.prepare('DELETE FROM sessions').run();}
 insert(e){const result=this.db.prepare('INSERT INTO events(at,route,outcome,preview,first,second,net_pct,elapsed,reason,detail) VALUES (?,?,?,?,?,?,?,?,?,?)').run(e.at,e.route,e.outcome,e.previewUsd??null,e.firstUsd??null,e.secondUsd??null,e.netPct??null,e.elapsedMs??null,e.reason||'',JSON.stringify(e.detail||{}));return Number(result.lastInsertRowid);}
 events(limit=60,offset=0,filter='all'){const w=filter==='all'?'':'AND outcome=?';const params=filter==='all'?[limit,offset]:[filter,limit,offset];return this.db.prepare(`SELECT id,at,route,outcome,preview as previewUsd,first as firstUsd,second as secondUsd,net_pct as netPct,elapsed as elapsedMs,reason,detail FROM events WHERE 1=1 ${w} ORDER BY at DESC,id DESC LIMIT ? OFFSET ?`).all(...params).map(r=>({...r,detail:JSON.parse(r.detail||'{}')}));}
 bestQualified(){const r=this.db.prepare("SELECT id,at,route,preview as previewUsd,first as firstUsd,second as secondUsd,net_pct as netPct,elapsed as elapsedMs FROM events WHERE outcome='qualified' ORDER BY second DESC LIMIT 1").get();return r||null;}
 summary(){const since=Date.now()-86400000;
  const d=this.db.prepare(`SELECT COUNT(*) inspected, SUM(CASE WHEN outcome='qualified' THEN 1 ELSE 0 END) qualified,MAX(CASE WHEN outcome='qualified' THEN second END) best,AVG(CASE WHEN outcome='qualified' THEN second END) average FROM events WHERE at>=?`).get(since);
  const causes=this.db.prepare('SELECT reason,COUNT(*) n FROM events WHERE at>=? AND outcome!=? GROUP BY reason ORDER BY n DESC LIMIT 6').all(since,'qualified');
  const buckets=this.db.prepare(`SELECT CAST(at/3600000 AS INTEGER)*3600000 hour,COUNT(*) checked,SUM(CASE WHEN outcome='qualified' THEN 1 ELSE 0 END) qualified FROM events WHERE at>=? GROUP BY hour ORDER BY hour`).all(since);
  const total=this.db.prepare('SELECT COUNT(*) total FROM events').get();return {last24h:{inspected:d.inspected||0,qualified:d.qualified||0,best:d.best??null,average:d.average??null},total:total.total,causes,buckets};
 }
 purge(days){this.db.prepare('DELETE FROM events WHERE at<?').run(Date.now()-days*86400000);this.db.prepare('DELETE FROM sessions WHERE expires<=?').run(Date.now());}
 close(){this.db.close();}
}
module.exports={Store};
