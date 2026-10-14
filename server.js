'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const { WebSocketServer } = require('ws');
const { DatabaseSync } = require('node:sqlite');

const PORT = +process.env.PORT || 3000, HOST = process.env.HOST || '0.0.0.0';
const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'poker.db');

// ---- SQLite ----
const db = new DatabaseSync(DB_PATH);
db.exec('PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY, data TEXT NOT NULL, host_key TEXT NOT NULL, updated INTEGER NOT NULL)');
const qGet = db.prepare('SELECT data, host_key FROM sessions WHERE id=?');
const qPut = db.prepare('INSERT INTO sessions(id,data,host_key,updated) VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data, updated=excluded.updated');
const qOld = db.prepare('DELETE FROM sessions WHERE updated<?');
const cleanup = () => qOld.run(Date.now() - 30 * 864e5);   // Sessions älter als 30 Tage löschen
cleanup(); setInterval(cleanup, 864e5);

// ---- Session-Logik ----
const cache = new Map();
const str = (v, n) => String(v ?? '').slice(0, n);
const code = () => Array.from({ length: 6 }, () => 'ABCDEFGHJKMNPQRSTUVWXYZ23456789'[crypto.randomInt(31)]).join('');
const same = (a, b) => { const x = Buffer.from(String(a)), y = Buffer.from(String(b)); return x.length === y.length && crypto.timingSafeEqual(x, y); };

function clean(p) {
  const o = {};
  if ('sprint' in p) o.sprint = str(p.sprint, 80);
  if ('cap' in p) o.cap = str(p.cap, 10);
  if ('deck' in p) o.deck = str(p.deck, 40);
  if (Array.isArray(p.decks)) o.decks = p.decks.slice(0, 20).map(d => ({ id: str(d.id, 40), name: str(d.name, 40), cards: (Array.isArray(d.cards) ? d.cards : []).slice(0, 30).map(c => str(c, 8)) }));
  if (Array.isArray(p.stories)) o.stories = p.stories.slice(0, 300).map(x => ({ t: str(x.t, 200), est: x.est == null ? null : str(x.est, 8) }));
  if ('cs' in p) o.cs = Number.isInteger(p.cs) ? p.cs : null;
  if ('revealed' in p) o.revealed = !!p.revealed;
  if (Number.isInteger(p.round)) o.round = p.round;
  return o;
}
function load(id) {
  if (cache.has(id)) return cache.get(id);
  const r = qGet.get(id); if (!r) return null;
  const sess = { id, hostKey: r.host_key, s: JSON.parse(r.data), clients: new Set() };
  cache.set(id, sess); return sess;
}
const persist = sess => qPut.run(sess.id, JSON.stringify(sess.s), sess.hostKey, Date.now());
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const err = (c, msg) => send(c.ws, { t: 'error', msg });

function snap(sess, c) {            // Stimmen anderer bleiben bis zum Aufdecken serverseitig verborgen
  const s = sess.s, names = [...sess.clients].map(x => x.name), votes = {};
  for (const n of names) if (s.votes[n] !== undefined) votes[n] = (s.revealed || n === c.name) ? s.votes[n] : '■';
  return { t: 'state', sid: sess.id, host: c.host, s: { sprint: s.sprint, cap: s.cap, deck: s.deck, decks: s.decks, stories: s.stories, cs: s.cs, revealed: s.revealed, round: s.round, names, votes } };
}
const broadcast = sess => { for (const c of sess.clients) send(c.ws, snap(sess, c)); };
function join(c, sess, name, host) {
  for (const o of [...sess.clients]) if (o.name === name) { sess.clients.delete(o); o.sess = null; o.ws.close(); }
  Object.assign(c, { name, host, sess }); sess.clients.add(c); broadcast(sess);
}
function handle(c, m) {
  if (!c.sess && m.t === 'create') {
    const name = str(m.name, 30).trim(); if (!name) return err(c, 'Name fehlt');
    let id; do { id = code(); } while (load(id));
    const sess = { id, hostKey: crypto.randomBytes(16).toString('hex'), clients: new Set(),
      s: { sprint: '', cap: '', deck: 'fib', decks: [], stories: [], cs: null, revealed: false, round: 0, votes: {} } };
    Object.assign(sess.s, clean(m.patch || {}));
    cache.set(id, sess); persist(sess);
    send(c.ws, { t: 'created', sid: id, hostKey: sess.hostKey });
    join(c, sess, name, true);
  } else if (!c.sess && m.t === 'join') {
    const sess = load(str(m.sid, 6).toUpperCase()), name = str(m.name, 30).trim();
    if (!sess) return err(c, 'Session nicht gefunden'); if (!name) return err(c, 'Name fehlt');
    join(c, sess, name, !!m.hostKey && same(m.hostKey, sess.hostKey));
  } else if (c.sess) {
    const s = c.sess.s;
    if (m.t === 'vote' && !s.revealed) { s.votes[c.name] = str(m.card, 8); persist(c.sess); broadcast(c.sess); }
    else if (m.t === 'patch' && c.host && m.patch) {
      const o = clean(m.patch);
      if (o.round !== undefined && o.round > s.round) s.votes = {};
      Object.assign(s, o); persist(c.sess); broadcast(c.sess);
    } else if (m.t === 'patch') send(c.ws, snap(c.sess, c));
  }
}

// ---- HTTP + WebSocket ----
const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/' || u === '/index.html') {
    fs.readFile(path.join(__dirname, 'index.html'), (e, b) => {
      if (e) { res.writeHead(500); return res.end('index.html fehlt'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer',
        'Content-Security-Policy': "default-src 'self'; img-src 'self' data:; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self' ws: wss:" });
      res.end(b);
    });
  } else if (u === '/healthz') res.end('ok');
  else { res.writeHead(404); res.end('Not found'); }
});
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 256 * 1024 });
wss.on('connection', (ws, req) => {
  const o = req.headers.origin;
  try { if (o && new URL(o).host !== req.headers.host) return ws.close(); } catch { return ws.close(); }
  const c = { ws, name: null, host: false, sess: null, n: 0, t: Date.now() };
  ws.alive = true; ws.on('pong', () => { ws.alive = true; });
  ws.on('message', buf => {
    const now = Date.now(); if (now - c.t > 1000) { c.t = now; c.n = 0; } if (++c.n > 20) return;
    let m; try { m = JSON.parse(buf); } catch { return; }
    if (m && typeof m === 'object') handle(c, m);
  });
  ws.on('close', () => {
    const s = c.sess; if (!s) return;
    s.clients.delete(c);
    if (s.clients.size) broadcast(s); else cache.delete(s.id);
  });
});
setInterval(() => wss.clients.forEach(ws => { if (!ws.alive) return ws.terminate(); ws.alive = false; ws.ping(); }), 25000);
const stop = () => { try { db.close(); } catch {} process.exit(0); };
process.on('SIGTERM', stop); process.on('SIGINT', stop);
server.listen(PORT, HOST, () => console.log(`Scrum Poker läuft auf ${HOST}:${PORT}, DB: ${DB_PATH}`));
