// Rivals Arena server: static file host + WebSocket relay with rooms, presence and lobby list.
const http = require("http"), fs = require("fs"), path = require("path");
const { WebSocketServer } = require("ws");
const PORT = process.env.PORT || 3000, MAX_ROOM = 8;
const html = () => fs.readFileSync(path.join(__dirname, "public", "index.html"));
const server = http.createServer((req, res) => {
  if (req.url === "/health") return res.end("ok");
  if (req.url === "/" || req.url.startsWith("/?")) { res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" }); return res.end(html()); }
  res.writeHead(404); res.end("Not found");
});
const wss = new WebSocketServer({ server });
const clients = new Set(), rooms = new Map();
const dirtyRooms = new Set(); let lobbiesDirty = true, nid = 1;
const send = (c, m) => { if (c.ws.readyState === 1) c.ws.send(JSON.stringify(m)); };
const CODE = /^[A-Z0-9_-]{1,16}$/, TOPIC = /^[a-z][a-z0-9_.-]{0,31}$/;
function leave(c) {
  if (!c.room) return;
  const set = rooms.get(c.room);
  if (set) { set.delete(c); if (!set.size) rooms.delete(c.room); else dirtyRooms.add(c.room); }
  c.room = null; c.pres = {}; c.info = null; lobbiesDirty = true;
}
function lobbies() {
  const m = {};
  for (const c of clients) if (c.room) {
    const l = m[c.room] || (m[c.room] = { code: c.room, count: 0, pub: false, host: "" });
    l.count++; if (c.info && c.info.pub) { l.pub = true; l.host = l.host || String(c.info.host || "").slice(0, 14); }
  }
  return Object.values(m).filter(l => l.pub);
}
wss.on("connection", ws => {
  const c = { id: "p" + (nid++) + Math.random().toString(36).slice(2, 5), ws, room: null, pres: {}, info: null };
  clients.add(c); send(c, { t: "hello", id: c.id }); send(c, { t: "lobbies", list: lobbies() });
  ws.on("message", raw => {
    if (raw.length > 4096) return;
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === "join") {
      const code = String(m.room || "").toUpperCase(); if (!CODE.test(code)) return;
      leave(c);
      const set = rooms.get(code) || new Set();
      if (set.size >= MAX_ROOM) return send(c, { t: "full", room: code });
      set.add(c); rooms.set(code, set); c.room = code; dirtyRooms.add(code); lobbiesDirty = true;
    } else if (m.t === "leave") leave(c);
    else if (!c.room) return;
    else if (m.t === "pres" && m.patch && typeof m.patch === "object") {
      for (const k of Object.keys(m.patch).slice(0, 24)) { if (m.patch[k] === null) delete c.pres[k]; else c.pres[k] = m.patch[k]; }
      dirtyRooms.add(c.room);
    } else if (m.t === "emit" && TOPIC.test(m.topic || "")) {
      for (const o of rooms.get(c.room)) send(o, { t: "ev", topic: m.topic, data: m.data, from: c.id });
    } else if (m.t === "pub") { c.info = m.info && typeof m.info === "object" ? { pub: !!m.info.pub, host: m.info.host } : null; lobbiesDirty = true; }
  });
  ws.on("close", () => { leave(c); clients.delete(c); });
});
setInterval(() => {
  for (const code of dirtyRooms) {
    const set = rooms.get(code); if (!set) continue;
    const list = [...set].map(c => ({ peer: c.id, presence: c.pres }));
    for (const c of set) send(c, { t: "peers", room: code, list });
  }
  dirtyRooms.clear();
  if (lobbiesDirty) { lobbiesDirty = false; const l = lobbies(); for (const c of clients) send(c, { t: "lobbies", list: l }); }
}, 40);
server.listen(PORT, () => console.log("Rivals Arena running on port " + PORT));
