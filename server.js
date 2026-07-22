const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const DATA_DIR = path.join(__dirname, 'data');
const ROOMS_FILE = path.join(DATA_DIR, 'rooms.json');
const MSGS_DIR = path.join(DATA_DIR, 'messages');

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(MSGS_DIR)) fs.mkdirSync(MSGS_DIR, { recursive: true });

function loadJSON(file, fallback) {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch (e) {
    return fallback;
  }
}
function saveJSON(file, data) {
  try {
    fs.writeFileSync(file, JSON.stringify(data));
  } catch (e) {
    console.error('save failed', file, e.message);
  }
}

let rooms = loadJSON(ROOMS_FILE, []); // [{id, name, createdAt, private}]
function saveRooms() { saveJSON(ROOMS_FILE, rooms); }

function msgsFile(roomId) { return path.join(MSGS_DIR, roomId + '.json'); }
function loadMessages(roomId) { return loadJSON(msgsFile(roomId), []); }
function saveMessages(roomId, msgs) {
  if (msgs.length > 300) msgs = msgs.slice(msgs.length - 300);
  saveJSON(msgsFile(roomId), msgs);
}

function genId(len) {
  len = len || 6;
  const chars = 'abcdefghjkmnpqrstuvwxyz23456789';
  let s = '';
  for (let i = 0; i < len; i++) s += chars[crypto.randomInt(chars.length)];
  return s;
}

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// roomId -> Set of ws connections
const roomClients = new Map();
function onlineCount(roomId) {
  return (roomClients.get(roomId) || new Set()).size;
}

// ---- REST API ----

app.get('/api/rooms', (req, res) => {
  const list = rooms
    .filter(r => !r.private)
    .map(r => ({ id: r.id, name: r.name, createdAt: r.createdAt, online: onlineCount(r.id) }))
    .sort((a, b) => b.createdAt - a.createdAt);
  res.json(list);
});

app.post('/api/rooms', (req, res) => {
  let name = String((req.body && req.body.name) || '').trim().slice(0, 28).replace(/\s+/g, '-');
  const isPrivate = !!(req.body && req.body.private);
  if (!name) return res.status(400).json({ error: 'name required' });
  const room = { id: genId(6), name, createdAt: Date.now(), private: isPrivate };
  rooms.push(room);
  saveRooms();
  res.json(room);
});

app.get('/api/rooms/:id', (req, res) => {
  const room = rooms.find(r => r.id === req.params.id);
  if (!room) return res.status(404).json({ error: 'not found' });
  res.json({ id: room.id, name: room.name });
});

app.get('/api/rooms/:id/messages', (req, res) => {
  const room = rooms.find(r => r.id === req.params.id);
  if (!room) return res.status(404).json({ error: 'not found' });
  res.json(loadMessages(room.id));
});

// ---- WebSocket (real-time chat) ----

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

function broadcast(roomId, payload) {
  const set = roomClients.get(roomId);
  if (!set) return;
  const data = JSON.stringify(payload);
  for (const client of set) {
    if (client.readyState === WebSocket.OPEN) client.send(data);
  }
}

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://placeholder');
  const roomId = (url.searchParams.get('room') || '').trim();
  const nick = (url.searchParams.get('nick') || 'anon').trim().slice(0, 20) || 'anon';

  const room = rooms.find(r => r.id === roomId);
  if (!room) {
    ws.send(JSON.stringify({ type: 'error', text: 'room not found' }));
    ws.close();
    return;
  }

  ws.roomId = roomId;
  ws.nick = nick;
  ws.lastMsgTimes = [];

  if (!roomClients.has(roomId)) roomClients.set(roomId, new Set());
  roomClients.get(roomId).add(ws);

  broadcast(roomId, { type: 'system', text: nick + ' приєднався(лась)', ts: Date.now() });
  broadcast(roomId, { type: 'presence', count: onlineCount(roomId) });

  ws.on('message', (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch (e) { return; }

    if (data.type === 'chat') {
      const now = Date.now();
      ws.lastMsgTimes = ws.lastMsgTimes.filter(t => now - t < 3000);
      if (ws.lastMsgTimes.length >= 5) return; // basic flood guard
      ws.lastMsgTimes.push(now);

      const text = String(data.text || '').slice(0, 500).trim();
      if (!text) return;
      const msg = { nick: ws.nick, text: text, ts: now };
      const msgs = loadMessages(roomId);
      msgs.push(msg);
      saveMessages(roomId, msgs);
      broadcast(roomId, Object.assign({ type: 'chat' }, msg));
    } else if (data.type === 'nick') {
      const newNick = String(data.nick || '').slice(0, 20).trim();
      if (newNick && newNick !== ws.nick) {
        const old = ws.nick;
        ws.nick = newNick;
        broadcast(roomId, { type: 'system', text: old + ' тепер відомий(а) як ' + newNick, ts: Date.now() });
      }
    }
  });

  ws.on('close', () => {
    const set = roomClients.get(roomId);
    if (set) {
      set.delete(ws);
      if (set.size === 0) roomClients.delete(roomId);
    }
    broadcast(roomId, { type: 'system', text: ws.nick + ' вийшов(ла)', ts: Date.now() });
    broadcast(roomId, { type: 'presence', count: onlineCount(roomId) });
  });
});

server.listen(PORT, () => {
  console.log('DARKWIRE server running on port ' + PORT);
});
