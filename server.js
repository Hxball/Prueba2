const http = require('http');
const fs = require('fs');
const path = require('path');
const { randomUUID } = require('crypto');
const WebSocket = require('ws');

const PORT = process.env.PORT || 10000;
const rooms = new Map();

const server = http.createServer((req, res) => {
  const pathname = new URL(req.url, `http://${req.headers.host || 'localhost'}`).pathname;

  if (pathname === '/' || pathname === '/index.html') {
    fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
      if (err) {
        res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
        return res.end('No se pudo cargar el juego.');
      }

      res.writeHead(200, {
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-cache'
      });
      res.end(data);
    });
    return;
  }

  if (pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    return res.end('ok');
  }

  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('No encontrado');
});

const wss = new WebSocket.Server({
  noServer: true,
  maxPayload: 64 * 1024
});

server.on('upgrade', (req, socket, head) => {
  let url;

  try {
    url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  } catch {
    socket.destroy();
    return;
  }

  const roomName = url.searchParams.get('room') || '';

  if (url.pathname !== '/ws' || !/^mh-[a-z0-9]{3,6}$/.test(roomName)) {
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit('connection', ws, roomName);
  });
});

function send(ws, message) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(message));
  }
}

function broadcast(room, message, except = null) {
  for (const client of room.clients) {
    if (client !== except) {
      send(client, message);
    }
  }
}

wss.on('connection', (ws, roomName) => {
  let room = rooms.get(roomName);

  if (!room) {
    room = { clients: new Set() };
    rooms.set(roomName, room);
  }

  ws.id = randomUUID();
  ws.presence = {};
  ws.roomName = roomName;
  ws.isAlive = true;

  const existing = [...room.clients].map((client) => ({
    peer: client.id,
    presence: client.presence
  }));

  room.clients.add(ws);

  send(ws, {
    t: 'hi',
    id: ws.id,
    peers: existing
  });

  broadcast(room, {
    t: 'j',
    peer: ws.id
  }, ws);

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (raw) => {
    let message;

    try {
      message = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (
      !message ||
      message.t !== 'p' ||
      !message.d ||
      typeof message.d !== 'object' ||
      Array.isArray(message.d)
    ) {
      return;
    }

    for (const [key, value] of Object.entries(message.d)) {
      if (
        key !== '__proto__' &&
        key !== 'constructor' &&
        key !== 'prototype'
      ) {
        ws.presence[key] = value;
      }
    }

    broadcast(room, {
      t: 'u',
      peer: ws.id,
      d: message.d
    }, ws);
  });

  ws.on('close', () => {
    room.clients.delete(ws);

    broadcast(room, {
      t: 'l',
      peer: ws.id
    });

    if (room.clients.size === 0) {
      rooms.delete(roomName);
    }
  });

  ws.on('error', () => {});
});

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }

    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

wss.on('close', () => clearInterval(heartbeat));

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Juego listo en el puerto ${PORT}`);
});
