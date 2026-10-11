
const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const rooms = new Map();

const ALLOWED_ORIGIN =
  "https://icy-cell-1947.mohamedmahmoud.workers.dev";

// Serve existing static files, if present.
app.use(express.static(__dirname));

// Health check
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    service: "CALL APP",
    websocket: "/ws"
  });
});

// TURN configuration for the Cloudflare frontend
app.get("/turn-config", (req, res) => {
  res.setHeader("Vary", "Origin");
  res.setHeader("Cache-Control", "no-store");

  const origin = req.headers.origin;

  if (origin !== ALLOWED_ORIGIN) {
    return res.status(403).json({
      error: "Origin not allowed"
    });
  }

  res.setHeader(
    "Access-Control-Allow-Origin",
    ALLOWED_ORIGIN
  );

  const username = process.env.TURN_USERNAME;
  const credential = process.env.TURN_CREDENTIAL;

  if (!username || !credential) {
    return res.status(503).json({
      error: "TURN configuration is missing"
    });
  }

  res.json({
    iceServers: [
      {
        urls: [
          "turn:global.relay.metered.ca:80",
          "turn:global.relay.metered.ca:80?transport=tcp",
          "turn:global.relay.metered.ca:443",
          "turns:global.relay.metered.ca:443?transport=tcp"
        ],
        username,
        credential
      }
    ]
  });
});

// Get or create a room
function getRoom(roomName) {
  if (!rooms.has(roomName)) {
    rooms.set(roomName, new Set());
  }

  return rooms.get(roomName);
}

// Send a JSON message
function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

// WebSocket connections
wss.on("connection", (ws, request) => {
  const url = new URL(
    request.url,
    `http://${request.headers.host}`
  );

  if (url.pathname !== "/ws") {
    ws.close(1008, "Invalid WebSocket path");
    return;
  }

  const room =
    url.searchParams.get("room") || "family";

  if (!/^[a-zA-Z0-9_-]{1,50}$/.test(room)) {
    send(ws, {
      type: "error",
      message: "Invalid room name"
    });

    ws.close(1008, "Invalid room name");
    return;
  }

  const clientId =
    `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;

  const session = {
    ws,
    clientId,
    room,
    joined: false
  };

  ws.session = session;

  send(ws, {
    type: "connected",
    room,
    clientId
  });

  ws.on("message", (message) => {
    let data;

    try {
      data = JSON.parse(message.toString());
    } catch {
      send(ws, {
        type: "error",
        message: "Invalid JSON message"
      });
      return;
    }

    // Join room
    if (data.type === "join") {
      if (session.joined) {
        return;
      }

      const roomUsers = getRoom(room);

      const otherUsers = [...roomUsers].filter(
        (user) => user !== session && user.joined
      );

      session.joined = true;
      roomUsers.add(session);

      send(ws, {
        type: "room_joined",
        room,
        clientId,
        users: otherUsers.length
      });

      // Notify existing users about the new participant.
      for (const other of otherUsers) {
        send(other.ws, {
          type: "peer_joined",
          clientId,
          room
        });
      }

      return;
    }

    // WebRTC signaling
    if (data.type === "signal") {
      if (!session.joined) {
        send(ws, {
          type: "error",
          message: "Join a room before sending signals"
        });
        return;
      }

      const target = data.target;
      const signalData = data.data;
      const roomUsers = rooms.get(room);

      if (!roomUsers || !signalData) {
        return;
      }

      if (target) {
        for (const other of roomUsers) {
          if (
            other.clientId === target &&
            other.joined &&
            other !== session
          ) {
            send(other.ws, {
              type: "signal",
              from: clientId,
              data: signalData
            });
            break;
          }
        }
      } else {
        for (const other of roomUsers) {
          if (other !== session && other.joined) {
            send(other.ws, {
              type: "signal",
              from: clientId,
              data: signalData
            });
          }
        }
      }

      return;
    }

    // Leave room
    if (data.type === "leave") {
      removeFromRoom(session);

      try {
        ws.close(1000, "Left room");
      } catch {}

      return;
    }

    // Ping
    if (data.type === "ping") {
      send(ws, {
        type: "pong",
        timestamp: Date.now()
      });
      return;
    }

    send(ws, {
      type: "error",
      message: "Unknown message type"
    });
  });

  ws.on("close", () => {
    removeFromRoom(session);
  });

  ws.on("error", () => {
    removeFromRoom(session);
  });
});

// Remove a user from a room
function removeFromRoom(session) {
  const roomUsers = rooms.get(session.room);

  if (!roomUsers || !roomUsers.has(session)) {
    return;
  }

  roomUsers.delete(session);
  session.joined = false;

  for (const other of roomUsers) {
    if (other.joined) {
      send(other.ws, {
        type: "peer_left",
        clientId: session.clientId
      });
    }
  }

  if (roomUsers.size === 0) {
    rooms.delete(session.room);
  }
}

// Start server
const PORT = process.env.PORT || 10000;

server.listen(PORT, "0.0.0.0", () => {
  console.log(`CALL APP running on port ${PORT}`);
});
