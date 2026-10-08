const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

const wss = new WebSocket.Server({ server });

const rooms = new Map();

// Serve CALL APP frontend
app.use(express.static(__dirname));

// Health check
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    service: "CALL APP",
    websocket: "/ws"
  });
});

// TURN configuration for the frontend
app.get("/turn-config", (req, res) => {
  res.json({
    iceServers: [
      {
        urls: "stun:stun.relay.metered.ca:80"
      },
      {
        urls: [
          "turn:global.relay.metered.ca:80",
          "turn:global.relay.metered.ca:80?transport=tcp",
          "turn:global.relay.metered.ca:443",
          "turns:global.relay.metered.ca:443?transport=tcp"
        ],
        username: process.env.TURN_USERNAME,
        credential: process.env.TURN_CREDENTIAL
      }
    ]
  });
});

// Get or create room
function getRoom(roomName) {
  if (!rooms.has(roomName)) {
    rooms.set(roomName, new Set());
  }

  return rooms.get(roomName);
}

// Send JSON message
function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

// WebSocket connection
wss.on("connection", (ws, request) => {
  const url = new URL(
    request.url,
    `http://${request.headers.host}`
  );

  const room =
    url.searchParams.get("room") || "family";

  if (!/^[a-zA-Z0-9_-]{1,50}$/.test(room)) {
    send(ws, {
      type: "error",
      message: "Invalid room name"
    });

    ws.close();
    return;
  }

  const clientId =
    `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;

  const session = {
    ws,
    clientId,
    room,
    joined: false,
    joinedAt: Date.now()
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
      const roomUsers = getRoom(room);

      session.joined = true;

      const otherUsers = [...roomUsers].filter(
        (user) => user !== session && user.joined
      );

      roomUsers.add(session);

      send(ws, {
        type: "room_joined",
        room,
        clientId,
        users: otherUsers.length
      });

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
      const target = data.target || null;
      const signalData = data.data || null;

      const roomUsers = rooms.get(room);

      if (!roomUsers) {
        return;
      }

      if (target) {
        for (const other of roomUsers) {
          if (other.clientId === target) {
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

    // Unknown message
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

// Remove user from room
function removeFromRoom(session) {
  const roomUsers = rooms.get(session.room);

  if (!roomUsers) {
    return;
  }

  if (roomUsers.has(session)) {
    roomUsers.delete(session);

    for (const other of roomUsers) {
      if (other.joined) {
        send(other.ws, {
          type: "peer_left",
          clientId: session.clientId
        });
      }
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
