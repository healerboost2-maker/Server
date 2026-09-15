"use strict";

const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 8080;

const STATIONS = {
  AM: {
    txPath: "/amtx",
    rxPath: "/amrx",
    transmitter: null,
    receivers: new Set(),
    metadata: null
  },

  FM: {
    txPath: "/fmtx",
    rxPath: "/fmrx",
    transmitter: null,
    receivers: new Set(),
    metadata: null
  }
};

function now() {
  return new Date().toISOString();
}

function log(message, ...args) {
  console.log(`[${now()}] ${message}`, ...args);
}

function sendJson(ws, payload) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return false;
  }

  try {
    ws.send(JSON.stringify(payload));
    return true;
  } catch (error) {
    log("[ERROR] Failed to send JSON:", error.message);
    return false;
  }
}

function getClientIp(request) {
  const forwarded = request.headers["x-forwarded-for"];

  if (forwarded) {
    return forwarded.split(",")[0].trim();
  }

  return (
    request.socket.remoteAddress ||
    "unknown"
  ).replace("::ffff:", "");
}

function cleanValue(value) {
  if (value === undefined || value === null) {
    return null;
  }

  return value;
}

function buildMetadata(stationKey, registration) {
  return {
    casterId: cleanValue(registration.casterId),
    station: cleanValue(registration.station) || stationKey,
    channel: cleanValue(registration.channel) || stationKey,
    format: cleanValue(registration.format),
    bitrate: cleanValue(registration.bitrate),
    sampleRate: cleanValue(registration.sampleRate),
    channels: cleanValue(registration.channels),
    client: cleanValue(registration.client),
    version: cleanValue(registration.version)
  };
}

function getPublicStationStatus(stationKey) {
  const room = STATIONS[stationKey];
  const transmitter = room.transmitter;

  return {
    station: stationKey,
    transmitterActive: Boolean(
      transmitter &&
      transmitter.readyState === WebSocket.OPEN &&
      transmitter.registered === true
    ),
    transmitter: transmitter
      ? {
          casterId: transmitter.casterId || null,
          ip: transmitter.ip || null,
          connectedAt: transmitter.connectedAt || null
        }
      : null,
    receivers: room.receivers.size,
    metadata: room.metadata
  };
}

function sendReceiverStatus(stationKey, receiver) {
  const room = STATIONS[stationKey];

  sendJson(receiver, {
    type: "status",
    station: stationKey,
    transmitterActive: Boolean(
      room.transmitter &&
      room.transmitter.readyState === WebSocket.OPEN &&
      room.transmitter.registered === true
    ),
    receivers: room.receivers.size,
    metadata: room.metadata
  });
}

function broadcastToReceivers(stationKey, payload) {
  const room = STATIONS[stationKey];

  for (const receiver of room.receivers) {
    if (receiver.readyState === WebSocket.OPEN) {
      sendJson(receiver, payload);
    }
  }
}

function broadcastStreamInfo(stationKey) {
  const room = STATIONS[stationKey];

  broadcastToReceivers(stationKey, {
    type: "stream-info",
    station: stationKey,
    transmitterActive: Boolean(room.transmitter),
    metadata: room.metadata
  });
}

function clearTransmitter(stationKey, ws) {
  const room = STATIONS[stationKey];

  if (room.transmitter !== ws) {
    return;
  }

  room.transmitter = null;
  room.metadata = null;

  log(`[${stationKey}] Transmitter disconnected`);

  broadcastToReceivers(stationKey, {
    type: "status",
    station: stationKey,
    transmitterActive: false,
    receivers: room.receivers.size,
    metadata: null
  });
}

function rejectTransmitter(ws, stationKey, reason, extra = {}) {
  log(`[${stationKey}] Transmitter rejected: ${reason}`);

  sendJson(ws, {
    type: "transmitter-rejected",
    reason,
    station: stationKey,
    ...extra
  });

  setTimeout(() => {
    if (
      ws.readyState === WebSocket.OPEN ||
      ws.readyState === WebSocket.CONNECTING
    ) {
      ws.close(1008, reason);
    }
  }, 50);
}

function isValidRegistration(registration) {
  return (
    registration &&
    typeof registration === "object" &&
    registration.type === "register-transmitter"
  );
}

function handleTransmitterMessage(stationKey, ws, data, isBinary) {
  const room = STATIONS[stationKey];

  log(
    `[${stationKey}] Message received: ${
      isBinary
        ? `BINARY ${data.length} bytes`
        : data.toString()
    }`
  );

  /*
   * The first message must be the transmitter registration JSON.
   */
  if (!ws.registered) {
    if (isBinary) {
      rejectTransmitter(
        ws,
        stationKey,
        "REGISTRATION_REQUIRED"
      );
      return;
    }

    let registration;

    try {
      registration = JSON.parse(data.toString());
    } catch (error) {
      rejectTransmitter(
        ws,
        stationKey,
        "INVALID_REGISTRATION_JSON"
      );
      return;
    }

    log(`[${stationKey}] Registration payload:`, registration);

    if (!isValidRegistration(registration)) {
      rejectTransmitter(
        ws,
        stationKey,
        "INVALID_REGISTRATION_TYPE"
      );
      return;
    }

    /*
     * Only one transmitter is allowed per station.
     * AM and FM have separate transmitter slots.
     */
    if (
      room.transmitter &&
      room.transmitter !== ws &&
      (
        room.transmitter.readyState === WebSocket.OPEN ||
        room.transmitter.readyState === WebSocket.CONNECTING
      )
    ) {
      rejectTransmitter(
        ws,
        stationKey,
        "ANOTHER_TRANSMITTER_ACTIVE",
        {
          activeCasterId: room.transmitter.casterId || null
        }
      );
      return;
    }

    ws.registered = true;
    ws.casterId = registration.casterId || "unknown";
    ws.registration = registration;

    room.transmitter = ws;
    room.metadata = buildMetadata(stationKey, registration);

    log(
      `[+] ${stationKey} transmitter registered:`,
      room.metadata
    );

    sendJson(ws, {
      type: "transmitter-accepted",
      station: stationKey,
      endpoint: STATIONS[stationKey].txPath,
      server: "AudioBridge",
      ...room.metadata
    });

    broadcastStreamInfo(stationKey);

    return;
  }

  /*
   * Ignore messages from an old transmitter socket after replacement.
   */
  if (room.transmitter !== ws) {
    log(
      `[${stationKey}] Ignoring message from inactive transmitter`
    );
    return;
  }

  /*
   * Forward audio packets unchanged.
   * No sample-rate, channel, bitrate, or PCM conversion is performed.
   */
  for (const receiver of room.receivers) {
    if (receiver.readyState === WebSocket.OPEN) {
      try {
        receiver.send(data, {
          binary: isBinary
        });
      } catch (error) {
        log(
          `[${stationKey}] Failed to forward audio:`,
          error.message
        );
      }
    }
  }
}

function handleReceiverMessage(stationKey, ws, data, isBinary) {
  /*
   * Receivers normally do not need to send messages.
   * Respond to a JSON status request if one is received.
   */
  if (isBinary) {
    return;
  }

  let message;

  try {
    message = JSON.parse(data.toString());
  } catch (error) {
    return;
  }

  if (message.type === "get-status") {
    sendReceiverStatus(stationKey, ws);
  }
}

function createWebSocketServer() {
  const wss = new WebSocket.Server({
    noServer: true,
    perMessageDeflate: false,
    maxPayload: 50 * 1024 * 1024
  });

  wss.on("connection", (ws, request, stationKey, role) => {
    const room = STATIONS[stationKey];

    ws.stationKey = stationKey;
    ws.role = role;
    ws.ip = getClientIp(request);
    ws.connectedAt = now();
    ws.registered = false;
    ws.isAlive = true;

    ws.on("pong", () => {
      ws.isAlive = true;
    });

    if (role === "transmitter") {
      log(
        `[+] TRANSMITTER ${stationKey} connected from ${ws.ip}`
      );

      /*
       * Do not assign the transmitter slot until valid registration
       * has been received.
       */
      sendJson(ws, {
        type: "registration-required",
        station: stationKey,
        endpoint: room.txPath
      });

      ws.on("message", (data, isBinary) => {
        handleTransmitterMessage(
          stationKey,
          ws,
          data,
          isBinary
        );
      });

      ws.on("close", () => {
        clearTransmitter(stationKey, ws);
      });

      ws.on("error", (error) => {
        log(
          `[${stationKey}] Transmitter socket error:`,
          error.message
        );
      });

      return;
    }

    if (role === "receiver") {
      room.receivers.add(ws);

      log(
        `[+] RECEIVER ${stationKey} connected from ${ws.ip}`
      );

      sendJson(ws, {
        type: "receiver-connected",
        station: stationKey,
        endpoint: room.rxPath
      });

      sendReceiverStatus(stationKey, ws);

      ws.on("message", (data, isBinary) => {
        handleReceiverMessage(
          stationKey,
          ws,
          data,
          isBinary
        );
      });

      ws.on("close", () => {
        room.receivers.delete(ws);

        log(
          `[-] RECEIVER ${stationKey} disconnected`
        );
      });

      ws.on("error", (error) => {
        log(
          `[${stationKey}] Receiver socket error:`,
          error.message
        );
      });
    }
  });

  return wss;
}

const wss = createWebSocketServer();

const server = http.createServer((request, response) => {
  const url = new URL(
    request.url,
    `http://${request.headers.host || "localhost"}`
  );

  if (url.pathname === "/health") {
    response.writeHead(200, {
      "Content-Type": "application/json",
      "Cache-Control": "no-store"
    });

    response.end(
      JSON.stringify({
        ok: true,
        service: "AudioBridge",
        time: now()
      })
    );

    return;
  }

  if (url.pathname === "/status") {
    response.writeHead(200, {
      "Content-Type": "application/json",
      "Cache-Control": "no-store"
    });

    response.end(
      JSON.stringify({
        ok: true,
        service: "AudioBridge",
        time: now(),
        stations: {
          AM: getPublicStationStatus("AM"),
          FM: getPublicStationStatus("FM")
        }
      })
    );

    return;
  }

  if (
    url.pathname === "/amtx" ||
    url.pathname === "/amrx" ||
    url.pathname === "/fmtx" ||
    url.pathname === "/fmrx"
  ) {
    response.writeHead(426, {
      "Content-Type": "application/json"
    });

    response.end(
      JSON.stringify({
        error: "WebSocket upgrade required"
      })
    );

    return;
  }

  response.writeHead(200, {
    "Content-Type": "text/plain; charset=utf-8"
  });

  response.end("AudioBridge Server Running");
});

server.on("upgrade", (request, socket, head) => {
  const url = new URL(
    request.url,
    `http://${request.headers.host || "localhost"}`
  );

  let stationKey = null;
  let role = null;

  if (url.pathname === "/amtx") {
    stationKey = "AM";
    role = "transmitter";
  } else if (url.pathname === "/amrx") {
    stationKey = "AM";
    role = "receiver";
  } else if (url.pathname === "/fmtx") {
    stationKey = "FM";
    role = "transmitter";
  } else if (url.pathname === "/fmrx") {
    stationKey = "FM";
    role = "receiver";
  }

  if (!stationKey || !role) {
    socket.write(
      "HTTP/1.1 404 Not Found\r\n" +
      "Connection: close\r\n" +
      "\r\n"
    );

    socket.destroy();
    return;
  }

  wss.handleUpgrade(
    request,
    socket,
    head,
    (ws) => {
      wss.emit(
        "connection",
        ws,
        request,
        stationKey,
        role
      );
    }
  );
});

/*
 * Heartbeat to clean up dead WebSocket connections.
 */
const heartbeatTimer = setInterval(() => {
  for (const stationKey of Object.keys(STATIONS)) {
    const room = STATIONS[stationKey];

    const sockets = [];

    if (room.transmitter) {
      sockets.push(room.transmitter);
    }

    for (const receiver of room.receivers) {
      sockets.push(receiver);
    }

    for (const ws of sockets) {
      if (ws.isAlive === false) {
        log(
          `[${stationKey}] Terminating inactive ${ws.role}`
        );

        try {
          ws.terminate();
        } catch (error) {
          // Ignore termination errors
        }

        continue;
      }

      ws.isAlive = false;

      try {
        ws.ping();
      } catch (error) {
        // Ignore ping errors
      }
    }
  }
}, 30000);

server.listen(PORT, "0.0.0.0", () => {
  console.log("======================================");
  console.log("       AUDIOBRIDGE SERVER");
  console.log("======================================");
  console.log(`PORT: ${PORT}`);
  console.log("AM TX: /amtx");
  console.log("AM RX: /amrx");
  console.log("FM TX: /fmtx");
  console.log("FM RX: /fmrx");
  console.log("HEALTH: /health");
  console.log("STATUS: /status");
  console.log("======================================");
});

function shutdown(signal) {
  log(`${signal} received. Shutting down...`);

  clearInterval(heartbeatTimer);

  for (const stationKey of Object.keys(STATIONS)) {
    const room = STATIONS[stationKey];

    if (room.transmitter) {
      try {
        room.transmitter.close(1001, "Server shutting down");
      } catch (error) {
        // Ignore close errors
      }
    }

    for (const receiver of room.receivers) {
      try {
        receiver.close(1001, "Server shutting down");
      } catch (error) {
        // Ignore close errors
      }
    }
  }

  wss.close(() => {
    server.close(() => {
      process.exit(0);
    });
  });

  setTimeout(() => {
    process.exit(0);
  }, 5000);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));