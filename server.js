
"use strict";

const http = require("http");
const WebSocket = require("ws");

const PORT = Number(process.env.PORT) || 8080;

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
    log("[ERROR] Could not send JSON:", error.message);
    return false;
  }
}

function getClientIp(request) {
  const forwarded = request.headers["x-forwarded-for"];

  if (forwarded) {
    return forwarded.split(",")[0].trim();
  }

  return String(request.socket.remoteAddress || "unknown")
    .replace("::ffff:", "");
}

function cleanValue(value) {
  return value === undefined || value === null ? null : value;
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

function isSocketOpen(ws) {
  return Boolean(
    ws &&
    ws.readyState === WebSocket.OPEN
  );
}

function isActiveTransmitter(ws) {
  return Boolean(
    ws &&
    ws.registered === true &&
    isSocketOpen(ws)
  );
}

function getPublicStationStatus(stationKey) {
  const room = STATIONS[stationKey];
  const transmitter = room.transmitter;

  return {
    station: stationKey,
    transmitterActive: isActiveTransmitter(transmitter),
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
    transmitterActive: isActiveTransmitter(room.transmitter),
    receivers: room.receivers.size,
    metadata: room.metadata
  });
}

function broadcastToReceivers(stationKey, payload) {
  const room = STATIONS[stationKey];

  for (const receiver of room.receivers) {
    if (!isSocketOpen(receiver)) {
      continue;
    }

    try {
      receiver.send(JSON.stringify(payload));
    } catch (error) {
      log(
        `[${stationKey}] Receiver JSON send failed:`,
        error.message
      );
    }
  }
}

function broadcastStreamInfo(stationKey) {
  const room = STATIONS[stationKey];

  broadcastToReceivers(stationKey, {
    type: "stream-info",
    station: stationKey,
    transmitterActive: isActiveTransmitter(room.transmitter),
    metadata: room.metadata
  });
}

function clearTransmitter(stationKey, ws) {
  const room = STATIONS[stationKey];

  /*
   * Do not clear a newer transmitter when an old socket closes.
   */
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

  /*
   * Send the rejection only after the registration has been
   * received and checked.
   */
  sendJson(ws, {
    type: "transmitter-rejected",
    station: stationKey,
    reason,
    ...extra
  });

  setTimeout(() => {
    if (
      ws.readyState === WebSocket.OPEN ||
      ws.readyState === WebSocket.CONNECTING
    ) {
      try {
        ws.close(1008, reason);
      } catch (error) {
        log(
          `[${stationKey}] Failed to close rejected transmitter:`,
          error.message
        );
      }
    }
  }, 100);
}

function validRegistration(registration) {
  return Boolean(
    registration &&
    typeof registration === "object" &&
    registration.type === "register-transmitter"
  );
}

function handleTransmitterMessage(stationKey, ws, data, isBinary) {
  const room = STATIONS[stationKey];

  /*
   * The first message must be registration JSON.
   */
  if (!ws.registered) {
    if (isBinary) {
      log(
        `[${stationKey}] Binary packet received before registration`
      );

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

    log(
      `[${stationKey}] Registration payload received:`,
      registration
    );

    if (!validRegistration(registration)) {
      rejectTransmitter(
        ws,
        stationKey,
        "INVALID_REGISTRATION_TYPE"
      );

      return;
    }

    /*
     * Only one transmitter is allowed for each station.
     * AM and FM are completely independent.
     */
    if (isActiveTransmitter(room.transmitter)) {
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
    room.metadata = buildMetadata(
      stationKey,
      registration
    );

    log(
      `[+] ${stationKey} transmitter registered:`,
      room.metadata
    );

    /*
     * This is the only initial response sent after registration.
     * Do not send registration-required when the socket connects.
     */
    sendJson(ws, {
      type: "transmitter-accepted",
      station: stationKey,
      endpoint: room.txPath,
      server: "AudioBridge",
      ...room.metadata
    });

    broadcastStreamInfo(stationKey);

    return;
  }

  /*
   * Ignore packets from an obsolete transmitter socket.
   */
  if (room.transmitter !== ws) {
    log(
      `[${stationKey}] Ignoring packet from inactive transmitter`
    );

    return;
  }

  /*
   * Forward audio data exactly as received.
   * No conversion or format enforcement is performed.
   */
  for (const receiver of room.receivers) {
    if (!isSocketOpen(receiver)) {
      continue;
    }

    try {
      receiver.send(data, {
        binary: isBinary
      });
    } catch (error) {
      log(
        `[${stationKey}] Audio forwarding failed:`,
        error.message
      );
    }
  }
}

function handleReceiverMessage(stationKey, ws, data, isBinary) {
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
     * IMPORTANT:
     * Do not send registration-required here.
     * The Python transmitter expects to send its registration
     * immediately after the WebSocket connection opens.
     */

    ws.on("message", (data, isBinary) => {
      handleTransmitterMessage(
        stationKey,
        ws,
        data,
        isBinary
      );
    });

    ws.on("close", (code, reason) => {
      log(
        `[${stationKey}] Transmitter socket closed. ` +
        `Code: ${code}, Reason: ${
          reason ? reason.toString() : "(none)"
        }`
      );

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

    ws.on("close", (code, reason) => {
      room.receivers.delete(ws);

      log(
        `[-] RECEIVER ${stationKey} disconnected. ` +
        `Code: ${code}, Reason: ${
          reason ? reason.toString() : "(none)"
        }`
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

    response.end(JSON.stringify({
      ok: true,
      service: "AudioBridge",
      time: now()
    }));

    return;
  }

  if (url.pathname === "/status") {
    response.writeHead(200, {
      "Content-Type": "application/json",
      "Cache-Control": "no-store"
    });

    response.end(JSON.stringify({
      ok: true,
      service: "AudioBridge",
      time: now(),
      stations: {
        AM: getPublicStationStatus("AM"),
        FM: getPublicStationStatus("FM")
      }
    }));

    return;
  }

  const websocketPaths = [
    "/amtx",
    "/amrx",
    "/fmtx",
    "/fmrx"
  ];

  if (websocketPaths.includes(url.pathname)) {
    response.writeHead(426, {
      "Content-Type": "application/json"
    });

    response.end(JSON.stringify({
      error: "WebSocket upgrade required"
    }));

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

  switch (url.pathname) {
    case "/amtx":
      stationKey = "AM";
      role = "transmitter";
      break;

    case "/amrx":
      stationKey = "AM";
      role = "receiver";
      break;

    case "/fmtx":
      stationKey = "FM";
      role = "transmitter";
      break;

    case "/fmrx":
      stationKey = "FM";
      role = "receiver";
      break;
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
        room.transmitter.close(
          1001,
          "Server shutting down"
        );
      } catch (error) {
        // Ignore close errors
      }
    }

    for (const receiver of room.receivers) {
      try {
        receiver.close(
          1001,
          "Server shutting down"
        );
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