
"use strict";

const http = require("http");
const WebSocket = require("ws");

const PORT = Number(process.env.PORT) || 8080;

<<<<<<< HEAD
// ============================================================
// SERVER CONFIGURATION
// ============================================================

const SERVER_NAME = "AudioBridge";
const SERVER_VERSION = "3.0.0";

const REGISTRATION_TIMEOUT_MS = 10000;
const HEARTBEAT_INTERVAL_MS = 30000;

// Prevent a slow receiver from accumulating unlimited audio.
const MAX_RECEIVER_BUFFERED_BYTES = 2 * 1024 * 1024;

// ============================================================
// ROOMS
// ============================================================

const rooms = {
    am: {
        name: "AM",

        transmitters: new Set(),
        receivers: new Set(),

        config: {
            sampleRate: 44100,
            channels: 2
        }
    },

    fm: {
        name: "FM",

        transmitters: new Set(),
        receivers: new Set(),

        config: {
            sampleRate: 44100,
            channels: 2
        }
    }
};

// ============================================================
// HELPERS
// ============================================================

function sendJson(ws, payload) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        return false;
    }

    try {
        ws.send(JSON.stringify(payload));
        return true;
    } catch (error) {
        console.error("[WS JSON SEND ERROR]", error.message);
        return false;
    }
}


function getRoomFromPath(pathname) {
    if (
        pathname === "/amtx" ||
        pathname === "/amrx"
    ) {
        return "am";
    }

    if (
        pathname === "/fmtx" ||
        pathname === "/fmrx"
    ) {
        return "fm";
    }

    return null;
}


function isTransmitterPath(pathname) {
    return (
        pathname === "/amtx" ||
        pathname === "/fmtx"
    );
}


function isReceiverPath(pathname) {
    return (
        pathname === "/amrx" ||
        pathname === "/fmrx"
    );
}


function removeFromRoom(ws) {
    if (!ws || !ws.stationRoom) {
        return;
    }

    const room = rooms[ws.stationRoom];

    if (!room) {
        return;
    }

    room.transmitters.delete(ws);
    room.receivers.delete(ws);
}


function closeSocket(ws, code, reason) {
    try {
        if (ws.readyState === WebSocket.OPEN) {
            ws.close(code, reason);
        } else if (ws.readyState === WebSocket.CONNECTING) {
            ws.terminate();
        }
    } catch (error) {
        try {
            ws.terminate();
        } catch (_) {
            // Ignore.
        }
    }
}


function getActiveCasterId(room) {
    for (const tx of room.transmitters) {
        if (
            tx.registered &&
            tx.casterId
        ) {
            return tx.casterId;
        }
    }

    return null;
}

=======
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
>>>>>>> 21bcc463542065cef4b396a440ece9ca58ed3d8b

function log(message, ...args) {
  console.log(`[${now()}] ${message}`, ...args);
}

function sendJson(ws, payload) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return false;
  }

<<<<<<< HEAD
    let url;

    try {
        url = new URL(
            req.url,
            `http://${req.headers.host || "localhost"}`
        );
    } catch (error) {

        res.writeHead(400, {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        });

        res.end(JSON.stringify({
            status: "error",
            error: "Invalid URL"
        }));

        return;
    }
=======
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
>>>>>>> 21bcc463542065cef4b396a440ece9ca58ed3d8b

    if (!validRegistration(registration)) {
      rejectTransmitter(
        ws,
        stationKey,
        "INVALID_REGISTRATION_TYPE"
      );

<<<<<<< HEAD
    // --------------------------------------------------------
    // HEALTH
    // --------------------------------------------------------

    if (url.pathname === "/health") {

        res.writeHead(200, {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        });

        res.end(JSON.stringify({
            status: "ok",
            service: SERVER_NAME,
            version: SERVER_VERSION,
            websocket: true,

            endpoints: {
                amtx: "/amtx",
                amrx: "/amrx",
                fmtx: "/fmtx",
                fmrx: "/fmrx"
            },

            timestamp: new Date().toISOString()
        }));

        return;
=======
      return;
>>>>>>> 21bcc463542065cef4b396a440ece9ca58ed3d8b
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

<<<<<<< HEAD
    // --------------------------------------------------------
    // STATUS
    // --------------------------------------------------------

    if (url.pathname === "/status") {

        res.writeHead(200, {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        });

        res.end(JSON.stringify({

            service: SERVER_NAME,
            version: SERVER_VERSION,

            am: {
                transmitters:
                    rooms.am.transmitters.size,

                receivers:
                    rooms.am.receivers.size,

                activeCasterId:
                    getActiveCasterId(rooms.am),

                sampleRate:
                    rooms.am.config.sampleRate,

                channels:
                    rooms.am.config.channels
            },

            fm: {
                transmitters:
                    rooms.fm.transmitters.size,

                receivers:
                    rooms.fm.receivers.size,

                activeCasterId:
                    getActiveCasterId(rooms.fm),

                sampleRate:
                    rooms.fm.config.sampleRate,

                channels:
                    rooms.fm.config.channels
            },

            timestamp:
                new Date().toISOString()

        }));

        return;
=======
      return;
>>>>>>> 21bcc463542065cef4b396a440ece9ca58ed3d8b
    }

    ws.registered = true;
    ws.casterId = registration.casterId || "unknown";
    ws.registration = registration;

    room.transmitter = ws;
    room.metadata = buildMetadata(
      stationKey,
      registration
    );

<<<<<<< HEAD
    res.writeHead(200, {
        "Content-Type": "text/plain; charset=utf-8",
        "Cache-Control": "no-cache"
    });

    res.end(
        "AudioBridge Multi-Stream Server Running\n\n" +

        "Server:\n" +
        `${SERVER_NAME} ${SERVER_VERSION}\n\n` +

        "WebSocket Endpoints:\n\n" +

        "AM Transmitter:\n" +
        "/amtx\n\n" +

        "AM Receiver:\n" +
        "/amrx\n\n" +

        "FM Transmitter:\n" +
        "/fmtx\n\n" +

        "FM Receiver:\n" +
        "/fmrx\n\n" +

        "HTTP:\n" +
        "/health\n" +
        "/status\n"
    );
});
=======
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
>>>>>>> 21bcc463542065cef4b396a440ece9ca58ed3d8b

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
<<<<<<< HEAD

    noServer: true,

    // Audio should NOT be compressed.
    // Compression adds CPU overhead and latency.
    perMessageDeflate: false,

    // Prevent excessive incoming WebSocket frame sizes.
    maxPayload: 2 * 1024 * 1024
=======
  noServer: true,
  perMessageDeflate: false,
  maxPayload: 50 * 1024 * 1024
>>>>>>> 21bcc463542065cef4b396a440ece9ca58ed3d8b
});

wss.on("connection", (ws, request, stationKey, role) => {
  const room = STATIONS[stationKey];

<<<<<<< HEAD
// ============================================================
// WEBSOCKET UPGRADE
// ============================================================

server.on("upgrade", (request, socket, head) => {

    let url;

    try {

        url = new URL(
            request.url,
            `http://${request.headers.host || "localhost"}`
        );

    } catch (error) {

        socket.write(
            "HTTP/1.1 400 Bad Request\r\n" +
            "Connection: close\r\n" +
            "\r\n"
        );

        socket.destroy();

        return;
    }


    const pathname =
        url.pathname.toLowerCase();


    // --------------------------------------------------------
    // VALID ENDPOINTS
    // --------------------------------------------------------

    const validPaths = new Set([
        "/amtx",
        "/amrx",
        "/fmtx",
        "/fmrx"
    ]);


    if (!validPaths.has(pathname)) {

        socket.write(
            "HTTP/1.1 404 Not Found\r\n" +
            "Connection: close\r\n" +
            "\r\n"
        );

        socket.destroy();

        return;
    }


    // --------------------------------------------------------
    // HANDLE WEBSOCKET
    // --------------------------------------------------------

    wss.handleUpgrade(
        request,
        socket,
        head,
        (ws) => {

            wss.emit(
                "connection",
                ws,
                request
            );

        }
    );
});


// ============================================================
// CONNECTION
// ============================================================

wss.on("connection", (ws, req) => {

    const clientIp =
        req.headers["x-forwarded-for"] ||
        req.socket.remoteAddress;


    let url;

    try {

        url = new URL(
            req.url,
            `http://${req.headers.host || "localhost"}`
        );

    } catch (error) {

        closeSocket(
            ws,
            1008,
            "Invalid URL"
        );

        return;
    }


    const pathname =
        url.pathname.toLowerCase();


    // ========================================================
    // DETERMINE STATION
    // ========================================================

    const station =
        getRoomFromPath(pathname);


    const isTransmitter =
        isTransmitterPath(pathname);


    const isReceiver =
        isReceiverPath(pathname);


    // ========================================================
    // VALIDATION
    // ========================================================

    if (
        !station ||
        (!isTransmitter && !isReceiver)
    ) {

        closeSocket(
            ws,
            1008,
            "Invalid WebSocket endpoint"
        );

        return;
    }


    const room =
        rooms[station];


    // ========================================================
    // CLIENT STATE
    // ========================================================

    ws.stationRoom = station;

    ws.endpoint = pathname;

    ws.isTransmitter =
        isTransmitter;

    ws.isReceiver =
        isReceiver;

    ws.registered =
        !isTransmitter;

    ws.casterId = null;

    ws.stationName = null;

    ws.format = null;

    ws.bitrate = null;

    ws.sampleRate =
        room.config.sampleRate;

    ws.channels =
        room.config.channels;

    ws.isAlive = true;

    ws.registrationTimer = null;


    console.log(
        `[+] ${isTransmitter ? "TRANSMITTER" : "RECEIVER"} ` +
        `${station.toUpperCase()} ` +
        `connected from ${clientIp}`
=======
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
>>>>>>> 21bcc463542065cef4b396a440ece9ca58ed3d8b
    );

    /*
     * IMPORTANT:
     * Do not send registration-required here.
     * The Python transmitter expects to send its registration
     * immediately after the WebSocket connection opens.
     */

<<<<<<< HEAD
    // ========================================================
    // HEARTBEAT
    // ========================================================

    ws.on("pong", () => {
        ws.isAlive = true;
    });


    // ========================================================
    // RECEIVER
    // ========================================================

    if (isReceiver) {

        room.receivers.add(ws);

        console.log(
            `[RX ${station.toUpperCase()}] ` +
            `Receiver connected. ` +
            `Active receivers: ${room.receivers.size}`
        );


        sendJson(ws, {
            type: "status",
            role: "receiver",
            station: station.toUpperCase(),
            sampleRate: room.config.sampleRate,
            channels: room.config.channels,
            endpoint: pathname,
            server: SERVER_NAME,
            version: SERVER_VERSION
        });
    }


    // ========================================================
    // TRANSMITTER
    // ========================================================

    if (isTransmitter) {

        /*
         * IMPORTANT:
         *
         * Do NOT add the transmitter to room.transmitters yet.
         *
         * The caster must first send:
         *
         * register-transmitter
         *
         * and receive:
         *
         * transmitter-accepted
         */

        ws.registrationTimer =
            setTimeout(() => {

                if (
                    ws.readyState === WebSocket.OPEN &&
                    !ws.registered
                ) {

                    console.log(
                        `[TX ${station.toUpperCase()}] ` +
                        `Registration timeout from ${clientIp}`
                    );

                    sendJson(ws, {
                        type: "transmitter-rejected",
                        reason: "REGISTRATION_TIMEOUT"
                    });

                    closeSocket(
                        ws,
                        1008,
                        "Transmitter registration timeout"
                    );
                }

            }, REGISTRATION_TIMEOUT_MS);
    }


    // ========================================================
    // RECEIVE DATA
    // ========================================================

    ws.on("message", (message, isBinary) => {

        // ====================================================
        // RECEIVER CANNOT SEND DATA
        // ====================================================

        if (ws.isReceiver) {

            console.warn(
                `[RX ${station.toUpperCase()}] ` +
                `Ignored data from receiver`
            );

            return;
        }


        // ====================================================
        // TRANSMITTER REGISTRATION
        // ====================================================

        if (
            ws.isTransmitter &&
            !ws.registered
        ) {

            if (isBinary) {

                console.warn(
                    `[TX ${station.toUpperCase()}] ` +
                    `Binary data received before registration`
                );

                sendJson(ws, {
                    type: "transmitter-rejected",
                    reason: "REGISTRATION_REQUIRED"
                });

                closeSocket(
                    ws,
                    1008,
                    "Register transmitter before sending audio"
                );

                return;
            }


            let payload;

            try {

                payload =
                    JSON.parse(
                        message.toString()
                    );

            } catch (error) {

                sendJson(ws, {
                    type: "transmitter-rejected",
                    reason: "INVALID_REGISTRATION_JSON"
                });

                closeSocket(
                    ws,
                    1008,
                    "Invalid registration JSON"
                );

                return;
            }


            if (
                !payload ||
                payload.type !==
                    "register-transmitter"
            ) {

                sendJson(ws, {
                    type: "transmitter-rejected",
                    reason: "REGISTRATION_REQUIRED"
                });

                closeSocket(
                    ws,
                    1008,
                    "register-transmitter required"
                );

                return;
            }


            // =================================================
            // VALIDATE CASTER ID
            // =================================================

            const casterId =
                String(
                    payload.casterId || ""
                ).trim();


            if (!casterId) {

                sendJson(ws, {
                    type: "transmitter-rejected",
                    reason: "MISSING_CASTER_ID"
                });

                closeSocket(
                    ws,
                    1008,
                    "casterId is required"
                );

                return;
            }


            // =================================================
            // VALIDATE CHANNEL
            // =================================================

            const requestedChannel =
                String(
                    payload.channel || ""
                )
                .trim()
                .toLowerCase();


            if (
                requestedChannel &&
                requestedChannel !== station
            ) {

                sendJson(ws, {
                    type: "transmitter-rejected",
                    reason: "CHANNEL_MISMATCH",
                    expectedChannel:
                        station.toUpperCase()
                });

                closeSocket(
                    ws,
                    1008,
                    "Channel does not match endpoint"
                );

                return;
            }


            // =================================================
            // ONLY ONE ACTIVE TRANSMITTER PER ROOM
            // =================================================

            const existingTransmitter =
                Array.from(
                    room.transmitters
                ).find(
                    tx =>
                        tx.registered &&
                        tx.readyState ===
                            WebSocket.OPEN
                );


            if (existingTransmitter) {

                const activeCasterId =
                    existingTransmitter.casterId ||
                    "UNKNOWN";


                console.log(
                    `[TX ${station.toUpperCase()}] ` +
                    `Rejected ${casterId}; ` +
                    `active transmitter: ${activeCasterId}`
                );


                sendJson(ws, {

                    type:
                        "transmitter-rejected",

                    reason:
                        "ANOTHER_TX_ACTIVE",

                    activeCasterId:
                        activeCasterId
                });


                closeSocket(
                    ws,
                    1008,
                    "Another transmitter is already active"
                );

                return;
            }


            // =================================================
            // ACCEPT REGISTRATION
            // =================================================

            ws.casterId =
                casterId;

            ws.stationName =
                String(
                    payload.station || ""
                ).trim();

            ws.format =
                String(
                    payload.format || "Opus"
                ).trim();

            ws.bitrate =
                String(
                    payload.bitrate || "128 kbps"
                ).trim();

            ws.sampleRate =
                Number(
                    payload.sampleRate ||
                    room.config.sampleRate
                );

            ws.channels =
                Number(
                    payload.channels ||
                    room.config.channels
                );

            ws.registered = true;


            if (ws.registrationTimer) {

                clearTimeout(
                    ws.registrationTimer
                );

                ws.registrationTimer = null;
            }


            room.transmitters.add(ws);


            console.log(
                `[TX ${station.toUpperCase()}] ` +
                `ACCEPTED caster=${ws.casterId} ` +
                `station="${ws.stationName}" ` +
                `format=${ws.format} ` +
                `bitrate=${ws.bitrate} ` +
                `sampleRate=${ws.sampleRate} ` +
                `channels=${ws.channels}`
            );


            // =================================================
            // ACCEPT RESPONSE
            // =================================================

            sendJson(ws, {

                type:
                    "transmitter-accepted",

                role:
                    "transmitter",

                station:
                    station.toUpperCase(),

                casterId:
                    ws.casterId,

                sampleRate:
                    room.config.sampleRate,

                channels:
                    room.config.channels,

                endpoint:
                    pathname,

                server:
                    SERVER_NAME,

                version:
                    SERVER_VERSION
            });


            // =================================================
            // DO NOT PROCESS REGISTRATION AS AUDIO
            // =================================================

            return;
        }


        // ====================================================
        // TRANSMITTER MUST BE REGISTERED
        // ====================================================

        if (
            ws.isTransmitter &&
            !ws.registered
        ) {

            return;
        }


        // ====================================================
        // ONLY BINARY AUDIO IS ACCEPTED AFTER REGISTRATION
        // ====================================================

        if (!isBinary) {

            console.warn(
                `[TX ${station.toUpperCase()}] ` +
                `Ignoring non-binary message ` +
                `from ${ws.casterId || "unknown"}`
            );

            return;
        }


        // ====================================================
        // CURRENT ROOM
        // ====================================================

        const currentRoom =
            rooms[ws.stationRoom];


        if (!currentRoom) {
            return;
        }


        // ====================================================
        // BROADCAST AUDIO TO RECEIVERS
        // ====================================================

        let delivered = 0;
        let skipped = 0;


        currentRoom.receivers.forEach(
            (receiver) => {

                if (
                    receiver.readyState !==
                    WebSocket.OPEN
                ) {

                    skipped++;
                    return;
                }


                // ------------------------------------------------
                // Protect server from a stalled receiver.
                // ------------------------------------------------

                if (
                    receiver.bufferedAmount >
                    MAX_RECEIVER_BUFFERED_BYTES
                ) {

                    skipped++;

                    console.warn(
                        `[RX ${station.toUpperCase()}] ` +
                        `Slow receiver detected; ` +
                        `buffered=${receiver.bufferedAmount} bytes`
                    );

                    try {

                        receiver.close(
                            1008,
                            "Receiver too slow"
                        );

                    } catch (_) {
                        try {
                            receiver.terminate();
                        } catch (_) {
                            // Ignore.
                        }
                    }

                    return;
                }


                try {

                    receiver.send(
                        message,
                        {
                            binary: true
                        }
                    );

                    delivered++;

                } catch (error) {

                    skipped++;

                    console.error(
                        `[SEND ERROR ${station.toUpperCase()}] ` +
                        `${error.message}`
                    );

                    try {
                        receiver.terminate();
                    } catch (_) {
                        // Ignore.
                    }
                }

            }
        );


        // --------------------------------------------------------
        // Optional low-frequency diagnostic.
        // Do NOT log every audio packet.
        // --------------------------------------------------------

        ws.audioPackets =
            (ws.audioPackets || 0) + 1;

        ws.audioBytes =
            (ws.audioBytes || 0) +
            message.length;

        if (
            ws.audioPackets % 500 === 0
        ) {

            console.log(
                `[AUDIO ${station.toUpperCase()}] ` +
                `caster=${ws.casterId} ` +
                `packets=${ws.audioPackets} ` +
                `bytes=${ws.audioBytes} ` +
                `receivers=${currentRoom.receivers.size} ` +
                `delivered=${delivered} ` +
                `skipped=${skipped}`
            );
        }

    });


    // ========================================================
    // CLOSE
    // ========================================================

    ws.on("close", (code, reasonBuffer) => {

        if (ws.registrationTimer) {

            clearTimeout(
                ws.registrationTimer
            );

            ws.registrationTimer = null;
        }


        const reason =
            reasonBuffer
                ? reasonBuffer.toString()
                : "";


        removeFromRoom(ws);


        if (ws.isTransmitter) {

            console.log(
                `[-] TX ` +
                `${ws.stationRoom.toUpperCase()} ` +
                `caster=${ws.casterId || "unregistered"} ` +
                `disconnected ` +
                `(code ${code}` +
                `${reason ? `, reason=${reason}` : ""})`
            );

        } else {

            console.log(
                `[-] RX ` +
                `${ws.stationRoom.toUpperCase()} ` +
                `disconnected ` +
                `(code ${code}` +
                `${reason ? `, reason=${reason}` : ""})`
            );
        }

    });


    // ========================================================
    // ERROR
    // ========================================================

    ws.on("error", (error) => {

        console.error(
            `[WS ERROR ${station.toUpperCase()} ` +
            `${ws.casterId || clientIp}]`,
            error.message
        );

=======
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
>>>>>>> 21bcc463542065cef4b396a440ece9ca58ed3d8b
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

<<<<<<< HEAD
const heartbeatTimer =
    setInterval(() => {

        wss.clients.forEach((ws) => {

            if (
                ws.readyState !==
                WebSocket.OPEN
            ) {
                return;
            }


            // ------------------------------------------------
            // Dead connection
            // ------------------------------------------------

            if (ws.isAlive === false) {

                console.log(
                    `[TIMEOUT] ` +
                    `${ws.stationRoom || "UNKNOWN"} ` +
                    `${ws.casterId || "client"}`
                );

                try {
                    ws.terminate();
                } catch (_) {
                    // Ignore.
                }

                return;
            }


            ws.isAlive = false;


            // ------------------------------------------------
            // Ping
            // ------------------------------------------------

            try {
                ws.ping();
            } catch (error) {

                console.error(
                    `[PING ERROR]`,
                    error.message
                );

                try {
                    ws.terminate();
                } catch (_) {
                    // Ignore.
                }
            }

        });

    }, HEARTBEAT_INTERVAL_MS);


// ============================================================
// WEBSOCKET SERVER CLOSE
// ============================================================

wss.on("close", () => {

    clearInterval(
        heartbeatTimer
    );

});


// ============================================================
// GRACEFUL SHUTDOWN
// ============================================================

function gracefulShutdown(signal) {

    console.log(
        `${signal} received. Shutting down...`
    );


    clearInterval(
        heartbeatTimer
    );


    wss.clients.forEach((ws) => {

        try {

            ws.close(
                1001,
                "Server shutting down"
            );

        } catch (_) {

            try {
                ws.terminate();
            } catch (_) {
                // Ignore.
            }

        }

    });


    server.close(() => {

        console.log(
            "Server closed."
        );

        process.exit(0);

    });


    // Safety fallback.
    setTimeout(() => {

        process.exit(0);

    }, 5000);
}


process.on(
    "SIGTERM",
    () => gracefulShutdown("SIGTERM")
);

process.on(
    "SIGINT",
    () => gracefulShutdown("SIGINT")
);


// ============================================================
// START
// ============================================================

server.listen(
    PORT,
    "0.0.0.0",
    () => {

        console.log("");
        console.log(
            "======================================"
        );

        console.log(
            "       AUDIOBRIDGE SERVER"
        );

        console.log(
            "======================================"
        );

        console.log(
            `VERSION: ${SERVER_VERSION}`
        );

        console.log(
            `PORT: ${PORT}`
        );

        console.log("");

        console.log(
            "AM TX: /amtx"
        );

        console.log(
            "AM RX: /amrx"
        );

        console.log("");

        console.log(
            "FM TX: /fmtx"
        );

        console.log(
            "FM RX: /fmrx"
        );

        console.log("");

        console.log(
            "HEALTH: /health"
        );

        console.log(
            "STATUS: /status"
        );

        console.log("");

        console.log(
            "Registration protocol: ENABLED"
        );

        console.log(
            "One active transmitter per station: ENABLED"
        );

        console.log(
            "WebSocket heartbeat: ENABLED"
        );

        console.log(
            "Audio compression: DISABLED"
        );

        console.log("");

        console.log(
            "======================================"
        );

        console.log("");

    }
);
=======
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
>>>>>>> 21bcc463542065cef4b396a440ece9ca58ed3d8b
