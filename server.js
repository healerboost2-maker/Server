
const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 10000;

// ============================================================
// STATION ROOMS
// ============================================================

const rooms = {
    am: {
        station: "AM",
        transmitter: null,
        receivers: new Set(),
        format: null,
        bitrate: null,
        sampleRate: null,
        channels: null,
        metadata: null
    },

    fm: {
        station: "FM",
        transmitter: null,
        receivers: new Set(),
        format: null,
        bitrate: null,
        sampleRate: null,
        channels: null,
        metadata: null
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
        console.error("[SEND JSON ERROR]", error.message);
        return false;
    }
}

function getClientIp(req) {
    const forwarded = req.headers["x-forwarded-for"];

    if (forwarded) {
        return String(forwarded).split(",")[0].trim();
    }

    return req.socket.remoteAddress || "unknown";
}

function getRoomMetadata(room) {
    return {
        format: room.format,
        bitrate: room.bitrate,
        sampleRate: room.sampleRate,
        channels: room.channels,
        metadata: room.metadata
    };
}

function sendRoomStatus(ws, role, station, room, endpoint) {
    sendJson(ws, {
        type: "status",
        role,
        station: station.toUpperCase(),
        endpoint,
        server: "AudioBridge",
        transmitterActive: Boolean(room.transmitter),
        ...getRoomMetadata(room)
    });
}

function clearRoomMetadata(room) {
    room.format = null;
    room.bitrate = null;
    room.sampleRate = null;
    room.channels = null;
    room.metadata = null;
}

// ============================================================
// HTTP SERVER
// ============================================================

const server = http.createServer((req, res) => {
    let url;

    try {
        url = new URL(
            req.url,
            `http://${req.headers.host || "localhost"}`
        );
    } catch (error) {
        res.writeHead(400, {
            "Content-Type": "text/plain"
        });

        res.end("Bad Request");
        return;
    }

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
            service: "AudioBridge",
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
    }

    // --------------------------------------------------------
    // STATUS
    // --------------------------------------------------------

    if (url.pathname === "/status") {
        res.writeHead(200, {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        });

        res.end(JSON.stringify({
            service: "AudioBridge",

            am: {
                transmitters: rooms.am.transmitter ? 1 : 0,
                receivers: rooms.am.receivers.size,
                ...getRoomMetadata(rooms.am)
            },

            fm: {
                transmitters: rooms.fm.transmitter ? 1 : 0,
                receivers: rooms.fm.receivers.size,
                ...getRoomMetadata(rooms.fm)
            },

            timestamp: new Date().toISOString()
        }));

        return;
    }

    // --------------------------------------------------------
    // ROOT
    // --------------------------------------------------------

    res.writeHead(200, {
        "Content-Type": "text/plain",
        "Cache-Control": "no-cache"
    });

    res.end(
        "AudioBridge Multi-Stream Server Running\n\n" +
        "WebSocket Endpoints:\n\n" +
        "AM Transmitter: /amtx\n" +
        "AM Receiver:    /amrx\n\n" +
        "FM Transmitter: /fmtx\n" +
        "FM Receiver:    /fmrx\n\n" +
        "HTTP Endpoints:\n" +
        "/health\n" +
        "/status\n"
    );
});

// ============================================================
// WEBSOCKET SERVER
// ============================================================

const wss = new WebSocket.Server({
    noServer: true,
    perMessageDeflate: false
});

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

    const pathname = url.pathname.toLowerCase();

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

    wss.handleUpgrade(
        request,
        socket,
        head,
        (ws) => {
            wss.emit("connection", ws, request);
        }
    );
});

// ============================================================
// WEBSOCKET CONNECTION
// ============================================================

wss.on("connection", (ws, req) => {
    let url;

    try {
        url = new URL(
            req.url,
            `http://${req.headers.host || "localhost"}`
        );
    } catch (error) {
        ws.close(1008, "Invalid URL");
        return;
    }

    const pathname = url.pathname.toLowerCase();
    const clientIp = getClientIp(req);

    let station;
    let isTransmitter;
    let isReceiver;

    if (pathname === "/amtx") {
        station = "am";
        isTransmitter = true;
        isReceiver = false;
    } else if (pathname === "/amrx") {
        station = "am";
        isTransmitter = false;
        isReceiver = true;
    } else if (pathname === "/fmtx") {
        station = "fm";
        isTransmitter = true;
        isReceiver = false;
    } else if (pathname === "/fmrx") {
        station = "fm";
        isTransmitter = false;
        isReceiver = true;
    } else {
        ws.close(1008, "Invalid WebSocket endpoint");
        return;
    }

    const room = rooms[station];

    ws.stationRoom = station;
    ws.endpoint = pathname;
    ws.isTransmitter = isTransmitter;
    ws.isReceiver = isReceiver;
    ws.isAlive = true;
    ws.registered = false;
    ws.casterId = null;
    ws.stationName = null;

    console.log(
        `[+] ${isTransmitter ? "TRANSMITTER" : "RECEIVER"} ` +
        `${station.toUpperCase()} connected from ${clientIp}`
    );

    // ========================================================
    // HEARTBEAT
    // ========================================================

    ws.on("pong", () => {
        ws.isAlive = true;
    });

    // ========================================================
    // RECEIVER CONNECTION
    // ========================================================

    if (isReceiver) {
        room.receivers.add(ws);

        sendRoomStatus(
            ws,
            "receiver",
            station,
            room,
            pathname
        );

        console.log(
            `[RX ${station.toUpperCase()}] ` +
            `Active receivers: ${room.receivers.size}`
        );
    }

    // ========================================================
    // TRANSMITTER REGISTRATION
    // ========================================================

    ws.on("message", (message, isBinary) => {
        // Receivers are not allowed to send anything.
        if (ws.isReceiver) {
            return;
        }

        if (!ws.isTransmitter) {
            return;
        }

        // ----------------------------------------------------
        // REGISTRATION MUST HAPPEN FIRST
        // ----------------------------------------------------

        if (!ws.registered) {
            if (isBinary || Buffer.isBuffer(message)) {
                sendJson(ws, {
                    type: "transmitter-rejected",
                    reason: "REGISTRATION_REQUIRED"
                });

                ws.close(1008, "Registration required");
                return;
            }

            let registration;

            try {
                registration = JSON.parse(message.toString());
            } catch (error) {
                sendJson(ws, {
                    type: "transmitter-rejected",
                    reason: "INVALID_REGISTRATION"
                });

                ws.close(1008, "Invalid registration");
                return;
            }

            if (
                !registration ||
                registration.type !== "register-transmitter"
            ) {
                sendJson(ws, {
                    type: "transmitter-rejected",
                    reason: "INVALID_REGISTRATION"
                });

                ws.close(1008, "Invalid registration");
                return;
            }

            // ------------------------------------------------
            // SINGLE TRANSMITTER LOCK PER STATION
            // ------------------------------------------------

            if (
                room.transmitter &&
                room.transmitter.readyState === WebSocket.OPEN
            ) {
                console.log(
                    `[REJECTED ${station.toUpperCase()}] ` +
                    `Another transmitter is already active.`
                );

                sendJson(ws, {
                    type: "transmitter-rejected",
                    reason: "ANOTHER_TRANSMITTER_ACTIVE",
                    activeCasterId: room.transmitter.casterId || null,
                    station: station.toUpperCase()
                });

                ws.close(1008, "Another transmitter is already active");
                return;
            }

            // Remove stale transmitter reference if necessary.
            if (
                room.transmitter &&
                room.transmitter.readyState !== WebSocket.OPEN
            ) {
                room.transmitter = null;
            }

            // ------------------------------------------------
            // ACCEPT THE TRANSMITTER'S OWN FORMAT
            // ------------------------------------------------

            ws.registered = true;
            ws.casterId = registration.casterId || null;
            ws.stationName = registration.station || null;

            room.transmitter = ws;

            // Do not force or modify these values.
            // Store exactly what the transmitter registered.
            room.format =
                registration.format !== undefined
                    ? registration.format
                    : null;

            room.bitrate =
                registration.bitrate !== undefined
                    ? registration.bitrate
                    : null;

            room.sampleRate =
                registration.sampleRate !== undefined
                    ? registration.sampleRate
                    : null;

            room.channels =
                registration.channels !== undefined
                    ? registration.channels
                    : null;

            room.metadata = {
                casterId: registration.casterId || null,
                station: registration.station || null,
                channel: registration.channel || station.toUpperCase(),
                client: registration.client || null,
                version: registration.version || null
            };

            console.log(
                `[ACCEPTED ${station.toUpperCase()}] ` +
                `Caster: ${ws.casterId || "unknown"} | ` +
                `Format: ${room.format ?? "unspecified"} | ` +
                `Bitrate: ${room.bitrate ?? "unspecified"} | ` +
                `Sample rate: ${room.sampleRate ?? "unspecified"} | ` +
                `Channels: ${room.channels ?? "unspecified"}`
            );

            sendJson(ws, {
                type: "transmitter-accepted",
                station: station.toUpperCase(),
                endpoint: pathname,
                server: "AudioBridge",
                ...getRoomMetadata(room)
            });

            // Inform all currently connected receivers.
            room.receivers.forEach((receiver) => {
                sendJson(receiver, {
                    type: "stream-info",
                    station: station.toUpperCase(),
                    transmitterActive: true,
                    ...getRoomMetadata(room)
                });
            });

            return;
        }

        // ----------------------------------------------------
        // AUDIO DATA
        // ----------------------------------------------------

        if (room.transmitter !== ws) {
            return;
        }

        // Forward the exact original packet without transcoding.
        room.receivers.forEach((receiver) => {
            if (receiver.readyState !== WebSocket.OPEN) {
                return;
            }

            try {
                receiver.send(message, {
                    binary: isBinary
                });
            } catch (error) {
                console.error(
                    `[AUDIO SEND ERROR ${station.toUpperCase()}]`,
                    error.message
                );
            }
        });
    });

    // ========================================================
    // CLOSE
    // ========================================================

    ws.on("close", (code, reason) => {
        const currentRoom = rooms[ws.stationRoom];

        if (!currentRoom) {
            return;
        }

        if (ws.isTransmitter) {
            if (currentRoom.transmitter === ws) {
                currentRoom.transmitter = null;
                clearRoomMetadata(currentRoom);

                console.log(
                    `[-] TX ${station.toUpperCase()} disconnected ` +
                    `(code ${code})`
                );

                // Tell receivers that the stream ended.
                currentRoom.receivers.forEach((receiver) => {
                    sendJson(receiver, {
                        type: "stream-info",
                        station: station.toUpperCase(),
                        transmitterActive: false,
                        format: null,
                        bitrate: null,
                        sampleRate: null,
                        channels: null,
                        metadata: null
                    });
                });
            }
        }

        if (ws.isReceiver) {
            currentRoom.receivers.delete(ws);

            console.log(
                `[-] RX ${station.toUpperCase()} disconnected ` +
                `(code ${code})`
            );
        }
    });

    // ========================================================
    // ERROR
    // ========================================================

    ws.on("error", (error) => {
        console.error(
            `[WS ERROR ${station.toUpperCase()} ${clientIp}]`,
            error.message
        );
    });
});

// ============================================================
// HEARTBEAT TIMER
// ============================================================

const heartbeatTimer = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) {
            console.log(
                `[TIMEOUT] ${ws.stationRoom || "UNKNOWN"} client`
            );

            ws.terminate();
            return;
        }

        ws.isAlive = false;

        try {
            ws.ping();
        } catch (error) {
            console.error("[PING ERROR]", error.message);
        }
    });
}, 30000);

// ============================================================
// GRACEFUL SHUTDOWN
// ============================================================

function shutdown(signal) {
    console.log(`${signal} received. Shutting down...`);

    clearInterval(heartbeatTimer);

    wss.clients.forEach((ws) => {
        try {
            ws.close(1001, "Server shutting down");
        } catch (error) {
            // Ignore close errors.
        }
    });

    server.close(() => {
        console.log("Server closed.");
        process.exit(0);
    });
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

// ============================================================
// START SERVER
// ============================================================

server.listen(PORT, "0.0.0.0", () => {
    console.log("");
    console.log("======================================");
    console.log("       AUDIOBRIDGE SERVER");
    console.log("======================================");
    console.log(`PORT: ${PORT}`);
    console.log("");
    console.log("AM TX: /amtx");
    console.log("AM RX: /amrx");
    console.log("");
    console.log("FM TX: /fmtx");
    console.log("FM RX: /fmrx");
    console.log("");
    console.log("HEALTH: /health");
    console.log("STATUS: /status");
    console.log("======================================");
    console.log("");
});