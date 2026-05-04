export default {
  async fetch(request) {
    const url = new URL(request.url);

    // 🌐 Browser test (open root in browser)
    if (url.pathname === "/") {
      return new Response(
        JSON.stringify({
          status: "ok",
          message: "Worker is alive",
          websocket_path: "/vless",
          time: new Date().toISOString(),
          note: "Use a VLESS client to connect via WebSocket"
        }, null, 2),
        {
          status: 200,
          headers: { "content-type": "application/json" },
        }
      );
    }

    // 🔍 Optional debug endpoint
    if (url.pathname === "/debug") {
      return new Response(
        JSON.stringify({
          headers: Object.fromEntries(request.headers),
          cf: request.cf || null,
        }, null, 2),
        {
          status: 200,
          headers: { "content-type": "application/json" },
        }
      );
    }

    // 🚫 Wrong path
    if (url.pathname !== "/vless") {
      return new Response("Not Found", { status: 404 });
    }

    // ❌ Not a WebSocket request
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 400 });
    }

    // 🔌 WebSocket handling
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    handleConnection(server);

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  },
};

const UUID = "IxUgbfuIKqyv1EZlIyFcdGfDxHWrewwv";

async function handleConnection(ws) {
  ws.accept();

  // 👇 Let client know WS is established
  ws.send("connected");

  let remoteWriter = null;
  let remoteReader = null;

  ws.addEventListener("message", async (event) => {
    const chunk = new Uint8Array(event.data);

    if (!remoteWriter) {
      const parsed = parseVLESS(chunk);

      if (!parsed || parsed.uuid !== UUID) {
        ws.send("invalid uuid");
        ws.close();
        return;
      }

      const { address, port, rawData } = parsed;

      ws.send(`connecting to ${address}:${port}`);

      try {
        const tcp = await connectTCP(address, port);

        ws.send("tcp connected");

        remoteWriter = tcp.writable.getWriter();
        remoteReader = tcp.readable.getReader();

        if (rawData.byteLength > 0) {
          await remoteWriter.write(rawData);
        }

        pipeRemoteToWS(remoteReader, ws);

      } catch (e) {
        ws.send("connection failed");
        ws.close();
      }

    } else {
      try {
        await remoteWriter.write(chunk);
      } catch {
        ws.close();
      }
    }
  });

  ws.addEventListener("close", () => {
    try { remoteWriter?.close(); } catch {}
    try { remoteReader?.cancel(); } catch {}
  });
}

async function pipeRemoteToWS(reader, ws) {
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      ws.send(value);
    }
  } catch {}
  ws.close();
}

function parseVLESS(buffer) {
  try {
    const uuid = [...buffer.slice(1, 17)]
      .map(b => b.toString(16).padStart(2, "0"))
      .join("");

    const cmd = buffer[17];
    if (cmd !== 1) return null;

    const addrType = buffer[18];
    let addr = "";
    let portIndex;

    if (addrType === 1) {
      addr = buffer.slice(19, 23).join(".");
      portIndex = 23;
    } else if (addrType === 2) {
      const len = buffer[19];
      addr = new TextDecoder().decode(buffer.slice(20, 20 + len));
      portIndex = 20 + len;
    } else {
      return null;
    }

    const port = (buffer[portIndex] << 8) + buffer[portIndex + 1];
    const rawData = buffer.slice(portIndex + 2);

    return { uuid, address: addr, port, rawData };

  } catch {
    return null;
  }
}

async function connectTCP(host, port) {
  return await globalThis.connect({
    hostname: host,
    port: port,
  });
}
