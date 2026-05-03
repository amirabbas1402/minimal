export default {
  async fetch(request) {
    const url = new URL(request.url);

    // Only accept WebSocket on specific path
    if (url.pathname !== "/vless") {
      return new Response("Not Found", { status: 404 });
    }

    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 400 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    handleVLESS(server);

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  },
};

const UUID = "792401002";

async function handleVLESS(ws) {
  ws.accept();

  ws.addEventListener("message", async (event) => {
    const data = new Uint8Array(event.data);

    // ---- Minimal VLESS parsing ----
    const version = data[0];
    const uuid = [...data.slice(1, 17)]
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");

    if (!matchUUID(uuid, UUID)) {
      ws.close();
      return;
    }

    const cmd = data[17]; // 1 = TCP
    if (cmd !== 1) {
      ws.close();
      return;
    }

    const addrType = data[18];
    let addr = "";
    let portIndex;

    if (addrType === 1) {
      // IPv4
      addr = data.slice(19, 23).join(".");
      portIndex = 23;
    } else if (addrType === 2) {
      // domain
      const len = data[19];
      addr = new TextDecoder().decode(data.slice(20, 20 + len));
      portIndex = 20 + len;
    } else {
      ws.close();
      return;
    }

    const port = (data[portIndex] << 8) + data[portIndex + 1];

    // ---- Forward using fetch (HTTP only) ----
    try {
      const response = await fetch(`https://${addr}:${port}`, {
        method: "GET",
      });

      const body = await response.arrayBuffer();
      ws.send(body);
    } catch (e) {
      ws.close();
    }
  });
}

function matchUUID(a, b) {
  return a.replace(/-/g, "").toLowerCase() === b.replace(/-/g, "").toLowerCase();
}
