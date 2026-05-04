export default {
  async fetch(request) {
    const url = new URL(request.url);

    // 🌐 health check
    if (url.pathname === "/") {
      return new Response("WS HTTP Relay is running", { status: 200 });
    }

    // 🔌 WebSocket endpoint
    if (url.pathname !== "/ws") {
      return new Response("Not Found", { status: 404 });
    }

    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 400 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    handleWS(server);

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  },
};

async function handleWS(ws) {
  ws.accept();

  ws.addEventListener("message", async (event) => {
    try {
      const msg = JSON.parse(event.data);

      // Expected format:
      // {
      //   url: "https://example.com",
      //   method: "GET",
      //   headers: {},
      //   body: "optional"
      // }

      const resp = await fetch(msg.url, {
        method: msg.method || "GET",
        headers: msg.headers || {},
        body: msg.body || null,
      });

      // Send response headers first
      ws.send(JSON.stringify({
        type: "headers",
        status: resp.status,
        headers: Object.fromEntries(resp.headers),
      }));

      // Stream body
      const reader = resp.body.getReader();

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        // send as base64 (safe over WS)
        ws.send(JSON.stringify({
          type: "chunk",
          data: btoa(String.fromCharCode(...value))
        }));
      }

      ws.send(JSON.stringify({ type: "end" }));

    } catch (e) {
      ws.send(JSON.stringify({
        type: "error",
        message: e.toString()
      }));
    }
  });
}
