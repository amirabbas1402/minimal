export default {
  async fetch(request) {
    const url = new URL(request.url);

    // Simple health check
    if (url.pathname === "/") {
      return new Response("Worker is running", { status: 200 });
    }

    // WebSocket endpoint
    if (url.pathname !== "/ws") {
      return new Response("Not Found", { status: 404 });
    }

    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 400 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    server.accept();

    // 🔁 Connect to backend WS server (replace this)
    const backend = new WebSocket("ws://103.83.86.246:8080");

    backend.addEventListener("open", () => {
      server.send("Connected to backend");
    });

    backend.addEventListener("message", (event) => {
      server.send(event.data);
    });

    server.addEventListener("message", (event) => {
      backend.send(event.data);
    });

    server.addEventListener("close", () => {
      backend.close();
    });

    backend.addEventListener("close", () => {
      server.close();
    });

    return new Response(null, {
      status: 101,
      webSocket: client
    });
  }
};
