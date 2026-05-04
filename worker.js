export default {
  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname === "/") {
      return new Response("WS relay active");
    }

    if (url.pathname !== "/vless") {
      return new Response("Not Found", { status: 404 });
    }

    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 400 });
    }

    // Forward WS directly to VPS
    const target = "ws://103.83.86.246:8080/vless";

    return fetch(target, {
      method: request.method,
      headers: request.headers,
      body: request.body
    });
  }
};
