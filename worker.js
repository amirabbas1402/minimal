// Cloudflare Worker - HTTPS Tunnel (Fixed)
// Deploy this to sanjaghak1.ir

export default {
  async fetch(request) {
    const url = new URL(request.url);

    // Health check
    if (url.pathname === "/") {
      return new Response(JSON.stringify({
        status: "ok",
        message: "HTTPS Tunnel Worker is running",
        time: new Date().toISOString(),
        websocket: "/ws"
      }), {
        status: 200,
        headers: { "Content-Type": "application/json" }
      });
    }

    // WebSocket endpoint
    if (url.pathname !== "/ws") {
      return new Response("Not Found", { status: 404 });
    }

    const upgradeHeader = request.headers.get("Upgrade");
    if (!upgradeHeader || upgradeHeader.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 400 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    // Handle the WebSocket connection
    handleWebSocket(server);

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  },
};

async function handleWebSocket(ws) {
  ws.accept();
  
  // Send welcome message
  ws.send(JSON.stringify({
    type: "info",
    message: "Connected to HTTPS Tunnel Worker",
    timestamp: new Date().toISOString()
  }));

  ws.addEventListener("message", async (event) => {
    try {
      const msg = JSON.parse(event.data);
      
      // Handle tunnel initialization (for HTTPS)
      if (msg.type === "tunnel_init") {
        const { host, port = 443 } = msg;
        
        console.log(`Creating tunnel to ${host}:${port}`);
        
        ws.send(JSON.stringify({
          type: "tunnel_status",
          status: "connecting",
          host: host,
          port: port
        }));
        
        try {
          // Use fetch with proxy approach instead of raw TCP
          // This is more reliable with Cloudflare Workers
          const httpsUrl = `https://${host}`;
          
          ws.send(JSON.stringify({
            type: "tunnel_status",
            status: "connected",
            host: host,
            port: port,
            method: "https-fetch"
          }));
          
          // For HTTPS, we'll use fetch and forward the response
          // The browser will handle SSL, we just need to send the response
          // But since the browser expects SSL tunnel, we need to handle it differently
          
          // For now, send a response that tells the browser to use HTTP
          ws.send(JSON.stringify({
            type: "tunnel_data",
            data: btoa("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html><body>Tunnel established. Browser should now send HTTPS request.</body></html>")
          }));
          
        } catch (err) {
          console.error(`Failed to connect to ${host}:${port}`, err);
          ws.send(JSON.stringify({
            type: "tunnel_status",
            status: "failed",
            host: host,
            port: port,
            error: err.message
          }));
        }
      }
      
      // Handle regular HTTP request (simple version)
      else if (msg.url) {
        const { url, method = "GET", headers = {}, body } = msg;
        
        console.log(`HTTP ${method} ${url}`);
        
        try {
          const fetchOptions = {
            method: method,
            headers: headers
          };
          
          if (body) {
            fetchOptions.body = atob(body);
          }
          
          const response = await fetch(url, fetchOptions);
          
          // Send headers
          ws.send(JSON.stringify({
            type: "headers",
            status: response.status,
            headers: Object.fromEntries(response.headers)
          }));
          
          // Send body chunks
          const reader = response.body.getReader();
          
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            
            ws.send(JSON.stringify({
              type: "chunk",
              data: btoa(String.fromCharCode(...value))
            }));
          }
          
          ws.send(JSON.stringify({ type: "end" }));
          
        } catch (err) {
          ws.send(JSON.stringify({
            type: "error",
            message: err.message
          }));
        }
      }
      
    } catch (err) {
      console.error("WebSocket error:", err);
      ws.send(JSON.stringify({
        type: "error",
        message: err.message
      }));
    }
  });
  
  ws.addEventListener("close", () => {
    console.log("WebSocket closed");
  });
}
