// Cloudflare Worker - HTTPS Tunnel
// Deploy this to sanjaghak1.ir

import { connect } from 'cloudflare:sockets';

export default {
  async fetch(request) {
    const url = new URL(request.url);

    // Health check
    if (url.pathname === "/") {
      return new Response(JSON.stringify({
        status: "ok",
        message: "HTTPS Tunnel Worker is running",
        time: new Date().toISOString(),
        note: "WebSocket endpoint at /ws"
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

  let activeTunnel = null;

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
          // Create TCP connection to target server
          const tcpSocket = connect({
            hostname: host,
            port: port,
            allowHalfOpen: true
          });
          
          activeTunnel = tcpSocket;
          
          ws.send(JSON.stringify({
            type: "tunnel_status",
            status: "connected",
            host: host,
            port: port
          }));
          
          // Forward data from TCP to WebSocket
          const tcpReader = tcpSocket.readable.getReader();
          const forwardToWebSocket = async () => {
            try {
              while (true) {
                const { done, value } = await tcpReader.read();
                if (done) break;
                
                // Send TCP data to client via WebSocket
                ws.send(JSON.stringify({
                  type: "tunnel_data",
                  data: btoa(String.fromCharCode(...value))
                }));
              }
            } catch (err) {
              console.error("TCP->WS error:", err);
            } finally {
              tcpReader.releaseLock();
              ws.send(JSON.stringify({ type: "tunnel_close" }));
            }
          };
          
          // Forward data from WebSocket to TCP
          const wsToTcp = async () => {
            const writer = tcpSocket.writable.getWriter();
            
            // Create a message handler for this specific tunnel
            const messageHandler = async (event) => {
              try {
                const data = JSON.parse(event.data);
                if (data.type === "tunnel_data") {
                  const binaryData = Uint8Array.from(atob(data.data), c => c.charCodeAt(0));
                  await writer.write(binaryData);
                }
              } catch (err) {
                console.error("WS->TCP error:", err);
              }
            };
            
            ws.addEventListener("message", messageHandler);
            
            // Wait for close
            await tcpSocket.closed;
            writer.releaseLock();
            ws.removeEventListener("message", messageHandler);
          };
          
          // Run both directions
          forwardToWebSocket();
          wsToTcp();
          
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
      
      // Handle regular HTTP request
      else if (msg.url) {
        const { url, method = "GET", headers = {}, body } = msg;
        
        console.log(`HTTP ${method} ${url}`);
        
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
    if (activeTunnel) {
      activeTunnel.close();
    }
  });
}
