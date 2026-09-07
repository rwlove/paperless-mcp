#!/usr/bin/env node
import { SSEServerTransport } from "@modelcontextprotocol/sdk/server/sse.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";
import express from "express";
import { randomUUID } from "node:crypto";
import { parseArgs } from "node:util";
import {
  createMcpServer,
  getBearerToken,
  sendUnauthorized,
} from "./server";
const { version } = require("../package.json") as { version: string };

const {
  values: { baseUrl, token, http: useHttp, port, publicUrl, "no-auth": noAuth },
} = parseArgs({
  options: {
    baseUrl: { type: "string" },
    token: { type: "string" },
    http: { type: "boolean", default: false },
    port: { type: "string" },
    publicUrl: { type: "string", default: "" },
    "no-auth": { type: "boolean", default: false },
  },
  allowPositionals: true,
});

const resolvedBaseUrl = baseUrl || process.env.PAPERLESS_URL;
const resolvedToken = token || process.env.PAPERLESS_API_KEY;
const resolvedPublicUrl =
  publicUrl || process.env.PAPERLESS_PUBLIC_URL || resolvedBaseUrl;
const resolvedPort = port ? parseInt(port, 10) : 3000;

if (!resolvedBaseUrl) {
  console.error(
    "Usage: paperless-mcp --baseUrl <url> --token <token> [--http] [--port <port>] [--publicUrl <url>] [--no-auth]"
  );
  console.error(
    "Or set PAPERLESS_URL and PAPERLESS_API_KEY environment variables."
  );
  process.exit(1);
}

if (!useHttp && !resolvedToken) {
  console.error(
    "Usage: paperless-mcp --baseUrl <url> --token <token> [--http] [--port <port>] [--publicUrl <url>] [--no-auth]"
  );
  console.error(
    "Or set PAPERLESS_URL and PAPERLESS_API_KEY environment variables."
  );
  process.exit(1);
}

if (noAuth && !resolvedToken) {
  console.error(
    "--no-auth allows unauthenticated requests to use the server's Paperless token, " +
      "but no server token is configured. Provide --token <token> or set PAPERLESS_API_KEY, " +
      "or drop --no-auth and have clients authenticate with 'Authorization: Bearer <token>'."
  );
  process.exit(1);
}

function buildServer(requestToken: string) {
  return createMcpServer({
    baseUrl: resolvedBaseUrl!,
    token: requestToken,
    version,
    publicUrl: resolvedPublicUrl!,
  });
}

async function main() {
  if (useHttp) {
    if (noAuth) {
      console.log(
        "[paperless-mcp] --no-auth is enabled: requests without an 'Authorization: Bearer' header " +
          "will use the server's Paperless token. Only use this on a trusted/local network."
      );
    } else if (resolvedToken) {
      console.log(
        "[paperless-mcp] A server token is configured, but unauthenticated requests are rejected. " +
          "Clients must send 'Authorization: Bearer <paperless-token>'. " +
          "To use the server token for unauthenticated requests instead, restart with the --no-auth flag " +
          "(trusted/local networks only)."
      );
    }

    const app = express();
    app.use(express.json());

    // Store transports for each session
    const sseTransports: Record<string, SSEServerTransport> = {};

    // Stateful Streamable HTTP: one transport + server per session, keyed by
    // Mcp-Session-Id. The Kuadrant mcp-gateway broker negotiates 2025-11-25
    // (stateful) and drops any upstream that runs stateless (empty session,
    // GET /mcp -> 405). SDK >=1.30 negotiates 2025-11-25 natively, so no
    // protocol shim is needed — only a real session + notification stream.
    // workaround: https://github.com/Kuadrant/mcp-gateway/issues/1419 — return
    // to a stateless transport when the broker recovers stale sessions itself.
    const httpTransports: Record<string, StreamableHTTPServerTransport> = {};

    app.post("/mcp", async (req, res) => {
      const sessionId = req.headers["mcp-session-id"] as string | undefined;
      let transport = sessionId ? httpTransports[sessionId] : undefined;

      if (!transport) {
        if (sessionId || !isInitializeRequest(req.body)) {
          // A non-initialize request must carry a known session id.
          res.status(400).json({
            jsonrpc: "2.0",
            error: {
              code: -32000,
              message: "Bad Request: No valid session ID provided",
            },
            id: null,
          });
          return;
        }

        const requestToken = getBearerToken(req, {
          fallbackToken: resolvedToken,
          allowAnonymous: noAuth,
        });
        if (!requestToken) {
          sendUnauthorized(res);
          return;
        }

        const newTransport = new StreamableHTTPServerTransport({
          sessionIdGenerator: () => randomUUID(),
          onsessioninitialized: (sid) => {
            httpTransports[sid] = newTransport;
          },
        });
        newTransport.onclose = () => {
          if (newTransport.sessionId) {
            delete httpTransports[newTransport.sessionId];
          }
        };
        const server = buildServer(requestToken);
        await server.connect(newTransport);
        transport = newTransport;
      }

      try {
        await transport.handleRequest(req, res, req.body);
      } catch (error) {
        console.error("Error handling MCP request:", error);
        if (!res.headersSent) {
          res.status(500).json({
            jsonrpc: "2.0",
            error: {
              code: -32603,
              message: "Internal server error",
            },
            id: null,
          });
        }
      }
    });

    // GET (server->client notification stream) and DELETE (session teardown)
    // are served from the existing session's transport.
    const handleSessionRequest = async (
      req: express.Request,
      res: express.Response
    ) => {
      const sessionId = req.headers["mcp-session-id"] as string | undefined;
      const transport = sessionId ? httpTransports[sessionId] : undefined;
      if (!transport) {
        res.status(400).send("Invalid or missing session ID");
        return;
      }
      await transport.handleRequest(req, res);
    };

    app.get("/mcp", handleSessionRequest);
    app.delete("/mcp", handleSessionRequest);

    app.get("/sse", async (req, res) => {
      console.log("SSE request received");
      const requestToken = getBearerToken(req, {
        fallbackToken: resolvedToken,
        allowAnonymous: noAuth,
      });
      if (!requestToken) {
        sendUnauthorized(res);
        return;
      }
      try {
        const server = buildServer(requestToken);
        const transport = new SSEServerTransport("/messages", res);
        sseTransports[transport.sessionId] = transport;
        res.on("close", () => {
          delete sseTransports[transport.sessionId];
          transport.close();
        });
        await server.connect(transport);
      } catch (error) {
        console.error("Error handling SSE request:", error);
        if (!res.headersSent) {
          res.status(500).json({
            jsonrpc: "2.0",
            error: {
              code: -32603,
              message: "Internal server error",
            },
            id: null,
          });
        }
      }
    });

    app.post("/messages", async (req, res) => {
      const sessionId = req.query.sessionId as string;
      const transport = sseTransports[sessionId];
      if (transport) {
        await transport.handlePostMessage(req, res, req.body);
      } else {
        res.status(400).send("No transport found for sessionId");
      }
    });

    app.listen(resolvedPort, () => {
      console.log(
        `MCP Stateful Streamable HTTP Server listening on port ${resolvedPort}`
      );
    });
    // await new Promise((resolve) => setTimeout(resolve, 1000000));
  } else {
    const server = buildServer(resolvedToken!);
    const transport = new StdioServerTransport();
    await server.connect(transport);
  }
}

main().catch((e) => console.error(e.message));
