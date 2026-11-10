/**
 * The History Server answers requests for a running application with a 302 to
 * the same URL when its cached UI is stale (ApplicationCacheCheckFilter). The
 * Python server's urllib3 follows it; this port must too.
 */

import assert from "node:assert/strict";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { after, before, describe, it } from "node:test";

import { NotFoundException, RestClient } from "../src/api/httpClient.js";

describe("RestClient redirects", () => {
  let server: http.Server;
  let base = "";
  const seen: string[] = [];

  before(async () => {
    let first = true;
    server = http.createServer((req, res) => {
      seen.push(req.url ?? "");
      if (req.url?.startsWith("/api/v1/applications/app-1/1/stages")) {
        if (first) {
          first = false;
          res.writeHead(302, { Location: req.url });
          res.end();
          return;
        }
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end('[{"stageId": 0}]');
        return;
      }
      if (req.url === "/loop") {
        res.writeHead(302, { Location: "/loop" });
        res.end();
        return;
      }
      res.writeHead(404);
      res.end("no such app");
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  });

  after(() => server.close());

  it("follows the History Server's 302 for a stale running app and keeps the query", async () => {
    const client = new RestClient(`${base}/api/v1`);
    const stages = await client.get<Array<{ stageId: number }>>("/applications/app-1/1/stages", { withSummaries: true });
    assert.deepEqual(stages, [{ stageId: 0 }]);
    assert.deepEqual(seen.slice(0, 2), [
      "/api/v1/applications/app-1/1/stages?withSummaries=true",
      "/api/v1/applications/app-1/1/stages?withSummaries=true",
    ]);
  });

  it("stops after a bounded number of redirects", async () => {
    const client = new RestClient(base);
    await assert.rejects(client.get("/loop"), (error: Error) => error.message.startsWith("(302)"));
  });

  it("still raises status errors", async () => {
    const client = new RestClient(base);
    await assert.rejects(client.get("/missing"), NotFoundException);
  });
});
