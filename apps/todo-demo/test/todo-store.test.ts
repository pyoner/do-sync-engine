import { exports } from "cloudflare:workers";
import { describe, expect, it } from "vite-plus/test";

describe("TodoStore typed-rpc transport", () => {
  it("accepts a WebSocket connection", async () => {
    const response = await exports.default.fetch(
      new Request("https://example.com/api/todos", { headers: { Upgrade: "websocket" } }),
    );
    expect(response.status).toBe(101);
    response.webSocket?.accept();
    response.webSocket?.close();
  });
});
