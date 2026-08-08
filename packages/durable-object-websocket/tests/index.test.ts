import { exports, env } from "cloudflare:workers";
import { describe, expect, it } from "vite-plus/test";
import { newWebSocketRpcSession, type RpcStub } from "capnweb";
import type { FixtureSyncObject } from "./cloudflare-worker.ts";

type Event = { topic: { hash: string; name: string; params: unknown[] }; value: unknown };
type Subscription = { unsubscribe(): boolean };
type Api = {
  subscribe(query: "counter", params: [string], listener: (event: Event) => void): Subscription;
  sync(mutation: "increment", params: [string, number]): void;
};
const worker = exports as unknown as { default: { fetch(request: Request): Promise<Response> } };
void (env as typeof env & { FIXTURE_SYNC_OBJECT: DurableObjectNamespace<FixtureSyncObject> });

async function connect(): Promise<RpcStub<Api>> {
  const response = await worker.default.fetch(
    new Request("https://example.com", { headers: { Upgrade: "websocket" } }),
  );
  const socket = response.webSocket!;
  socket.accept();
  return newWebSocketRpcSession<Api>(socket);
}

describe("Durable Object Cap’n Web transport", () => {
  it("rejects non-WebSocket requests", async () => {
    const response = await worker.default.fetch(new Request("https://example.com"));
    expect([response.status, await response.text()]).toEqual([
      400,
      "This endpoint only accepts WebSocket requests.",
    ]);
  });

  it("subscribes, syncs, and unsubscribes", async () => {
    const api = await connect();
    const events: Event[] = [];
    const subscription = await api.subscribe(
      "counter",
      ["alpha"],
      (event) => void events.push(event),
    );
    expect(events[0]?.value).toEqual({ key: "alpha", value: 0 });
    await api.sync("increment", ["alpha", 2]);
    expect(events.at(-1)?.value).toEqual({ key: "alpha", value: 2 });
    expect(await subscription.unsubscribe()).toBe(true);
    await api.sync("increment", ["alpha", 1]);
    expect(events.at(-1)?.value).toEqual({ key: "alpha", value: 2 });
    api[Symbol.dispose]();
  });
});
