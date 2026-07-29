import { Effect, Exit } from "effect";
import { exports, env } from "cloudflare:workers";
import { evictDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vite-plus/test";
import { SyncEngine, toTables } from "@do-sync-engine/core";
import { SubscriptionRegistry } from "../src/subscriptions.ts";
import { decodeClientCommand } from "../src/protocol.ts";
import type { FixtureSyncObject } from "./cloudflare-worker.ts";
const worker = exports as unknown as { default: { fetch(request: Request): Promise<Response> } };
const fixtureEnv = env as typeof env & {
  FIXTURE_SYNC_OBJECT: DurableObjectNamespace<FixtureSyncObject>;
};

type Message = {
  type: string;
  requestId?: string;
  topic?: { hash: string; name: string; params: unknown[] };
  value?: unknown;
  removed?: boolean;
  message?: string;
};
function connect() {
  return worker.default.fetch(
    new Request("https://example.com", { headers: { Upgrade: "websocket" } }),
  );
}
function read(socket: WebSocket): Promise<Message> {
  return new Promise((resolve) =>
    socket.addEventListener("message", (event) => resolve(JSON.parse(String(event.data))), {
      once: true,
    }),
  );
}

describe("Durable Object WebSocket transport", () => {
  it("decodes valid commands and preserves protocol errors", async () => {
    const decode = (message: string | ArrayBuffer) =>
      Effect.runPromiseExit(decodeClientCommand(message));
    expect(
      await decode('{"type":"subscribe","requestId":"s","query":"counter","params":["alpha"]}'),
    ).toEqual(
      Exit.succeed({ type: "subscribe", requestId: "s", query: "counter", params: ["alpha"] }),
    );
    expect(
      await decode('{"type":"unsubscribe","requestId":"u","topic":{"name":"counter","params":[]}}'),
    ).toEqual(
      Exit.succeed({
        type: "unsubscribe",
        requestId: "u",
        topic: { name: "counter", params: [] },
      }),
    );
    expect(
      await decode('{"type":"sync","requestId":"m","mutation":"increment","params":["alpha",1]}'),
    ).toEqual(
      Exit.succeed({
        type: "sync",
        requestId: "m",
        mutation: "increment",
        params: ["alpha", 1],
      }),
    );
    expect(await decode(new ArrayBuffer(0))).toEqual(
      Exit.fail({ type: "error", message: "Expected text WebSocket message" }),
    );
    expect(await decode('{"type":"subscribe","requestId":"s","query":"counter"}')).toEqual(
      Exit.fail({ type: "error", requestId: "s", message: "query and params required" }),
    );
    expect(await decode('{"type":"unsubscribe","requestId":"u"}')).toEqual(
      Exit.fail({ type: "error", requestId: "u", message: "topic required" }),
    );
    expect(await decode('{"type":"sync","requestId":"m","mutation":"increment"}')).toEqual(
      Exit.fail({ type: "error", requestId: "m", message: "mutation and params required" }),
    );
    expect(await decode('{"type":"wat","requestId":"x"}')).toEqual(
      Exit.fail({ type: "error", requestId: "x", message: "Unknown message type" }),
    );
    expect(await decode("not json")).toEqual(
      Exit.fail({ type: "error", message: "Invalid JSON message" }),
    );
    expect(await decode("null")).toEqual(
      Exit.fail({ type: "error", message: "Invalid JSON message" }),
    );
    expect(await decode("[]")).toEqual(
      Exit.fail({ type: "error", message: "Invalid JSON message" }),
    );
    expect(await decode('{"type":"sync","params":[]}')).toEqual(
      Exit.fail({ type: "error", message: "requestId required" }),
    );
    expect(await decode('{"type":"sync","requestId":" ","params":[]}')).toEqual(
      Exit.fail({ type: "error", message: "requestId required" }),
    );
  });
  it("subscribes, syncs, restores after hibernation, and unsubscribes", async () => {
    const rejected = await worker.default.fetch(new Request("https://example.com"));
    expect([rejected.status, await rejected.text()]).toEqual([426, "Expected WebSocket"]);
    const response = await connect();
    const socket = response.webSocket!;
    socket.accept();
    try {
      socket.send(
        JSON.stringify({
          type: "subscribe",
          requestId: "sub",
          query: "counter",
          params: ["alpha"],
        }),
      );
      const first = await read(socket);
      expect(first.requestId).toBe("sub");
      expect(first.value).toEqual({ key: "alpha", value: 0 });
      socket.send(
        JSON.stringify({
          type: "subscribe",
          requestId: "sub-again",
          query: "counter",
          params: ["alpha"],
        }),
      );
      expect((await read(socket)).requestId).toBe("sub-again");
      socket.send(
        JSON.stringify({
          type: "sync",
          requestId: "inc",
          mutation: "increment",
          params: ["alpha", 2],
        }),
      );
      expect((await read(socket)).value).toEqual({ key: "alpha", value: 2 });
      expect((await read(socket)).type).toBe("synced");
      await evictDurableObject(fixtureEnv.FIXTURE_SYNC_OBJECT.getByName("default"), {
        webSockets: "hibernate",
      });
      socket.send(
        JSON.stringify({
          type: "sync",
          requestId: "inc2",
          mutation: "increment",
          params: ["alpha", 1],
        }),
      );
      expect((await read(socket)).value).toEqual({ key: "alpha", value: 3 });
      expect((await read(socket)).type).toBe("synced");
      socket.send(
        JSON.stringify({
          type: "unsubscribe",
          requestId: "unsub",
          topic: JSON.parse(JSON.stringify(first.topic)),
        }),
      );
      expect((await read(socket)).removed).toBe(true);
      await evictDurableObject(fixtureEnv.FIXTURE_SYNC_OBJECT.getByName("default"), {
        webSockets: "hibernate",
      });
      socket.send(
        JSON.stringify({
          type: "sync",
          requestId: "post-unsub",
          mutation: "increment",
          params: ["alpha", 1],
        }),
      );
      expect((await read(socket)).type).toBe("synced");

      for (const [requestId, message, expected] of [
        ["bad-sub", { type: "subscribe", requestId: "bad-sub" }, "query and params required"],
        ["bad-unsub", { type: "unsubscribe", requestId: "bad-unsub" }, "topic required"],
        ["bad-sync", { type: "sync", requestId: "bad-sync" }, "mutation and params required"],
        ["unknown", { type: "wat", requestId: "unknown" }, "Unknown message type"],
      ] as const) {
        socket.send(JSON.stringify(message));
        const result = await read(socket);
        expect([result.requestId, result.message]).toEqual([requestId, expected]);
      }
      for (const message of ["not json", "null"]) {
        socket.send(message);
        expect(await read(socket)).toEqual({ type: "error", message: "Invalid JSON message" });
      }
      socket.send(new Uint8Array([1, 2]));
      expect(await read(socket)).toEqual({
        type: "error",
        message: "Expected text WebSocket message",
      });
      socket.send(JSON.stringify({}));
      expect(await read(socket)).toEqual({ type: "error", message: "requestId required" });
      socket.send(
        JSON.stringify({
          type: "subscribe",
          requestId: "missing-query",
          query: "missing",
          params: ["x"],
        }),
      );
      expect((await read(socket)).message).toContain("Unknown query: missing");
      socket.send(
        JSON.stringify({
          type: "sync",
          requestId: "missing-mutation",
          mutation: "missing",
          params: ["x"],
        }),
      );
      expect((await read(socket)).message).toContain("Unknown mutation: missing");
      socket.send(
        JSON.stringify({
          type: "unsubscribe",
          requestId: "again",
          topic: { name: "counter", params: [] },
        }),
      );
      expect((await read(socket)).removed).toBe(false);
      socket.send(
        JSON.stringify({
          type: "subscribe",
          requestId: "too-large",
          query: "counter",
          params: ["x".repeat(20_000)],
        }),
      );
      expect((await read(socket)).message).toBeDefined();
      socket.close();
    } finally {
      socket.close();
    }
  });
});
it("does not retain duplicate listeners when restoring a socket", async () => {
  const engine = new SyncEngine({
    queries: {
      counter: {
        tables: toTables(["counters"]),
        run: () => Effect.succeed({ value: 0 }),
      },
    },
    mutations: {
      increment: {
        tables: toTables(["counters"]),
        run: () => Effect.succeed(undefined),
      },
    },
  });
  const messages: unknown[] = [];
  let attachment: unknown;
  const socket = {
    deserializeAttachment: () => attachment,
    serializeAttachment: (value: unknown) => {
      attachment = value;
    },
  } as WebSocket;
  const registry = new SubscriptionRegistry(engine, (_ws, message) => messages.push(message));
  const topic = await Effect.runPromise(engine.createTopic("counter", []));
  attachment = { topics: [topic] };
  await Effect.runPromise(registry.restore(socket));
  messages.length = 0;
  await Effect.runPromise(registry.restore(socket));
  messages.length = 0;
  await Effect.runPromise(engine.sync("increment", []));
  expect(messages).toHaveLength(1);
});
