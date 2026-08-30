import { evictDurableObject, runInDurableObject } from "cloudflare:test";
import { env, exports } from "cloudflare:workers";
import { describe, expect, it } from "vite-plus/test";
import { SYNC_METHOD } from "../src/index.ts";

const worker = exports as unknown as {
  default: { fetch(request: Request): Promise<Response> };
};

type ResponseMessage = {
  id: string | null;
  result?: unknown;
  error?: { code?: number; message?: string };
};

type SyncNotification = {
  jsonrpc: "2.0";
  method: typeof SYNC_METHOD;
  params: Array<{
    topic: { name: string; params: unknown[] };
    value: unknown;
  }>;
};

type WebSocketTestClient = {
  subscribe(topic: unknown): Promise<ResponseMessage>;
  unsubscribe(topic: unknown): Promise<ResponseMessage>;
  sync(name: string, params: unknown[]): Promise<ResponseMessage>;
  waitForSync(predicate?: (value: SyncNotification) => boolean): Promise<SyncNotification>;
  syncCount(): number;
  sendRaw(message: string): void;
  waitForFrame<T>(predicate: (value: unknown) => value is T): Promise<T>;
  close(): void;
};

function isResponseMessage(value: unknown): value is ResponseMessage {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    "id" in value &&
    (typeof value.id === "string" || value.id === null)
  );
}

function isSyncNotification(value: unknown): value is SyncNotification {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    "jsonrpc" in value &&
    value.jsonrpc === "2.0" &&
    "method" in value &&
    value.method === SYNC_METHOD &&
    "params" in value &&
    Array.isArray(value.params)
  );
}

function hasCounterValue(notification: SyncNotification, expected: number): boolean {
  return notification.params.some(
    (event) =>
      typeof event.value === "object" &&
      event.value !== null &&
      !Array.isArray(event.value) &&
      "value" in event.value &&
      event.value.value === expected,
  );
}

async function connect(): Promise<WebSocketTestClient> {
  const response = await worker.default.fetch(
    new Request("https://example.com", { headers: { Upgrade: "websocket" } }),
  );
  const socket = response.webSocket!;
  socket.accept();
  const frames: unknown[] = [];
  const waiters = new Set<{
    predicate: (value: unknown) => boolean;
    resolve(value: unknown): void;
  }>();
  const push = (value: unknown): void => {
    frames.push(value);
    for (const waiter of waiters) {
      if (!waiter.predicate(value)) continue;
      waiters.delete(waiter);
      waiter.resolve(value);
    }
  };
  socket.addEventListener("message", (event: MessageEvent) => push(JSON.parse(String(event.data))));
  let requestId = 0;
  const waitForFrame = <T>(predicate: (value: unknown) => value is T): Promise<T> => {
    const existing = frames.find(predicate);
    if (existing !== undefined) return Promise.resolve(existing);
    return new Promise<T>((resolve) => {
      waiters.add({ predicate, resolve });
    });
  };
  const call = async (method: string, params: unknown[]): Promise<ResponseMessage> => {
    const id = String(++requestId);
    const response = waitForFrame(
      (value): value is ResponseMessage => isResponseMessage(value) && value.id === id,
    );
    socket.send(JSON.stringify({ jsonrpc: "2.0", id, method, params }));
    return response;
  };
  return {
    subscribe: (topic) => call("subscribe", [topic]),
    unsubscribe: (topic) => call("unsubscribe", [topic]),
    sync: (name, params) => call("sync", [name, params]),
    waitForSync: (predicate = () => true) =>
      waitForFrame(
        (value): value is SyncNotification => isSyncNotification(value) && predicate(value),
      ),
    syncCount: () => frames.filter(isSyncNotification).length,
    sendRaw: (message) => socket.send(message),
    waitForFrame,
    close: () => socket.close(),
  };
}

describe("Durable Object typed-rpc WebSocket transport", () => {
  it("rejects non-WebSocket requests", async () => {
    const response = await worker.default.fetch(new Request("https://example.com"));
    expect([response.status, await response.text()]).toEqual([
      400,
      "This endpoint only accepts WebSocket requests.",
    ]);
  });

  it("batches unique topics for every subscribed socket", async () => {
    const first = await connect();
    const second = await connect();
    try {
      const alpha = { name: "counter", params: ["batch-alpha"] };
      const beta = { name: "counter", params: ["batch-beta"] };
      expect((await first.subscribe(alpha)).result).toBeNull();
      expect((await first.waitForSync()).params).toEqual([
        { topic: alpha, value: { key: "batch-alpha", value: 0 } },
      ]);
      expect((await first.subscribe(beta)).result).toBeNull();
      expect(
        (await first.waitForSync((value) => value.params[0]?.topic.params[0] === "batch-beta"))
          .params,
      ).toEqual([{ topic: beta, value: { key: "batch-beta", value: 0 } }]);
      expect((await first.subscribe(alpha)).result).toBeNull();
      expect((await second.subscribe(alpha)).result).toBeNull();
      expect((await second.waitForSync()).params).toEqual([
        { topic: alpha, value: { key: "batch-alpha", value: 0 } },
      ]);
      expect((await first.sync("increment", ["batch-alpha", 2])).result).toBeNull();
      expect((await first.waitForSync((value) => hasCounterValue(value, 2))).params).toEqual([
        { topic: alpha, value: { key: "batch-alpha", value: 2 } },
      ]);
      expect(
        (await first.waitForSync((value) => value.params[0]?.topic.params[0] === "batch-beta"))
          .params,
      ).toEqual([{ topic: beta, value: { key: "batch-beta", value: 0 } }]);
      expect((await second.waitForSync((value) => hasCounterValue(value, 2))).params).toEqual([
        { topic: alpha, value: { key: "batch-alpha", value: 2 } },
      ]);
      expect((await first.unsubscribe(beta)).result).toBeNull();
      expect((await first.sync("increment", ["batch-alpha", 1])).result).toBeNull();
      expect((await first.waitForSync((value) => hasCounterValue(value, 3))).params).toEqual([
        { topic: alpha, value: { key: "batch-alpha", value: 3 } },
      ]);
      expect((await second.waitForSync((value) => hasCounterValue(value, 3))).params).toEqual([
        { topic: alpha, value: { key: "batch-alpha", value: 3 } },
      ]);
      expect(first.syncCount()).toBe(6);
      expect(second.syncCount()).toBe(3);
    } finally {
      first.close();
      second.close();
    }
  });

  it("does not send empty sync notifications to idle sockets", async () => {
    const idle = await connect();
    const active = await connect();
    try {
      const topic = { name: "counter", params: ["active"] };
      await active.subscribe(topic);
      await active.waitForSync();
      await active.sync("increment", ["active", 1]);
      await active.waitForSync((value) => hasCounterValue(value, 1));
      expect(idle.syncCount()).toBe(0);
    } finally {
      idle.close();
      active.close();
    }
  });

  it("returns unknown queries as RPC errors", async () => {
    const connection = await connect();
    try {
      expect((await connection.subscribe({ name: "unknown", params: [] })).error).toBeDefined();
    } finally {
      connection.close();
    }
  });

  it("returns parse errors for malformed JSON", async () => {
    const connection = await connect();
    try {
      connection.sendRaw("{");
      const response = await connection.waitForFrame(
        (value): value is ResponseMessage =>
          isResponseMessage(value) && value.id === null && value.error?.code === -32700,
      );
      expect(response.error?.code).toBe(-32700);
    } finally {
      connection.close();
    }
  });

  it("stores topics under the configured attachment key and removes them on close", async () => {
    const connection = await connect();
    try {
      const topic = { name: "counter", params: ["close-cleanup"] };
      await runInDurableObject(env.FIXTURE_SYNC_OBJECT.getByName("default"), (instance) => {
        (instance as unknown as { ctx: DurableObjectState }).ctx
          .getWebSockets()[0]
          ?.serializeAttachment({
            session: "keep",
          });
      });
      await connection.subscribe(topic);
      await connection.waitForSync();
      await runInDurableObject(env.FIXTURE_SYNC_OBJECT.getByName("default"), (instance) => {
        const socket = (instance as unknown as { ctx: DurableObjectState }).ctx.getWebSockets()[0]!;
        expect(socket.deserializeAttachment()).toEqual({
          session: "keep",
          "fixture-topics": [topic],
        });
        instance.webSocketClose(socket, 1005, "");
        expect(socket.deserializeAttachment()).toEqual({ session: "keep" });
      });
    } finally {
      connection.close();
    }
  });

  it("removes stored topics on WebSocket errors", async () => {
    const connection = await connect();
    try {
      const topic = { name: "counter", params: ["error-cleanup"] };
      await runInDurableObject(env.FIXTURE_SYNC_OBJECT.getByName("default"), (instance) => {
        (instance as unknown as { ctx: DurableObjectState }).ctx
          .getWebSockets()[0]
          ?.serializeAttachment({
            session: "keep",
          });
      });
      await connection.subscribe(topic);
      await connection.waitForSync();
      const warn = console.warn;
      const error = new Error("test error");
      const warnings: unknown[][] = [];
      console.warn = (...args: unknown[]) => warnings.push(args);
      try {
        await runInDurableObject(env.FIXTURE_SYNC_OBJECT.getByName("default"), (instance) => {
          const socket = (
            instance as unknown as { ctx: DurableObjectState }
          ).ctx.getWebSockets()[0]!;
          instance.webSocketError(socket, error);
          expect(socket.deserializeAttachment()).toEqual({ session: "keep" });
        });
      } finally {
        console.warn = warn;
      }
      expect(warnings).toContainEqual(["WebSocket failed", error]);
    } finally {
      connection.close();
    }
  });

  it("restores subscriptions after Durable Object eviction without another initial event", async () => {
    const connection = await connect();
    try {
      const topic = { name: "counter", params: ["restore"] };
      await connection.subscribe(topic);
      await connection.waitForSync();
      const beforeEviction = connection.syncCount();
      await evictDurableObject(env.FIXTURE_SYNC_OBJECT.getByName("default"));
      expect((await connection.sync("increment", ["restore", 3])).result).toBeNull();
      expect((await connection.waitForSync((value) => hasCounterValue(value, 3))).params).toEqual([
        { topic, value: { key: "restore", value: 3 } },
      ]);
      expect(connection.syncCount()).toBe(beforeEviction + 1);
    } finally {
      connection.close();
    }
  });
});
