import { SyncEngine, toTables } from "@do-sync-engine/core";
import type { Mutation, Query } from "@do-sync-engine/core";
import { evictDurableObject } from "cloudflare:test";
import { env, exports } from "cloudflare:workers";
import { describe, expect, it, vi } from "vite-plus/test";
import { SYNC_METHOD } from "../src/index.ts";
import { createServiceSessions } from "../src/service-session.ts";

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

async function rpcRequest(
  sessions: { handle(connection: object, message: string): Promise<void> },
  connection: object,
  sent: unknown[],
  request: { readonly id: string; readonly [key: string]: unknown },
) {
  await sessions.handle(connection, JSON.stringify(request));
  return sent.find(
    (value): value is { readonly id: string } =>
      typeof value === "object" && value !== null && "id" in value && value.id === request.id,
  );
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
      expect(
        (await first.waitForSync((value) => value.params.length === 2 && hasCounterValue(value, 2)))
          .params,
      ).toEqual([
        { topic: alpha, value: { key: "batch-alpha", value: 2 } },
        { topic: beta, value: { key: "batch-beta", value: 0 } },
      ]);
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
      expect(first.syncCount()).toBe(4);
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

  it("rolls back failed subscription persistence and preserves failed unsubscriptions", async () => {
    let value = 0;
    const queries = {
      counter: {
        tables: toTables(["counters"]),
        run: () => value,
      },
    } satisfies { counter: Query<[], number> };
    const mutations = {
      increment: {
        tables: toTables(["counters"]),
        run: () => {
          value += 1;
        },
      },
    } satisfies { increment: Mutation<[], void> };
    const engine = new SyncEngine({ queries, mutations });
    let persistenceError: Error | null = null;
    const notifications: unknown[] = [];
    const sessions = createServiceSessions(engine);
    const connection = {};
    const connected = sessions.connect(connection, {
      topics: [],
      persist: () => persistenceError ?? undefined,
      send: (request) => {
        notifications.push(JSON.parse(request));
      },
      fail: (error) => {
        throw error;
      },
    });
    if (connected instanceof Error) throw connected;

    const topic = { name: "counter", params: [] };
    persistenceError = new Error("persistence failed");
    expect(
      await rpcRequest(sessions, connection, notifications, {
        jsonrpc: "2.0",
        id: "failed-subscribe",
        method: "subscribe",
        params: [topic],
      }),
    ).toMatchObject({ error: { message: "persistence failed" } });
    notifications.length = 0;

    persistenceError = null;
    expect(
      await rpcRequest(sessions, connection, notifications, {
        jsonrpc: "2.0",
        id: "retried-subscribe",
        method: "subscribe",
        params: [topic],
      }),
    ).toMatchObject({ result: null });
    expect(notifications.filter(isSyncNotification)).toEqual([
      { jsonrpc: "2.0", method: SYNC_METHOD, params: [{ topic, value: 0 }] },
    ]);

    notifications.length = 0;
    persistenceError = new Error("persistence failed");
    expect(
      await rpcRequest(sessions, connection, notifications, {
        jsonrpc: "2.0",
        id: "failed-unsubscribe",
        method: "unsubscribe",
        params: [topic],
      }),
    ).toMatchObject({ error: { message: "persistence failed" } });
    expect(
      await rpcRequest(sessions, connection, notifications, {
        jsonrpc: "2.0",
        id: "sync-after-failed-unsubscribe",
        method: "sync",
        params: ["increment", []],
      }),
    ).toMatchObject({ result: null });
    expect(notifications.filter(isSyncNotification)).toEqual([
      { jsonrpc: "2.0", method: SYNC_METHOD, params: [{ topic, value: 1 }] },
    ]);
    sessions.close(connection);
  });

  it("removes partially restored listeners when normalization fails", () => {
    const queries = {
      counter: {
        tables: toTables(["counters"]),
        run: () => 0,
      },
    } satisfies { counter: Query<[], number> };
    const mutations = {
      increment: {
        tables: toTables(["counters"]),
        run: () => {},
      },
    } satisfies { increment: Mutation<[], void> };
    const engine = new SyncEngine({ queries, mutations });
    const unsubscribe = vi.spyOn(engine, "unsubscribe");
    const sessions = createServiceSessions(engine);
    const restored = sessions.connect(
      {},
      {
        topics: [{ name: "counter", params: [] }],
        persist: () => new Error("persistence failed"),
        send: () => {},
        fail: (error) => {
          throw error;
        },
      },
    );
    expect(restored).toBeInstanceOf(Error);
    expect(unsubscribe).toHaveBeenCalledTimes(1);
  });

  it("flushes earlier events before reporting a later query error", async () => {
    let broken = false;
    const queries = {
      first: {
        tables: toTables(["items"]),
        run: () => "first",
      },
      second: {
        tables: toTables(["items"]),
        run: () => {
          if (broken) throw new Error("broken");
          return "second";
        },
      },
    } satisfies { first: Query<[], string>; second: Query<[], string> };
    const mutations = {
      touch: {
        tables: toTables(["items"]),
        run: () => {
          broken = true;
        },
      },
    } satisfies { touch: Mutation<[], void> };
    const notifications: unknown[] = [];
    const sessions = createServiceSessions(new SyncEngine({ queries, mutations }));
    const connection = {};
    const connected = sessions.connect(connection, {
      topics: [],
      persist: () => {},
      send: (request) => {
        notifications.push(JSON.parse(request));
      },
      fail: (error) => {
        throw error;
      },
    });
    if (connected instanceof Error) throw connected;

    await rpcRequest(sessions, connection, notifications, {
      jsonrpc: "2.0",
      id: "subscribe-first",
      method: "subscribe",
      params: [{ name: "first", params: [] }],
    });
    await rpcRequest(sessions, connection, notifications, {
      jsonrpc: "2.0",
      id: "subscribe-second",
      method: "subscribe",
      params: [{ name: "second", params: [] }],
    });
    notifications.length = 0;

    expect(
      await rpcRequest(sessions, connection, notifications, {
        jsonrpc: "2.0",
        id: "failed-sync",
        method: "sync",
        params: ["touch", []],
      }),
    ).toMatchObject({ error: { message: "Query execution failed" } });
    expect(notifications.filter(isSyncNotification)).toEqual([
      {
        jsonrpc: "2.0",
        method: SYNC_METHOD,
        params: [{ topic: { name: "first", params: [] }, value: "first" }],
      },
    ]);
    sessions.close(connection);
  });
  it("rejects duplicate session connections", () => {
    const engine = new SyncEngine({
      queries: {
        counter: {
          tables: toTables(["counters"]),
          run: () => 0,
        },
      } satisfies { counter: Query<[], number> },
      mutations: {
        increment: {
          tables: toTables(["counters"]),
          run: () => {},
        },
      } satisfies { increment: Mutation<[], void> },
    });
    const sessions = createServiceSessions(engine);
    const connection = {};
    const adapter = {
      topics: [],
      persist: () => {},
      send: () => {},
      fail: () => {},
    };

    expect(sessions.connect(connection, adapter)).toBeUndefined();
    expect(sessions.connect(connection, adapter)).toMatchObject({
      message: "Service session already connected",
    });
    expect(sessions.has(connection)).toBe(true);
    sessions.close(connection);
  });
  it("removes a session before reporting notification failure", async () => {
    const engine = new SyncEngine({
      queries: {
        counter: {
          tables: toTables(["counters"]),
          run: () => 0,
        },
      } satisfies { counter: Query<[], number> },
      mutations: {
        increment: {
          tables: toTables(["counters"]),
          run: () => {},
        },
      } satisfies { increment: Mutation<[], void> },
    });
    const sessions = createServiceSessions(engine);
    const connection = {};
    const unsubscribe = vi.spyOn(engine, "unsubscribe");
    const failed = vi.fn();
    const connected = sessions.connect(connection, {
      topics: [],
      persist: () => {},
      send: () => new Error("notification failed"),
      fail: (error) => {
        expect(sessions.has(connection)).toBe(false);
        failed(error);
      },
    });
    if (connected instanceof Error) throw connected;

    await sessions.handle(
      connection,
      JSON.stringify({
        jsonrpc: "2.0",
        id: "subscribe",
        method: "subscribe",
        params: [{ name: "counter", params: [] }],
      }),
    );
    expect(failed).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ message: "notification failed" }),
    );
    expect(unsubscribe).toHaveBeenCalledTimes(1);
    expect(sessions.has(connection)).toBe(false);
  });
});
