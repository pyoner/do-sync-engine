import { SyncEngine, toTables } from "@do-sync-engine/core";
import type { Mutation, Query } from "@do-sync-engine/core";
import { evictDurableObject } from "cloudflare:test";
import { env, exports } from "cloudflare:workers";
import { describe, expect, it, vi } from "vite-plus/test";
import { SYNC_METHOD } from "../src/index.ts";
import { createService } from "../src/service.ts";

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

type MessageQueue = {
  readonly messages: unknown[];
  push(value: unknown): void;
  waitFor<T>(predicate: (value: unknown) => value is T): Promise<T>;
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

function createMessageQueue(): MessageQueue {
  const messages: unknown[] = [];
  const waiters = new Set<(value: unknown) => void>();
  const push = (value: unknown): void => {
    messages.push(value);
    for (const waiter of waiters) waiter(value);
  };
  const waitFor = <T>(predicate: (value: unknown) => value is T): Promise<T> => {
    const existing = messages.find(predicate);
    if (existing !== undefined) return Promise.resolve(existing);

    const { promise, resolve } = Promise.withResolvers<T>();
    const waiter = (value: unknown): void => {
      if (!predicate(value)) return;
      waiters.delete(waiter);
      resolve(value);
    };
    waiters.add(waiter);
    return promise;
  };
  return { messages, push, waitFor };
}

async function connect() {
  const response = await worker.default.fetch(
    new Request("https://example.com", { headers: { Upgrade: "websocket" } }),
  );
  const socket = response.webSocket!;
  socket.accept();
  const queue = createMessageQueue();
  socket.addEventListener("message", (event: MessageEvent) =>
    queue.push(JSON.parse(String(event.data))),
  );
  let requestId = 0;
  const call = (method: string, ...params: unknown[]) => {
    const id = String(++requestId);
    socket.send(JSON.stringify({ jsonrpc: "2.0", id, method, params }));
    return id;
  };
  return { socket, queue, call };
}

function responseFor(queue: MessageQueue, id: string): Promise<ResponseMessage> {
  return queue.waitFor(
    (value): value is ResponseMessage => isResponseMessage(value) && value.id === id,
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

      const alphaSubscription = first.call("subscribe", alpha);
      expect((await responseFor(first.queue, alphaSubscription)).result).toBeNull();
      expect((await first.queue.waitFor(isSyncNotification)).params).toEqual([
        { topic: alpha, value: { key: "batch-alpha", value: 0 } },
      ]);

      const betaSubscription = first.call("subscribe", beta);
      expect((await responseFor(first.queue, betaSubscription)).result).toBeNull();
      const betaInitial = await first.queue.waitFor(
        (value): value is SyncNotification =>
          isSyncNotification(value) && value.params[0]?.topic.params[0] === "batch-beta",
      );
      expect(betaInitial.params).toEqual([{ topic: beta, value: { key: "batch-beta", value: 0 } }]);

      const duplicateSubscription = first.call("subscribe", alpha);
      expect((await responseFor(first.queue, duplicateSubscription)).result).toBeNull();

      const secondSubscription = second.call("subscribe", alpha);
      expect((await responseFor(second.queue, secondSubscription)).result).toBeNull();
      expect((await second.queue.waitFor(isSyncNotification)).params).toEqual([
        { topic: alpha, value: { key: "batch-alpha", value: 0 } },
      ]);

      const firstSync = first.call("sync", "increment", ["batch-alpha", 2]);
      expect((await responseFor(first.queue, firstSync)).result).toBeNull();
      const firstBatch = await first.queue.waitFor(
        (value): value is SyncNotification =>
          isSyncNotification(value) && value.params.length === 2 && hasCounterValue(value, 2),
      );
      expect(firstBatch.params).toEqual([
        { topic: alpha, value: { key: "batch-alpha", value: 2 } },
        { topic: beta, value: { key: "batch-beta", value: 0 } },
      ]);

      const secondBatch = await second.queue.waitFor(
        (value): value is SyncNotification =>
          isSyncNotification(value) && value.params.length === 1 && hasCounterValue(value, 2),
      );
      expect(secondBatch.params).toEqual([
        { topic: alpha, value: { key: "batch-alpha", value: 2 } },
      ]);

      const unsubscribe = first.call("unsubscribe", beta);
      expect((await responseFor(first.queue, unsubscribe)).result).toBeNull();
      const reducedSync = first.call("sync", "increment", ["batch-alpha", 1]);
      expect((await responseFor(first.queue, reducedSync)).result).toBeNull();
      const reducedBatch = await first.queue.waitFor(
        (value): value is SyncNotification =>
          isSyncNotification(value) && value.params.length === 1 && hasCounterValue(value, 3),
      );
      expect(reducedBatch.params).toEqual([
        { topic: alpha, value: { key: "batch-alpha", value: 3 } },
      ]);
      const secondReducedBatch = await second.queue.waitFor(
        (value): value is SyncNotification =>
          isSyncNotification(value) && value.params.length === 1 && hasCounterValue(value, 3),
      );
      expect(secondReducedBatch.params).toEqual([
        { topic: alpha, value: { key: "batch-alpha", value: 3 } },
      ]);
      expect(first.queue.messages.filter(isSyncNotification)).toHaveLength(4);
      expect(second.queue.messages.filter(isSyncNotification)).toHaveLength(3);
    } finally {
      first.socket.close();
      second.socket.close();
    }
  });

  it("returns unknown queries as RPC errors", async () => {
    const connection = await connect();
    try {
      const id = connection.call("subscribe", { name: "unknown", params: [] });
      expect((await responseFor(connection.queue, id)).error).toBeDefined();
    } finally {
      connection.socket.close();
    }
  });

  it("returns parse errors for malformed JSON", async () => {
    const connection = await connect();
    try {
      connection.socket.send("{");
      const response = await connection.queue.waitFor(
        (value): value is ResponseMessage =>
          isResponseMessage(value) && value.id === null && value.error?.code === -32700,
      );
      expect(response.error?.code).toBe(-32700);
    } finally {
      connection.socket.close();
    }
  });

  it("restores subscriptions after Durable Object eviction without another initial event", async () => {
    const connection = await connect();
    try {
      const topic = { name: "counter", params: ["restore"] };
      const subscription = connection.call("subscribe", topic);
      await responseFor(connection.queue, subscription);
      expect((await connection.queue.waitFor(isSyncNotification)).params).toEqual([
        { topic, value: { key: "restore", value: 0 } },
      ]);
      const beforeEviction = connection.queue.messages.filter(isSyncNotification).length;

      await evictDurableObject(env.FIXTURE_SYNC_OBJECT.getByName("default"));
      const sync = connection.call("sync", "increment", ["restore", 3]);
      expect((await responseFor(connection.queue, sync)).result).toBeNull();
      const event = await connection.queue.waitFor(
        (value): value is SyncNotification =>
          isSyncNotification(value) && hasCounterValue(value, 3),
      );
      expect(event.params).toEqual([{ topic, value: { key: "restore", value: 3 } }]);
      expect(connection.queue.messages.filter(isSyncNotification)).toHaveLength(beforeEviction + 1);
    } finally {
      connection.socket.close();
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
    const session = createService(engine)({
      topics: [],
      persist: () => persistenceError ?? undefined,
      notify: (request) => {
        notifications.push(request);
      },
      fail: (error) => {
        throw error;
      },
    });
    if (session instanceof Error) throw session;

    const topic = { name: "counter", params: [] };
    persistenceError = new Error("persistence failed");
    expect(
      await session.handle({
        jsonrpc: "2.0",
        id: "failed-subscribe",
        method: "subscribe",
        params: [topic],
      }),
    ).toMatchObject({ error: { message: "persistence failed" } });
    expect(notifications).toHaveLength(0);

    persistenceError = null;
    expect(
      await session.handle({
        jsonrpc: "2.0",
        id: "retried-subscribe",
        method: "subscribe",
        params: [topic],
      }),
    ).toMatchObject({ result: null });
    expect(notifications).toEqual([
      { jsonrpc: "2.0", method: SYNC_METHOD, params: [{ topic, value: 0 }] },
    ]);

    notifications.length = 0;
    persistenceError = new Error("persistence failed");
    expect(
      await session.handle({
        jsonrpc: "2.0",
        id: "failed-unsubscribe",
        method: "unsubscribe",
        params: [topic],
      }),
    ).toMatchObject({ error: { message: "persistence failed" } });
    expect(
      await session.handle({
        jsonrpc: "2.0",
        id: "sync-after-failed-unsubscribe",
        method: "sync",
        params: ["increment", []],
      }),
    ).toMatchObject({ result: null });
    expect(notifications).toEqual([
      { jsonrpc: "2.0", method: SYNC_METHOD, params: [{ topic, value: 1 }] },
    ]);
    session.close();
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
    const restored = createService(engine)({
      topics: [{ name: "counter", params: [] }],
      persist: () => new Error("persistence failed"),
      notify: () => {},
      fail: (error) => {
        throw error;
      },
    });
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
    const session = createService(new SyncEngine({ queries, mutations }))({
      topics: [],
      persist: () => {},
      notify: (request) => {
        notifications.push(request);
      },
      fail: (error) => {
        throw error;
      },
    });
    if (session instanceof Error) throw session;

    await session.handle({
      jsonrpc: "2.0",
      id: "subscribe-first",
      method: "subscribe",
      params: [{ name: "first", params: [] }],
    });
    await session.handle({
      jsonrpc: "2.0",
      id: "subscribe-second",
      method: "subscribe",
      params: [{ name: "second", params: [] }],
    });
    notifications.length = 0;

    expect(
      await session.handle({
        jsonrpc: "2.0",
        id: "failed-sync",
        method: "sync",
        params: ["touch", []],
      }),
    ).toMatchObject({ error: { message: "Query execution failed" } });
    expect(notifications).toEqual([
      {
        jsonrpc: "2.0",
        method: SYNC_METHOD,
        params: [{ topic: { name: "first", params: [] }, value: "first" }],
      },
    ]);
    session.close();
  });
});
