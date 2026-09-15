import { exports } from "cloudflare:workers";
import { RpcStub } from "capnweb";
import { describe, expect, it } from "vite-plus/test";
import { newWebSocketRpcSession, type WebSocketRpcClient } from "../src/client.ts";
import { SocketService, type RpcListener } from "../src/service.ts";
import type { FixtureMutations, FixtureQueries } from "./cloudflare-worker.ts";
import { SyncEngine } from "@do-sync-engine/core";

const worker = exports as unknown as {
  default: { fetch(request: Request): Promise<Response> };
};

type Client = WebSocketRpcClient<FixtureQueries, FixtureMutations>;

async function connect(): Promise<{ client: Client; socket: WebSocket }> {
  const response = await worker.default.fetch(
    new Request("https://example.com", { headers: { Upgrade: "websocket" } }),
  );
  const socket = response.webSocket!;
  socket.accept();
  const client = newWebSocketRpcSession<FixtureQueries, FixtureMutations>(socket);
  return { client, socket };
}

function waitForCallback<T>(
  record: { events: T[]; waiters: Set<(event: T) => void> },
  predicate: (event: T) => boolean,
): Promise<T> {
  const existing = record.events.find(predicate);
  if (existing !== undefined) return Promise.resolve(existing);
  return new Promise<T>((resolve) => {
    const waiter = (event: T) => {
      if (!predicate(event)) return;
      record.waiters.delete(waiter);
      resolve(event);
    };
    record.waiters.add(waiter);
  });
}

function createListener<T>() {
  const record = {
    events: [] as T[],
    waiters: new Set<(event: T) => void>(),
  };
  const listener = (event: T) => {
    record.events.push(event);
    for (const waiter of [...record.waiters]) waiter(event);
  };
  return {
    record,
    listener,
    wait: (predicate: (event: T) => boolean) => waitForCallback(record, predicate),
  };
}

describe("Durable Object Capnweb WebSocket transport", () => {
  it("rejects non-WebSocket requests", async () => {
    const response = await worker.default.fetch(new Request("https://example.com"));
    expect([response.status, await response.text()]).toEqual([
      400,
      "This endpoint only accepts WebSocket requests.",
    ]);
  });

  it("handles subscribe, parameter-stub disposal, and mutation updates", async () => {
    const { client, socket } = await connect();
    try {
      const key = `sub-${crypto.randomUUID()}`;
      const topic = await client.createTopic("counter", [key]);
      if (topic instanceof Error) throw topic;

      const { record, listener, wait } = createListener<{
        value: { key: string; value: number };
      }>();
      const subId = await client.subscribe(topic, listener);
      expect(typeof subId).toBe("string");

      const initial = await wait((e) => e.value.key === key && e.value.value === 0);
      expect(initial.value.value).toBe(0);

      const syncResult = await client.sync("increment", [key, 2]);
      expect(syncResult).toBeUndefined();

      const updated = await wait((e) => e.value.key === key && e.value.value === 2);
      expect(updated.value.value).toBe(2);
      expect(record.events.length).toBe(2);
    } finally {
      client[Symbol.dispose]();
      socket.close();
    }
  });

  it("manages subscriber deduplication and multiple callbacks", async () => {
    const { client, socket } = await connect();
    try {
      const key = `multi-${crypto.randomUUID()}`;
      const topic = await client.createTopic("counter", [key]);
      if (topic instanceof Error) throw topic;

      const first = createListener<{ value: { key: string; value: number } }>();
      const id1 = await client.subscribe(topic, first.listener);
      expect(typeof id1).toBe("string");
      await first.wait((e) => e.value.value === 0);

      // Resubscribing same callback/topic returns same subscription ID and repeats initial delivery
      const id2 = await client.subscribe(topic, first.listener);
      expect(id2).toBe(id1);
      expect(first.record.events.length).toBe(2);

      // Different callback on same topic returns distinct subscription ID and receives initial delivery
      const second = createListener<{ value: { key: string; value: number } }>();
      const id3 = await client.subscribe(topic, second.listener);
      expect(typeof id3).toBe("string");
      expect(id3).not.toBe(id1);
      await second.wait((e) => e.value.value === 0);

      // Mutating triggers both callbacks once
      await client.sync("increment", [key, 1]);
      await first.wait((e) => e.value.value === 1);
      await second.wait((e) => e.value.value === 1);
      expect(first.record.events.length).toBe(3);
      expect(second.record.events.length).toBe(2);
    } finally {
      client[Symbol.dispose]();
      socket.close();
    }
  });

  it("handles unsubscriptions by subscription ID", async () => {
    const { client, socket } = await connect();
    try {
      const key1 = `unsub1-${crypto.randomUUID()}`;
      const key2 = `unsub2-${crypto.randomUUID()}`;
      const topic1 = await client.createTopic("counter", [key1]);
      const topic2 = await client.createTopic("counter", [key2]);
      if (topic1 instanceof Error || topic2 instanceof Error) throw new Error("Topic error");

      const cb1 = createListener<{ value: { key: string; value: number } }>();
      const cb2 = createListener<{ value: { key: string; value: number } }>();

      const id1 = await client.subscribe(topic1, cb1.listener);
      if (id1 instanceof Error) throw id1;
      const id2 = await client.subscribe(topic2, cb2.listener);
      if (id2 instanceof Error) throw id2;
      await cb1.wait((e) => e.value.value === 0);
      await cb2.wait((e) => e.value.value === 0);

      const unsub1 = await client.unsubscribe(id1);
      expect(unsub1).toBeUndefined();

      await client.sync("increment", [key1, 1]);
      await client.sync("increment", [key2, 5]);
      await cb2.wait((e) => e.value.value === 5);
      expect(cb1.record.events.length).toBe(1);

      const unsub2 = await client.unsubscribe(id2);
      expect(unsub2).toBeUndefined();

      // Unknown ID removal is no-op
      const noopUnsub = await client.unsubscribe("non-existent-id");
      expect(noopUnsub).toBeUndefined();
    } finally {
      client[Symbol.dispose]();
      socket.close();
    }
  });

  it("isolates connection namespaces, preserves idle connections, and cleans up on close", async () => {
    const first = await connect();
    const second = await connect();
    const idle = await connect();
    try {
      const key = `iso-${crypto.randomUUID()}`;
      const topic = await first.client.createTopic("counter", [key]);
      if (topic instanceof Error) throw topic;

      const cb1 = createListener<{ value: { key: string; value: number } }>();
      const cb2 = createListener<{ value: { key: string; value: number } }>();

      const id1 = await first.client.subscribe(topic, cb1.listener);
      if (id1 instanceof Error) throw id1;
      const id2 = await second.client.subscribe(topic, cb2.listener);
      if (id2 instanceof Error) throw id2;
      await cb1.wait((e) => e.value.value === 0);
      await cb2.wait((e) => e.value.value === 0);

      await first.client.unsubscribe(id1);
      await first.client.sync("increment", [key, 4]);
      await cb2.wait((e) => e.value.value === 4);
      expect(cb1.record.events.length).toBe(1);

      first.client[Symbol.dispose]();
      first.socket.close();

      await second.client.sync("increment", [key, 3]);
      await cb2.wait((e) => e.value.value === 7);
      expect(cb2.record.events.length).toBe(3);
    } finally {
      second.client[Symbol.dispose]();
      second.socket.close();
      idle.client[Symbol.dispose]();
      idle.socket.close();
    }
  });

  it("returns errors as values for unknown query or mutation and invalid listenerId", async () => {
    const { client, socket } = await connect();
    try {
      const badTopic = { name: "unknownQuery", params: [] } as never;
      const cb = createListener<unknown>();
      const subscribeError = await client.subscribe(badTopic, cb.listener as never);
      expect(subscribeError).toBeInstanceOf(Error);

      const syncError = await client.sync("unknownMutation" as never, [] as never);
      expect(syncError).toBeInstanceOf(Error);

      const malformedStub = new RpcStub(
        Object.assign(() => {}, { listenerId: "" }),
      ) as unknown as RpcListener;
      const goodTopic = await client.createTopic("counter", ["valid"]);
      if (goodTopic instanceof Error) throw goodTopic;
      const idError = await client.subscribe(goodTopic, malformedStub);
      expect(idError).toBeInstanceOf(Error);
      expect((idError as Error).message).toBe("RPC listenerId must be a non-empty string");
    } finally {
      client[Symbol.dispose]();
      socket.close();
    }
  });

  it("supports bigint and preserves null vs undefined topic distinction", async () => {
    const { client, socket } = await connect();
    try {
      const topicUndef = await client.createTopic("echoParams", [1n, undefined]);
      const topicNull = await client.createTopic("echoParams", [1n, null]);
      if (topicUndef instanceof Error || topicNull instanceof Error) throw new Error("Topic error");

      const cbUndef = createListener<unknown>();
      const cbNull = createListener<unknown>();

      await client.subscribe(topicUndef, cbUndef.listener);
      await client.subscribe(topicNull, cbNull.listener);
      await cbUndef.wait((e) => e !== undefined);
      await cbNull.wait((e) => e !== undefined);
    } finally {
      client[Symbol.dispose]();
      socket.close();
    }
  });

  it("guards against deferred listenerId registration when session is closed concurrently", async () => {
    const engine = new SyncEngine<string, FixtureQueries, FixtureMutations>({
      queries: {
        counter: {
          tables: new Set(),
          run: (key) => ({ key, value: 0 }),
        },
        echoParams: {
          tables: new Set(),
          run: (count, optional) => ({ count, optional }),
        },
      },
      mutations: {
        increment: {
          tables: new Set(),
          run: () => {},
        },
      },
    });
    const service = new SocketService<FixtureQueries, FixtureMutations>(engine);
    let resolveListenerId!: (id: string) => void;
    const deferredPromise = new Promise<string>((resolve) => {
      resolveListenerId = resolve;
    });
    const deferredListener = Object.assign(() => {}, {
      get listenerId() {
        return deferredPromise;
      },
    });
    const stub = new RpcStub(deferredListener) as unknown as RpcListener;
    const topic = engine.createTopic("counter", ["deferred"]);
    if (topic instanceof Error) throw topic;

    const subscribePromise = service.subscribe(topic, stub);
    service[Symbol.dispose]();
    resolveListenerId("delayed-id");

    const result = await subscribePromise;
    expect(result).toBeInstanceOf(Error);
    expect((result as Error).message).toBe("WebSocket RPC session is closed");
    stub[Symbol.dispose]();

    let subscriptionCount = 0;
    for (const _sub of engine.subscriptions()) subscriptionCount++;
    expect(subscriptionCount).toBe(0);
  });

  it("enforces static compile-time type negative constraints", () => {
    if (false as boolean) {
      const dummyClient = null as unknown as WebSocketRpcClient<FixtureQueries, FixtureMutations>;
      const dummyTopic = null as unknown as { readonly name: "counter"; readonly params: [string] };

      // @ts-expect-error - Unknown query name
      void dummyClient.createTopic("unknownQuery", ["val"]);

      // @ts-expect-error - Wrong parameter type
      void dummyClient.createTopic("counter", [123]);

      // @ts-expect-error - Event object passed instead of callable listener
      void dummyClient.subscribe(dummyTopic, { value: 123 });

      // @ts-expect-error - Callback receiving mismatched result type
      void dummyClient.subscribe(dummyTopic, (_event: { value: { wrongProperty: boolean } }) => {});

      // @ts-expect-error - Unknown mutation name
      void dummyClient.sync("unknownMutation", []);

      // @ts-expect-error - Wrong mutation params
      void dummyClient.sync("increment", ["alpha", "not-a-number"]);
    }
  });
});
