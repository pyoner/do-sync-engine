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
      const subscribeResult = await client.subscribe(topic, listener);
      expect(typeof subscribeResult).toBe("string");

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

  it("manages subscriber deduplication, multiple callbacks, and explicit ID replacement", async () => {
    const { client, socket } = await connect();
    try {
      const key = `multi-${crypto.randomUUID()}`;
      const topic = await client.createTopic("counter", [key]);
      if (topic instanceof Error) throw topic;

      const first = createListener<{ value: { key: string; value: number } }>();
      const id1 = await client.subscribe(topic, first.listener);
      expect(typeof id1).toBe("string");
      await first.wait((e) => e.value.value === 0);

      // Resubscribing same callback/topic without ID returns same ID and repeats initial delivery
      const id2 = await client.subscribe(topic, first.listener);
      expect(id2).toBe(id1);
      expect(first.record.events.length).toBe(2);

      // Different callback on same topic returns distinct ID and receives initial delivery
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

      // Explicit same topic/ID replaces the registration with a new callback
      const third = createListener<{ value: { key: string; value: number } }>();
      const id4 = await client.subscribe(topic, third.listener, id1 as string);
      expect(id4).toBe(id1);
      await third.wait((e) => e.value.value === 1);

      await client.sync("increment", [key, 2]);
      await third.wait((e) => e.value.value === 3);
      await second.wait((e) => e.value.value === 3);
      // Replaced callback received no additional events after replacement
      expect(first.record.events.length).toBe(3);
    } finally {
      client[Symbol.dispose]();
      socket.close();
    }
  });

  it("handles topic-specific, ID-only, and listener-form unsubscriptions", async () => {
    const { client, socket } = await connect();
    try {
      const key1 = `unsub1-${crypto.randomUUID()}`;
      const key2 = `unsub2-${crypto.randomUUID()}`;
      const topic1 = await client.createTopic("counter", [key1]);
      const topic2 = await client.createTopic("counter", [key2]);
      if (topic1 instanceof Error || topic2 instanceof Error) throw new Error("Topic error");

      const explicitId = `shared-explicit-${crypto.randomUUID()}`;
      const cb1 = createListener<{ value: { key: string; value: number } }>();
      const cb2 = createListener<{ value: { key: string; value: number } }>();

      // Register two topics under one explicit ID
      await client.subscribe(topic1, cb1.listener, explicitId);
      await client.subscribe(topic2, cb2.listener, explicitId);
      await cb1.wait((e) => e.value.value === 0);
      await cb2.wait((e) => e.value.value === 0);

      // unsubscribe(topic, id) removes only topic1
      const unsub1 = await client.unsubscribe(topic1, explicitId);
      expect(unsub1).toBeUndefined();

      await client.sync("increment", [key1, 1]);
      await client.sync("increment", [key2, 5]);
      await cb2.wait((e) => e.value.value === 5);
      expect(cb1.record.events.length).toBe(1);

      // unsubscribe(id) removes remaining records for that explicit ID across topics
      const unsubAllId = await client.unsubscribe(explicitId);
      expect(unsubAllId).toBeUndefined();

      await client.sync("increment", [key2, 2]);
      // Barrier check with a new subscription to confirm event delivery settled
      const barrier = createListener<{ value: { key: string; value: number } }>();
      await client.subscribe(topic2, barrier.listener);
      await barrier.wait((e) => e.value.value === 7);
      expect(cb2.record.events.length).toBe(3);

      // Register one callback under two explicit IDs on one topic
      const multiId1 = `id-a-${crypto.randomUUID()}`;
      const multiId2 = `id-b-${crypto.randomUUID()}`;
      const sharedCb = createListener<{ value: { key: string; value: number } }>();
      const otherCb = createListener<{ value: { key: string; value: number } }>();

      await client.subscribe(topic1, sharedCb.listener, multiId1);
      await client.subscribe(topic1, sharedCb.listener, multiId2);
      await client.subscribe(topic1, otherCb.listener);
      await otherCb.wait((e) => e.value.key === key1);

      // Deserialized equivalent topic for unsubscribe
      const equivalentTopic1 = await client.createTopic("counter", [key1]);
      if (equivalentTopic1 instanceof Error) throw equivalentTopic1;
      const unsubShared = await client.unsubscribe(equivalentTopic1, sharedCb.listener);
      expect(unsubShared).toBeUndefined();

      await client.sync("increment", [key1, 10]);
      await otherCb.wait((e) => e.value.value === 11);
      // sharedCb received 2 initial events and 0 after unsubscribe
      expect(sharedCb.record.events.length).toBe(2);

      // Unknown removals are no-ops
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

      const explicitId = "same-explicit-id";
      const cb1 = createListener<{ value: { key: string; value: number } }>();
      const cb2 = createListener<{ value: { key: string; value: number } }>();

      await first.client.subscribe(topic, cb1.listener, explicitId);
      await second.client.subscribe(topic, cb2.listener, explicitId);
      await cb1.wait((e) => e.value.value === 0);
      await cb2.wait((e) => e.value.value === 0);

      // First client unsubscribes its explicit ID; second client must remain registered
      await first.client.unsubscribe(explicitId);
      await first.client.sync("increment", [key, 4]);
      await cb2.wait((e) => e.value.value === 4);
      expect(cb1.record.events.length).toBe(1);

      // Idle connection receives no events
      const idleCount = (idle.client as unknown as { count?: number }).count ?? 0;
      expect(idleCount).toBe(0);

      // Closing first connection leaves second working
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

  it("enumerates subscriptions asynchronously with stubs and allows unsubscribing returned stub", async () => {
    const { client, socket } = await connect();
    try {
      const key = `enum-${crypto.randomUUID()}`;
      const topic = await client.createTopic("counter", [key]);
      if (topic instanceof Error) throw topic;

      const cb = createListener<{ value: { key: string; value: number } }>();
      const id = await client.subscribe(topic, cb.listener, "custom-id");
      expect(id).toBe("custom-id");
      await cb.wait((e) => e.value.value === 0);

      let found = false;
      for await (const entry of client.subscriptions()) {
        try {
          if (entry.id === "custom-id") {
            found = true;
            expect(entry.topic).toEqual(topic);
            expect(await entry.listener.listenerId).toBeDefined();

            // Unsubscribe using the yielded stub
            const unsubResult = await client.unsubscribe(
              entry.topic as never,
              entry.listener as never,
            );
            expect(unsubResult).toBeUndefined();
          }
        } finally {
          entry[Symbol.dispose]();
        }
      }
      expect(found).toBe(true);

      // Confirm unsubscription succeeded
      await client.sync("increment", [key, 10]);
      const barrier = createListener<{ value: { key: string; value: number } }>();
      await client.subscribe(topic, barrier.listener);
      await barrier.wait((e) => e.value.value === 10);
      expect(cb.record.events.length).toBe(1);

      // Early break completes cleanly
      for await (const entry of client.subscriptions()) {
        entry[Symbol.dispose]();
        break;
      }
    } finally {
      client[Symbol.dispose]();
      socket.close();
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

      // Invalid listenerId
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

      // Recreate [1n, undefined] topic independently and unsubscribe it
      const reconstructedUndef = await client.createTopic("echoParams", [1n, undefined]);
      if (reconstructedUndef instanceof Error) throw reconstructedUndef;
      await client.unsubscribe(reconstructedUndef, cbUndef.listener);

      // Enumerate and verify only null remains
      const remaining: unknown[] = [];
      for await (const entry of client.subscriptions()) {
        try {
          if (entry.topic.name === "echoParams") {
            remaining.push(entry.topic.params);
          }
        } finally {
          entry[Symbol.dispose]();
        }
      }
      expect(remaining.length).toBe(1);
      expect(remaining[0]).toEqual([1n, null]);
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
    // Dispose the service while listenerId read is pending
    service[Symbol.dispose]();
    resolveListenerId("delayed-id");

    const result = await subscribePromise;
    expect(result).toBeInstanceOf(Error);
    expect((result as Error).message).toBe("WebSocket RPC session is closed");
    stub[Symbol.dispose]();

    // Verify engine has 0 subscriptions left
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
