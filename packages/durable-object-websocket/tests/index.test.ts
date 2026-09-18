import { exports } from "cloudflare:workers";
import { newWebSocketRpcSession, RpcStub } from "capnweb";
import { describe, expect, it } from "vite-plus/test";
import { SocketService, type RpcClient, type RpcListener, type Service } from "../src/service.ts";
import type { FixtureMutations, FixtureQueries } from "./cloudflare-worker.ts";
import { SyncEngine, type Query } from "@do-sync-engine/core";

const worker = exports as unknown as {
  default: { fetch(request: Request): Promise<Response> };
};

type Client = RpcClient<FixtureQueries, FixtureMutations>;

async function connect(): Promise<{ client: Client; socket: WebSocket }> {
  const response = await worker.default.fetch(
    new Request("https://example.com", { headers: { Upgrade: "websocket" } }),
  );
  const socket = response.webSocket!;
  socket.accept();
  const client = newWebSocketRpcSession<Service<FixtureQueries, FixtureMutations>>(socket);
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

function createDisposableRpcListener(onDispose: () => void): RpcListener {
  const owned = Object.assign((_event: unknown) => {}, {
    [Symbol.dispose]: onDispose,
  });
  return Object.assign((_event: unknown) => {}, {
    dup: () => owned,
    [Symbol.dispose]: () => {},
  }) as unknown as RpcListener;
}

async function subscribeWithStub<T>(
  client: Client,
  topic: unknown,
  listener: (event: T) => void,
): Promise<void | Error> {
  const stub = new RpcStub(listener);
  try {
    return await client.subscribe(topic as never, stub as never);
  } finally {
    stub[Symbol.dispose]();
  }
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
      const result = await subscribeWithStub(client, topic, listener);
      expect(result).toBeUndefined();

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

  it("keeps separately serialized topics isolated by identity", async () => {
    const first = await connect();
    const second = await connect();
    try {
      const key = `multi-${crypto.randomUUID()}`;
      const topic = await first.client.createTopic("counter", [key]);
      if (topic instanceof Error) throw topic;

      const firstListener = createListener<{ value: { key: string; value: number } }>();
      const secondListener = createListener<{ value: { key: string; value: number } }>();
      const otherSocketListener = createListener<{ value: { key: string; value: number } }>();

      expect(await subscribeWithStub(first.client, topic, firstListener.listener)).toBeUndefined();
      await firstListener.wait((e) => e.value.value === 0);
      expect(await subscribeWithStub(first.client, topic, secondListener.listener)).toBeUndefined();
      await secondListener.wait((e) => e.value.value === 0);
      expect(
        await subscribeWithStub(second.client, topic, otherSocketListener.listener),
      ).toBeUndefined();
      await otherSocketListener.wait((e) => e.value.value === 0);

      await first.client.sync("increment", [key, 1]);
      await secondListener.wait((e) => e.value.value === 1);
      await otherSocketListener.wait((e) => e.value.value === 1);
      expect(firstListener.record.events.length).toBe(2);
      expect(secondListener.record.events.length).toBe(2);
      expect(otherSocketListener.record.events.length).toBe(2);
    } finally {
      first.client[Symbol.dispose]();
      first.socket.close();
      second.client[Symbol.dispose]();
      second.socket.close();
    }
  });

  it("treats serialized topic unsubscription as identity-specific", async () => {
    const { client, socket } = await connect();
    try {
      const key1 = `unsub1-${crypto.randomUUID()}`;
      const key2 = `unsub2-${crypto.randomUUID()}`;
      const topic1 = await client.createTopic("counter", [key1]);
      const topic2 = await client.createTopic("counter", [key2]);
      const unknownTopic = await client.createTopic("counter", ["unknown"]);
      if (topic1 instanceof Error || topic2 instanceof Error || unknownTopic instanceof Error) {
        throw new Error("Topic error");
      }

      const cb1 = createListener<{ value: { key: string; value: number } }>();
      const cb2 = createListener<{ value: { key: string; value: number } }>();
      expect(await subscribeWithStub(client, topic1, cb1.listener)).toBeUndefined();
      expect(await subscribeWithStub(client, topic2, cb2.listener)).toBeUndefined();
      await cb1.wait((e) => e.value.value === 0);
      await cb2.wait((e) => e.value.value === 0);

      expect(await client.unsubscribe(topic1)).toBeUndefined();
      await client.sync("increment", [key1, 1]);
      await client.sync("increment", [key2, 5]);
      await cb1.wait((e) => e.value.value === 1);
      await cb2.wait((e) => e.value.value === 5);
      expect(cb1.record.events.length).toBe(3);
      expect(cb2.record.events.length).toBe(3);

      expect(await client.unsubscribe(topic1)).toBeUndefined();
      expect(await client.unsubscribe(topic2)).toBeUndefined();
      expect(await client.unsubscribe(unknownTopic)).toBeUndefined();
    } finally {
      client[Symbol.dispose]();
      socket.close();
    }
  });

  it("isolates connections and preserves active listeners after another closes", async () => {
    const first = await connect();
    const second = await connect();
    const idle = await connect();
    try {
      const key = `iso-${crypto.randomUUID()}`;
      const topic = await first.client.createTopic("counter", [key]);
      if (topic instanceof Error) throw topic;

      const cb1 = createListener<{ value: { key: string; value: number } }>();
      const cb2 = createListener<{ value: { key: string; value: number } }>();
      expect(await subscribeWithStub(first.client, topic, cb1.listener)).toBeUndefined();
      expect(await subscribeWithStub(second.client, topic, cb2.listener)).toBeUndefined();
      await cb1.wait((e) => e.value.value === 0);
      expect(await first.client.unsubscribe(topic)).toBeUndefined();
      await first.client.sync("increment", [key, 4]);
      await cb1.wait((e) => e.value.value === 4);
      await cb2.wait((e) => e.value.value === 4);
      expect(cb1.record.events.length).toBe(2);

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

  it("disposes replaced and failed RPC listeners", async () => {
    type DirectQueries = { value: Query<[], number> };
    const pair = new WebSocketPair();
    const server = pair[1];
    server.accept();
    let shouldFail = false;
    const engine = new SyncEngine<WebSocket, DirectQueries, {}, Disposable>({
      queries: {
        value: {
          tables: new Set(),
          run: () => {
            if (shouldFail) throw new Error("query failed");
            return 0;
          },
        },
      },
      mutations: {},
    });
    const service = new SocketService<DirectQueries, {}>(engine, server);
    const topic = engine.createTopic("value", []);
    if (topic instanceof Error) throw topic;
    const disposed: string[] = [];
    const first = createDisposableRpcListener(() => disposed.push("first"));
    const second = createDisposableRpcListener(() => disposed.push("second"));
    const failing = createDisposableRpcListener(() => disposed.push("failing"));

    try {
      expect(service.subscribe(topic, first)).toBeUndefined();
      expect(disposed).toEqual([]);

      shouldFail = true;
      expect(service.subscribe(topic, failing)).toBeInstanceOf(Error);
      expect(disposed).toEqual(["failing"]);
      expect([...engine.subscriptions(topic)]).toHaveLength(1);
      const [activeSub] = [...engine.subscriptions(topic)];
      expect(activeSub?.listener).toBeDefined();

      shouldFail = false;
      expect(service.subscribe(topic, second)).toBeUndefined();
      expect(disposed).toEqual(["failing", "first"]);
      expect([...engine.subscriptions(topic)]).toHaveLength(1);

      expect(service.unsubscribe(topic)).toBeUndefined();
      expect(disposed).toEqual(["failing", "first", "second"]);
      expect([...engine.subscriptions(topic)]).toHaveLength(0);
    } finally {
      service[Symbol.dispose]();
      first[Symbol.dispose]();
      second[Symbol.dispose]();
      failing[Symbol.dispose]();
      server.close();
    }
  });

  it("disposes socket subscriptions, asserts identity-distinct disposal on close, and rejects subscriptions after close", async () => {
    const pair = new WebSocketPair();
    const server = pair[1];
    server.accept();
    const engine = new SyncEngine<WebSocket, FixtureQueries, FixtureMutations, Disposable>({
      queries: {
        counter: { tables: new Set(), run: (key) => ({ key, value: 0 }) },
        echoParams: { tables: new Set(), run: (count, optional) => ({ count, optional }) },
      },
      mutations: { increment: { tables: new Set(), run: () => undefined } },
    });
    const service = new SocketService<FixtureQueries, FixtureMutations>(engine, server);
    const topic = engine.createTopic("counter", ["cleanup"]);
    const distinctTopic = engine.createTopic("counter", ["distinct"]);
    if (topic instanceof Error || distinctTopic instanceof Error) throw new Error("Topic error");
    const disposed: string[] = [];
    const stub = createDisposableRpcListener(() => disposed.push("stub"));
    const distinctStub = createDisposableRpcListener(() => disposed.push("distinctStub"));

    try {
      expect(service.subscribe(topic, stub)).toBeUndefined();
      expect(service.subscribe(distinctTopic, distinctStub)).toBeUndefined();
      expect([...engine.subscriptions()]).toHaveLength(2);
      service[Symbol.dispose]();
      expect([...engine.subscriptions()]).toHaveLength(0);
      expect(disposed).toEqual(["stub", "distinctStub"]);
      const result = service.subscribe(topic, stub);
      expect(result).toBeInstanceOf(Error);
      expect((result as Error).message).toBe("WebSocket RPC session is closed");
      expect([...engine.subscriptions()]).toHaveLength(0);
    } finally {
      stub[Symbol.dispose]();
      distinctStub[Symbol.dispose]();
      server.close();
    }
  });

  it("returns errors as values for unknown query or mutation", async () => {
    const { client, socket } = await connect();
    try {
      const badTopic = { name: "unknownQuery", params: [] } as never;
      const cb = createListener<unknown>();
      const subscribeError = await subscribeWithStub(client, badTopic, cb.listener);
      expect(subscribeError).toBeInstanceOf(Error);

      const syncError = await client.sync("unknownMutation" as never, [] as never);
      expect(syncError).toBeInstanceOf(Error);
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

      await subscribeWithStub(client, topicUndef, cbUndef.listener);
      await subscribeWithStub(client, topicNull, cbNull.listener);
      await cbUndef.wait((e) => e !== undefined);
      await cbNull.wait((e) => e !== undefined);
    } finally {
      client[Symbol.dispose]();
      socket.close();
    }
  });

  it("enforces static compile-time type negative constraints", () => {
    if (false as boolean) {
      const dummyClient = null as unknown as RpcClient<FixtureQueries, FixtureMutations>;
      const dummyService = null as unknown as Service<FixtureQueries, FixtureMutations>;
      const dummyTopic = null as unknown as { readonly name: "counter"; readonly params: [string] };
      const dummyListener = (() => {}) as never;

      // @ts-expect-error - Unknown query name
      void dummyClient.createTopic("unknownQuery", ["val"]);

      // @ts-expect-error - Wrong parameter type
      void dummyClient.createTopic("counter", [123]);

      // @ts-expect-error - Event object passed instead of callable listener
      void dummyClient.subscribe(dummyTopic, { value: 123 });

      // @ts-expect-error - Callback receiving mismatched result type
      void dummyClient.subscribe(dummyTopic, (_event: { value: { wrongProperty: boolean } }) => {});

      // @ts-expect-error - Removed 3-argument subscribe overload
      void dummyClient.subscribe(dummyTopic, dummyListener, "id");

      // @ts-expect-error - Unsubscribe requires a topic
      void dummyClient.unsubscribe("id");
      void dummyClient.unsubscribe(dummyTopic);

      // @ts-expect-error - Unknown mutation name
      void dummyClient.sync("unknownMutation", []);

      void dummyService.sync("increment", ["alpha", 1]);
      void dummyService.unsubscribe(dummyTopic);

      // @ts-expect-error - Service has no subscriptions method
      void dummyService.subscriptions();

      // @ts-expect-error - Service has no 3-argument subscribe overload
      void dummyService.subscribe(dummyTopic, dummyListener, "id");

      // @ts-expect-error - Service unsubscribe accepts only a topic
      void dummyService.unsubscribe("id");

      // @ts-expect-error - Wrong mutation params
      void dummyClient.sync("increment", ["alpha", "not-a-number"]);
    }
  });
});
