import { SyncEngine, toTables } from "@do-sync-engine/core";
import type { ListenerEvent, Mutation, Query } from "@do-sync-engine/core";
import { exports } from "cloudflare:workers";
import { newWebSocketRpcSession, type RpcStub } from "capnweb";
import { describe, expect, it, vi } from "vite-plus/test";
import { ServerAPI } from "../src/index.ts";
import type { FixtureMutations, FixtureQueries } from "./cloudflare-worker.ts";

type CounterEvent = ListenerEvent<"counter", [string], { key: string; value: number }>;
const worker = exports as unknown as { default: { fetch(request: Request): Promise<Response> } };

async function connect(): Promise<RpcStub<ServerAPI<FixtureQueries, FixtureMutations>>> {
  const response = await worker.default.fetch(
    new Request("https://example.com", { headers: { Upgrade: "websocket" } }),
  );
  const socket = response.webSocket!;
  socket.accept();
  return newWebSocketRpcSession<ServerAPI<FixtureQueries, FixtureMutations>>(socket);
}

describe("Durable Object Cap’n Web transport", () => {
  it("rejects non-WebSocket requests", async () => {
    const response = await worker.default.fetch(new Request("https://example.com"));
    expect([response.status, await response.text()]).toEqual([
      400,
      "This endpoint only accepts WebSocket requests.",
    ]);
  });

  it("retains callbacks after subscribe returns across shared-engine connections", async () => {
    const apiA = await connect();
    const apiB = await connect();

    try {
      const topicA = await apiA.createTopic("counter", ["alpha"]);
      if (topicA instanceof Error) throw topicA;
      const topicB = await apiB.createTopic("counter", ["alpha"]);
      if (topicB instanceof Error) throw topicB;

      const eventsA: CounterEvent[] = [];
      const eventsB: CounterEvent[] = [];
      const subscribedA = await apiA.subscribe(topicA, (event) => {
        eventsA.push(event);
      });
      if (subscribedA instanceof Error) throw subscribedA;
      const subscribedB = await apiB.subscribe(topicB, (event) => {
        eventsB.push(event);
      });
      if (subscribedB instanceof Error) throw subscribedB;

      await expect.poll(() => eventsA.at(-1)?.value).toEqual({ key: "alpha", value: 0 });
      await expect.poll(() => eventsB.at(-1)?.value).toEqual({ key: "alpha", value: 0 });
      expect(eventsA).toHaveLength(1);
      expect(eventsB).toHaveLength(1);

      const synced = await apiA.sync("increment", ["alpha", 2]);
      if (synced instanceof Error) throw synced;

      await expect.poll(() => eventsA.at(-1)?.value).toEqual({ key: "alpha", value: 2 });
      await expect.poll(() => eventsB.at(-1)?.value).toEqual({ key: "alpha", value: 2 });
      expect(eventsA).toHaveLength(2);
      expect(eventsB).toHaveLength(2);
    } finally {
      apiA[Symbol.dispose]();
      apiB[Symbol.dispose]();
    }
  });

  it("replaces and tears down one listener per connection topic", async () => {
    const apiA = await connect();
    const apiB = await connect();
    let apiADisposed = false;

    try {
      const topicA = await apiA.createTopic("counter", ["beta"]);
      if (topicA instanceof Error) throw topicA;
      const topicB = await apiB.createTopic("counter", ["beta"]);
      if (topicB instanceof Error) throw topicB;

      const firstAEvents: CounterEvent[] = [];
      const secondAEvents: CounterEvent[] = [];
      const bEvents: CounterEvent[] = [];
      const firstListener = (event: CounterEvent) => {
        firstAEvents.push(event);
      };
      const secondListener = (event: CounterEvent) => {
        secondAEvents.push(event);
      };
      const subscribedA = await apiA.subscribe(topicA, firstListener);
      if (subscribedA instanceof Error) throw subscribedA;
      const subscribedB = await apiB.subscribe(topicB, (event) => {
        bEvents.push(event);
      });
      if (subscribedB instanceof Error) throw subscribedB;

      await expect.poll(() => firstAEvents.at(-1)?.value).toEqual({ key: "beta", value: 0 });
      await expect.poll(() => bEvents.at(-1)?.value).toEqual({ key: "beta", value: 0 });

      const replaced = await apiA.subscribe(topicA, secondListener);
      if (replaced instanceof Error) throw replaced;
      await expect.poll(() => secondAEvents.at(-1)?.value).toEqual({ key: "beta", value: 0 });

      const firstSync = await apiB.sync("increment", ["beta", 1]);
      if (firstSync instanceof Error) throw firstSync;
      await expect.poll(() => secondAEvents.at(-1)?.value).toEqual({ key: "beta", value: 1 });
      await expect.poll(() => bEvents.at(-1)?.value).toEqual({ key: "beta", value: 1 });
      expect(firstAEvents).toHaveLength(1);

      await apiA.unsubscribe(topicA, firstListener);
      const secondSync = await apiB.sync("increment", ["beta", 1]);
      if (secondSync instanceof Error) throw secondSync;
      await expect.poll(() => bEvents.at(-1)?.value).toEqual({ key: "beta", value: 2 });
      expect(firstAEvents).toHaveLength(1);
      expect(secondAEvents).toHaveLength(2);

      apiA[Symbol.dispose]();
      apiADisposed = true;
      const thirdSync = await apiB.sync("increment", ["beta", 1]);
      if (thirdSync instanceof Error) throw thirdSync;
      await expect.poll(() => bEvents.at(-1)?.value).toEqual({ key: "beta", value: 3 });
    } finally {
      if (!apiADisposed) apiA[Symbol.dispose]();
      apiB[Symbol.dispose]();
    }
  });

  it("cleans a rejected callback without breaking its WebSocket", async () => {
    const api = await connect();
    const warning = vi.spyOn(console, "warn").mockImplementation(() => {});

    try {
      const topic = await api.createTopic("counter", ["alpha"]);
      if (topic instanceof Error) throw topic;
      const events: CounterEvent[] = [];
      const subscribed = await api.subscribe(topic, (event) => {
        events.push(event);
        throw new Error("listener failed");
      });
      if (subscribed instanceof Error) throw subscribed;

      await expect
        .poll(() =>
          warning.mock.calls.some(
            ([message]) => message === "Failed to deliver subscription update",
          ),
        )
        .toBe(true);
      expect(events).toHaveLength(1);

      const synced = await api.sync("increment", ["alpha", 1]);
      if (synced instanceof Error) throw synced;
      expect(events).toHaveLength(1);
      expect(warning).toHaveBeenCalledTimes(1);

      const stillUsable = await api.createTopic("counter", ["beta"]);
      if (stillUsable instanceof Error) throw stillUsable;
    } finally {
      warning.mockRestore();
      api[Symbol.dispose]();
    }
  });

  it("returns unknown-query failures as values", async () => {
    const api = await connect();

    try {
      const topic = await api.createTopic("unknown" as "counter", ["alpha"]);
      expect(topic).toBeInstanceOf(Error);
    } finally {
      api[Symbol.dispose]();
    }
  });

  it("keeps a prior local listener after a failed replacement and disposes it", async () => {
    type MemoryQueries = { counter: Query<[], number> };
    type MemoryMutations = { increment: Mutation<[], void> };

    let counter = 0;
    let shouldFail = false;
    const counterTables = toTables(["counter"]);
    const engine = new SyncEngine<MemoryQueries, MemoryMutations>({
      queries: {
        counter: {
          tables: counterTables,
          run: () => {
            if (shouldFail) throw new Error("query failed");
            return counter;
          },
        },
      },
      mutations: {
        increment: {
          tables: counterTables,
          run: () => {
            counter += 1;
          },
        },
      },
    });
    const api = new ServerAPI(engine);
    const topic = api.createTopic("counter", []);
    if (topic instanceof Error) throw topic;

    const firstEvents: number[] = [];
    const secondEvents: number[] = [];
    const firstListener = (event: ListenerEvent<"counter", [], number>) => {
      firstEvents.push(event.value);
    };
    const secondListener = (event: ListenerEvent<"counter", [], number>) => {
      secondEvents.push(event.value);
    };
    const subscribed = api.subscribe(topic, firstListener);
    if (subscribed instanceof Error) throw subscribed;
    await expect.poll(() => firstEvents).toEqual([0]);

    shouldFail = true;
    const replacement = api.subscribe(topic, secondListener);
    expect(replacement).toBeInstanceOf(Error);

    shouldFail = false;
    const synced = engine.sync("increment", []);
    if (synced instanceof Error) throw synced;
    await expect.poll(() => firstEvents).toEqual([0, 1]);
    expect(secondEvents).toEqual([]);

    api[Symbol.dispose]();
    const syncedAfterDispose = engine.sync("increment", []);
    if (syncedAfterDispose instanceof Error) throw syncedAfterDispose;
    expect(firstEvents).toEqual([0, 1]);
    expect(secondEvents).toEqual([]);
  });
});
