import { expect, test } from "vite-plus/test";
import { SyncEngine } from "../src/index.js";
import type {
  Mutation,
  Publish,
  Query,
  SubscriptionId,
  SyncEngineBase,
  Topic,
} from "../src/index.js";

test("exports canonical topic and listener APIs", async () => {
  const queries = {
    numbers: {
      tables: ["numbers"],
      run: () => 1,
    } satisfies Query<[], number>,
  };
  const mutations = {
    noop: {
      tables: [],
      run: () => ({ ok: true }),
    } satisfies Mutation<[], { ok: boolean }>,
  };
  const engine = new SyncEngine({ queries, mutations });

  if (false as boolean) {
    // @ts-expect-error — snapshot is no longer a constructor option
    new SyncEngine({ queries, mutations, snapshot: { subscriptions: [] } });
    // @ts-expect-error — snapshot is no longer an engine method
    engine.snapshot();
    // @ts-expect-error — Snapshot is no longer exported
    const removedSnapshot = undefined as import("../src/index.js").Snapshot;
    // @ts-expect-error — Subscription is no longer exported
    const removedSubscription = undefined as import("../src/index.js").Subscription;
    void removedSnapshot;
    void removedSubscription;
  }
  const topic = await engine.createTopic("numbers", []);

  expect(topic).toEqual({
    name: "numbers",
    params: [],
    hash: "7847f04c5bf09defec728bc6476dd97e2ff6f42f192ee38632308a5713d2f43f",
  });

  const publish: Publish = () => {};
  const subscriptionId: SubscriptionId = engine.subscribe(topic, publish);
  expect(subscriptionId).toBeTypeOf("number");
  expect(Object.getOwnPropertyNames(SyncEngine.prototype).sort()).toEqual([
    "constructor",
    "createTopic",
    "mutate",
    "publish",
    "subscribe",
    "sync",
    "unsubscribe",
  ]);
  expect(engine.unsubscribe(subscriptionId)).toBe(true);
  expect(engine.unsubscribe(subscriptionId)).toBe(false);
});

test("typed topic params, listener values, mutations, and sync", async () => {
  const queries = {
    numbers: {
      tables: ["numbers"],
      run: () => [1, 2, 3],
    } satisfies Query<[], number[]>,
  };
  const mutations = {
    noop: {
      tables: ["numbers"],
      run: () => ({ ok: true }),
    } satisfies Mutation<[], { ok: boolean }>,
  };
  const engine: SyncEngineBase<typeof queries, typeof mutations> = new SyncEngine({
    queries,
    mutations,
  });
  const topic: Topic<"numbers", []> = await engine.createTopic("numbers", []);
  const events: Array<{ topic: Topic<"numbers", []>; value: number[] }> = [];
  const publish: Publish = (publishedTopic, value) => {
    events.push({
      topic: publishedTopic as Topic<"numbers", []>,
      value: value as number[],
    });
  };

  const subscriptionId: SubscriptionId = engine.subscribe(topic, publish);
  await engine.sync("noop", []);

  expect(subscriptionId).toBeTypeOf("number");
  expect(events).toEqual([{ topic, value: [1, 2, 3] }]);

  if (false as boolean) {
    // @ts-expect-error — unknown topic names are rejected
    void engine.createTopic("missing", []);
    // @ts-expect-error — createTopic params must be an empty tuple
    void engine.createTopic("numbers", [1]);
    // @ts-expect-error — subscribe callback must receive a topic and value
    void engine.subscribe(topic, (value: number) => value.toFixed());
    // @ts-expect-error — sync expects no params
    void engine.sync("noop", [1]);
  }
});

test("typed createTopic params and listener handle", async () => {
  const queries = {
    numbers: {
      tables: ["numbers"],
      run: (value: number) => value,
    } satisfies Query<[number], number>,
  };
  const mutations = {
    noop: {
      tables: [],
      run: () => ({}),
    } satisfies Mutation<[], Record<string, never>>,
  };
  const engine = new SyncEngine({ queries, mutations });
  const topic = await engine.createTopic("numbers", [42]);
  const publish: Publish = () => {};

  if (false as boolean) {
    // @ts-expect-error — unknown query name
    void engine.createTopic("missing", [42]);
    // @ts-expect-error — query param must be a number
    void engine.createTopic("numbers", ["42"]);
    // @ts-expect-error — subscribe callback belongs in the second position
    void engine.subscribe(topic, [42]);
  }

  const firstId = engine.subscribe(topic, publish);
  expect(firstId).toBeTypeOf("number");
  expect(engine.unsubscribe(firstId)).toBe(true);
});
