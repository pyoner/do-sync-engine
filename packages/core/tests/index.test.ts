import { expect, test } from "vite-plus/test";
import { SyncEngine } from "../src/index.js";
import type {
  Branded,
  Mutation,
  Publish,
  ListenerEvent,
  Query,
  ListenerId,
  SyncEngineBase,
  Topic,
  TopicHash,
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
    const brandedString = undefined as unknown as Branded<string, "TestString">;
    const stringValue: string = brandedString;
    const brandedNumber = undefined as unknown as Branded<number, "TestNumber">;
    const numberValue: number = brandedNumber;
    const brandedBoolean = undefined as unknown as Branded<boolean, "TestBoolean">;
    const booleanValue: boolean = brandedBoolean;
    const brandedBigInt = undefined as unknown as Branded<bigint, "TestBigInt">;
    const bigIntValue: bigint = brandedBigInt;
    const brandedSymbol = undefined as unknown as Branded<symbol, "TestSymbol">;
    const symbolValue: symbol = brandedSymbol;
    // @ts-expect-error — raw strings are not ListenerId values
    const rawListenerId: ListenerId = "listener-id";
    // @ts-expect-error — raw strings are not TopicHash values
    const rawTopicHash: TopicHash = "hash";
    const otherId = undefined as unknown as Branded<number, "OtherId">;
    // @ts-expect-error — differently tagged numbers are not ListenerId values
    const otherListenerId: ListenerId = otherId;
    void stringValue;
    void numberValue;
    void booleanValue;
    void bigIntValue;
    void symbolValue;
    void rawListenerId;
    void rawTopicHash;
    void otherListenerId;
  }

  const topic = await engine.createTopic("numbers", []);
  const topicHash: TopicHash = topic.hash;
  expect(topicHash).toBeTypeOf("string");

  expect(topic).toEqual({
    name: "numbers",
    params: [],
    hash: "7847f04c5bf09defec728bc6476dd97e2ff6f42f192ee38632308a5713d2f43f",
  });

  const publish: Publish = () => {};
  const listenerId: ListenerId = engine.subscribe(topic, publish);
  expect(listenerId).toMatch(/^[\da-f]{8}(?:-[\da-f]{4}){3}-[\da-f]{12}$/);
  expect(Object.getOwnPropertyNames(SyncEngine.prototype).sort()).toEqual([
    "constructor",
    "createTopic",
    "mutate",
    "publish",
    "query",
    "subscribe",
    "sync",
    "unsubscribe",
  ]);
  expect(engine.unsubscribe(listenerId)).toBe(true);
  expect(engine.unsubscribe(listenerId)).toBe(false);
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

  const listenerId = engine.subscribe(topic, ({ topic: publishedTopic, value }) => {
    events.push({ topic: publishedTopic, value });
  });
  engine.sync("noop", []);

  expect(listenerId).toMatch(/^[\da-f]{8}(?:-[\da-f]{4}){3}-[\da-f]{12}$/);
  expect(events).toEqual([{ topic, value: [1, 2, 3] }]);

  if (false as boolean) {
    // @ts-expect-error — unknown topic names are rejected
    void engine.createTopic("missing", []);
    // @ts-expect-error — createTopic params must be an empty tuple
    void engine.createTopic("numbers", [1]);
    // @ts-expect-error — subscribe callback must receive a listener event
    void engine.subscribe(topic, (value: number) => value.toFixed());
    // @ts-expect-error — sync expects no params
    engine.sync("noop", [1]);
    const name = topic.name;
    // @ts-expect-error — Topic properties are readonly
    topic.name = name;
    const params = topic.params;
    // @ts-expect-error — Topic properties are readonly
    topic.params = params;
    const hash = topic.hash;
    // @ts-expect-error — Topic properties are readonly
    topic.hash = hash;
    const event: ListenerEvent = { topic, value: [] };
    // @ts-expect-error — ListenerEvent properties are readonly
    event.topic = topic;
    // @ts-expect-error — ListenerEvent properties are readonly
    event.value = [];
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

  const listenerId = engine.subscribe(topic, publish);
  expect(listenerId).toMatch(/^[\da-f]{8}(?:-[\da-f]{4}){3}-[\da-f]{12}$/);
  expect(engine.unsubscribe(listenerId)).toBe(true);
});
