import { Effect, Equal, Hash, Schema } from "effect";
import { expect, test } from "vite-plus/test";
import { SyncEngine, Topic, TopicSchema, toTables } from "../src/index.js";
import { validateTopic } from "../src/helpers.js";
import type {
  Branded,
  Mutation,
  Listener,
  ListenerEvent,
  Query,
  ListenerId,
  SyncEngineInterface,
} from "../src/index.js";

test("exports canonical topic and listener APIs", async () => {
  const queries = {
    numbers: {
      tables: toTables(["numbers"]),
      run: () => 1,
    } satisfies Query<[], number>,
  };
  const mutations = {
    noop: {
      tables: toTables([]),
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
    const otherId = undefined as unknown as Branded<number, "OtherId">;
    // @ts-expect-error — differently tagged numbers are not ListenerId values
    const otherListenerId: ListenerId = otherId;
    void stringValue;
    void numberValue;
    void booleanValue;
    void bigIntValue;
    void symbolValue;
    void rawListenerId;
    void otherListenerId;
  }

  const topic = await Effect.runPromise(engine.createTopic("numbers", []));
  expect(topic).toBeInstanceOf(Topic);

  expect(topic).toEqual({ name: "numbers", params: [] });

  const decodedTopic = Schema.decodeUnknownSync(TopicSchema)({
    name: topic.name,
    params: topic.params,
  });
  expect(decodedTopic).toBeInstanceOf(Topic);
  expect(Equal.equals(topic, decodedTopic)).toBe(true);
  expect(Hash.hash(decodedTopic)).toBe(Hash.hash(topic));

  const listener: Listener = () => {};
  const listenerId: ListenerId = engine.subscribe(topic, listener);
  expect(listenerId).toMatch(/^[\da-f]{8}(?:-[\da-f]{4}){3}-[\da-f]{12}$/);
  expect(() =>
    validateTopic(new Topic({ name: "numbers", params: [new Map()] }), new Set(["numbers"])),
  ).toThrow("plain objects and arrays");
  expect(() =>
    validateTopic(new Topic({ name: "numbers", params: [() => 1] }), new Set(["numbers"])),
  ).toThrow("JSON-safe values");
  expect(() =>
    validateTopic(new Topic({ name: "numbers", params: [Symbol("x")] }), new Set(["numbers"])),
  ).toThrow("JSON-safe values");
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
      tables: toTables(["numbers"]),
      run: () => [1, 2, 3],
    } satisfies Query<[], number[]>,
  };
  const mutations = {
    noop: {
      tables: toTables(["numbers"]),
      run: () => ({ ok: true }),
    } satisfies Mutation<[], { ok: boolean }>,
  };
  const engine: SyncEngineInterface<typeof queries, typeof mutations> = new SyncEngine({
    queries,
    mutations,
  });
  const topic: Topic<"numbers", []> = await Effect.runPromise(engine.createTopic("numbers", []));
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
      tables: toTables(["numbers"]),
      run: (value: number) => value,
    } satisfies Query<[number], number>,
  };
  const mutations = {
    noop: {
      tables: toTables([]),
      run: () => ({}),
    } satisfies Mutation<[], Record<string, never>>,
  };
  const engine = new SyncEngine({ queries, mutations });
  const topic = await Effect.runPromise(engine.createTopic("numbers", [42]));
  const listener: Listener = () => {};

  if (false as boolean) {
    // @ts-expect-error — unknown query name
    void engine.createTopic("missing", [42]);
    // @ts-expect-error — query param must be a number
    void engine.createTopic("numbers", ["42"]);
    // @ts-expect-error — subscribe callback belongs in the second position
    void engine.subscribe(topic, [42]);
  }

  const listenerId = engine.subscribe(topic, listener);
  expect(listenerId).toMatch(/^[\da-f]{8}(?:-[\da-f]{4}){3}-[\da-f]{12}$/);
  expect(engine.unsubscribe(listenerId)).toBe(true);
});
