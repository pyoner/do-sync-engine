import { expect, test } from "vite-plus/test";
import { SyncEngine, toTables } from "../src/index.js";
import type {
  Branded,
  Mutation,
  Listener,
  ListenerEvent,
  Query,
  ListenerId,
  SyncEngineInterface,
  Topic,
} from "../src/index.js";

function expectOk<T>(value: T): Exclude<T, Error> {
  if (value instanceof Error) throw value;
  return value as Exclude<T, Error>;
}

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
    void otherListenerId;
    void rawListenerId;
  }

  const topic = expectOk(engine.createTopic("numbers", []));

  expect(topic).toEqual({
    name: "numbers",
    params: [],
  });

  const listener: Listener = () => {};
  const listenerId: ListenerId = expectOk(engine.subscribe(topic, listener));
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
  const topic: Topic<"numbers", []> = expectOk(engine.createTopic("numbers", []));
  const events: Array<{ topic: Topic<"numbers", []>; value: number[] }> = [];

  const listenerId = expectOk(
    engine.subscribe(topic, ({ topic: publishedTopic, value }) => {
      events.push({ topic: publishedTopic, value });
    }),
  );
  expectOk(engine.sync("noop", []));

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
  const topic = expectOk(engine.createTopic("numbers", [42]));
  const listener: Listener = () => {};

  if (false as boolean) {
    // @ts-expect-error — unknown query name
    void engine.createTopic("missing", [42]);
    // @ts-expect-error — query param must be a number
    void engine.createTopic("numbers", ["42"]);
    // @ts-expect-error — subscribe callback belongs in the second position
    void engine.subscribe(topic, [42]);
  }

  const listenerId = expectOk(engine.subscribe(topic, listener));
  expect(listenerId).toMatch(/^[\da-f]{8}(?:-[\da-f]{4}){3}-[\da-f]{12}$/);
  expect(engine.unsubscribe(listenerId)).toBe(true);
});

test("compares structurally equal topics", async () => {
  const queries = {
    numbers: {
      tables: toTables(["numbers"]),
      run: (filter: { page: number; search: string }) => filter.page,
    } satisfies Query<[{ page: number; search: string }], number>,
  };
  const mutations = {
    noop: {
      tables: toTables([]),
      run: () => ({}),
    } satisfies Mutation<[], Record<string, never>>,
  };
  const engine = new SyncEngine({ queries, mutations });
  const firstTopic = expectOk(engine.createTopic("numbers", [{ page: 1, search: "one" }]));
  const secondTopic = expectOk(engine.createTopic("numbers", [{ search: "one", page: 1 }]));
  const listener: Listener = () => {};

  const listenerId = expectOk(engine.subscribe(firstTopic, listener));

  expect(engine.subscribe(secondTopic, listener)).toBe(listenerId);
  expect(engine.unsubscribe(listenerId)).toBe(true);
});
