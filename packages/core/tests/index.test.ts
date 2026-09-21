import { expect, test } from "vite-plus/test";
import { QueryExecutionError, SyncEngine, toTables } from "../src/index.js";
import type {
  BaseParams,
  Branded,
  Mutation,
  Listener,
  ListenerEvent,
  Query,
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
  type ListenerProperties = { marker: string };
  const engine = new SyncEngine<string, typeof queries, typeof mutations, ListenerProperties>({
    queries,
    mutations,
    createId: () => crypto.randomUUID(),
  });

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
    const validParams: BaseParams = [{ nested: ["value"] }];
    // @ts-expect-error — BaseParams must be an array
    const invalidParams: BaseParams = "value";
    void validParams;
    void invalidParams;
    void stringValue;
    void numberValue;
    void booleanValue;
    void bigIntValue;
    void symbolValue;
  }

  const topic = expectOk(engine.createTopic("numbers", []));

  expect(topic).toEqual({
    name: "numbers",
    params: [],
  });

  const listener: Listener<
    ListenerEvent<Topic<"numbers", []>, number>,
    ListenerProperties
  > = Object.assign(() => {}, { marker: "numbers" });
  const listenerId = expectOk(engine.subscribe(topic, listener));
  const [subscription] = [...engine.subscriptions(topic)];
  expect(subscription?.listener).toBe(listener);
  expect(subscription?.listener.marker).toBe("numbers");
  expect(Object.getOwnPropertyNames(SyncEngine.prototype).sort()).toEqual([
    "constructor",
    "createTopic",
    "mutate",
    "publish",
    "query",
    "subscribe",
    "subscriptions",
    "sync",
    "unsubscribe",
  ]);
  expect(listenerId).toBeDefined();
  expectOk(engine.unsubscribe(topic, listener));
  expectOk(engine.unsubscribe(topic, listener));
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
  const engine: SyncEngineInterface<string, typeof queries, typeof mutations> = new SyncEngine<
    string,
    typeof queries,
    typeof mutations
  >({
    queries,
    mutations,
    createId: () => crypto.randomUUID(),
  });
  const topic: Topic<"numbers", []> = expectOk(engine.createTopic("numbers", []));
  const events: Array<{ topic: Topic<"numbers", []>; value: number[] }> = [];

  const listener: Listener<ListenerEvent<Topic<"numbers", []>, number[]>> = ({
    topic: publishedTopic,
    value,
  }) => {
    events.push({ topic: publishedTopic, value });
  };
  expectOk(engine.subscribe(topic, listener));
  expectOk(engine.sync("noop", []));
  expect(events).toEqual([
    { topic, value: [1, 2, 3] },
    { topic, value: [1, 2, 3] },
  ]);

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
  const engine = new SyncEngine({ queries, mutations, createId: () => crypto.randomUUID() });
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

  expectOk(engine.subscribe(topic, listener));
  expectOk(engine.unsubscribe(topic, listener));
});

test("uses topic identity for listener registration", async () => {
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
  const engine = new SyncEngine({ queries, mutations, createId: () => crypto.randomUUID() });
  const firstTopic = expectOk(engine.createTopic("numbers", [{ page: 1, search: "one" }]));
  const secondTopic = expectOk(engine.createTopic("numbers", [{ search: "one", page: 1 }]));
  const emptyTopic = expectOk(engine.createTopic("numbers", [{ page: 1, search: "one" }]));
  const listener: Listener = () => {};

  const firstId = expectOk(engine.subscribe(firstTopic, listener));
  const secondId = expectOk(engine.subscribe(secondTopic, listener));
  expect(secondId).not.toBe(firstId);
  expect([...engine.subscriptions(firstTopic)]).toEqual([
    { id: firstId, topic: firstTopic, listener },
  ]);
  expect([...engine.subscriptions(emptyTopic)]).toEqual([]);
  engine.unsubscribe(firstTopic, listener);
  expect([...engine.subscriptions(firstTopic)]).toEqual([]);
  expect(engine.subscribe(secondTopic, listener)).toBe(secondId);
});

test("supports explicit IDs and every unsubscribe form", () => {
  const queries = {
    value: { tables: toTables(["value"]), run: () => 1 } satisfies Query<[], number>,
  };
  const engine = new SyncEngine({
    queries,
    mutations: { noop: { tables: toTables(["value"]), run: () => null } },
  });
  const topic = expectOk(engine.createTopic("value", []));
  const isolatedTopic = expectOk(engine.createTopic("value", []));
  const first: number[] = [];
  const second: number[] = [];
  const firstListener: Listener = () => first.push(1);
  const secondListener: Listener = () => second.push(1);

  expect(engine.subscribe(topic, firstListener, "first")).toBe("first");
  expect(engine.subscribe(topic, secondListener, "second")).toBe("second");
  engine.unsubscribe(topic, firstListener);
  engine.sync("noop", []);
  expect(engine.subscribe(topic, firstListener, "first")).toBe("first");
  engine.sync("noop", []);
  engine.unsubscribe("first");
  engine.sync("noop", []);
  engine.unsubscribe(topic, "second");
  expect(engine.subscribe(isolatedTopic, () => {}, "isolated")).toBe("isolated");
  engine.unsubscribe("isolated");
  engine.sync("noop", []);
  expect(first).toEqual([1, 1, 1]);
  expect(second).toEqual([1, 1, 1, 1]);
});

test("preserves an explicit listener when replacement query fails", () => {
  let shouldFail = false;
  const originalEvents: number[] = [];
  const replacementEvents: number[] = [];
  type ListenerProperties = { source: string };
  const queries = {
    value: {
      tables: toTables(["value"]),
      run: () => {
        if (shouldFail) throw new Error("query failed");
        return 1;
      },
    },
  };
  const mutations = { touch: { tables: toTables(["value"]), run: () => undefined } };
  const engine = new SyncEngine<string, typeof queries, typeof mutations, ListenerProperties>({
    queries,
    mutations,
  });
  const topic = expectOk(engine.createTopic("value", []));
  const original = Object.assign(() => originalEvents.push(1), { source: "original" });
  const replacement = Object.assign(() => replacementEvents.push(1), { source: "replacement" });

  expect(engine.subscribe(topic, original, "same")).toBe("same");
  shouldFail = true;
  const replacementResult = engine.subscribe(topic, replacement, "same");
  expect(replacementResult).toBeInstanceOf(QueryExecutionError);
  expect((replacementResult as Error).message).toBe("Query execution failed");
  const [subscription] = [...engine.subscriptions(topic)];
  expect(subscription?.listener).toBe(original);
  expect(subscription?.listener.source).toBe("original");

  shouldFail = false;
  expect(engine.sync("touch", [])).toBeUndefined();
  expect(originalEvents).toEqual([1, 1]);
  expect(replacementEvents).toEqual([]);
});

test("enumerates active subscriptions", () => {
  const engine = new SyncEngine({
    queries: { value: { tables: toTables(["value"]), run: () => 1 } },
    mutations: {},
  });
  const topic = expectOk(engine.createTopic("value", []));
  const firstListener: Listener = () => undefined;
  const secondListener: Listener = () => undefined;
  expect(engine.subscribe(topic, firstListener, "first")).toBe("first");
  expect(engine.subscribe(topic, secondListener, "second")).toBe("second");

  expect([...engine.subscriptions()]).toEqual([
    { id: "first", topic, listener: firstListener },
    { id: "second", topic, listener: secondListener },
  ]);
  engine.unsubscribe("first");
  expect([...engine.subscriptions()]).toEqual([{ id: "second", topic, listener: secondListener }]);
});

test("requires and uses numeric ID factories", () => {
  const engine = new SyncEngine<number, { value: Query<[], number> }, {}>({
    queries: { value: { tables: toTables([]), run: () => 1 } },
    mutations: {},
    createId: () => 7,
  });
  const topic = expectOk(engine.createTopic("value", []));
  expect(engine.subscribe(topic, () => undefined)).toBe(7);
});
