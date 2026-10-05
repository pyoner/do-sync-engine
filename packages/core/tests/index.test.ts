import { expect, test } from "vite-plus/test";
import { Context, Effect, Layer, Schema } from "effect";
import {
  MissingSubscriptionIdError,
  UnknownMutationError,
  UnknownQueryError,
  makeSyncEngine,
  syncEngineLayer,
  toTables,
} from "../src/index.js";
import type {
  BaseParams,
  Mutation,
  Listener,
  ListenerEvent,
  Query,
  SyncEngine,
  Topic,
} from "../src/index.js";

test("exports canonical topic and listener APIs", async () => {
  const queries = {
    numbers: {
      tables: toTables(["numbers"]),
      run: () => Effect.succeed(1),
    } satisfies Query<[], number>,
  };
  const mutations = {
    noop: {
      tables: toTables([]),
      run: () => Effect.succeed({ ok: true }),
    } satisfies Mutation<[], { ok: boolean }>,
  };
  type ListenerProperties = { marker: string };
  const engine = Effect.runSync(
    makeSyncEngine<string, typeof queries, typeof mutations, ListenerProperties>({
      queries,
      mutations,
      createId: () => crypto.randomUUID(),
    }),
  );

  if (false as boolean) {
    const validParams: BaseParams = [{ nested: ["value"] }];
    // @ts-expect-error — BaseParams must be an array
    const invalidParams: BaseParams = "value";
    void validParams;
    void invalidParams;
  }

  const topic = Effect.runSync(engine.createTopic("numbers", []));

  expect(topic).toEqual({
    name: "numbers",
    params: [],
  });

  const listener: Listener<
    ListenerEvent<Topic<"numbers", []>, number>,
    ListenerProperties
  > = Object.assign(() => {}, { marker: "numbers" });
  const listenerId = Effect.runSync(engine.subscribe(topic, listener));
  const subscription = Effect.runSync(engine.subscriptions(topic))[0];
  expect(subscription?.listener).toBe(listener);
  expect(subscription?.listener.marker).toBe("numbers");
  expect(listenerId).toBeDefined();
  Effect.runSync(engine.unsubscribe(topic, listener));
  Effect.runSync(engine.unsubscribe(topic, listener));
});

test("typed topic params, listener values, mutations, and sync", async () => {
  const queries = {
    numbers: {
      tables: toTables(["numbers"]),
      run: () => Effect.succeed([1, 2, 3]),
    } satisfies Query<[], number[]>,
  };
  const mutations = {
    noop: {
      tables: toTables(["numbers"]),
      run: () => Effect.succeed({ ok: true }),
    } satisfies Mutation<[], { ok: boolean }>,
  };
  const engine: SyncEngine<string, typeof queries, typeof mutations> = Effect.runSync(
    makeSyncEngine<string, typeof queries, typeof mutations>({
      queries,
      mutations,
      createId: () => crypto.randomUUID(),
    }),
  );
  const topic: Topic<"numbers", []> = Effect.runSync(engine.createTopic("numbers", []));
  const events: Array<{ topic: Topic<"numbers", []>; value: number[] }> = [];

  const listener: Listener<ListenerEvent<Topic<"numbers", []>, number[]>> = ({
    topic: publishedTopic,
    value,
  }) => {
    events.push({ topic: publishedTopic, value });
  };
  Effect.runSync(engine.subscribe(topic, listener));
  Effect.runSync(engine.sync("noop", []));
  expect(events).toEqual([{ topic, value: [1, 2, 3] }]);

  if (false as boolean) {
    // @ts-expect-error — unknown topic names are rejected
    void engine.createTopic("missing", []);
    // @ts-expect-error — createTopic params must be an empty tuple
    void engine.createTopic("numbers", [1]);
    // @ts-expect-error — subscribe callback must receive a listener event
    void engine.subscribe(topic, (value: number) => value.toFixed());
    // @ts-expect-error — sync expects no params
    Effect.runSync(engine.sync("noop", [1]));
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

test("delivers each listener only events it has not already received", () => {
  let rows = ["a"];
  const queries = {
    rows: {
      tables: toTables(["rows"]),
      run: () => Effect.sync(() => [...rows]),
    } satisfies Query<[], string[]>,
  };
  const mutations = {
    set: {
      tables: toTables(["rows"]),
      run: (next: string[]) =>
        Effect.sync(() => {
          rows = next;
        }),
    } satisfies Mutation<[string[]], void>,
  };
  const engine = Effect.runSync(
    makeSyncEngine<string, typeof queries, typeof mutations>({ queries, mutations }),
  );
  const topic = Effect.runSync(engine.createTopic("rows", []));
  const first: string[][] = [];
  const late: string[][] = [];

  Effect.runSync(engine.subscribe(topic, ({ value }) => first.push(value), "first"));
  Effect.runSync(engine.sync("set", [["a"]]));
  expect(first).toEqual([["a"]]);

  Effect.runSync(engine.sync("set", [["b"]]));
  const lateListener = ({ value }: ListenerEvent<Topic<"rows", []>, string[]>) => late.push(value);
  Effect.runSync(engine.subscribe(topic, lateListener, "late"));
  Effect.runSync(engine.sync("set", [["b"]]));
  Effect.runSync(engine.sync("set", [["a"]]));
  expect(first).toEqual([["a"], ["b"], ["a"]]);
  expect(late).toEqual([["b"], ["a"]]);

  Effect.runSync(engine.subscribe(topic, lateListener, "late"));
  expect(late).toEqual([["b"], ["a"], ["a"]]);
});
test("typed createTopic params and listener handle", async () => {
  const queries = {
    numbers: {
      tables: toTables(["numbers"]),
      run: (value: number) => Effect.succeed(value),
    } satisfies Query<[number], number>,
  };
  const mutations = {
    noop: {
      tables: toTables([]),
      run: () => Effect.succeed({}),
    } satisfies Mutation<[], Record<string, never>>,
  };
  const engine = Effect.runSync(
    makeSyncEngine({ queries, mutations, createId: () => crypto.randomUUID() }),
  );
  const topic = Effect.runSync(engine.createTopic("numbers", [42]));
  const listener: Listener = () => {};

  if (false as boolean) {
    // @ts-expect-error — unknown query name
    void engine.createTopic("missing", [42]);
    // @ts-expect-error — query param must be a number
    void engine.createTopic("numbers", ["42"]);
    // @ts-expect-error — subscribe callback belongs in the second position
    void engine.subscribe(topic, [42]);
  }

  Effect.runSync(engine.subscribe(topic, listener));
  Effect.runSync(engine.unsubscribe(topic, listener));
});

test("uses structural topic equality for listener registration", () => {
  let offset = 0;
  const queries = {
    numbers: {
      tables: toTables(["numbers"]),
      run: (filter: { page: { current: number; total: number }; search: string }) =>
        Effect.sync(() => filter.page.current + filter.page.total + offset),
    } satisfies Query<[{ page: { current: number; total: number }; search: string }], number>,
  };
  const mutations = {
    noop: {
      tables: toTables(["numbers"]),
      run: () =>
        Effect.sync(() => {
          offset++;
          return {};
        }),
    } satisfies Mutation<[], Record<string, never>>,
  };
  const engine = Effect.runSync(
    makeSyncEngine({ queries, mutations, createId: () => crypto.randomUUID() }),
  );
  const firstTopic = Effect.runSync(
    engine.createTopic("numbers", [{ page: { current: 1, total: 2 }, search: "one" }]),
  );
  const equivalentTopic = Effect.runSync(
    engine.createTopic("numbers", [{ search: "one", page: { total: 2, current: 1 } }]),
  );
  const distinctTopic = Effect.runSync(
    engine.createTopic("numbers", [{ page: { current: 2, total: 2 }, search: "one" }]),
  );
  const firstEvents: number[] = [];
  const secondEvents: number[] = [];
  const firstListener: Listener = ({ value }) => firstEvents.push(value);
  const secondListener: Listener = ({ value }) => secondEvents.push(value);

  const firstId = Effect.runSync(engine.subscribe(firstTopic, firstListener));
  const secondId = Effect.runSync(engine.subscribe(equivalentTopic, secondListener));
  expect(secondId).not.toBe(firstId);
  expect(Effect.runSync(engine.subscriptions(equivalentTopic)).map(({ id }) => id)).toEqual([
    firstId,
    secondId,
  ]);
  expect(Effect.runSync(engine.subscriptions(distinctTopic))).toEqual([]);

  Effect.runSync(engine.sync("noop", []));
  expect(firstEvents).toEqual([3, 4]);
  expect(secondEvents).toEqual([3, 4]);

  Effect.runSync(engine.unsubscribe(distinctTopic, firstListener));
  expect(Effect.runSync(engine.subscriptions(firstTopic))).toHaveLength(2);
  Effect.runSync(engine.unsubscribe(equivalentTopic, firstListener));
  expect(Effect.runSync(engine.subscriptions(firstTopic)).map(({ id }) => id)).toEqual([secondId]);
  Effect.runSync(engine.sync("noop", []));
  expect(firstEvents).toEqual([3, 4]);
  expect(secondEvents).toEqual([3, 4, 5]);

  Effect.runSync(engine.unsubscribe(firstTopic, secondListener));
  expect(Effect.runSync(engine.subscriptions())).toEqual([]);
});

test("supports explicit IDs and every unsubscribe form", () => {
  let n = 0;
  const queries = {
    value: { tables: toTables(["value"]), run: () => Effect.sync(() => n) } satisfies Query<
      [],
      number
    >,
  };
  const engine = Effect.runSync(
    makeSyncEngine({
      queries,
      mutations: {
        noop: { tables: toTables(["value"]), run: () => Effect.sync(() => void n++) },
      },
    }),
  );
  const topic = Effect.runSync(engine.createTopic("value", []));
  const isolatedTopic = Effect.runSync(engine.createTopic("value", []));
  const first: number[] = [];
  const second: number[] = [];
  const firstListener: Listener = ({ value }) => first.push(value);
  const secondListener: Listener = ({ value }) => second.push(value);

  expect(Effect.runSync(engine.subscribe(topic, firstListener, "first"))).toBe("first");
  expect(Effect.runSync(engine.subscribe(topic, secondListener, "second"))).toBe("second");
  Effect.runSync(engine.unsubscribe(topic, firstListener));
  Effect.runSync(engine.sync("noop", []));
  expect(Effect.runSync(engine.subscribe(topic, firstListener, "first"))).toBe("first");
  Effect.runSync(engine.sync("noop", []));
  Effect.runSync(engine.unsubscribe("first"));
  Effect.runSync(engine.sync("noop", []));
  Effect.runSync(engine.unsubscribe(topic, "second"));
  expect(Effect.runSync(engine.subscribe(isolatedTopic, () => {}, "isolated"))).toBe("isolated");
  Effect.runSync(engine.unsubscribe("isolated"));
  Effect.runSync(engine.sync("noop", []));
  expect(first).toEqual([0, 1, 2]);
  expect(second).toEqual([0, 1, 2, 3]);
});

class QueryFailed extends Schema.TaggedError<QueryFailed>()("QueryFailed", {}) {}

test("preserves an explicit listener when replacement query fails", () => {
  let shouldFail = false;
  let n = 0;
  const originalEvents: number[] = [];
  const replacementEvents: number[] = [];
  type ListenerProperties = { source: string };
  const queries = {
    value: {
      tables: toTables(["value"]),
      run: () => (shouldFail ? Effect.fail(new QueryFailed()) : Effect.sync(() => n)),
    },
  };
  const mutations = {
    touch: { tables: toTables(["value"]), run: () => Effect.sync(() => void n++) },
  };
  const engine = Effect.runSync(
    makeSyncEngine<string, typeof queries, typeof mutations, ListenerProperties>({
      queries,
      mutations,
    }),
  );
  const topic = Effect.runSync(engine.createTopic("value", []));
  const original = Object.assign(() => originalEvents.push(1), { source: "original" });
  const replacement = Object.assign(() => replacementEvents.push(1), { source: "replacement" });

  expect(Effect.runSync(engine.subscribe(topic, original, "same"))).toBe("same");
  shouldFail = true;
  expect(Effect.runSync(Effect.flip(engine.subscribe(topic, replacement, "same")))).toBeInstanceOf(
    QueryFailed,
  );
  const subscription = Effect.runSync(engine.subscriptions(topic))[0];
  expect(subscription?.listener).toBe(original);
  expect(subscription?.listener.source).toBe("original");

  shouldFail = false;
  expect(Effect.runSync(engine.sync("touch", []))).toBeUndefined();
  expect(originalEvents).toEqual([1, 1]);
  expect(replacementEvents).toEqual([]);
});

test("enumerates active subscriptions", () => {
  const engine = Effect.runSync(
    makeSyncEngine({
      queries: { value: { tables: toTables(["value"]), run: () => Effect.succeed(1) } },
      mutations: {},
    }),
  );
  const topic = Effect.runSync(engine.createTopic("value", []));
  const firstListener: Listener = () => undefined;
  const secondListener: Listener = () => undefined;
  expect(Effect.runSync(engine.subscribe(topic, firstListener, "first"))).toBe("first");
  expect(Effect.runSync(engine.subscribe(topic, secondListener, "second"))).toBe("second");

  expect(Effect.runSync(engine.subscriptions())).toEqual([
    { id: "first", topic, listener: firstListener },
    { id: "second", topic, listener: secondListener },
  ]);
  Effect.runSync(engine.unsubscribe("first"));
  expect(Effect.runSync(engine.subscriptions())).toEqual([
    { id: "second", topic, listener: secondListener },
  ]);
});

test("passes subscription arguments to numeric ID factories", () => {
  const received: unknown[][] = [];
  const engine = Effect.runSync(
    makeSyncEngine<number, { value: Query<[], number> }, {}, { marker: string }>({
      queries: { value: { tables: toTables([]), run: () => Effect.succeed(1) } },
      mutations: {},
      createId: (topic, listener) => {
        expect(listener.marker).toBe("value");
        received.push([topic, listener]);
        return 7;
      },
    }),
  );
  const topic = Effect.runSync(engine.createTopic("value", []));
  const listener = Object.assign(() => undefined, { marker: "value" });
  expect(Effect.runSync(engine.subscribe(topic, listener))).toBe(7);
  expect(received).toEqual([[topic, listener]]);
});

test("builds the engine from a Context.Service layer with query services", () => {
  class Db extends Context.Service<Db, { rows: number[] }>()("test/Db") {}
  const queries = {
    rows: {
      tables: toTables(["rows"]),
      run: () => Db.useSync((db) => [...db.rows]),
    } satisfies Query<[], number[], never, Db>,
  };
  const mutations = {
    add: {
      tables: toTables(["rows"]),
      run: (n: number) => Db.useSync((db) => void db.rows.push(n)),
    } satisfies Mutation<[number], void, never, Db>,
  };
  class Engine extends Context.Service<
    Engine,
    SyncEngine<string, typeof queries, typeof mutations>
  >()("test/Engine") {}

  const events: number[][] = [];
  const program = Effect.gen(function* () {
    const engine = yield* Engine;
    const topic = yield* engine.createTopic("rows", []);
    yield* engine.subscribe(topic, ({ value }) => events.push(value), "a");
    yield* engine.sync("add", [2]);
  });

  Effect.runSync(
    program.pipe(
      Effect.provide(
        syncEngineLayer(Engine, { queries, mutations }).pipe(
          Layer.provide(Layer.succeed(Db, { rows: [1] })),
        ),
      ),
    ),
  );
  expect(events).toEqual([[1], [1, 2]]);

  if (false as boolean) {
    // @ts-expect-error — Db must be provided
    Effect.runSync(program.pipe(Effect.provide(syncEngineLayer(Engine, { queries, mutations }))));
  }
});

test("fails with tagged errors for unknown names and missing ids", () => {
  const queries = {
    value: { tables: toTables(["value"]), run: () => Effect.succeed(1) } satisfies Query<
      [],
      number
    >,
  };
  const mutations = {
    noop: { tables: toTables([]), run: () => Effect.void } satisfies Mutation<[], void>,
  };
  const engine = Effect.runSync(makeSyncEngine({ queries, mutations }));

  const unknownQuery = Effect.runSync(
    Effect.flip(engine.createTopic("missing" as never, [] as never)),
  );
  expect(unknownQuery).toBeInstanceOf(UnknownQueryError);
  expect(unknownQuery.message).toBe("Unknown query: missing");

  const unknownMutation = Effect.runSync(Effect.flip(engine.sync("missing" as never, [] as never)));
  expect(unknownMutation).toBeInstanceOf(UnknownMutationError);
  expect(unknownMutation.message).toBe("Unknown mutation: missing");

  const topic = Effect.runSync(engine.createTopic("value", []));
  expect(Effect.runSync(Effect.flip(engine.subscribe(topic, () => {})))).toBeInstanceOf(
    MissingSubscriptionIdError,
  );
});

const dedupeCases: Array<[string, () => unknown, () => unknown]> = [
  ["Date", () => new Date(1), () => new Date(2)],
  ["Map", () => new Map([[1, { a: 1 }]]), () => new Map([[1, { a: 2 }]])],
  ["Set", () => new Set([1, 2]), () => new Set([1, 3])],
  ["bigint", () => 10n, () => 11n],
  ["Uint8Array", () => new Uint8Array([1, 2]), () => new Uint8Array([1, 3])],
  ["NaN", () => ({ n: NaN }), () => ({ n: 1 })],
  ["undefined property", () => ({ a: undefined }), () => ({})],
];

test.each(dedupeCases)("dedupes %s values by structural equality", (_name, same, changed) => {
  let next: () => unknown = same;
  const queries = {
    value: { tables: toTables(["value"]), run: () => Effect.sync(() => next()) },
  };
  const mutations = { touch: { tables: toTables(["value"]), run: () => Effect.void } };
  const engine = Effect.runSync(makeSyncEngine({ queries, mutations }));
  const topic = Effect.runSync(engine.createTopic("value", []));
  const events: unknown[] = [];
  Effect.runSync(engine.subscribe(topic, ({ value }) => events.push(value), "a"));

  next = same; // a fresh, structurally equal instance
  Effect.runSync(engine.sync("touch", []));
  expect(events).toHaveLength(1);

  next = changed;
  Effect.runSync(engine.sync("touch", []));
  expect(events).toHaveLength(2);
});

test("delivers an in-place mutated query result", () => {
  const rows: number[] = [1];
  const queries = {
    rows: { tables: toTables(["rows"]), run: () => Effect.succeed(rows) },
  };
  const mutations = {
    add: {
      tables: toTables(["rows"]),
      run: (n: number) => Effect.sync(() => void rows.push(n)),
    },
  };
  const engine = Effect.runSync(makeSyncEngine({ queries, mutations }));
  const topic = Effect.runSync(engine.createTopic("rows", []));
  const seen: number[] = [];
  Effect.runSync(engine.subscribe(topic, ({ value }) => seen.push(value.length), "a"));

  Effect.runSync(engine.sync("add", [2]));
  expect(seen).toEqual([1, 2]);
});

test("delivers again when the last event cannot be snapshotted", () => {
  const n = 0;
  const queries = {
    fn: { tables: toTables(["fn"]), run: () => Effect.sync(() => ({ call: () => n })) },
  };
  const mutations = { touch: { tables: toTables(["fn"]), run: () => Effect.void } };
  const engine = Effect.runSync(makeSyncEngine({ queries, mutations }));
  const topic = Effect.runSync(engine.createTopic("fn", []));
  const events: unknown[] = [];
  Effect.runSync(engine.subscribe(topic, (event) => events.push(event), "a"));
  Effect.runSync(engine.sync("touch", []));
  expect(events).toHaveLength(2);
});

test("dedupes class instance values despite the clone losing the prototype", () => {
  class Point {
    constructor(readonly x: number) {}
  }
  const queries = {
    point: { tables: toTables(["point"]), run: () => Effect.sync(() => new Point(1)) },
  };
  const mutations = { touch: { tables: toTables(["point"]), run: () => Effect.void } };
  const engine = Effect.runSync(makeSyncEngine({ queries, mutations }));
  const topic = Effect.runSync(engine.createTopic("point", []));
  const events: unknown[] = [];
  Effect.runSync(engine.subscribe(topic, (event) => events.push(event), "a"));
  Effect.runSync(engine.sync("touch", []));
  expect(events).toHaveLength(1);
});
