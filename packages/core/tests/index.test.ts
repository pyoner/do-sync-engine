import { Effect, Equal, Exit, Hash, Option, Schema } from "effect";
import { expect, test } from "vite-plus/test";
import {
  SyncEngine,
  Topic,
  TopicBuildError,
  TopicSchema,
  TopicValidationError,
  UnknownMutationError,
  UnknownQueryError,
  toTables,
} from "../src/index.js";
import { buildTopic, clone, validateTopic } from "../src/helpers.js";
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
      run: () => Effect.succeed(1),
    } satisfies Query<[], number>,
  };
  const mutations = {
    noop: {
      tables: toTables([]),
      run: () => Effect.succeed({ ok: true }),
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
  const listenerId = await Effect.runPromise(engine.subscribe(topic, listener));
  expect(listenerId).toMatch(/^[\da-f]{8}(?:-[\da-f]{4}){3}-[\da-f]{12}$/);

  const invalidTopic = await Effect.runPromiseExit(
    validateTopic(new Topic({ name: "numbers", params: [new Map()] }), new Set(["numbers"])),
  );
  expect(Exit.isFailure(invalidTopic)).toBe(true);
  if (Exit.isFailure(invalidTopic)) {
    const error = Exit.findErrorOption(invalidTopic);
    expect(Option.isSome(error)).toBe(true);
    if (Option.isSome(error)) expect(error.value).toBeInstanceOf(TopicValidationError);
  }

  const unsupportedFunction = await Effect.runPromiseExit(
    validateTopic(new Topic({ name: "numbers", params: [() => 1] }), new Set(["numbers"])),
  );
  expect(Exit.isFailure(unsupportedFunction)).toBe(true);
  if (Exit.isFailure(unsupportedFunction)) {
    const error = Exit.findErrorOption(unsupportedFunction);
    expect(Option.isSome(error)).toBe(true);
    if (Option.isSome(error)) expect(error.value).toBeInstanceOf(TopicValidationError);
  }

  const unsupportedSymbol = await Effect.runPromiseExit(
    validateTopic(new Topic({ name: "numbers", params: [Symbol("x")] }), new Set(["numbers"])),
  );
  expect(Exit.isFailure(unsupportedSymbol)).toBe(true);
  if (Exit.isFailure(unsupportedSymbol)) {
    const error = Exit.findErrorOption(unsupportedSymbol);
    expect(Option.isSome(error)).toBe(true);
    if (Option.isSome(error)) expect(error.value).toBeInstanceOf(TopicValidationError);
  }

  const unknownQuery = await Effect.runPromiseExit(
    validateTopic(new Topic({ name: "missing", params: [] }), new Set(["numbers"])),
  );
  expect(Exit.isFailure(unknownQuery)).toBe(true);
  if (Exit.isFailure(unknownQuery)) {
    const error = Exit.findErrorOption(unknownQuery);
    expect(Option.isSome(error)).toBe(true);
    if (Option.isSome(error)) expect(error.value).toBeInstanceOf(UnknownQueryError);
  }

  const cloneFailure = await Effect.runPromiseExit(clone(() => 1));
  expect(Exit.isFailure(cloneFailure)).toBe(true);
  if (Exit.isFailure(cloneFailure)) {
    const error = Exit.findErrorOption(cloneFailure);
    expect(Option.isSome(error)).toBe(true);
    if (Option.isSome(error)) {
      expect(error.value).toBeInstanceOf(TopicBuildError);
      expect(error.value.operation).toBe("clone");
    }
  }

  const serializationFailure = await Effect.runPromiseExit(buildTopic("numbers", [1n]));
  expect(Exit.isFailure(serializationFailure)).toBe(true);
  if (Exit.isFailure(serializationFailure)) {
    const error = Exit.findErrorOption(serializationFailure);
    expect(Option.isSome(error)).toBe(true);
    if (Option.isSome(error)) {
      expect(error.value).toBeInstanceOf(TopicBuildError);
      expect(error.value.operation).toBe("serialize");
    }
  }
  const unknownQueries: Record<string, Query<unknown[], unknown>> = {};
  const unknownMutations: Record<string, Mutation<unknown[], unknown>> = {};
  const unknownEngine = new SyncEngine({ queries: unknownQueries, mutations: unknownMutations });
  const unknownMutationFailure = await Effect.runPromiseExit(unknownEngine.sync("missing", []));
  expect(Exit.isFailure(unknownMutationFailure)).toBe(true);
  if (Exit.isFailure(unknownMutationFailure)) {
    const error = Exit.findErrorOption(unknownMutationFailure);
    expect(Option.isSome(error)).toBe(true);
    if (Option.isSome(error)) expect(error.value).toBeInstanceOf(UnknownMutationError);
  }
  const unknownQueryFailure = await Effect.runPromiseExit(
    unknownEngine.query(new Topic({ name: "missing", params: [] })),
  );
  expect(Exit.isFailure(unknownQueryFailure)).toBe(true);
  if (Exit.isFailure(unknownQueryFailure)) {
    const error = Exit.findErrorOption(unknownQueryFailure);
    expect(Option.isSome(error)).toBe(true);
    if (Option.isSome(error)) expect(error.value).toBeInstanceOf(UnknownQueryError);
  }

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
  expect(await Effect.runPromise(engine.unsubscribe(listenerId))).toBe(true);
  expect(await Effect.runPromise(engine.unsubscribe(listenerId))).toBe(false);
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
  const engine: SyncEngineInterface<typeof queries, typeof mutations> = new SyncEngine({
    queries,
    mutations,
  });
  const topic: Topic<"numbers", []> = await Effect.runPromise(engine.createTopic("numbers", []));
  const events: Array<{ topic: Topic<"numbers", []>; value: number[] }> = [];

  const listenerId = await Effect.runPromise(
    engine.subscribe(topic, ({ topic: publishedTopic, value }) => {
      events.push({ topic: publishedTopic, value });
    }),
  );
  await Effect.runPromise(engine.sync("noop", []));

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
    void engine.sync("noop", [1]);
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
      run: (value: number) => Effect.succeed(value),
    } satisfies Query<[number], number>,
  };
  const mutations = {
    noop: {
      tables: toTables([]),
      run: () => Effect.succeed({}),
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

  const listenerId = await Effect.runPromise(engine.subscribe(topic, listener));
  expect(listenerId).toMatch(/^[\da-f]{8}(?:-[\da-f]{4}){3}-[\da-f]{12}$/);
  expect(await Effect.runPromise(engine.unsubscribe(listenerId))).toBe(true);
});
