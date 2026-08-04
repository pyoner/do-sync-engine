import { Effect, Equal, Exit, Hash, Schema } from "effect";
import { expect, test } from "vite-plus/test";
import { SyncEngine, Topic, TopicSchema, toTables } from "../src/index.js";
import type { Mutation, Query } from "../src/index.js";

test("topics retain params and use structural equality", () => {
  const queries = {
    numbers: { tables: toTables(["numbers"]), run: () => Effect.succeed(1) } satisfies Query<
      [],
      number
    >,
    byOptions: {
      tables: toTables(["numbers"]),
      run: (options: Map<string, number>) => Effect.succeed(options.get("page") ?? 0),
    } satisfies Query<[Map<string, number>], number>,
  };
  const mutations = {
    noop: {
      tables: toTables(["numbers"]),
      run: () => Effect.succeed(undefined),
    } satisfies Mutation<[], void>,
  };
  const engine = new SyncEngine({ queries, mutations });
  const params: [Map<string, number>] = [new Map([["page", 1]])];
  const topic = Effect.runSync(engine.createTopic("byOptions", params));
  expect(topic.params).toBe(params);
  const equivalent = Effect.runSync(engine.createTopic("byOptions", [new Map([["page", 1]])]));
  const firstEvents: unknown[] = [];
  const secondEvents: unknown[] = [];
  const firstListener = (event: unknown) => firstEvents.push(event);
  const secondListener = (event: unknown) => secondEvents.push(event);
  Effect.runSync(engine.subscribe(topic, firstListener));
  Effect.runSync(engine.subscribe(equivalent, firstListener));
  Effect.runSync(engine.subscribe(equivalent, secondListener));
  expect(firstEvents).toHaveLength(1);
  expect(secondEvents).toHaveLength(1);
  Effect.runSync(engine.sync("noop", []));
  expect(firstEvents).toHaveLength(2);
  expect(secondEvents).toHaveLength(2);
  Effect.runSync(engine.unsubscribe(topic, firstListener));
  Effect.runSync(engine.unsubscribe(equivalent, firstListener));
  Effect.runSync(engine.sync("noop", []));
  expect(firstEvents).toHaveLength(2);
  expect(secondEvents).toHaveLength(3);
  Effect.runSync(engine.unsubscribe(topic, secondListener));
  const registry = (engine as unknown as { registry: { backing: Map<unknown, unknown> } }).registry;
  expect(registry.backing.size).toBe(0);
});

test("removes failed initial subscriptions", () => {
  const failing = new SyncEngine({
    queries: {
      failing: {
        tables: toTables(["numbers"]),
        run: () => Effect.fail(new Error("query failed")).pipe(Effect.as<number>(0)),
      } satisfies Query<[], number, Error>,
    },
    mutations: {},
  });
  const topic = Effect.runSync(failing.createTopic("failing", []));
  expect(Effect.runSyncExit(failing.subscribe(topic, () => {}))).toMatchObject({ _tag: "Failure" });
  const registry = (failing as unknown as { registry: { backing: Map<unknown, unknown> } })
    .registry;
  expect(registry.backing.size).toBe(0);
});

test("topic schema preserves canonical equality and hash", () => {
  const topic = new Topic({ name: "numbers", params: [] });
  const decoded = Schema.decodeUnknownSync(TopicSchema)({ name: topic.name, params: topic.params });
  expect(decoded).toBeInstanceOf(Topic);
  expect(Equal.equals(topic, decoded)).toBe(true);
  expect(Hash.hash(topic)).toBe(Hash.hash(decoded));
});

test("unknown query and mutation names fail", () => {
  class TestEngine extends SyncEngine<
    Record<string, Query<unknown[], unknown>>,
    Record<string, Mutation<unknown[], unknown>>
  > {
    runQuery(name: string, params: unknown[]) {
      return this.query(name, params);
    }
  }
  const engine = new TestEngine({
    queries: {},
    mutations: {},
  });
  expect(Exit.isFailure(Effect.runSyncExit(engine.createTopic("missing", [])))).toBe(true);
  expect(Exit.isFailure(Effect.runSyncExit(engine.runQuery("missing", [])))).toBe(true);
  expect(Exit.isFailure(Effect.runSyncExit(engine.sync("missing", [])))).toBe(true);
});
