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
  const events: unknown[] = [];
  const listener = (event: unknown) => events.push(event);
  const first = Effect.runSync(engine.subscribe(topic, listener));
  const second = Effect.runSync(engine.subscribe(equivalent, listener));
  expect(second).toBe(first);
  Effect.runSync(engine.sync("noop", []));
  expect(events).toHaveLength(1);
  expect(Effect.runSync(engine.unsubscribe(first))).toBe(true);
  expect(Effect.runSync(engine.unsubscribe(first))).toBe(false);
  const asyncEvents: Promise<void>[] = [];
  const asyncId = Effect.runSync(
    engine.subscribe(topic, () => {
      asyncEvents.push(Promise.resolve());
    }),
  );
  Effect.runSync(engine.sync("noop", []));
  expect(asyncEvents).toHaveLength(1);
  expect(Effect.runSync(engine.unsubscribe(asyncId))).toBe(true);
});

test("topic schema preserves canonical equality and hash", () => {
  const topic = new Topic({ name: "numbers", params: [] });
  const decoded = Schema.decodeUnknownSync(TopicSchema)({ name: topic.name, params: topic.params });
  expect(decoded).toBeInstanceOf(Topic);
  expect(Equal.equals(topic, decoded)).toBe(true);
  expect(Hash.hash(topic)).toBe(Hash.hash(decoded));
});

test("unknown query and mutation names fail", () => {
  const engine = new SyncEngine({
    queries: {} as Record<string, Query<unknown[], unknown>>,
    mutations: {} as Record<string, Mutation<unknown[], unknown>>,
  });
  expect(Exit.isFailure(Effect.runSyncExit(engine.createTopic("missing", [])))).toBe(true);
  expect(
    Exit.isFailure(
      Effect.runSyncExit(
        engine.query(
          new Topic<string, unknown[]>({
            name: "missing",
            params: [],
          }),
        ),
      ),
    ),
  ).toBe(true);
  expect(Exit.isFailure(Effect.runSyncExit(engine.sync("missing", [])))).toBe(true);
});
