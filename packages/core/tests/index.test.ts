import { expect, test } from "vite-plus/test";
import { SyncEngine } from "../src/index.js";
import type { Mutation, Query, SubscriptionId, SyncEngineInterface } from "../src/index.js";

test("exports typed SyncEngine API", () => {
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

  const subscriptionId: SubscriptionId = engine.subscribe("numbers");

  expect(subscriptionId).toBeTypeOf("number");

  expect(Object.getOwnPropertyNames(SyncEngine.prototype).sort()).toEqual([
    "constructor",
    "mutate",
    "publish",
    "snapshot",
    "subscribe",
    "sync",
    "unsubscribe",
  ]);

  expect(engine.unsubscribe(subscriptionId)).toBe(true);
  expect(engine.unsubscribe(subscriptionId)).toBe(false);
});

test("typed query/mutation params and results", async () => {
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
  const engine: SyncEngineInterface<typeof queries, typeof mutations> = new SyncEngine({
    queries,
    mutations,
  });

  const subscriptionId: SubscriptionId = engine.subscribe("numbers");
  const affectedTables = await engine.mutate("noop");
  expect(subscriptionId).toBeTypeOf("number");
  expect(affectedTables).toEqual(["numbers"]);

  const syncResult = await engine.sync("noop");
  expect(syncResult.affectedTables).toEqual(["numbers"]);
  expect(syncResult.results[0].result).toEqual([1, 2, 3]);

  if (false as boolean) {
    // @ts-expect-error — "missing" is not a known query on the interface type
    void engine.subscribe("missing");
    // @ts-expect-error — query expects no params
    void engine.subscribe("numbers", 1);
    // @ts-expect-error — mutation expects no params
    void engine.mutate("noop", 1);
    // @ts-expect-error — sync expects no params
    void engine.sync("noop", 1);
  }
});

test("type errors for wrong query name or wrong param types", () => {
  const engine = new SyncEngine({
    queries: {
      numbers: {
        tables: ["numbers"],
        run: (x: number) => x,
      } satisfies Query<[number], number>,
    },
    mutations: {
      noop: {
        tables: [],
        run: () => ({}),
      } satisfies Mutation<[], Record<string, never>>,
    },
  });

  // Non-runnable block: type-check only, ensures generics aren't widened to any
  if (false as boolean) {
    // @ts-expect-error — "missing" is not a known query
    void engine.subscribe("missing");
    // @ts-expect-error — mutate expects no params but 1 is given
    void engine.mutate("noop", 1);
    // @ts-expect-error — sync expects no params but 1 is given
    void engine.sync("noop", 1);
  }

  // Confirm the valid calls still work
  engine.subscribe("numbers", 42);
  engine.unsubscribe(engine.subscribe("numbers", 42));
  void engine.mutate("noop");
  void engine.sync("noop");
});
