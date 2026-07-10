import { expect, test } from "vite-plus/test";
import { SyncEngine } from "../src/index.js";
import type {
  Mutation,
  Query,
  QueryCallback,
  QueryResult,
  SubscriptionId,
  SyncEngineBase,
} from "../src/index.js";

function captureQueryResult<Name extends string, Result>() {
  const results: QueryResult<Name, Result>[] = [];
  const callback: QueryCallback<Name, Result> = (result) => {
    results.push(result);
  };
  return { callback, results };
}

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

  const callback: QueryCallback<"numbers", number> = () => {};
  const subscriptionId: SubscriptionId = engine.subscribe("numbers", [], callback);

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
  const engine: SyncEngineBase<typeof queries, typeof mutations> = new SyncEngine({
    queries,
    mutations,
  });

  const captured = captureQueryResult<"numbers", number[]>();
  const subscriptionId: SubscriptionId = engine.subscribe("numbers", [], captured.callback);
  const affectedTables = await engine.mutate("noop", []);
  expect(subscriptionId).toBeTypeOf("number");
  expect(affectedTables).toEqual(["numbers"]);

  const syncPromise: Promise<void> = engine.sync("noop", []);
  await syncPromise;
  expect(captured.results).toEqual([
    {
      subscriptionId,
      query: "numbers",
      params: [],
      result: [1, 2, 3],
    },
  ]);

  if (false as boolean) {
    // @ts-expect-error — "missing" is not a known query on the interface type
    void engine.subscribe("missing", [], captured.callback);
    // @ts-expect-error — query params must be an empty tuple
    void engine.subscribe("numbers", [1], captured.callback);
    const wrongCallback: QueryCallback<"numbers", string> = ({ result }) => {
      result.toUpperCase();
    };
    // @ts-expect-error — callback result must match the query result
    void engine.subscribe("numbers", [], wrongCallback);
    // @ts-expect-error — callback belongs in the third position
    void engine.subscribe("numbers", captured.callback, []);
    // @ts-expect-error — mutation expects no params
    void engine.mutate("noop", [1]);
    // @ts-expect-error — sync expects no params
    void engine.sync("noop", [1]);
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

  const callback: QueryCallback<"numbers", number> = () => {};
  const wrongCallback: QueryCallback<"numbers", string> = ({ result }) => {
    result.toUpperCase();
  };

  // Non-runnable block: type-check only, ensures generics aren't widened to any
  if (false as boolean) {
    // @ts-expect-error — "missing" is not a known query
    void engine.subscribe("missing", [42], callback);
    // @ts-expect-error — callback result must match the query result
    void engine.subscribe("numbers", [42], wrongCallback);
    // @ts-expect-error — query param must be a number
    void engine.subscribe("numbers", ["42"], callback);
    // @ts-expect-error — callback belongs in the third position
    void engine.subscribe("numbers", callback, [42]);
    // @ts-expect-error — mutate expects no params but 1 is given
    void engine.mutate("noop", [1]);
    // @ts-expect-error — sync expects no params but 1 is given
    void engine.sync("noop", [1]);
  }

  // Confirm the valid calls still work
  engine.subscribe("numbers", [42], callback);
  engine.unsubscribe(engine.subscribe("numbers", [42], callback));
  void engine.mutate("noop", []);
  void engine.sync("noop", []);
});
