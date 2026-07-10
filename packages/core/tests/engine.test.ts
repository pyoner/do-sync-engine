import { beforeEach, describe, expect, test } from "vite-plus/test";
import { SyncEngine } from "../src/engine.js";
import { NodeSqliteStorage } from "./helpers.js";
import type { Mutation, Query, QueryCallback, QueryResult, Snapshot } from "../src/index.js";
import type { MutationMetadata, SqlRow } from "./helpers.js";

function captureQueryResult<Name extends string, Result>() {
  const results: QueryResult<Name, Result>[] = [];
  const callback: QueryCallback<Name, unknown> = (result) => {
    results.push(result as QueryResult<Name, Result>);
  };
  return { callback, results };
}

const noopCallback: QueryCallback = () => {};

function asFixtureCallback<Name extends string, Result>(
  callback: QueryCallback<Name, Result>,
): QueryCallback<Name, unknown> {
  return callback as QueryCallback<Name, unknown>;
}

function setupDb(storage: NodeSqliteStorage) {
  storage.exec("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)");
  storage.exec("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT)");
  storage.exec(`INSERT INTO users (name) VALUES ('alice')`);
  storage.exec(`INSERT INTO users (name) VALUES ('bob')`);
  storage.exec(`INSERT INTO posts (user_id, title) VALUES (1, 'hello')`);
}

function readTablesFromSql(sql: string) {
  const lower = sql.toLowerCase();
  return [
    ...(/\b(from|join)\s+users\b/.test(lower) ? ["users"] : []),
    ...(/\b(from|join)\s+posts\b/.test(lower) ? ["posts"] : []),
  ];
}

function writeTablesFromSql(sql: string) {
  const lower = sql.toLowerCase();
  if (/^\s*(insert\s+into|update|delete\s+from)\s+users\b/.test(lower)) return ["users"];
  if (/^\s*(insert\s+into|update|delete\s+from)\s+posts\b/.test(lower)) return ["posts"];
  return [];
}

describe("SyncEngine", () => {
  let storage: NodeSqliteStorage;
  let engine: SyncEngine<any, any>;
  let allUsers: Query<[], SqlRow[]>;
  let userById: Query<[number], SqlRow[]>;
  let postsOnly: Query<[], SqlRow[]>;
  let asyncUsers: Query<[], SqlRow[]>;
  let failingUsers: Query<[], SqlRow[]>;
  let insertUser: Mutation<[string], MutationMetadata>;
  let updateUserName: Mutation<[string, number], MutationMetadata>;

  beforeEach(() => {
    storage = new NodeSqliteStorage();
    setupDb(storage);

    const allUsersSql = "SELECT * FROM users ORDER BY id";
    allUsers = {
      tables: readTablesFromSql(allUsersSql),
      run: () => storage.query(allUsersSql),
    };

    const userByIdSql = "SELECT * FROM users WHERE id = ?";
    userById = {
      tables: readTablesFromSql(userByIdSql),
      run: (id) => storage.query(userByIdSql, id),
    };

    const postsOnlySql = "SELECT * FROM posts ORDER BY id";
    postsOnly = {
      tables: readTablesFromSql(postsOnlySql),
      run: () => storage.query(postsOnlySql),
    };

    const asyncUsersSql = "SELECT * FROM users ORDER BY id";
    asyncUsers = {
      tables: readTablesFromSql(asyncUsersSql),
      run: async () => {
        return storage.query(asyncUsersSql);
      },
    };

    failingUsers = {
      tables: ["users"],
      run: () => {
        throw new Error("query failed");
      },
    };

    const insertUserSql = "INSERT INTO users (name) VALUES (?)";
    insertUser = {
      tables: writeTablesFromSql(insertUserSql),
      run: (name) => storage.execute(insertUserSql, name),
    };

    const updateUserNameSql = "UPDATE users SET name = ? WHERE id = ?";
    updateUserName = {
      tables: writeTablesFromSql(updateUserNameSql),
      run: (name, id) => storage.execute(updateUserNameSql, name, id),
    };

    engine = new SyncEngine({
      queries: { allUsers, userById, postsOnly, asyncUsers, failingUsers },
      mutations: { insertUser, updateUserName },
    });
  });

  describe("queries and mutations", () => {
    test("mutate runs only the mutation and returns affected tables", async () => {
      let queryRunCount = 0;
      const trackingQuery: Query<[], SqlRow[]> = {
        tables: ["users"],
        run: () => {
          queryRunCount += 1;
          throw new Error("query should not run");
        },
      };
      engine = new SyncEngine({
        queries: { users: trackingQuery, allUsers },
        mutations: { insertUser },
      });
      engine.subscribe("users", [], noopCallback);

      const result = await engine.mutate("insertUser", ["charlie"]);
      expect(result).toEqual(["users"]);

      const rows = storage.query("SELECT * FROM users ORDER BY id");
      expect(rows.some((row: SqlRow) => row.name === "charlie")).toBe(true);

      expect(queryRunCount).toBe(0);
    });

    test("sync runs matching query and delivers callback payload", async () => {
      const captured = captureQueryResult<"allUsers", SqlRow[]>();
      const subscriptionId = engine.subscribe("allUsers", [], asFixtureCallback(captured.callback));
      await engine.sync("insertUser", ["charlie"]);

      expect(captured.results).toHaveLength(1);
      const queryResult = captured.results[0];
      expect(queryResult.subscriptionId).toBe(subscriptionId);
      expect(queryResult.query).toBe("allUsers");
      expect(queryResult.params).toEqual([]);
      expect(queryResult.result.some((row) => row.name === "charlie")).toBe(true);
    });

    test("sync skips subscriptions whose tables do not overlap", async () => {
      let runCount = 0;
      const countingQuery: Query<[], SqlRow[]> = {
        tables: [...postsOnly.tables],
        run: () => {
          runCount += 1;
          return storage.query("SELECT * FROM posts ORDER BY id");
        },
      };
      engine = new SyncEngine({
        queries: { postsOnly: countingQuery, allUsers },
        mutations: { insertUser },
      });
      const postsResults = captureQueryResult<"postsOnly", SqlRow[]>();
      const usersResults = captureQueryResult<"allUsers", SqlRow[]>();
      engine.subscribe("postsOnly", [], asFixtureCallback(postsResults.callback));
      engine.subscribe("allUsers", [], asFixtureCallback(usersResults.callback));

      await engine.sync("insertUser", ["charlie"]);

      expect(runCount).toBe(0);
      expect(postsResults.results).toEqual([]);
      expect(usersResults.results).toHaveLength(1);
      expect(usersResults.results[0].query).toBe("allUsers");
    });

    test("sync does not run queries that were never subscribed", async () => {
      let runCount = 0;
      const trackingQuery: Query<[], SqlRow[]> = {
        tables: ["users"],
        run: () => {
          runCount += 1;
          return storage.query("SELECT * FROM users ORDER BY id");
        },
      };
      engine = new SyncEngine({
        queries: { users: trackingQuery, allUsers },
        mutations: { insertUser },
      });

      await engine.sync("insertUser", ["charlie"]);

      expect(runCount).toBe(0);
    });

    test("unsubscribe returns true when removing, false otherwise", async () => {
      const id = engine.subscribe("allUsers", [], noopCallback);
      expect(engine.unsubscribe(id)).toBe(true);
      expect(engine.unsubscribe(id)).toBe(false);
    });

    test("unsubscribe stops future sync from producing results", async () => {
      const captured = captureQueryResult<"allUsers", SqlRow[]>();
      const id = engine.subscribe("allUsers", [], asFixtureCallback(captured.callback));
      expect(engine.unsubscribe(id)).toBe(true);

      await engine.sync("insertUser", ["charlie"]);
      await engine.sync("insertUser", ["dave"]);

      expect(captured.results).toEqual([]);
    });

    test("parameterized subscriptions pass explicit params to query.run and callback payload", async () => {
      const runParams: number[] = [];
      const trackingQuery: Query<[number], SqlRow[]> = {
        tables: [...userById.tables],
        run: (id) => {
          runParams.push(id);
          return storage.query("SELECT * FROM users WHERE id = ?", id);
        },
      };
      engine = new SyncEngine({
        queries: { userById: trackingQuery },
        mutations: { updateUserName },
      });
      const captured = captureQueryResult<"userById", SqlRow[]>();
      const subscriptionId = engine.subscribe(
        "userById",
        [2],
        asFixtureCallback(captured.callback),
      );
      await engine.sync("updateUserName", ["bob_updated", 2]);

      expect(runParams).toEqual([2]);
      expect(captured.results).toHaveLength(1);
      const queryResult = captured.results[0];
      expect(queryResult.subscriptionId).toBe(subscriptionId);
      expect(queryResult.query).toBe("userById");
      expect(queryResult.params).toEqual([2]);
      expect(queryResult.result).toHaveLength(1);
      expect(queryResult.result[0].name).toBe("bob_updated");
    });

    test("duplicate subscriptions are independent and produce duplicate callbacks", async () => {
      const captured = captureQueryResult<"allUsers", SqlRow[]>();
      const firstId = engine.subscribe("allUsers", [], asFixtureCallback(captured.callback));
      const secondId = engine.subscribe("allUsers", [], asFixtureCallback(captured.callback));
      expect(firstId).not.toBe(secondId);

      await engine.sync("insertUser", ["charlie"]);
      expect(captured.results).toHaveLength(2);
      expect(captured.results[0].subscriptionId).toBe(firstId);
      expect(captured.results[1].subscriptionId).toBe(secondId);

      captured.results.length = 0;
      engine.unsubscribe(firstId);
      await engine.sync("insertUser", ["dave"]);
      expect(captured.results).toHaveLength(1);
      expect(captured.results[0].subscriptionId).toBe(secondId);
    });

    test("sync awaits async mutation, query, and callback before resolving", async () => {
      const createDeferred = () => {
        let resolve!: () => void;
        const promise = new Promise<void>((res) => {
          resolve = res;
        });
        return { promise, resolve };
      };

      const mutatorGate = createDeferred();
      const queryGate = createDeferred();
      const callbackGate = createDeferred();
      let callbackStartedResolve!: () => void;
      const callbackStarted = new Promise<void>((resolve) => {
        callbackStartedResolve = resolve;
      });
      let syncResolved = false;
      let callbackFinished = false;
      let callbackPayload: QueryResult<"gated", SqlRow[]> | undefined;

      const gatedMutation: Mutation<[string], MutationMetadata> = {
        tables: ["users"],
        run: async (name) => {
          await mutatorGate.promise;
          return storage.execute("INSERT INTO users (name) VALUES (?)", name);
        },
      };
      const gatedQuery: Query<[], SqlRow[]> = {
        tables: ["users"],
        run: async () => {
          await queryGate.promise;
          return storage.query("SELECT * FROM users ORDER BY id");
        },
      };
      const callback: QueryCallback<"gated", SqlRow[]> = async (result) => {
        callbackStartedResolve();
        await callbackGate.promise;
        callbackPayload = result;
        callbackFinished = true;
      };
      engine = new SyncEngine({
        queries: { gated: gatedQuery, allUsers },
        mutations: { gatedMutator: gatedMutation },
      });

      engine.subscribe("gated", [], asFixtureCallback(callback));
      const resultPromise: Promise<void> = engine.sync("gatedMutator", ["charlie"]);
      void resultPromise.then(() => {
        syncResolved = true;
      });

      await Promise.resolve();
      expect(syncResolved).toBe(false);

      mutatorGate.resolve();
      await Promise.resolve();
      expect(syncResolved).toBe(false);

      queryGate.resolve();
      await callbackStarted;
      expect(callbackFinished).toBe(false);
      expect(syncResolved).toBe(false);

      callbackGate.resolve();
      await resultPromise;

      expect(syncResolved).toBe(true);
      expect(callbackFinished).toBe(true);
      expect(callbackPayload?.query).toBe("gated");
      expect(callbackPayload?.params).toEqual([]);
      expect(callbackPayload?.result).toHaveLength(3);
    });

    test("sync rejects when a query fails", async () => {
      engine.subscribe("failingUsers", [], noopCallback);
      await expect(engine.sync("insertUser", ["charlie"])).rejects.toThrow("query failed");
    });

    test("subscribe rejects unknown query key", () => {
      expect(() => engine.subscribe("nonexistent", [], noopCallback)).toThrow("Unknown query");
    });

    test("mutate rejects unknown mutation key", async () => {
      await expect(engine.mutate("nonexistent", [])).rejects.toThrow("Unknown mutation");
    });

    test("sync omits a callback when the subscription is removed while its query runs", async () => {
      let queryStartedResolve!: () => void;
      let queryGateResolve!: () => void;
      const queryStarted = new Promise<void>((resolve) => {
        queryStartedResolve = resolve;
      });
      const queryGate = new Promise<void>((resolve) => {
        queryGateResolve = resolve;
      });

      const gatedQuery: Query<[], SqlRow[]> = {
        tables: ["users"],
        run: async () => {
          queryStartedResolve();
          await queryGate;
          return storage.query("SELECT * FROM users ORDER BY id");
        },
      };

      engine = new SyncEngine({
        queries: { gated: gatedQuery, allUsers },
        mutations: { insertUser },
      });

      const captured = captureQueryResult<"gated", SqlRow[]>();
      const subscriptionId = engine.subscribe("gated", [], asFixtureCallback(captured.callback));
      const resultPromise: Promise<void> = engine.sync("insertUser", ["charlie"]);

      await queryStarted;
      expect(engine.unsubscribe(subscriptionId)).toBe(true);

      queryGateResolve();
      await resultPromise;
      expect(captured.results).toEqual([]);
    });
  });

  describe("structuredClone", () => {
    test("snapshot restore has no callbacks until a fresh subscription", async () => {
      const original = captureQueryResult<"userById", SqlRow[]>();
      engine.subscribe("userById", [2], asFixtureCallback(original.callback));
      const snap = engine.snapshot();
      const cloned = structuredClone(snap) as Snapshot<
        "allUsers" | "userById" | "postsOnly" | "asyncUsers" | "failingUsers"
      >;

      expect(cloned.subscriptions).toEqual([{ id: 1, query: "userById", params: [2] }]);

      let restoredQueryRuns = 0;
      const trackedUserById: Query<[number], SqlRow[]> = {
        ...userById,
        run: (id) => {
          restoredQueryRuns += 1;
          return userById.run(id);
        },
      };
      const restored = new SyncEngine({
        queries: {
          allUsers,
          userById: trackedUserById,
          postsOnly,
          asyncUsers,
          failingUsers,
        },
        mutations: { insertUser, updateUserName },
        snapshot: cloned,
      });

      await restored.sync("updateUserName", ["bob_updated", 2]);
      expect(restoredQueryRuns).toBe(0);
      expect(original.results).toEqual([]);

      const restoredResults = captureQueryResult<"userById", SqlRow[]>();
      const subscriptionId = restored.subscribe("userById", [2], restoredResults.callback);
      await restored.sync("updateUserName", ["bob_updated_again", 2]);

      expect(restoredQueryRuns).toBe(1);
      expect(restoredResults.results).toHaveLength(1);
      expect(restoredResults.results[0].subscriptionId).toBe(subscriptionId);
      expect(restoredResults.results[0].result[0].name).toBe("bob_updated_again");
    });

    test("subscribe rejects non-cloneable params", () => {
      expect(() => engine.subscribe("userById", [(() => 1) as never], noopCallback)).toThrow(
        "Subscription params must support structuredClone",
      );
    });

    test("snapshot is detached", () => {
      engine.subscribe("allUsers", [], noopCallback);
      const snap = engine.snapshot();
      const mutableSnapshot = snap as unknown as {
        nextSubscriptionId: number;
        subscriptions: unknown[];
      };
      mutableSnapshot.nextSubscriptionId = 999;
      mutableSnapshot.subscriptions = [];

      // Engine state should be unchanged
      const snap2 = engine.snapshot();
      expect(snap2.nextSubscriptionId).toBe(2); // one subscribe used id 1, so next is 2
      expect(snap2.subscriptions).toHaveLength(1);
    });

    test("constructor rejects snapshot with non-positive nextSubscriptionId", () => {
      expect(
        () =>
          new SyncEngine({
            queries: { allUsers },
            mutations: { insertUser },
            snapshot: { nextSubscriptionId: 0, subscriptions: [] },
          }),
      ).toThrow("Snapshot nextSubscriptionId must be a positive integer");
    });

    test("constructor rejects snapshot with non-array subscriptions", () => {
      expect(
        () =>
          new SyncEngine({
            queries: { allUsers },
            mutations: { insertUser },
            snapshot: { nextSubscriptionId: 1, subscriptions: null as never },
          }),
      ).toThrow("Snapshot subscriptions must be an array");
    });

    test("constructor rejects snapshot with duplicate subscription ids", () => {
      expect(
        () =>
          new SyncEngine({
            queries: { allUsers },
            mutations: { insertUser },
            snapshot: {
              nextSubscriptionId: 3,
              subscriptions: [
                { id: 1, query: "allUsers", params: [] },
                { id: 1, query: "allUsers", params: [] },
              ],
            },
          }),
      ).toThrow("Duplicate subscription id: 1");
    });

    test("constructor rejects snapshot with unknown query", () => {
      expect(
        () =>
          new SyncEngine({
            queries: { allUsers },
            mutations: { insertUser },
            snapshot: {
              nextSubscriptionId: 2,
              subscriptions: [{ id: 1, query: "unknown", params: [] }],
            } as unknown as Snapshot<"allUsers">,
          }),
      ).toThrow("Unknown query: unknown");
    });

    test("constructor throws TypeError for non-cloneable snapshot", () => {
      const fn = () => {};
      expect(
        () =>
          new SyncEngine({
            queries: { allUsers },
            mutations: { insertUser },
            snapshot: {
              nextSubscriptionId: 1,
              subscriptions: [{ id: 1, query: "allUsers", params: [] }],
            },
          }),
      ).not.toThrow(); // This one should work
      // A snapshot containing a function should fail
      expect(
        () =>
          new SyncEngine({
            queries: { allUsers },
            mutations: { insertUser },
            snapshot: { nextSubscriptionId: fn as never, subscriptions: [] },
          }),
      ).toThrow("Snapshot must support structuredClone");
    });
  });

  test("storage.close works", () => {
    storage.close();
  });
});
