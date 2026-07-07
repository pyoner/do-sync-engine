import { beforeEach, describe, expect, test } from "vite-plus/test";
import { SyncEngine } from "../src/engine.js";
import { NodeSqliteStorage } from "./helpers.js";
import type { Mutation, Query, Snapshot } from "../src/index.js";
import type { MutationMetadata, SqlRow } from "./helpers.js";

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
  let engine: SyncEngine;
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
    test("mutate runs matching query and returns metadata + results", async () => {
      engine.subscribe("allUsers");
      const result = await engine.mutate("insertUser", "charlie");

      expect(result.metadata).toHaveProperty("rowsAffected");
      expect(result.results).toHaveLength(1);
      const queryResult = result.results[0];
      expect(queryResult.query).toBe("allUsers");
      expect(Array.isArray(queryResult.result)).toBe(true);
      expect((queryResult.result as SqlRow[]).some((row) => row.name === "charlie")).toBe(true);
    });

    test("mutate skips subscriptions whose tables do not overlap", async () => {
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
      engine.subscribe("postsOnly");
      engine.subscribe("allUsers");

      const result = await engine.mutate("insertUser", "charlie");

      expect(runCount).toBe(0);
      expect(result.results).toHaveLength(1);
      expect(result.results[0].query).toBe("allUsers");
    });

    test("mutate does not run queries that were never subscribed", async () => {
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

      await engine.mutate("insertUser", "charlie");

      expect(runCount).toBe(0);
    });

    test("unsubscribe returns true when removing, false otherwise", async () => {
      const id = engine.subscribe("allUsers");
      expect(engine.unsubscribe(id)).toBe(true);
      expect(engine.unsubscribe(id)).toBe(false);
    });

    test("unsubscribe stops future mutations from producing results", async () => {
      const id = engine.subscribe("allUsers");
      expect(engine.unsubscribe(id)).toBe(true);

      const result = await engine.mutate("insertUser", "charlie");
      expect(result.results).toHaveLength(0);

      // Still no results after second mutate
      const result2 = await engine.mutate("insertUser", "dave");
      expect(result2.results).toHaveLength(0);
    });

    test("parameterized subscriptions pass rest params to query.run and return them in queryResult", async () => {
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
      engine.subscribe("userById", 2);
      const result = await engine.mutate("updateUserName", "bob_updated", 2);

      expect(runParams).toEqual([2]);
      expect(result.results).toHaveLength(1);
      expect(result.results[0].subscriptionId).toBe(1);
      expect(result.results[0].query).toBe("userById");
      expect(result.results[0].params).toEqual([2]);
      const rows = result.results[0].result as SqlRow[];
      expect(rows).toHaveLength(1);
      expect(rows[0].name).toBe("bob_updated");
    });

    test("duplicate subscriptions are independent and produce duplicate results", async () => {
      const firstId = engine.subscribe("allUsers");
      const secondId = engine.subscribe("allUsers");
      expect(firstId).not.toBe(secondId);

      const result = await engine.mutate("insertUser", "charlie");
      expect(result.results).toHaveLength(2);
      expect(result.results[0].subscriptionId).toBe(firstId);
      expect(result.results[1].subscriptionId).toBe(secondId);

      engine.unsubscribe(firstId);
      const result2 = await engine.mutate("insertUser", "dave");
      expect(result2.results).toHaveLength(1);
      expect(result2.results[0].subscriptionId).toBe(secondId);
    });

    test("mutate awaits async mutation and query before resolving", async () => {
      const createDeferred = () => {
        let resolve!: () => void;
        const promise = new Promise<void>((res) => {
          resolve = res;
        });
        return { promise, resolve };
      };

      const mutatorGate = createDeferred();
      const queryGate = createDeferred();
      let mutateResolved = false;

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
      engine = new SyncEngine({
        queries: { gated: gatedQuery, allUsers },
        mutations: { gatedMutator: gatedMutation },
      });

      engine.subscribe("gated");
      const resultPromise = engine.mutate("gatedMutator", "charlie");
      const mutatePromise = resultPromise.then(() => {
        mutateResolved = true;
      });

      await Promise.resolve();
      expect(mutateResolved).toBe(false);

      mutatorGate.resolve();
      await Promise.resolve();
      expect(mutateResolved).toBe(false);

      queryGate.resolve();
      await mutatePromise;

      expect(mutateResolved).toBe(true);
      const result = await resultPromise;
      expect(result.results).toHaveLength(1);
      const rows = result.results[0].result as SqlRow[];
      expect(rows).toHaveLength(3);
    });

    test("mutate rejects when a query fails", async () => {
      engine.subscribe("failingUsers");
      await expect(engine.mutate("insertUser", "charlie")).rejects.toThrow("query failed");
    });

    test("subscribed query without delivery callback returns a queryResult", async () => {
      engine.subscribe("allUsers");
      const result = await engine.mutate("insertUser", "charlie");
      expect(result.results).toHaveLength(1);
      expect(result.results[0].query).toBe("allUsers");
      const rows = result.results[0].result as SqlRow[];
      expect(rows.some((row) => row.name === "charlie")).toBe(true);
    });

    test("subscribe rejects unknown query key", () => {
      expect(() => engine.subscribe("nonexistent")).toThrow("Unknown query");
    });

    test("mutate rejects unknown mutation key", async () => {
      await expect(engine.mutate("nonexistent")).rejects.toThrow("Unknown mutation");
    });
  });

  describe("structuredClone", () => {
    test("snapshot survives structuredClone and restore", () => {
      engine.subscribe("userById", 2);
      const snap = engine.snapshot();
      const cloned = structuredClone(snap) as Snapshot<
        "allUsers" | "userById" | "postsOnly" | "asyncUsers" | "failingUsers"
      >;

      const restored = new SyncEngine({
        queries: { allUsers, userById, postsOnly, asyncUsers, failingUsers },
        mutations: { insertUser, updateUserName },
        snapshot: cloned,
      });

      // Mutating original snapshot does not affect restored
      return restored.mutate("updateUserName", "bob_updated", 2).then((result) => {
        expect(result.results).toHaveLength(1);
        const rows = result.results[0].result as SqlRow[];
        expect(rows).toHaveLength(1);
        expect(rows[0].name).toBe("bob_updated");
      });
    });

    test("subscribe rejects non-cloneable params", () => {
      expect(() => engine.subscribe("userById", (() => 1) as never)).toThrow(
        "Subscription params must support structuredClone",
      );
    });

    test("snapshot is detached", () => {
      engine.subscribe("allUsers");
      const snap = engine.snapshot();
      // Mutate the returned snapshot
      (snap as unknown as { nextSubscriptionId: number }).nextSubscriptionId = 999;
      (snap as unknown as { subscriptions: unknown[] }).subscriptions = [];

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
