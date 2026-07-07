import { beforeEach, describe, expect, test } from "vite-plus/test";
import { SyncEngine } from "../src/engine.js";
import { NodeSqliteStorage } from "./helpers.js";
import type { Mutator, Selector } from "../src/index.js";
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
  let allUsers: Selector<[], SqlRow[]>;
  let userById: Selector<[number], SqlRow[]>;
  let postsOnly: Selector<[], SqlRow[]>;
  let asyncUsers: Selector<[], SqlRow[]>;
  let failingUsers: Selector<[], SqlRow[]>;
  let insertUser: Mutator<[string], MutationMetadata>;
  let updateUserName: Mutator<[string, number], MutationMetadata>;

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
        throw new Error("selector failed");
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
      selectors: { allUsers, userById, postsOnly, asyncUsers, failingUsers },
      mutators: { insertUser, updateUserName },
    });
  });

  describe("Broker", () => {
    test("publish runs matching selector and returns metadata + selections", async () => {
      engine.subscribe("allUsers");
      const result = await engine.publish("insertUser", "charlie");

      expect(result.metadata).toHaveProperty("rowsAffected");
      expect(result.selections).toHaveLength(1);
      const selection = result.selections[0];
      expect(selection.selector).toBe("allUsers");
      expect(Array.isArray(selection.result)).toBe(true);
      expect((selection.result as SqlRow[]).some((row) => row.name === "charlie")).toBe(true);
    });

    test("publish skips subscriptions whose tables do not overlap", async () => {
      let runCount = 0;
      const countingSelector: Selector<[], SqlRow[]> = {
        tables: [...postsOnly.tables],
        run: () => {
          runCount += 1;
          return storage.query("SELECT * FROM posts ORDER BY id");
        },
      };
      engine = new SyncEngine({
        selectors: { postsOnly: countingSelector, allUsers },
        mutators: { insertUser },
      });
      engine.subscribe("postsOnly");
      engine.subscribe("allUsers");

      const result = await engine.publish("insertUser", "charlie");

      expect(runCount).toBe(0);
      expect(result.selections).toHaveLength(1);
      expect(result.selections[0].selector).toBe("allUsers");
    });

    test("publish does not run selectors that were never subscribed", async () => {
      let runCount = 0;
      const trackingSelector: Selector<[], SqlRow[]> = {
        tables: ["users"],
        run: () => {
          runCount += 1;
          return storage.query("SELECT * FROM users ORDER BY id");
        },
      };
      engine = new SyncEngine({
        selectors: { users: trackingSelector, allUsers },
        mutators: { insertUser },
      });

      await engine.publish("insertUser", "charlie");

      expect(runCount).toBe(0);
    });

    test("unsubscribe returns true when removing, false otherwise", async () => {
      const id = engine.subscribe("allUsers");
      expect(engine.unsubscribe(id)).toBe(true);
      expect(engine.unsubscribe(id)).toBe(false);
    });

    test("unsubscribe stops future publications from producing selections", async () => {
      const id = engine.subscribe("allUsers");
      expect(engine.unsubscribe(id)).toBe(true);

      const result = await engine.publish("insertUser", "charlie");
      expect(result.selections).toHaveLength(0);

      // Still no selections after second publish
      const result2 = await engine.publish("insertUser", "dave");
      expect(result2.selections).toHaveLength(0);
    });

    test("parameterized subscriptions pass tuple params to selector.run and return them in selection", async () => {
      const runParams: number[] = [];
      const trackingSelector: Selector<[number], SqlRow[]> = {
        tables: [...userById.tables],
        run: (id) => {
          runParams.push(id);
          return storage.query("SELECT * FROM users WHERE id = ?", id);
        },
      };
      engine = new SyncEngine({
        selectors: { userById: trackingSelector },
        mutators: { updateUserName },
      });
      engine.subscribe("userById", [2]);
      const result = await engine.publish("updateUserName", "bob_updated", 2);

      expect(runParams).toEqual([2]);
      expect(result.selections).toHaveLength(1);
      expect(result.selections[0].subscriptionId).toBe(1);
      expect(result.selections[0].selector).toBe("userById");
      expect(result.selections[0].params).toEqual([2]);
      const rows = result.selections[0].result as SqlRow[];
      expect(rows).toHaveLength(1);
      expect(rows[0].name).toBe("bob_updated");
    });

    test("duplicate subscriptions are independent and produce duplicate selections", async () => {
      const firstId = engine.subscribe("allUsers");
      const secondId = engine.subscribe("allUsers");
      expect(firstId).not.toBe(secondId);

      const result = await engine.publish("insertUser", "charlie");
      expect(result.selections).toHaveLength(2);
      expect(result.selections[0].subscriptionId).toBe(firstId);
      expect(result.selections[1].subscriptionId).toBe(secondId);

      engine.unsubscribe(firstId);
      const result2 = await engine.publish("insertUser", "dave");
      expect(result2.selections).toHaveLength(1);
      expect(result2.selections[0].subscriptionId).toBe(secondId);
    });

    test("publish awaits async mutator and selector before resolving", async () => {
      const createDeferred = () => {
        let resolve!: () => void;
        const promise = new Promise<void>((res) => {
          resolve = res;
        });
        return { promise, resolve };
      };

      const mutatorGate = createDeferred();
      const selectorGate = createDeferred();
      let publishResolved = false;

      const gatedMutator: Mutator<[string], MutationMetadata> = {
        tables: ["users"],
        run: async (name) => {
          await mutatorGate.promise;
          return storage.execute("INSERT INTO users (name) VALUES (?)", name);
        },
      };
      const gatedSelector: Selector<[], SqlRow[]> = {
        tables: ["users"],
        run: async () => {
          await selectorGate.promise;
          return storage.query("SELECT * FROM users ORDER BY id");
        },
      };
      engine = new SyncEngine({
        selectors: { gated: gatedSelector, allUsers },
        mutators: { gatedMutator },
      });

      engine.subscribe("gated");
      const resultPromise = engine.publish("gatedMutator", "charlie");
      const publishPromise = resultPromise.then(() => {
        publishResolved = true;
      });

      await Promise.resolve();
      expect(publishResolved).toBe(false);

      mutatorGate.resolve();
      await Promise.resolve();
      expect(publishResolved).toBe(false);

      selectorGate.resolve();
      await publishPromise;

      expect(publishResolved).toBe(true);
      const result = await resultPromise;
      expect(result.selections).toHaveLength(1);
      const rows = result.selections[0].result as SqlRow[];
      expect(rows).toHaveLength(3);
    });

    test("publish rejects when a selector fails", async () => {
      engine.subscribe("failingUsers");
      await expect(engine.publish("insertUser", "charlie")).rejects.toThrow("selector failed");
    });

    test("subscribed selector without delivery callback returns a selection", async () => {
      engine.subscribe("allUsers");
      const result = await engine.publish("insertUser", "charlie");
      expect(result.selections).toHaveLength(1);
      expect(result.selections[0].selector).toBe("allUsers");
      const rows = result.selections[0].result as SqlRow[];
      expect(rows.some((row) => row.name === "charlie")).toBe(true);
    });

    test("subscribe rejects unknown selector key", () => {
      expect(() => engine.subscribe("nonexistent")).toThrow("Unknown selector");
    });

    test("publish rejects unknown mutator key", async () => {
      await expect(engine.publish("nonexistent")).rejects.toThrow("Unknown mutator");
    });
  });

  describe("structuredClone", () => {
    test("snapshot survives structuredClone and restore", () => {
      engine.subscribe("userById", [2]);
      const snap = engine.snapshot();
      const cloned = structuredClone(snap);

      const restored = new SyncEngine({
        selectors: { allUsers, userById, postsOnly, asyncUsers, failingUsers },
        mutators: { insertUser, updateUserName },
        snapshot: cloned,
      });

      // Mutating original snapshot does not affect restored
      return restored.publish("updateUserName", "bob_updated", 2).then((result) => {
        expect(result.selections).toHaveLength(1);
        const rows = result.selections[0].result as SqlRow[];
        expect(rows).toHaveLength(1);
        expect(rows[0].name).toBe("bob_updated");
      });
    });

    test("subscribe rejects non-cloneable params", () => {
      expect(() => engine.subscribe("userById", [() => 1] as never)).toThrow(
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
            selectors: { allUsers },
            mutators: { insertUser },
            snapshot: { nextSubscriptionId: 0, subscriptions: [] },
          }),
      ).toThrow("Broker snapshot nextSubscriptionId must be a positive integer");
    });

    test("constructor rejects snapshot with non-array subscriptions", () => {
      expect(
        () =>
          new SyncEngine({
            selectors: { allUsers },
            mutators: { insertUser },
            snapshot: { nextSubscriptionId: 1, subscriptions: null as never },
          }),
      ).toThrow("Broker snapshot subscriptions must be an array");
    });

    test("constructor rejects snapshot with duplicate subscription ids", () => {
      expect(
        () =>
          new SyncEngine({
            selectors: { allUsers },
            mutators: { insertUser },
            snapshot: {
              nextSubscriptionId: 3,
              subscriptions: [
                { id: 1, selector: "allUsers", params: [] },
                { id: 1, selector: "allUsers", params: [] },
              ],
            },
          }),
      ).toThrow("Duplicate subscription id: 1");
    });

    test("constructor rejects snapshot with unknown selector", () => {
      expect(
        () =>
          new SyncEngine({
            selectors: { allUsers },
            mutators: { insertUser },
            snapshot: {
              nextSubscriptionId: 2,
              subscriptions: [{ id: 1, selector: "unknown", params: [] }],
            },
          }),
      ).toThrow("Unknown selector: unknown");
    });

    test("constructor throws TypeError for non-cloneable snapshot", () => {
      const fn = () => {};
      expect(
        () =>
          new SyncEngine({
            selectors: { allUsers },
            mutators: { insertUser },
            snapshot: {
              nextSubscriptionId: 1,
              subscriptions: [{ id: 1, selector: "allUsers", params: [] }],
            },
          }),
      ).not.toThrow(); // This one should work
      // A snapshot containing a function should fail
      expect(
        () =>
          new SyncEngine({
            selectors: { allUsers },
            mutators: { insertUser },
            snapshot: { nextSubscriptionId: fn as never, subscriptions: [] },
          }),
      ).toThrow("Broker snapshot must support structuredClone");
    });
  });

  test("storage.close works", () => {
    storage.close();
  });
});
