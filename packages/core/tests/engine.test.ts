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
  let insertUser: Mutator<[string], MutationMetadata>;
  let updateUserName: Mutator<[string, number], MutationMetadata>;

  beforeEach(() => {
    storage = new NodeSqliteStorage();
    setupDb(storage);
    engine = new SyncEngine();

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
  });

  describe("Broker", () => {
    test("publish runs matching selector callback and resolves void", async () => {
      const callbackResults: SqlRow[][] = [];
      const selector: Selector<[], SqlRow[]> = {
        tables: [...allUsers.tables],
        run: () => allUsers.run(),
      };

      engine.subscribe(selector, [], (result) => {
        callbackResults.push(result);
      });

      const publishResult = await engine.publish(insertUser, "charlie");

      expect(publishResult).toBeUndefined();
      expect(callbackResults).toHaveLength(1);
      expect(callbackResults[0].some((row) => row.name === "charlie")).toBe(true);
    });

    test("publish skips subscriptions whose tables do not overlap", async () => {
      const postsOnlySql = "SELECT * FROM posts ORDER BY id";
      let runCount = 0;
      let callbackCount = 0;
      const postsOnly: Selector<[], SqlRow[]> = {
        tables: readTablesFromSql(postsOnlySql),
        run: () => {
          runCount += 1;
          return storage.query(postsOnlySql);
        },
      };

      engine.subscribe(postsOnly, [], () => {
        callbackCount += 1;
      });
      await engine.publish(insertUser, "charlie");

      expect(runCount).toBe(0);
      expect(callbackCount).toBe(0);
    });

    test("publish does not run selectors that were never subscribed", async () => {
      let runCount = 0;
      const inactiveSelector: Selector<[], SqlRow[]> = {
        tables: ["users"],
        run: () => {
          runCount += 1;
          return storage.query("SELECT * FROM users ORDER BY id");
        },
      };

      void inactiveSelector;

      await engine.publish(insertUser, "charlie");

      expect(runCount).toBe(0);
    });

    test("unsubscribe stops future publications", async () => {
      let callbackCount = 0;
      const selector: Selector<[], SqlRow[]> = {
        tables: [...allUsers.tables],
        run: () => allUsers.run(),
      };

      const subscriptionId = engine.subscribe(selector, [], () => {
        callbackCount += 1;
      });
      expect(subscriptionId).toBeTypeOf("number");
      expect(engine.unsubscribe(subscriptionId)).toBe(true);

      await engine.publish(insertUser, "charlie");
      expect(callbackCount).toBe(0);

      expect(engine.unsubscribe(subscriptionId)).toBe(false);
      await engine.publish(insertUser, "dave");
      expect(callbackCount).toBe(0);
    });

    test("parameterized subscriptions pass tuple params to run and subscribe callback receives result selector and params", async () => {
      const runParams: number[] = [];
      let callbackResult: SqlRow[] = [];
      let callbackSelector: Selector<[number], SqlRow[]> | undefined;
      let callbackParams: readonly [number] | undefined;
      const selector: Selector<[number], SqlRow[]> = {
        tables: [...userById.tables],
        run: (id) => {
          runParams.push(id);
          return storage.query("SELECT * FROM users WHERE id = ?", id);
        },
      };

      engine.subscribe(selector, [2], (result, subscribedSelector, params) => {
        callbackResult = result;
        callbackSelector = subscribedSelector;
        callbackParams = params;
      });
      await engine.publish(updateUserName, "bob_updated", 2);

      expect(runParams).toEqual([2]);
      expect(callbackResult).toHaveLength(1);
      expect(callbackResult[0].name).toBe("bob_updated");
      expect(callbackSelector).toBe(selector);
      expect(callbackParams).toEqual([2]);
    });

    test("duplicate subscriptions are independent", async () => {
      let callbackCount = 0;
      const selector: Selector<[], SqlRow[]> = {
        tables: [...allUsers.tables],
        run: () => allUsers.run(),
      };

      const firstSubscriptionId = engine.subscribe(selector, [], () => {
        callbackCount += 1;
      });
      const secondSubscriptionId = engine.subscribe(selector, [], () => {
        callbackCount += 1;
      });
      expect(firstSubscriptionId).not.toBe(secondSubscriptionId);

      await engine.publish(insertUser, "charlie");
      expect(callbackCount).toBe(2);

      expect(engine.unsubscribe(firstSubscriptionId)).toBe(true);
      await engine.publish(insertUser, "dave");
      expect(callbackCount).toBe(3);
      expect(engine.unsubscribe(secondSubscriptionId)).toBe(true);
    });

    test("publish awaits async mutator, selector, and callback", async () => {
      const createDeferred = () => {
        let resolve!: () => void;
        const promise = new Promise<void>((res) => {
          resolve = res;
        });
        return { promise, resolve };
      };

      const mutatorGate = createDeferred();
      const selectorGate = createDeferred();
      const callbackGate = createDeferred();
      let publishResolved = false;
      let callbackRows: SqlRow[] = [];

      const asyncSelector: Selector<[], SqlRow[]> = {
        tables: ["users"],
        run: async () => {
          await selectorGate.promise;
          return storage.query("SELECT * FROM users ORDER BY id");
        },
      };
      const asyncMutator: Mutator<[string], MutationMetadata> = {
        tables: ["users"],
        run: async (name) => {
          await mutatorGate.promise;
          return storage.execute("INSERT INTO users (name) VALUES (?)", name);
        },
      };

      engine.subscribe(asyncSelector, [], async (result) => {
        await callbackGate.promise;
        callbackRows = result;
      });
      const publishPromise = engine.publish(asyncMutator, "charlie").then(() => {
        publishResolved = true;
      });

      await Promise.resolve();
      expect(publishResolved).toBe(false);

      mutatorGate.resolve();
      await Promise.resolve();
      expect(publishResolved).toBe(false);

      selectorGate.resolve();
      await Promise.resolve();
      expect(publishResolved).toBe(false);

      callbackGate.resolve();
      await publishPromise;

      expect(callbackRows).toHaveLength(3);
      expect(callbackRows[2].name).toBe("charlie");
    });

    test("publish rejects when a selector callback fails", async () => {
      const selector: Selector<[], SqlRow[]> = {
        tables: [...allUsers.tables],
        run: () => allUsers.run(),
      };

      engine.subscribe(selector, [], () => {
        throw new Error("callback failed");
      });

      await expect(engine.publish(insertUser, "charlie")).rejects.toThrow("callback failed");
    });

    test("subscribe without callback still runs matching selector", async () => {
      let runCount = 0;
      const selector: Selector<[], SqlRow[]> = {
        tables: ["users"],
        run: () => {
          runCount += 1;
          return storage.query("SELECT * FROM users ORDER BY id");
        },
      };

      engine.subscribe(selector);
      const publishResult = await engine.publish(insertUser, "charlie");

      expect(publishResult).toBeUndefined();
      expect(runCount).toBe(1);
    });
  });

  test("storage.close works", () => {
    storage.close();
  });
});
