import { beforeEach, describe, expect, test } from "vite-plus/test";
import { SyncEngine, toTables } from "../src/index.js";
import type { Listener, ListenerEvent, Mutation, Query } from "../src/index.js";
import { NodeSqliteStorage } from "./helpers.js";
import { readTablesFromSql, writeTablesFromSql } from "@do-sync-engine/utils";
import type { MutationMetadata, SqlRow } from "@do-sync-engine/utils";

function captureEvents() {
  const events: ListenerEvent[] = [];
  const listener: Listener = (event) => {
    events.push(event);
  };
  return { events, listener };
}

const noopPublish: Listener = () => {};

class ExposedEngine extends SyncEngine<any, any> {
  exposePublish(event: ListenerEvent) {
    return this.publish(event);
  }

  exposeQuery(name: string, params: unknown[]) {
    return this.query(name, params);
  }
}

function setupDb(storage: NodeSqliteStorage) {
  storage.exec("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)");
  storage.exec("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT)");
  storage.exec(`INSERT INTO users (name) VALUES ('alice')`);
  storage.exec(`INSERT INTO users (name) VALUES ('bob')`);
  storage.exec(`INSERT INTO posts (user_id, title) VALUES (1, 'hello')`);
}

describe("SyncEngine topics and events", () => {
  let storage: NodeSqliteStorage;
  let allUsers: Query<[], SqlRow[]>;
  let userById: Query<[number], SqlRow[]>;
  let postsOnly: Query<[], SqlRow[]>;
  let insertUser: Mutation<[string], MutationMetadata>;
  let updateUserName: Mutation<[string, number], MutationMetadata>;
  let engine: SyncEngine<any, any>;

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
      queries: { allUsers, userById, postsOnly },
      mutations: { insertUser, updateUserName },
    });
  });

  test("creates canonical SHA-256 topics", async () => {
    const first = await engine.createTopic("allUsers", []);
    const equivalent = await engine.createTopic("allUsers", []);
    const changedParams = await engine.createTopic("userById", [1]);
    const changedName = await engine.createTopic("postsOnly", []);
    const params = [1];
    const clonedTopic = await engine.createTopic("userById", params);
    params[0] = 2;
    expect(clonedTopic.params).toEqual([1]);

    expect(first).toEqual({
      name: "allUsers",
      params: [],
      hash: expect.stringMatching(/^[0-9a-f]{64}$/),
    });
    expect(equivalent).toEqual(first);
    expect(changedParams.hash).not.toBe(first.hash);
    expect(changedName.hash).not.toBe(first.hash);
  });

  test("runs queries through the protected helper", () => {
    const exposed = new ExposedEngine({ queries: { userById }, mutations: {} });

    expect(() => exposed.exposeQuery("missing", [])).toThrow("Unknown query: missing");
    expect(exposed.exposeQuery("userById", [2])).toEqual([{ id: 2, name: "bob" }]);
  });

  test("sync runs matching topics once and fans out the same event", async () => {
    const topic = await engine.createTopic("allUsers", []);
    const first = captureEvents();
    const second = captureEvents();
    engine.subscribe(topic, first.listener);
    engine.subscribe(topic, second.listener);

    engine.sync("insertUser", ["charlie"]);

    expect(first.events).toHaveLength(1);
    expect(second.events).toHaveLength(1);
    expect(first.events[0].topic).toEqual(topic);
    expect(second.events[0].topic).toEqual(topic);
    expect(first.events[0].value).toEqual(second.events[0].value);
  });

  test("query receives the topic params and skips non-overlapping tables", async () => {
    const runParams: number[] = [];
    const trackedUserById: Query<[number], SqlRow[]> = {
      tables: new Set(userById.tables),
      run: (id) => {
        runParams.push(id);
        return userById.run(id);
      },
    };
    let postsRuns = 0;
    const trackedPosts: Query<[], SqlRow[]> = {
      tables: new Set(postsOnly.tables),
      run: () => {
        postsRuns += 1;
        return postsOnly.run();
      },
    };
    engine = new SyncEngine({
      queries: { trackedUserById, trackedPosts },
      mutations: { updateUserName },
    });
    const topic = await engine.createTopic("trackedUserById", [2]);
    const postsTopic = await engine.createTopic("trackedPosts", []);
    const captured = captureEvents();
    engine.subscribe(topic, captured.listener);
    engine.subscribe(postsTopic, captured.listener);

    engine.sync("updateUserName", ["bob_updated", 2]);

    expect(runParams).toEqual([2]);
    expect(postsRuns).toBe(0);
    expect(captured.events).toHaveLength(1);
    expect(captured.events[0].topic).toEqual(topic);
    expect((captured.events[0].value as SqlRow[])[0].name).toBe("bob_updated");
  });

  test("does not run unsubscribed topics and rejects query errors", async () => {
    let queryRuns = 0;
    const neverQuery: Query<[], SqlRow[]> = {
      tables: toTables(["users"]),
      run: () => {
        queryRuns += 1;
        return [];
      },
    };
    const failingQuery: Query<[], SqlRow[]> = {
      tables: toTables(["users"]),
      run: () => {
        throw new Error("query failed");
      },
    };
    engine = new SyncEngine({
      queries: { neverQuery, failingQuery },
      mutations: { insertUser },
    });
    engine.sync("insertUser", ["charlie"]);
    expect(queryRuns).toBe(0);

    const failingTopic = await engine.createTopic("failingQuery", []);
    engine.subscribe(failingTopic, noopPublish);
    expect(() => engine.sync("insertUser", ["dave"])).toThrow("query failed");
  });

  test("duplicate listeners follow EventTarget semantics", async () => {
    const topic = await engine.createTopic("allUsers", []);
    const first = captureEvents();
    const second = captureEvents();
    const firstListenerId = engine.subscribe(topic, first.listener);
    expect(engine.subscribe(topic, first.listener)).toEqual(firstListenerId);
    const secondListenerId = engine.subscribe(topic, second.listener);
    expect(secondListenerId).not.toEqual(firstListenerId);

    engine.sync("insertUser", ["charlie"]);
    expect(first.events).toHaveLength(1);
    expect(second.events).toHaveLength(1);
    expect(engine.unsubscribe(firstListenerId)).toBe(true);
    engine.sync("insertUser", ["dave"]);
    expect(first.events).toHaveLength(1);
    expect(second.events).toHaveLength(2);
  });

  test("removes topics after their final listener unsubscribes", async () => {
    const topic = await engine.createTopic("allUsers", []);
    const first = captureEvents();
    const second = captureEvents();
    const firstListenerId = engine.subscribe(topic, first.listener);
    const secondListenerId = engine.subscribe(topic, second.listener);
    // Test-only access verifies the private topic lifecycle.
    const registry = (engine as unknown as { registry: Map<unknown, unknown> }).registry;

    expect(engine.unsubscribe(firstListenerId)).toBe(true);
    expect(registry.size).toBe(1);
    expect(engine.unsubscribe(secondListenerId)).toBe(true);
    expect(registry.size).toBe(0);
  });

  test("listener dispatch is scoped by topic hash", async () => {
    const exposed = new ExposedEngine({
      queries: { allUsers, postsOnly },
      mutations: {},
    });
    const usersTopic = await exposed.createTopic("allUsers", []);
    const postsTopic = await exposed.createTopic("postsOnly", []);
    const users = captureEvents();
    const posts = captureEvents();
    exposed.subscribe(usersTopic, users.listener);
    exposed.subscribe(postsTopic, posts.listener);

    exposed.exposePublish({ topic: usersTopic, value: 1 });
    expect(users.events).toEqual([{ topic: usersTopic, value: 1 }]);
    expect(posts.events).toEqual([]);
  });

  test("runs mutation, query, and listener synchronously", async () => {
    const calls: string[] = [];
    const synchronousQuery: Query<[], number> = {
      tables: toTables(["users"]),
      run: () => {
        calls.push("query");
        return 1;
      },
    };
    const synchronousMutation: Mutation<[], MutationMetadata> = {
      tables: toTables(["users"]),
      run: () => {
        calls.push("mutation");
        return storage.execute("INSERT INTO users (name) VALUES ('synchronous')");
      },
    };
    engine = new SyncEngine({
      queries: { synchronousQuery },
      mutations: { synchronousMutation },
    });
    const topic = await engine.createTopic("synchronousQuery", []);
    engine.subscribe(topic, () => calls.push("listener"));

    engine.sync("synchronousMutation", []);

    expect(calls).toEqual(["mutation", "query", "listener"]);
  });

  test("allows asynchronous listeners without delaying sync", async () => {
    const topic = await engine.createTopic("allUsers", []);
    let completed = false;
    engine.subscribe(topic, async () => {
      await Promise.resolve();
      completed = true;
    });

    expect(engine.sync("insertUser", ["charlie"])).toBeUndefined();
    expect(completed).toBe(false);

    await Promise.resolve();
    expect(completed).toBe(true);
  });

  test("validates manually supplied topics and hash collisions", async () => {
    const validTopic = await engine.createTopic("allUsers", []);
    expect(() =>
      engine.subscribe({ ...validTopic, name: "missing" } as never, noopPublish),
    ).toThrow("Unknown query: missing");
    expect(() => engine.subscribe({ ...validTopic, hash: "0" } as never, noopPublish)).toThrow(
      "Topic hash must be 64 lowercase hexadecimal characters",
    );
    engine.subscribe(validTopic, noopPublish);
    expect(() => engine.subscribe({ ...validTopic, params: [1] } as never, noopPublish)).toThrow(
      `Topic hash collision: ${validTopic.hash}`,
    );
    await expect(engine.createTopic("allUsers", [1n] as never)).rejects.toThrow();
  });
});
