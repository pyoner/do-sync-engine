import { afterEach, beforeEach, describe, expect, test } from "vite-plus/test";
import { DatabaseSync } from "node:sqlite";
import { createAdapter, type SqlRow } from "../src/index.ts";
import { SyncEngine, toTables } from "@do-sync-engine/core";
import type { Listener, ListenerEvent, Mutation, Query } from "@do-sync-engine/core";

function expectOk<T>(value: T): Exclude<T, Error> {
  if (value instanceof Error) throw value;
  return value as Exclude<T, Error>;
}

function captureEvents() {
  const events: ListenerEvent[] = [];
  const listener: Listener = (event) => {
    events.push(event);
  };
  return { events, listener };
}

const noopPublish: Listener = () => {};

class ExposedEngine extends SyncEngine {
  exposePublish(event: ListenerEvent) {
    return this.publish(event);
  }
}

function setupDb(storage: DatabaseSync) {
  storage.exec("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)");
  storage.exec("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT)");
  storage.exec(`INSERT INTO users (name) VALUES ('alice')`);
  storage.exec(`INSERT INTO users (name) VALUES ('bob')`);
  storage.exec(`INSERT INTO posts (user_id, title) VALUES (1, 'hello')`);
}

describe("SyncEngine topics and events", () => {
  let storage: DatabaseSync;
  let allUsers: Query<[], SqlRow[]>;
  let userById: Query<[number], SqlRow[]>;
  let postsOnly: Query<[], SqlRow[]>;
  let insertUser: Mutation<[string], unknown>;
  let updateUserName: Mutation<[string, number], unknown>;
  let engine: SyncEngine;

  beforeEach(() => {
    storage = new DatabaseSync(":memory:");
    setupDb(storage);
    const sql = expectOk(createAdapter(storage));

    const allUsersSql = "SELECT * FROM users ORDER BY id";
    allUsers = expectOk(sql(allUsersSql)) as Query<[], SqlRow[]>;
    const userByIdSql = "SELECT * FROM users WHERE id = ?";
    userById = expectOk(sql(userByIdSql)) as Query<[number], SqlRow[]>;
    const postsOnlySql = "SELECT * FROM posts ORDER BY id";
    postsOnly = expectOk(sql(postsOnlySql)) as Query<[], SqlRow[]>;
    const insertUserSql = "INSERT INTO users (name) VALUES (?)";
    insertUser = expectOk(sql(insertUserSql)) as Mutation<[string], unknown>;
    const updateUserNameSql = "UPDATE users SET name = ? WHERE id = ?";
    updateUserName = expectOk(sql(updateUserNameSql)) as Mutation<[string, number], unknown>;

    engine = new SyncEngine({
      queries: { allUsers, userById, postsOnly },
      mutations: { insertUser, updateUserName },
    });
  });

  afterEach(() => {
    storage.close();
  });

  test("creates cloned topics", async () => {
    const first = expectOk(engine.createTopic("allUsers", []));
    const equivalent = expectOk(engine.createTopic("allUsers", []));
    const changedParams = expectOk(engine.createTopic("userById", [1]));
    const changedName = expectOk(engine.createTopic("postsOnly", []));
    const params = [1];
    const clonedTopic = expectOk(engine.createTopic("userById", params));
    params[0] = 2;
    expect(clonedTopic.params).toEqual([1]);

    expect(first).toEqual({ name: "allUsers", params: [] });
    expect(equivalent).toEqual(first);
    expect(changedParams).not.toEqual(first);
    expect(changedName).not.toEqual(first);
  });

  test("runs queries through the public engine interface", async () => {
    const topic = expectOk(engine.createTopic("userById", [2]));
    expect(engine.query({ ...topic, name: "missing" } as never)).toBeInstanceOf(Error);
    expect(engine.query(topic)).toEqual([{ id: 2, name: "bob" }]);
  });

  test("sync runs matching topics once and fans out the same event", async () => {
    const topic = expectOk(engine.createTopic("allUsers", []));
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
    const topic = expectOk(engine.createTopic("trackedUserById", [2]));
    const postsTopic = expectOk(engine.createTopic("trackedPosts", []));
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

    const failingTopic = expectOk(engine.createTopic("failingQuery", []));
    engine.subscribe(failingTopic, noopPublish);
    const result = engine.sync("insertUser", ["dave"]);
    expect(result).toBeInstanceOf(Error);
  });
  test("duplicate listeners follow EventTarget semantics", async () => {
    const topic = expectOk(engine.createTopic("allUsers", []));
    const first = captureEvents();
    const second = captureEvents();
    const firstListenerId = expectOk(engine.subscribe(topic, first.listener));
    expect(expectOk(engine.subscribe(topic, first.listener))).toEqual(firstListenerId);
    const secondListenerId = expectOk(engine.subscribe(topic, second.listener));
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
    const topic = expectOk(engine.createTopic("allUsers", []));
    const first = captureEvents();
    const second = captureEvents();
    const firstListenerId = expectOk(engine.subscribe(topic, first.listener));
    const secondListenerId = expectOk(engine.subscribe(topic, second.listener));
    // Test-only access verifies the private topic lifecycle.
    const registry = (engine as unknown as { registry: Map<unknown, unknown> }).registry;

    expect(engine.unsubscribe(firstListenerId)).toBe(true);
    expect(registry.size).toBe(1);
    expect(engine.unsubscribe(secondListenerId)).toBe(true);
    expect(registry.size).toBe(0);
  });

  test("listener dispatch is scoped by topic", async () => {
    const exposed = new ExposedEngine({
      queries: { allUsers, postsOnly },
      mutations: {},
    });
    const usersTopic = expectOk(exposed.createTopic("allUsers", []));
    const postsTopic = expectOk(exposed.createTopic("postsOnly", []));
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
    const synchronousMutation = expectOk(
      expectOk(createAdapter(storage))("INSERT INTO users (name) VALUES ('synchronous')"),
    ) as Mutation<[], unknown>;
    const trackedSynchronousMutation: Mutation<[], unknown> = {
      ...synchronousMutation,
      run: () => {
        calls.push("mutation");
        return synchronousMutation.run();
      },
    };
    engine = new SyncEngine({
      queries: { synchronousQuery },
      mutations: { synchronousMutation: trackedSynchronousMutation },
    });
    const topic = expectOk(engine.createTopic("synchronousQuery", []));
    engine.subscribe(topic, () => calls.push("listener"));

    engine.sync("synchronousMutation", []);

    expect(calls).toEqual(["mutation", "query", "listener"]);
  });

  test("allows asynchronous listeners without delaying sync", async () => {
    const topic = expectOk(engine.createTopic("allUsers", []));
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

  test("validates topics when creating and querying", async () => {
    expect(engine.createTopic("missing", [])).toBeInstanceOf(Error);
    const validTopic = expectOk(engine.createTopic("allUsers", []));
    expect(engine.query({ ...validTopic, name: "missing" } as never)).toBeInstanceOf(Error);
    engine.subscribe(validTopic, noopPublish);
  });
});
