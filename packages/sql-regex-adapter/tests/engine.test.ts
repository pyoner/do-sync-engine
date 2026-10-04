import { afterEach, beforeEach, describe, expect, test } from "vite-plus/test";
import { DatabaseSync } from "node:sqlite";
import { Effect } from "effect";
import { createAdapter, type SqlAdapterError, type SqlRow } from "../src/index.ts";
import { makeSyncEngine, toTables } from "@do-sync-engine/core";
import type { Listener, ListenerEvent, Mutation, Query, SyncEngine } from "@do-sync-engine/core";

function captureEvents() {
  const events: ListenerEvent[] = [];
  const listener: Listener = (event) => {
    events.push(event);
  };
  return { events, listener };
}

const noopPublish: Listener = () => {};

type FixtureQueries = {
  allUsers: Query<[], SqlRow[], SqlAdapterError>;
  userById: Query<[number], SqlRow[], SqlAdapterError>;
  postsOnly: Query<[], SqlRow[], SqlAdapterError>;
};
type FixtureMutations = {
  insertUser: Mutation<[string], unknown, SqlAdapterError>;
  updateUserName: Mutation<[string, number], unknown, SqlAdapterError>;
};

function setupDb(storage: DatabaseSync) {
  storage.exec("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)");
  storage.exec("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT)");
  storage.exec(`INSERT INTO users (name) VALUES ('alice')`);
  storage.exec(`INSERT INTO users (name) VALUES ('bob')`);
  storage.exec(`INSERT INTO posts (user_id, title) VALUES (1, 'hello')`);
}

describe("SyncEngine topics and events", () => {
  let storage: DatabaseSync;
  let allUsers: Query<[], SqlRow[], SqlAdapterError>;
  let userById: Query<[number], SqlRow[], SqlAdapterError>;
  let postsOnly: Query<[], SqlRow[], SqlAdapterError>;
  let engine: SyncEngine<string, FixtureQueries, FixtureMutations>;
  let insertUser: Mutation<[string], unknown, SqlAdapterError>;
  let updateUserName: Mutation<[string, number], unknown, SqlAdapterError>;

  beforeEach(() => {
    storage = new DatabaseSync(":memory:");
    setupDb(storage);
    const sql = (statement: string) =>
      Effect.runSync(Effect.flatMap(createAdapter(storage), (adapt) => adapt(statement)));

    const allUsersSql = "SELECT * FROM users ORDER BY id";
    allUsers = sql(allUsersSql) as unknown as Query<[], SqlRow[], SqlAdapterError>;
    const userByIdSql = "SELECT * FROM users WHERE id = ?";
    userById = sql(userByIdSql) as unknown as Query<[number], SqlRow[], SqlAdapterError>;
    const postsOnlySql = "SELECT * FROM posts ORDER BY id";
    postsOnly = sql(postsOnlySql) as unknown as Query<[], SqlRow[], SqlAdapterError>;
    const insertUserSql = "INSERT INTO users (name) VALUES (?)";
    insertUser = sql(insertUserSql) as unknown as Mutation<[string], unknown, SqlAdapterError>;
    const updateUserNameSql = "UPDATE users SET name = ? WHERE id = ?";
    updateUserName = sql(updateUserNameSql) as unknown as Mutation<
      [string, number],
      unknown,
      SqlAdapterError
    >;

    engine = Effect.runSync(
      makeSyncEngine({
        queries: { allUsers, userById, postsOnly },
        mutations: { insertUser, updateUserName },
        createId: () => crypto.randomUUID(),
      }),
    );
  });

  afterEach(() => {
    storage.close();
  });

  test("creates topics", async () => {
    const first = Effect.runSync(engine.createTopic("allUsers", []));
    const equivalent = Effect.runSync(engine.createTopic("allUsers", []));
    const changedParams = Effect.runSync(engine.createTopic("userById", [1]));
    const changedName = Effect.runSync(engine.createTopic("postsOnly", []));

    expect(first).toEqual({ name: "allUsers", params: [] });
    expect(equivalent).toEqual(first);
    expect(changedParams).not.toEqual(first);
    expect(changedName).not.toEqual(first);
  });
  test("sync runs matching topics once and fans out the same event", async () => {
    const topic = Effect.runSync(engine.createTopic("allUsers", []));
    const first = captureEvents();
    const second = captureEvents();
    Effect.runSync(engine.subscribe(topic, first.listener));
    Effect.runSync(engine.subscribe(topic, second.listener));

    Effect.runSync(engine.sync("insertUser", ["charlie"]));

    expect(first.events).toHaveLength(2);
    expect(second.events).toHaveLength(2);
    expect(first.events[0].topic).toEqual(topic);
    expect(second.events[0].topic).toEqual(topic);
    expect(first.events[0].value).toEqual(second.events[0].value);
  });

  test("query receives the topic params and skips non-overlapping tables", async () => {
    const runParams: number[] = [];
    const trackedUserById: Query<[number], SqlRow[], SqlAdapterError> = {
      tables: new Set(userById.tables),
      run: (id) => {
        runParams.push(id);
        return userById.run(id);
      },
    };
    let postsRuns = 0;
    const trackedPosts: Query<[], SqlRow[], SqlAdapterError> = {
      tables: new Set(postsOnly.tables),
      run: () => {
        postsRuns += 1;
        return postsOnly.run();
      },
    };
    const engine = Effect.runSync(
      makeSyncEngine({
        queries: { trackedUserById, trackedPosts },
        mutations: { updateUserName },
        createId: () => crypto.randomUUID(),
      }),
    );
    const topic = Effect.runSync(engine.createTopic("trackedUserById", [2]));
    const postsTopic = Effect.runSync(engine.createTopic("trackedPosts", []));
    const captured = captureEvents();
    Effect.runSync(engine.subscribe(topic, captured.listener));
    Effect.runSync(engine.subscribe(postsTopic, captured.listener));

    Effect.runSync(engine.sync("updateUserName", ["bob_updated", 2]));

    expect(runParams).toEqual([2, 2]);
    expect(postsRuns).toBe(1);
    expect(captured.events).toHaveLength(3);
    expect(captured.events[0].topic).toEqual(topic);
    expect((captured.events[2].value as SqlRow[])[0].name).toBe("bob_updated");
  });

  test("does not run unsubscribed topics and rejects query errors", async () => {
    let queryRuns = 0;
    const neverQuery: Query<[], SqlRow[], SqlAdapterError> = {
      tables: toTables(["users"]),
      run: () =>
        Effect.sync(() => {
          queryRuns += 1;
          return [];
        }),
    };
    const failingQuery: Query<[], SqlRow[], Error> = {
      tables: toTables(["users"]),
      run: () => Effect.fail(new Error("query failed")),
    };
    const engine = Effect.runSync(
      makeSyncEngine({
        queries: { neverQuery, failingQuery },
        mutations: { insertUser },
        createId: () => crypto.randomUUID(),
      }),
    );
    Effect.runSync(engine.sync("insertUser", ["charlie"]));
    expect(queryRuns).toBe(0);
    const failingTopic = Effect.runSync(engine.createTopic("failingQuery", []));

    expect(Effect.runSync(Effect.flip(engine.subscribe(failingTopic, noopPublish)))).toBeInstanceOf(
      Error,
    );
  });
  test("duplicate listeners follow EventTarget semantics", async () => {
    const topic = Effect.runSync(engine.createTopic("allUsers", []));
    const first = captureEvents();
    const second = captureEvents();
    const firstId = Effect.runSync(engine.subscribe(topic, first.listener));
    expect(Effect.runSync(engine.subscribe(topic, first.listener))).toBe(firstId);
    Effect.runSync(engine.subscribe(topic, second.listener));

    Effect.runSync(engine.sync("insertUser", ["charlie"]));
    expect(first.events).toHaveLength(3);
    expect(second.events).toHaveLength(2);
    Effect.runSync(engine.unsubscribe(topic, first.listener));
    Effect.runSync(engine.sync("insertUser", ["dave"]));
    expect(first.events).toHaveLength(3);
    expect(second.events).toHaveLength(3);
  });

  test("removes topics after their final listener unsubscribes", async () => {
    const topic = Effect.runSync(engine.createTopic("allUsers", []));
    const first = captureEvents();
    const second = captureEvents();
    Effect.runSync(engine.subscribe(topic, first.listener));
    Effect.runSync(engine.subscribe(topic, second.listener));

    Effect.runSync(engine.unsubscribe(topic, first.listener));
    expect(Effect.runSync(engine.subscriptions())).toHaveLength(1);
    Effect.runSync(engine.unsubscribe(topic, second.listener));
    expect(Effect.runSync(engine.subscriptions())).toHaveLength(0);
  });

  test("runs mutation, query, and listener synchronously", async () => {
    const calls: string[] = [];
    let version = 0;
    const synchronousQuery: Query<[], number> = {
      tables: toTables(["users"]),
      run: () =>
        Effect.sync(() => {
          calls.push("query");
          return version;
        }),
    };
    const synchronousMutation = Effect.runSync(
      Effect.flatMap(createAdapter(storage), (adapt) =>
        adapt("INSERT INTO users (name) VALUES ('synchronous')"),
      ),
    ) as unknown as Mutation<[], unknown, SqlAdapterError>;
    const trackedSynchronousMutation: Mutation<[], unknown, SqlAdapterError> = {
      ...synchronousMutation,
      run: () => {
        calls.push("mutation");
        version += 1;
        return synchronousMutation.run();
      },
    };
    const engine = Effect.runSync(
      makeSyncEngine({
        queries: { synchronousQuery },
        mutations: { synchronousMutation: trackedSynchronousMutation },
        createId: () => crypto.randomUUID(),
      }),
    );
    const topic = Effect.runSync(engine.createTopic("synchronousQuery", []));
    Effect.runSync(engine.subscribe(topic, () => calls.push("listener")));

    Effect.runSync(engine.sync("synchronousMutation", []));

    expect(calls).toEqual(["query", "listener", "mutation", "query", "listener"]);
  });

  test("allows asynchronous listeners without delaying sync", async () => {
    const topic = Effect.runSync(engine.createTopic("allUsers", []));
    let completed = false;
    Effect.runSync(
      engine.subscribe(topic, async () => {
        await Promise.resolve();
        completed = true;
      }),
    );

    expect(Effect.runSync(engine.sync("insertUser", ["charlie"]))).toBeUndefined();
    expect(completed).toBe(false);

    await Promise.resolve();
    expect(completed).toBe(true);
  });
});
