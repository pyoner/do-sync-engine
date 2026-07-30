import { Effect, Exit, Option } from "effect";
import { afterEach, beforeEach, describe, expect, test } from "vite-plus/test";
import { DatabaseSync } from "node:sqlite";
import { createAdapter, type SqlAdapterError, type SqlRow } from "../src/index.ts";
import { SyncEngine, Topic, UnknownQueryError, toTables } from "@do-sync-engine/core";
import type { Listener, ListenerEvent, Mutation, Query } from "@do-sync-engine/core";

const runTopic = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(effect);

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
  let allUsers: Query<[], SqlRow[], SqlAdapterError>;
  let userById: Query<[number], SqlRow[], SqlAdapterError>;
  let postsOnly: Query<[], SqlRow[], SqlAdapterError>;
  let insertUser: Mutation<[string], unknown, SqlAdapterError>;
  let updateUserName: Mutation<[string, number], unknown, SqlAdapterError>;
  let engine: SyncEngine<any, any>;

  beforeEach(async () => {
    storage = new DatabaseSync(":memory:");
    setupDb(storage);
    const sql = await runTopic(createAdapter(storage));

    const allUsersSql = "SELECT * FROM users ORDER BY id";
    allUsers = (await runTopic(sql(allUsersSql))) as Query<[], SqlRow[], SqlAdapterError>;
    const userByIdSql = "SELECT * FROM users WHERE id = ?";
    userById = (await runTopic(sql(userByIdSql))) as Query<[number], SqlRow[], SqlAdapterError>;
    const postsOnlySql = "SELECT * FROM posts ORDER BY id";
    postsOnly = (await runTopic(sql(postsOnlySql))) as Query<[], SqlRow[], SqlAdapterError>;
    const insertUserSql = "INSERT INTO users (name) VALUES (?)";
    insertUser = (await runTopic(sql(insertUserSql))) as Mutation<
      [string],
      unknown,
      SqlAdapterError
    >;
    const updateUserNameSql = "UPDATE users SET name = ? WHERE id = ?";
    updateUserName = (await runTopic(sql(updateUserNameSql))) as Mutation<
      [string, number],
      unknown,
      SqlAdapterError
    >;

    engine = new SyncEngine({
      queries: { allUsers, userById, postsOnly },
      mutations: { insertUser, updateUserName },
    });
  });

  afterEach(() => {
    storage.close();
  });

  test("creates equal Effect topics", async () => {
    const first = await runTopic(engine.createTopic("allUsers", []));
    const equivalent = await runTopic(engine.createTopic("allUsers", []));
    const changedParams = await runTopic(engine.createTopic("userById", [1]));
    const changedName = await runTopic(engine.createTopic("postsOnly", []));
    const params = [1];
    const topic = await runTopic(engine.createTopic("userById", params));
    expect(topic.params).toBe(params);
    expect(first).toEqual({ name: "allUsers", params: [] });
    expect(equivalent).toEqual(first);
    expect(changedParams).not.toEqual(first);
    expect(changedName).not.toEqual(first);
  });

  test("runs queries through the public engine interface", async () => {
    const topic = await runTopic(engine.createTopic("userById", [2]));

    const missing = await Effect.runPromiseExit(
      engine.query(new Topic({ name: "missing", params: topic.params })),
    );
    expect(Exit.isFailure(missing)).toBe(true);
    if (Exit.isFailure(missing)) {
      const error = Exit.findErrorOption(missing);
      expect(Option.isSome(error)).toBe(true);
      if (Option.isSome(error)) {
        expect(error.value).toBeInstanceOf(UnknownQueryError);
        if (error.value instanceof UnknownQueryError) expect(error.value.query).toBe("missing");
      }
    }
    expect(await runTopic(engine.query(topic))).toEqual([{ id: 2, name: "bob" }]);
  });

  test("routes same-query topics by full parameters", async () => {
    const firstTopic = await runTopic(engine.createTopic("userById", [1]));
    const secondTopic = await runTopic(engine.createTopic("userById", [2]));
    const first = captureEvents();
    const second = captureEvents();
    await runTopic(engine.subscribe(firstTopic, first.listener));
    await runTopic(engine.subscribe(secondTopic, second.listener));

    await runTopic(engine.sync("updateUserName", ["alice-updated", 1]));

    expect(first.events).toHaveLength(1);
    expect(second.events).toHaveLength(1);
    expect(first.events[0].value).toEqual([{ id: 1, name: "alice-updated" }]);
    expect(second.events[0].value).toEqual([{ id: 2, name: "bob" }]);
  });

  test("sync runs matching topics once and fans out the same event", async () => {
    const topic = await runTopic(engine.createTopic("allUsers", []));
    const first = captureEvents();
    const second = captureEvents();
    await runTopic(engine.subscribe(topic, first.listener));
    await runTopic(engine.subscribe(topic, second.listener));

    await runTopic(engine.sync("insertUser", ["charlie"]));

    expect(first.events).toHaveLength(1);
    expect(second.events).toHaveLength(1);
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
    engine = new SyncEngine({
      queries: { trackedUserById, trackedPosts },
      mutations: { updateUserName },
    });
    const topic = await runTopic(engine.createTopic("trackedUserById", [2]));
    const postsTopic = await runTopic(engine.createTopic("trackedPosts", []));
    const captured = captureEvents();
    await runTopic(engine.subscribe(topic, captured.listener));
    await runTopic(engine.subscribe(postsTopic, captured.listener));

    await runTopic(engine.sync("updateUserName", ["bob_updated", 2]));

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
        return Effect.succeed([]);
      },
    };
    const failingQuery: Query<[], SqlRow[], Error> = {
      tables: toTables(["users"]),
      run: () => Effect.fail(new Error("query failed")),
    };
    engine = new SyncEngine({
      queries: { neverQuery, failingQuery },
      mutations: { insertUser },
    });
    await runTopic(engine.sync("insertUser", ["charlie"]));
    expect(queryRuns).toBe(0);

    const failingTopic = await runTopic(engine.createTopic("failingQuery", []));
    await runTopic(engine.subscribe(failingTopic, noopPublish));
    const failure = await Effect.runPromiseExit(engine.sync("insertUser", ["dave"]));
    expect(Exit.isFailure(failure)).toBe(true);
    if (Exit.isFailure(failure)) {
      const error = Exit.findErrorOption(failure);
      expect(Option.isSome(error)).toBe(true);
      if (Option.isSome(error)) expect(error.value).toMatchObject({ message: "query failed" });
    }
  });

  test("duplicate listeners follow EventTarget semantics", async () => {
    const topic = await runTopic(engine.createTopic("allUsers", []));
    const first = captureEvents();
    const second = captureEvents();
    const firstListenerId = await runTopic(engine.subscribe(topic, first.listener));
    expect(await runTopic(engine.subscribe(topic, first.listener))).toEqual(firstListenerId);
    const secondListenerId = await runTopic(engine.subscribe(topic, second.listener));
    expect(secondListenerId).not.toEqual(firstListenerId);

    await runTopic(engine.sync("insertUser", ["charlie"]));
    expect(first.events).toHaveLength(1);
    expect(second.events).toHaveLength(1);
    expect(await runTopic(engine.unsubscribe(firstListenerId))).toBe(true);
    await runTopic(engine.sync("insertUser", ["dave"]));
    expect(first.events).toHaveLength(1);
    expect(second.events).toHaveLength(2);
  });

  test("removes topics after their final listener unsubscribes", async () => {
    const topic = await runTopic(engine.createTopic("allUsers", []));
    const first = captureEvents();
    const second = captureEvents();
    const firstListenerId = await runTopic(engine.subscribe(topic, first.listener));
    const secondListenerId = await runTopic(engine.subscribe(topic, second.listener));
    // Test-only access verifies the private topic lifecycle.
    const registry = (engine as unknown as { registry: { backing: Map<unknown, unknown> } })
      .registry;

    expect(await runTopic(engine.unsubscribe(firstListenerId))).toBe(true);
    expect(registry.backing.size).toBe(1);
    expect(await runTopic(engine.unsubscribe(secondListenerId))).toBe(true);
    expect(registry.backing.size).toBe(0);
  });

  test("listener dispatch is scoped by topic hash", async () => {
    const exposed = new ExposedEngine({
      queries: { allUsers, postsOnly },
      mutations: {},
    });
    const usersTopic = await runTopic(exposed.createTopic("allUsers", []));
    const postsTopic = await runTopic(exposed.createTopic("postsOnly", []));
    const users = captureEvents();
    const posts = captureEvents();
    await runTopic(exposed.subscribe(usersTopic, users.listener));
    await runTopic(exposed.subscribe(postsTopic, posts.listener));

    exposed.exposePublish({ topic: usersTopic, value: 1 });
    expect(users.events).toEqual([{ topic: usersTopic, value: 1 }]);
    expect(posts.events).toEqual([]);
  });

  test("runs mutation, query, and listener synchronously", async () => {
    const calls: string[] = [];
    const synchronousQuery: Query<[], number> = {
      tables: toTables(["users"]),
      run: () =>
        Effect.sync(() => {
          calls.push("query");
          return 1;
        }),
    };
    const sql = await runTopic(createAdapter(storage));
    const synchronousMutation = (await runTopic(
      sql("INSERT INTO users (name) VALUES ('synchronous')"),
    )) as Mutation<[], unknown, SqlAdapterError>;
    const trackedSynchronousMutation: Mutation<[], unknown, SqlAdapterError> = {
      ...synchronousMutation,
      run: () =>
        Effect.gen(function* () {
          calls.push("mutation");
          return yield* synchronousMutation.run();
        }),
    };
    engine = new SyncEngine({
      queries: { synchronousQuery },
      mutations: { synchronousMutation: trackedSynchronousMutation },
    });
    const topic = await runTopic(engine.createTopic("synchronousQuery", []));
    await runTopic(engine.subscribe(topic, () => calls.push("listener")));

    await runTopic(engine.sync("synchronousMutation", []));

    expect(calls).toEqual(["mutation", "query", "listener"]);
  });

  test("allows asynchronous listeners without delaying sync", async () => {
    const topic = await runTopic(engine.createTopic("allUsers", []));
    let completed = false;
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    await runTopic(
      engine.subscribe(topic, async () => {
        await gate;
        completed = true;
      }),
    );

    await runTopic(engine.sync("insertUser", ["charlie"]));
    expect(completed).toBe(false);
    release();
    await Promise.resolve();
    expect(completed).toBe(true);
  });

  test("rejects unknown manually supplied topics", async () => {
    const validTopic = await runTopic(engine.createTopic("allUsers", []));
    const missing = await Effect.runPromiseExit(
      engine.subscribe(new Topic({ name: "missing", params: validTopic.params }), noopPublish),
    );
    expect(Exit.isFailure(missing)).toBe(true);
    if (Exit.isFailure(missing)) {
      const error = Exit.findErrorOption(missing);
      expect(Option.isSome(error)).toBe(true);
      if (Option.isSome(error)) {
        expect(error.value).toBeInstanceOf(UnknownQueryError);
        if (error.value instanceof UnknownQueryError) expect(error.value.query).toBe("missing");
      }
    }
    await runTopic(engine.subscribe(validTopic, noopPublish));
    await runTopic(
      engine.subscribe(new Topic({ name: validTopic.name, params: [1] }), noopPublish),
    );
    await runTopic(
      engine.subscribe(new Topic({ name: validTopic.name, params: [() => 1] }), noopPublish),
    );
  });
});
