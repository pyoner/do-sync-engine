import { beforeEach, describe, expect, test } from "vite-plus/test";
import { SyncEngine } from "../src/engine.js";
import type { Mutation, Publish, Query, Topic } from "../src/index.js";
import { NodeSqliteStorage } from "./helpers.js";
import type { MutationMetadata, SqlRow } from "./helpers.js";

function captureEvents() {
  const events: Array<{ topic: Topic; value: unknown }> = [];
  const publish: Publish = (topic, value) => {
    events.push({ topic, value });
  };
  return { events, publish };
}

const noopPublish: Publish = () => {};

class ExposedEngine extends SyncEngine<any, any> {
  exposePublish(topic: Topic, value: unknown) {
    return this.publish(topic, value);
  }
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

function createDeferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((res) => {
    resolve = res;
  });
  return { promise, resolve };
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

  test("sync runs matching topics once and fans out the same event", async () => {
    const topic = await engine.createTopic("allUsers", []);
    const first = captureEvents();
    const second = captureEvents();
    engine.subscribe(topic, first.publish);
    engine.subscribe(topic, second.publish);

    await engine.sync("insertUser", ["charlie"]);

    expect(first.events).toHaveLength(1);
    expect(second.events).toHaveLength(1);
    expect(first.events[0].topic).toEqual(topic);
    expect(second.events[0].topic).toEqual(topic);
    expect(first.events[0].value).toEqual(second.events[0].value);
  });

  test("query receives the topic params and skips non-overlapping tables", async () => {
    const runParams: number[] = [];
    const trackedUserById: Query<[number], SqlRow[]> = {
      tables: [...userById.tables],
      run: (id) => {
        runParams.push(id);
        return userById.run(id);
      },
    };
    let postsRuns = 0;
    const trackedPosts: Query<[], SqlRow[]> = {
      tables: [...postsOnly.tables],
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
    engine.subscribe(topic, captured.publish);
    engine.subscribe(postsTopic, captured.publish);

    await engine.sync("updateUserName", ["bob_updated", 2]);

    expect(runParams).toEqual([2]);
    expect(postsRuns).toBe(0);
    expect(captured.events).toHaveLength(1);
    expect(captured.events[0].topic).toEqual(topic);
    expect((captured.events[0].value as SqlRow[])[0].name).toBe("bob_updated");
  });

  test("does not run unsubscribed topics and rejects query errors", async () => {
    let queryRuns = 0;
    const neverQuery: Query<[], SqlRow[]> = {
      tables: ["users"],
      run: () => {
        queryRuns += 1;
        return [];
      },
    };
    const failingQuery: Query<[], SqlRow[]> = {
      tables: ["users"],
      run: () => {
        throw new Error("query failed");
      },
    };
    engine = new SyncEngine({
      queries: { neverQuery, failingQuery },
      mutations: { insertUser },
    });
    await engine.sync("insertUser", ["charlie"]);
    expect(queryRuns).toBe(0);

    const failingTopic = await engine.createTopic("failingQuery", []);
    engine.subscribe(failingTopic, noopPublish);
    await expect(engine.sync("insertUser", ["dave"])).rejects.toThrow("query failed");
  });

  test("duplicate listeners follow EventTarget semantics", async () => {
    const topic = await engine.createTopic("allUsers", []);
    const first = captureEvents();
    const second = captureEvents();
    const firstId = engine.subscribe(topic, first.publish);
    expect(engine.subscribe(topic, first.publish)).toBe(firstId);
    const secondId = engine.subscribe(topic, second.publish);
    expect(secondId).not.toBe(firstId);

    await engine.sync("insertUser", ["charlie"]);
    expect(first.events).toHaveLength(1);
    expect(second.events).toHaveLength(1);
    expect(engine.unsubscribe(firstId)).toBe(true);
    await engine.sync("insertUser", ["dave"]);
    expect(first.events).toHaveLength(1);
    expect(second.events).toHaveLength(2);
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
    exposed.subscribe(usersTopic, users.publish);
    exposed.subscribe(postsTopic, posts.publish);

    await exposed.exposePublish(usersTopic, 1);
    expect(users.events).toEqual([{ topic: usersTopic, value: 1 }]);
    expect(posts.events).toEqual([]);
  });

  test("awaits mutation, query, and listener", async () => {
    const mutationGate = createDeferred();
    const queryGate = createDeferred();
    const listenerGate = createDeferred();
    let listenerStarted!: () => void;
    const listenerStartedPromise = new Promise<void>((resolve) => {
      listenerStarted = resolve;
    });
    let listenerDone = false;
    const gatedQuery: Query<[], number> = {
      tables: ["users"],
      run: async () => {
        await queryGate.promise;
        return 1;
      },
    };
    const gatedMutation: Mutation<[], MutationMetadata> = {
      tables: ["users"],
      run: async () => {
        await mutationGate.promise;
        return storage.execute("INSERT INTO users (name) VALUES ('gated')");
      },
    };
    engine = new SyncEngine({
      queries: { gatedQuery },
      mutations: { gatedMutation },
    });
    const topic = await engine.createTopic("gatedQuery", []);
    engine.subscribe(topic, async () => {
      listenerStarted();
      await listenerGate.promise;
      listenerDone = true;
    });

    let syncResolved = false;
    const syncPromise = engine.sync("gatedMutation", []).then(() => {
      syncResolved = true;
    });
    await Promise.resolve();
    expect(syncResolved).toBe(false);
    mutationGate.resolve();
    await Promise.resolve();
    expect(syncResolved).toBe(false);
    queryGate.resolve();
    await listenerStartedPromise;
    expect(syncResolved).toBe(false);
    listenerGate.resolve();
    await syncPromise;
    expect(syncResolved).toBe(true);
    expect(listenerDone).toBe(true);
  });

  test("unsubscribe during an in-flight query suppresses delivery", async () => {
    const queryStarted = createDeferred();
    const queryGate = createDeferred();
    const gatedQuery: Query<[], SqlRow[]> = {
      tables: ["users"],
      run: async () => {
        queryStarted.resolve();
        await queryGate.promise;
        return storage.query("SELECT * FROM users ORDER BY id");
      },
    };
    engine = new SyncEngine({ queries: { gatedQuery }, mutations: { insertUser } });
    const topic = await engine.createTopic("gatedQuery", []);
    const captured = captureEvents();
    const id = engine.subscribe(topic, captured.publish);
    const syncPromise = engine.sync("insertUser", ["charlie"]);
    await queryStarted.promise;
    expect(engine.unsubscribe(id)).toBe(true);
    queryGate.resolve();
    await syncPromise;
    expect(captured.events).toEqual([]);
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
