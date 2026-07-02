import { beforeEach, describe, expect, test } from "vite-plus/test";
import { SyncEngine } from "../src/engine.js";
import { NodeSqliteStorage } from "./helpers.js";
import type { MutationResult, Mutator, Selector } from "../src/index.js";
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
  let userPosts: Selector<[], SqlRow[]>;
  let postCount: Selector<[], SqlRow[]>;
  let userById: Selector<[number], SqlRow[]>;
  let insertUser: Mutator<[string], MutationMetadata>;
  let deleteUser: Mutator<[number], MutationMetadata>;
  let insertPost: Mutator<[number, string], MutationMetadata>;
  let updateUserName: Mutator<[string, number], MutationMetadata>;

  const resultsFor = (result: MutationResult<MutationMetadata>, selector: object) =>
    result.recomputedSelectors.filter((entry) => entry.selector === selector);

  beforeEach(() => {
    storage = new NodeSqliteStorage();
    setupDb(storage);
    engine = new SyncEngine();

    const allUsersSql = "SELECT * FROM users ORDER BY id";
    allUsers = {
      tables: readTablesFromSql(allUsersSql),
      run: () => storage.query(allUsersSql),
    };

    const userPostsSql =
      "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id ORDER BY u.id, p.id";
    userPosts = {
      tables: readTablesFromSql(userPostsSql),
      run: () => storage.query(userPostsSql),
    };

    const postCountSql = "SELECT COUNT(*) as total_count FROM posts";
    postCount = {
      tables: readTablesFromSql(postCountSql),
      run: () => storage.query(postCountSql),
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

    const deleteUserSql = "DELETE FROM users WHERE id = ?";
    deleteUser = {
      tables: writeTablesFromSql(deleteUserSql),
      run: (id) => storage.execute(deleteUserSql, id),
    };

    const insertPostSql = "INSERT INTO posts (user_id, title) VALUES (?, ?)";
    insertPost = {
      tables: writeTablesFromSql(insertPostSql),
      run: (userId, title) => storage.execute(insertPostSql, userId, title),
    };

    const updateUserNameSql = "UPDATE users SET name = ? WHERE id = ?";
    updateUserName = {
      tables: writeTablesFromSql(updateUserNameSql),
      run: (name, id) => storage.execute(updateUserNameSql, name, id),
    };
  });

  test("query returns rows", async () => {
    const rows = await engine.query(allUsers);
    expect(rows.length).toBe(2);
    expect(rows[0].name).toBe("alice");
    expect(rows[1].name).toBe("bob");
  });

  test("mutate inserts and recomputes affected selectors", async () => {
    await engine.query(allUsers);
    await engine.query(postCount);

    const result = await engine.mutate(insertUser, "charlie");

    expect(result.metadata.rowsAffected).toBe(1);
    expect(result.tables).toEqual(["users"]);
    expect(resultsFor(result, allUsers)).toHaveLength(1);
    expect(resultsFor(result, postCount)).toHaveLength(0);

    const [allUsersResult] = resultsFor(result, allUsers);
    expect(allUsersResult.tables).toEqual(allUsers.tables);
    expect(allUsersResult.result).toHaveLength(3);
    expect((allUsersResult.result as SqlRow[]).some((row) => row.name === "charlie")).toBe(true);
  });

  test("mutate insertPost recomputes userPosts and postCount but not allUsers", async () => {
    await engine.query(allUsers);
    await engine.query(userPosts);
    await engine.query(postCount);

    const result = await engine.mutate(insertPost, 1, "world");

    expect(resultsFor(result, userPosts)).toHaveLength(1);
    expect(resultsFor(result, postCount)).toHaveLength(1);
    expect(resultsFor(result, allUsers)).toHaveLength(0);
    expect((resultsFor(result, postCount)[0].result as SqlRow[])[0].total_count).toBe(2);
  });

  test("mutate deleteUser recomputes allUsers and userPosts but not postCount", async () => {
    await engine.query(allUsers);
    await engine.query(userPosts);
    await engine.query(postCount);

    const result = await engine.mutate(deleteUser, 1);

    expect(resultsFor(result, allUsers)).toHaveLength(1);
    expect(resultsFor(result, userPosts)).toHaveLength(1);
    expect(resultsFor(result, postCount)).toHaveLength(0);
    expect(resultsFor(result, allUsers)[0].result as SqlRow[]).toHaveLength(1);
    expect((resultsFor(result, allUsers)[0].result as SqlRow[])[0].name).toBe("bob");
  });

  test("mutate updateUserName publishes tables and recompute entries keep selector tables", async () => {
    await engine.query(allUsers);
    await engine.query(userPosts);

    const result = await engine.mutate(updateUserName, "alice_updated", 1);

    expect(result.tables).toEqual(["users"]);

    const [allUsersResult] = resultsFor(result, allUsers);
    const [userPostsResult] = resultsFor(result, userPosts);

    expect(allUsersResult.tables).toEqual(allUsers.tables);
    expect(userPostsResult.tables).toEqual(userPosts.tables);
    expect((allUsersResult.result as SqlRow[]).some((row) => row.name === "alice_updated")).toBe(
      true,
    );
  });

  test("never-queried selectors are not recomputed", async () => {
    const inactiveSelector: Selector<[], SqlRow[]> = {
      tables: ["users"],
      run: () => storage.query("SELECT * FROM users ORDER BY id"),
    };

    const result = await engine.mutate(insertUser, "dave");

    expect(resultsFor(result, inactiveSelector)).toHaveLength(0);
  });

  test("selectors whose tables do not overlap mutator tables are not recomputed", async () => {
    const postsOnlySql = "SELECT * FROM posts ORDER BY id";
    const postsOnly: Selector<[], SqlRow[]> = {
      tables: readTablesFromSql(postsOnlySql),
      run: () => storage.query(postsOnlySql),
    };

    await engine.query(postsOnly);

    const result = await engine.mutate(insertUser, "dave");

    expect(resultsFor(result, postsOnly)).toHaveLength(0);
  });

  test("tracks unique selector and params combinations", async () => {
    await engine.query(userById, 1);
    await engine.query(userById, 1);
    await engine.query(userById, 2);

    const result = await engine.mutate(updateUserName, "alice_updated", 1);
    const userByIdResults = resultsFor(result, userById);

    expect(userByIdResults).toHaveLength(2);
    expect(userByIdResults.map((entry) => entry.params)).toEqual([[1], [2]]);
  });

  test("query and mutate await async selector and mutator runs", async () => {
    const asyncSelector: Selector<[], SqlRow[]> = {
      tables: ["users"],
      run: () => Promise.resolve(storage.query("SELECT * FROM users ORDER BY id")),
    };
    const asyncMutator: Mutator<[string], MutationMetadata> = {
      tables: ["users"],
      run: (name) => Promise.resolve(storage.execute("INSERT INTO users (name) VALUES (?)", name)),
    };

    const queryResult = await engine.query(asyncSelector);
    const mutationResult = await engine.mutate(asyncMutator, "charlie");

    expect(queryResult).toHaveLength(2);
    expect(mutationResult.metadata.rowsAffected).toBe(1);
    expect(resultsFor(mutationResult, asyncSelector)).toHaveLength(1);
    expect(resultsFor(mutationResult, asyncSelector)[0].result as SqlRow[]).toHaveLength(3);
  });

  test("storage.close works", () => {
    storage.close();
  });
});
