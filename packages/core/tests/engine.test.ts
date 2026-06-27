import { expect, test, describe, beforeEach } from "vite-plus/test";
import { SyncEngine } from "../src/engine.js";
import { NodeSqliteStorage } from "./helpers.js";
import type { SqlRow } from "../src/index.js";

function setupDb(storage: NodeSqliteStorage) {
  storage.exec("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)");
  storage.exec("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT)");
  storage.exec(`INSERT INTO users (name) VALUES ('alice')`);
  storage.exec(`INSERT INTO users (name) VALUES ('bob')`);
  storage.exec(`INSERT INTO posts (user_id, title) VALUES (1, 'hello')`);
}

describe("SyncEngine", () => {
  let storage: NodeSqliteStorage;
  let engine: SyncEngine;

  beforeEach(() => {
    storage = new NodeSqliteStorage();
    setupDb(storage);
    engine = new SyncEngine(storage);

    engine.registerSelector("allUsers", "SELECT * FROM users ORDER BY id");
    engine.registerSelector(
      "userPosts",
      "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id ORDER BY u.id, p.id",
    );
    engine.registerSelector("postCount", "SELECT COUNT(*) as total_count FROM posts");

    engine.registerMutator("insertUser", "INSERT INTO users (name) VALUES (?)");
    engine.registerMutator("deleteUser", "DELETE FROM users WHERE id = ?");
    engine.registerMutator("insertPost", "INSERT INTO posts (user_id, title) VALUES (?, ?)");
    engine.registerMutator("updateUserName", "UPDATE users SET name = ? WHERE id = ?");
  });

  test("query returns rows", () => {
    const rows = engine.query("allUsers");
    expect(rows.length).toBe(2);
    expect(rows[0].name).toBe("alice");
    expect(rows[1].name).toBe("bob");
  });

  test("mutate inserts and recomputes affected selectors", () => {
    const result = engine.mutate("insertUser", "charlie");
    expect(result.metadata.rowsAffected).toBe(1);
    expect(result.recomputedSelectors).toContain("allUsers");
    expect(result.recomputedSelectors).not.toContain("postCount");

    const users = result.recomputeResults.allUsers;
    expect(users.length).toBe(3);
    expect(users.some((r: SqlRow) => r.name === "charlie")).toBe(true);
  });

  test("mutate insertPost recomputes userPosts and postCount but not allUsers", () => {
    const result = engine.mutate("insertPost", 1, "world");
    expect(result.recomputedSelectors).toContain("userPosts");
    expect(result.recomputedSelectors).toContain("postCount");
    expect(result.recomputedSelectors).not.toContain("allUsers");

    const count = result.recomputeResults.postCount;
    expect(count[0].total_count).toBe(2);
  });

  test("mutate deleteUser recomputes allUsers and userPosts but not postCount", () => {
    const result = engine.mutate("deleteUser", 1);
    expect(result.recomputedSelectors).toContain("allUsers");
    expect(result.recomputedSelectors).toContain("userPosts");
    expect(result.recomputedSelectors).not.toContain("postCount");

    const users = result.recomputeResults.allUsers;
    expect(users.length).toBe(1);
    expect(users[0].name).toBe("bob");
  });

  test("mutate updateUserName recomputes allUsers", () => {
    const result = engine.mutate("updateUserName", "alice_updated", 1);
    expect(result.recomputedSelectors).toContain("allUsers");
    const users = result.recomputeResults.allUsers;
    expect(users.some((r: SqlRow) => r.name === "alice_updated")).toBe(true);
  });

  test("never-queried selectors are not recomputed", () => {
    engine.registerSelector("inactiveSelector", "SELECT * FROM posts");
    engine.registerMutator("insertForInactive", "INSERT INTO users (name) VALUES (?)");

    const result = engine.mutate("insertForInactive", "dave");
    // users table written → inactiveSelector reads posts, not users → should NOT be affected
    expect(result.recomputedSelectors).not.toContain("inactiveSelector");
  });

  test("selector reads posts, mutating users does not recompute it", () => {
    engine.registerSelector("postsOnly", "SELECT * FROM posts");
    // postsOnly reads posts, not users → insertUser should not recompute it
    const result = engine.mutate("insertUser", "dave");
    expect(result.recomputedSelectors).not.toContain("postsOnly");
  });

  test("error on registering non-SELECT as selector", () => {
    expect(() => engine.registerSelector("bad", "INSERT INTO users (name) VALUES (?)")).toThrow(
      "SELECT",
    );
  });

  test("error on registering non-mutation as mutator", () => {
    expect(() => engine.registerMutator("bad", "SELECT * FROM users")).toThrow(
      "INSERT/UPDATE/DELETE",
    );
  });

  test("error on querying unregistered selector", () => {
    expect(() => engine.query("nonexistent")).toThrow("not registered");
  });

  test("error on mutating unregistered mutator", () => {
    expect(() => engine.mutate("nonexistent")).toThrow("not registered");
  });

  test("re-registering selector updates dependencies", () => {
    engine.registerSelector("allUsers", "SELECT * FROM posts");
    const result = engine.mutate("insertUser", "dave");
    // allUsers now reads posts → insertUser writes users → no overlap
    expect(result.recomputedSelectors).not.toContain("allUsers");
  });

  test("storage.close works", () => {
    storage.close();
  });
});
