import { expect, test } from "vite-plus/test";
import { analyzeSql } from "../src/analyze.js";

test("SELECT * FROM users", () => {
  const result = analyzeSql("test", "SELECT * FROM users");
  expect(result.operation).toBe("select");
  expect(result.tables).toEqual(new Set(["users"]));
});

test("SELECT with JOIN across multiple tables", () => {
  const result = analyzeSql(
    "test",
    "SELECT u.id, p.title FROM users u JOIN posts p ON u.id = p.user_id",
  );
  expect(result.operation).toBe("select");
  expect(result.tables).toEqual(new Set(["users", "posts"]));
});

test("INSERT INTO users", () => {
  const result = analyzeSql("test", `INSERT INTO users (id, name) VALUES (1, 'alice')`);
  expect(result.operation).toBe("insert");
  expect(result.tables).toEqual(new Set(["users"]));
});

test("UPDATE users SET name", () => {
  const result = analyzeSql("test", `UPDATE users SET name = 'bob' WHERE id = 1`);
  expect(result.operation).toBe("update");
  expect(result.tables).toEqual(new Set(["users"]));
});

test("DELETE FROM users", () => {
  const result = analyzeSql("test", "DELETE FROM users WHERE id = 1");
  expect(result.operation).toBe("delete");
  expect(result.tables).toEqual(new Set(["users"]));
});

test("DELETE with subquery reads", () => {
  const result = analyzeSql("test", "DELETE FROM users WHERE id IN (SELECT user_id FROM posts)");
  expect(result.operation).toBe("delete");
  expect(result.tables).toEqual(new Set(["users"]));
});

test("Parameterized SELECT", () => {
  const result = analyzeSql("test", "SELECT * FROM users WHERE id = ?");
  expect(result.operation).toBe("select");
  expect(result.tables).toEqual(new Set(["users"]));
});
