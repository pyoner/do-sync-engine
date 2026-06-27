import { expect, test } from "vite-plus/test";
import { analyzeSql } from "../src/analyze.js";

test("SELECT * FROM users", () => {
  const result = analyzeSql("SELECT * FROM users");
  expect(result.statementType).toBe("select");
  expect(result.readTables).toEqual(new Set(["users"]));
  expect(result.writtenTables).toEqual(new Set());
});

test("SELECT with JOIN across multiple tables", () => {
  const result = analyzeSql("SELECT u.id, p.title FROM users u JOIN posts p ON u.id = p.user_id");
  expect(result.statementType).toBe("select");
  expect(result.readTables).toEqual(new Set(["users", "posts"]));
});

test("INSERT INTO users", () => {
  const result = analyzeSql(`INSERT INTO users (id, name) VALUES (1, 'alice')`);
  expect(result.statementType).toBe("insert");
  expect(result.writtenTables).toEqual(new Set(["users"]));
  expect(result.readTables).toEqual(new Set());
});

test("UPDATE users SET name", () => {
  const result = analyzeSql(`UPDATE users SET name = 'bob' WHERE id = 1`);
  expect(result.statementType).toBe("update");
  expect(result.writtenTables).toEqual(new Set(["users"]));
});

test("DELETE FROM users", () => {
  const result = analyzeSql("DELETE FROM users WHERE id = 1");
  expect(result.statementType).toBe("delete");
  expect(result.writtenTables).toEqual(new Set(["users"]));
});

test("DELETE with subquery reads", () => {
  const result = analyzeSql("DELETE FROM users WHERE id IN (SELECT user_id FROM posts)");
  expect(result.statementType).toBe("delete");
  expect(result.writtenTables).toEqual(new Set(["users"]));
  expect(result.readTables).toEqual(new Set(["posts"]));
});

test("Parameterized SELECT", () => {
  const result = analyzeSql("SELECT * FROM users WHERE id = ?");
  expect(result.statementType).toBe("select");
  expect(result.readTables).toEqual(new Set(["users"]));
});

test("AST is returned", () => {
  const result = analyzeSql("SELECT * FROM users");
  expect(result.ast).toBeDefined();
});
