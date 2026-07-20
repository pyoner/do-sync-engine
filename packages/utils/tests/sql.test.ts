import { describe, expect, test } from "vite-plus/test";
import { readTablesFromSql, writeTablesFromSql } from "../src/index.ts";

describe("readTablesFromSql", () => {
  test("extracts the table after FROM", () => {
    expect(readTablesFromSql("SELECT * FROM users ORDER BY id")).toEqual(new Set(["users"]));
  });

  test("extracts every table across FROM and JOIN", () => {
    const sql = "SELECT * FROM users JOIN posts ON posts.user_id = users.id";
    expect(readTablesFromSql(sql)).toEqual(new Set(["users", "posts"]));
  });

  test("is case-insensitive and normalizes to lowercase", () => {
    expect(readTablesFromSql("select Id from Todos")).toEqual(new Set(["todos"]));
  });
});

describe("writeTablesFromSql", () => {
  test.each([
    ["INSERT INTO users (name) VALUES (?)", "users"],
    ["  REPLACE INTO users (id) VALUES (?)", "users"],
    ["UPDATE users SET name = ? WHERE id = ?", "users"],
    ["DELETE FROM posts WHERE id = ?", "posts"],
  ])("extracts the target of %s", (sql, table) => {
    expect(writeTablesFromSql(sql)).toEqual(new Set([table]));
  });

  test("returns an empty set for read-only statements", () => {
    expect(writeTablesFromSql("SELECT * FROM users")).toEqual(new Set());
  });
});
