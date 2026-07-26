import { describe, expect, test } from "vite-plus/test";
import { DatabaseSync } from "node:sqlite";
import { createAdapter } from "../src/index.ts";
import { fixtures, operations, type Fixture } from "./fixture.ts";

function database(setup?: Fixture["setup"]): DatabaseSync {
  const db = new DatabaseSync(":memory:");
  if (setup) {
    try {
      for (const statement of setup.database) db.exec(statement);
      for (const statement of setup.seed) db.exec(statement);
      return db;
    } catch (error) {
      db.close();
      throw error;
    }
  }
  return db;
}

for (const operation of operations) {
  describe(`${operation} SQL`, () => {
    const fixture = fixtures(operation);
    for (const testData of fixture.testData) {
      test(testData.sql, () => {
        const db = database(fixture.setup);
        try {
          const op = createAdapter(db)(testData.sql);
          expect(op.tables).toEqual(new Set(testData.tables));
          const result = op.run();
          if (operation === "select") {
            expect(result).toBeInstanceOf(Array);
          } else if (typeof result === "object" && result !== null && "changes" in result) {
            expect(result.changes).toBeGreaterThan(0);
          } else {
            throw new TypeError("SQLite mutation returned no changes");
          }
        } finally {
          db.close();
        }
      });
    }
  });
}

test("executes through Cloudflare SqlStorage", () => {
  const calls: unknown[][] = [];
  const adapter = createAdapter({
    exec(sql: string, ...params: unknown[]) {
      calls.push([sql, ...params]);
      return { rowsWritten: 1 };
    },
  });
  expect(adapter("SELECT * FROM users").run("Ada")).toEqual({ rowsWritten: 1 });
  expect(calls).toEqual([["SELECT * FROM users", "Ada"]]);
});

test("rejects unsupported SQL and databases", () => {
  const db = database();
  try {
    expect(() => createAdapter(db)("CREATE TABLE users (id integer)")).toThrow(TypeError);
  } finally {
    db.close();
  }
  expect(() => createAdapter(Object.create(null))).toThrow(TypeError);
});
