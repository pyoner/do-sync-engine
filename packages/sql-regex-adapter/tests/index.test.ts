import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, test } from "vite-plus/test";
import { createAdapter } from "../src/index.ts";
import { DatabaseSync } from "node:sqlite";
import { parse } from "yaml";

type TestData = { sql: string; tables: string[] };
type Fixture = {
  setup: { database: string[]; seed: string[] };
  testData: TestData[];
};

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string");
}

function isTestData(value: unknown): value is TestData {
  if (typeof value !== "object" || value === null || !("sql" in value) || !("tables" in value))
    return false;
  return typeof value.sql === "string" && isStringArray(value.tables);
}

function isFixture(value: unknown): value is Fixture {
  if (
    typeof value !== "object" ||
    value === null ||
    !("setup" in value) ||
    !("testData" in value) ||
    typeof value.setup !== "object" ||
    value.setup === null ||
    !("database" in value.setup) ||
    !("seed" in value.setup)
  )
    return false;
  return (
    isStringArray(value.setup.database) &&
    isStringArray(value.setup.seed) &&
    Array.isArray(value.testData) &&
    value.testData.every(isTestData)
  );
}

function fixtures(name: string): Fixture {
  const value: unknown = parse(
    readFileSync(resolve(import.meta.dirname, "fixtures", `${name}.yaml`), "utf8"),
  );
  if (!isFixture(value)) throw new TypeError(`Invalid ${name} fixture`);
  return value;
}

const nodeCalls: unknown[][] = [];
const nodeDb = {
  prepare(sql: string) {
    return {
      all: (...params: unknown[]) => {
        nodeCalls.push(["all", sql, ...params]);
        return [{ id: 1 }];
      },
      run: (...params: unknown[]) => {
        nodeCalls.push(["run", sql, ...params]);
        return { changes: 1 };
      },
    };
  },
};
const nodeAdapter = createAdapter(nodeDb);
for (const operation of ["select", "update", "insert", "delete"] as const) {
  describe(`${operation} SQL`, () => {
    const fixture = fixtures(operation);
    for (const testData of fixture.testData) {
      test(testData.sql, () => {
        expect(nodeAdapter(testData.sql).tables).toEqual(new Set(testData.tables));
      });
      if (testData === fixture.testData[0]) {
        test(`${testData.sql} executes in SQLite`, () => {
          const db = new DatabaseSync(":memory:");
          try {
            for (const statement of fixture.setup.database) db.exec(statement);
            for (const statement of fixture.setup.seed) db.exec(statement);
            const result = createAdapter(db)(testData.sql).run();
            if (operation === "select") {
              expect(result).toEqual([{ id: 1, name: "Ada" }]);
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
    }
  });
}

test("executes through Node SQLite methods", () => {
  expect(nodeAdapter("SELECT * FROM users WHERE id = ?").run(42)).toEqual([{ id: 1 }]);
  expect(nodeAdapter("UPDATE users SET name = ?").run("Ada")).toEqual({ changes: 1 });
  expect(nodeCalls.at(-2)).toEqual(["all", "SELECT * FROM users WHERE id = ?", 42]);
  expect(nodeCalls.at(-1)).toEqual(["run", "UPDATE users SET name = ?", "Ada"]);
});

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

test("rejects unsupported SQL", () => {
  expect(() => nodeAdapter("CREATE TABLE users (id integer)")).toThrow(TypeError);
  expect(() => createAdapter(Object.create(null))).toThrow(TypeError);
});
