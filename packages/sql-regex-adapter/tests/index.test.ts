import { readFileSync } from "node:fs";
import { describe, expect, test } from "vite-plus/test";
import { createAdapter } from "../src/index.ts";
import { DatabaseSync } from "node:sqlite";

type Fixture = { sql: string; tables: string[] };
function fixtures(name: string): Fixture[] {
  const lines = readFileSync(new URL(`./fixtures/${name}.yaml`, import.meta.url), "utf8").split(
    "\n",
  );
  return lines.reduce<Fixture[]>((items, line) => {
    if (line.startsWith("- sql: ")) items.push({ sql: line.slice(7), tables: [] });
    else if (line.trimStart().startsWith("tables: "))
      items.at(-1)!.tables = line.slice(line.indexOf("[") + 1, -1).split(", ");
    return items;
  }, []);
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
    for (const fixture of fixtures(operation)) {
      test(fixture.sql, () => {
        expect(nodeAdapter(fixture.sql).tables).toEqual(new Set(fixture.tables));
      });
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

test("executes against node:sqlite", () => {
  const db = new DatabaseSync(":memory:");
  db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
  const adapter = createAdapter(db);
  expect(adapter("INSERT INTO users (name) VALUES (?)").run("Ada")).toMatchObject({ changes: 1 });
  expect(adapter("SELECT * FROM users").run()).toEqual([{ id: 1, name: "Ada" }]);
  db.close();
});
