import { DatabaseSync } from "node:sqlite";
import { describe, expect, test } from "vite-plus/test";
import { drizzle } from "drizzle-orm/durable-sqlite";
import { sqliteTable, text } from "drizzle-orm/sqlite-core";
import { adapter } from "../src/index.ts";

const users = sqliteTable("users", { name: text("name") });

describe("Drizzle SQLite adapter", () => {
  test("requires prepared query table metadata", () => {
    const db = new DatabaseSync(":memory:");
    db.exec("CREATE TABLE users (name TEXT)");
    const client = {
      sql: {
        exec() {
          return {
            toArray: () => [],
            raw: () => ({ toArray: () => [] }),
            next: () => ({ done: true, value: undefined }),
          };
        },
      },
    };
    const query = drizzle(client as never)
      .select()
      .from(users);
    expect(() => adapter(query)).toThrowError(
      new TypeError("adapter() could not read Drizzle table metadata"),
    );
  });

  test("rejects async prepared queries", () => {
    expect(() =>
      adapter({
        prepare: () => ({ mode: "async", queryMetadata: { tables: ["users"] } }),
      } as never),
    ).toThrowError(new TypeError("adapter() requires a synchronous Drizzle SQLite builder"));
  });
});
