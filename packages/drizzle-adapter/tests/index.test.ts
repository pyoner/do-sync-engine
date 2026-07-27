import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, test } from "vite-plus/test";
import { eq, sql } from "drizzle-orm";
import { drizzle } from "drizzle-orm/node-sqlite";
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";
import { adapter } from "../src/index.ts";

const users = sqliteTable("users", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  name: text("name").notNull(),
});

const databases: DatabaseSync[] = [];
function database() {
  const sqlite = new DatabaseSync(":memory:");
  databases.push(sqlite);
  sqlite.exec("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)");
  return drizzle({ client: sqlite });
}

afterEach(() => {
  for (const database of databases.splice(0)) database.close();
});

describe("Drizzle SQLite adapter", () => {
  test("adapts and runs a real select builder", () => {
    const db = database();
    db.insert(users).values({ name: "Ada" }).run();
    const query = adapter(db.select().from(users).where(eq(users.name, "Ada")));

    expect(query.tables).toEqual(new Set(["users"]));
    const result = query.run();
    expect(result).toEqual([{ id: 1, name: "Ada" }]);
    expect(result).not.toBeInstanceOf(Promise);
  });

  test("adapts and runs a real insert builder", () => {
    const db = database();
    const mutation = adapter(db.insert(users).values({ name: sql.placeholder("name") }));

    expect(mutation.tables).toEqual(new Set(["users"]));
    expect(mutation.run({ name: "Ada" }).changes).toBe(1);
    expect(db.select().from(users).all()).toEqual([{ id: 1, name: "Ada" }]);
  });

  test("adapts and runs a real update builder", () => {
    const db = database();
    db.insert(users).values({ name: "Ada" }).run();
    const mutation = adapter(db.update(users).set({ name: "Grace" }).where(eq(users.id, 1)));
    expect(mutation.tables).toEqual(new Set(["users"]));
    expect(mutation.run().changes).toBe(1);
    expect(db.select().from(users).all()).toEqual([{ id: 1, name: "Grace" }]);
  });

  test("adapts and runs a real delete builder", () => {
    const db = database();
    db.insert(users).values({ name: "Ada" }).run();
    const mutation = adapter(db.delete(users).where(eq(users.id, 1)));

    expect(mutation.tables).toEqual(new Set(["users"]));
    expect(mutation.run().changes).toBe(1);
    expect(db.select().from(users).all()).toEqual([]);
  });
});
