import { Effect, Exit, Option } from "effect";
import { describe, expect, test } from "vite-plus/test";
import { DatabaseSync } from "node:sqlite";
import { createAdapter } from "../src/index";
import { fixtures, operations, type Fixture } from "./fixture";

const runEffect = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(effect);

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
      test(testData.sql, async () => {
        const db = database(fixture.setup);
        try {
          const adapter = await runEffect(createAdapter(db));
          const op = await runEffect(adapter(testData.sql));
          expect(op.tables).toEqual(new Set(testData.tables));
          const result = await runEffect(op.run(...(testData.params ?? [])));
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
test("executes through Cloudflare SqlStorage", async () => {
  const calls: unknown[][] = [];
  const adapter = await runEffect(
    createAdapter({
      exec(sql: string, ...params: (string | number | null)[]) {
        calls.push([sql, ...params]);
        return { rowsWritten: 1 };
      },
    }),
  );
  const op = await runEffect(adapter("SELECT * FROM users"));
  expect(await runEffect(op.run("Ada"))).toEqual({ rowsWritten: 1 });
  expect(calls).toEqual([["SELECT * FROM users", "Ada"]]);
});

test("rejects unsupported SQL and databases", async () => {
  const db = database();
  try {
    const invalidSql = await Effect.runPromiseExit(
      Effect.flatMap(createAdapter(db), (adapter) => adapter("CREATE TABLE users (id integer)")),
    );
    expect(Exit.isFailure(invalidSql)).toBe(true);
    if (Exit.isFailure(invalidSql)) {
      const error = Exit.findErrorOption(invalidSql);
      expect(Option.isSome(error)).toBe(true);
      if (Option.isSome(error)) {
        expect(error.value._tag).toBe("SqlAdapterError");
        expect(error.value.operation).toBe("adapter");
        expect(error.value.cause).toBeInstanceOf(TypeError);
      }
    }
  } finally {
    db.close();
  }
  const invalidDatabase = await Effect.runPromiseExit(createAdapter(Object.create(null)));
  expect(Exit.isFailure(invalidDatabase)).toBe(true);
  if (Exit.isFailure(invalidDatabase)) {
    const error = Exit.findErrorOption(invalidDatabase);
    expect(Option.isSome(error)).toBe(true);
    if (Option.isSome(error)) {
      expect(error.value._tag).toBe("SqlAdapterError");
      expect(error.value.operation).toBe("adapter");
      expect(error.value.cause).toBeInstanceOf(TypeError);
    }
  }
});
