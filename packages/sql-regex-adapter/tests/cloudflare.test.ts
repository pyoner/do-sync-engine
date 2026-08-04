import { Effect } from "effect";
import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, test } from "vite-plus/test";
import { createAdapter } from "../src/index";
import type { FixtureDatabase } from "./cloudflare-worker";
import { fixtures, operations } from "./fixture";

const { FIXTURE_DATABASE } = env as {
  FIXTURE_DATABASE: DurableObjectNamespace<FixtureDatabase>;
};

for (const operation of operations) {
  describe(`${operation} SQL`, () => {
    const fixture = fixtures(operation);
    for (const testData of fixture.testData) {
      test(testData.sql, async () => {
        const stub = FIXTURE_DATABASE.get(FIXTURE_DATABASE.newUniqueId());
        await runInDurableObject(stub, async (_instance, state) => {
          for (const statement of fixture.setup.database) state.storage.sql.exec(statement);
          for (const statement of fixture.setup.seed) state.storage.sql.exec(statement);
          const adapter = await Effect.runPromise(createAdapter(state.storage.sql));
          const op = await Effect.runPromise(adapter(testData.sql));
          expect(op.tables).toEqual(new Set(testData.tables));
          const result = (await Effect.runPromise(op.run(...(testData.params ?? [])))) as {
            rowsWritten: number;
            toArray(): unknown[];
          };
          const rows = result.toArray();
          if (operation === "select") {
            expect(rows.length).toBeGreaterThan(0);
          } else {
            expect(result.rowsWritten).toBeGreaterThan(0);
          }
        });
      });
    }
  });
}
