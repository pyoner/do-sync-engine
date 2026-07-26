import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, test } from "vite-plus/test";
import { createAdapter } from "../src/index.ts";
import type { FixtureDatabase } from "./cloudflare-worker.ts";
import { fixtures, operations } from "./fixture.ts";

const { FIXTURE_DATABASE } = env as {
  FIXTURE_DATABASE: DurableObjectNamespace<FixtureDatabase>;
};

for (const operation of operations) {
  describe(`${operation} SQL`, () => {
    const fixture = fixtures(operation);
    for (const testData of fixture.testData) {
      test(testData.sql, async () => {
        const stub = FIXTURE_DATABASE.get(FIXTURE_DATABASE.newUniqueId());
        await runInDurableObject(stub, (_instance, state) => {
          for (const statement of fixture.setup.database) state.storage.sql.exec(statement);
          for (const statement of fixture.setup.seed) state.storage.sql.exec(statement);
          const op = createAdapter(state.storage.sql)(testData.sql);
          expect(op.tables).toEqual(new Set(testData.tables));
          const result = op.run() as { rowsWritten: number; toArray(): unknown[] };
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
