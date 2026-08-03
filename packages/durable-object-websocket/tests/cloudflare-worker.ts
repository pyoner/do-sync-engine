declare global {
  namespace Cloudflare {
    interface Env {
      FIXTURE_SYNC_OBJECT: DurableObjectNamespace<FixtureSyncObject>;
    }
  }
}
import { Effect } from "effect";
import { SyncEngine, toTables } from "@do-sync-engine/core";
import type { Mutation, Query, SyncEngineInterface } from "@do-sync-engine/core";
import { DurableObjectWebSocket } from "../src/index.ts";

type FixtureQueries = { counter: Query<[string], { key: string; value: number }> };
type FixtureMutations = {
  increment: Mutation<[string, number], void>;
  fail: Mutation<[], void, Error>;
};
export class FixtureSyncObject extends DurableObjectWebSocket<
  Cloudflare.Env,
  FixtureQueries,
  FixtureMutations
> {
  constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
    super(ctx, env, () => {
      ctx.storage.sql.exec(
        "CREATE TABLE IF NOT EXISTS counters (key TEXT PRIMARY KEY, value INTEGER NOT NULL)",
      );
      const queries = {
        counter: {
          tables: toTables(["counters"]),
          run: (key: string) =>
            Effect.sync(() => ({
              key,
              value: Number(
                ctx.storage.sql
                  .exec<{ value: number }>("SELECT value FROM counters WHERE key = ?", key)
                  .toArray()[0]?.value ?? 0,
              ),
            })),
        },
      } satisfies FixtureQueries;
      const mutations = {
        increment: {
          tables: toTables(["counters"]),
          run: (key: string, amount: number) =>
            Effect.sync(() => {
              ctx.storage.sql.exec(
                "INSERT INTO counters (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = value + excluded.value",
                key,
                amount,
              );
            }),
        },
        fail: {
          tables: toTables([]),
          run: () => Effect.fail(new Error("fixture mutation failed")),
        },
      } satisfies FixtureMutations;
      return {
        engine: new SyncEngine({ queries, mutations }) as unknown as SyncEngineInterface<
          FixtureQueries,
          FixtureMutations
        >,
      };
    });
  }
}
export default {
  fetch(request: Request, env: Cloudflare.Env): Promise<Response> {
    return env.FIXTURE_SYNC_OBJECT.getByName("default").fetch(request);
  },
};
