import { defineConfig } from "vite-plus";
import { cloudflareTest } from "@cloudflare/vitest-pool-workers";

export default defineConfig({
  pack: {
    entry: ["src/client.ts", "src/server.ts"],
    exports: false,
    deps: { neverBundle: ["@do-sync-engine/core", "cloudflare:workers"] },
  },
  lint: { options: { typeAware: true, typeCheck: true } },
  fmt: {},
  test: {
    fileParallelism: false,
    projects: [
      {
        test: { name: "cloudflare", include: ["tests/index.test.ts"] },
        plugins: [
          cloudflareTest({
            main: "./tests/cloudflare-worker.ts",
            miniflare: {
              compatibilityDate: "2026-07-28",
              durableObjects: {
                FIXTURE_SYNC_OBJECT: { className: "FixtureSyncObject", useSQLite: true },
              },
            },
          }),
        ],
      },
    ],
  },
});
