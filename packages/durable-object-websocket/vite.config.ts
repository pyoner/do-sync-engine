import { defineConfig } from "vite-plus";
import { cloudflareTest } from "@cloudflare/vitest-pool-workers";

export default defineConfig({
  pack: { exports: true, external: ["@do-sync-engine/core"] },
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
