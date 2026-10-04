import { defineConfig } from "vite-plus";
import { cloudflareTest } from "@cloudflare/vitest-pool-workers";

export default defineConfig({
  pack: {
    exports: {
      devExports: true,
      customExports(exports, { isPublish }) {
        if (!isPublish) return exports;
        return Object.fromEntries(
          Object.entries(exports).map(([key, value]) => [
            key,
            typeof value === "string" && value.endsWith(".mjs")
              ? { types: value.replace(/\.mjs$/, ".d.mts"), default: value }
              : value,
          ]),
        );
      },
    },
  },
  lint: {
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  fmt: {},
  test: {
    fileParallelism: false,
    projects: [
      {
        test: {
          name: "node",
          include: ["tests/index.test.ts", "tests/engine.test.ts"],
        },
      },
      {
        plugins: [
          cloudflareTest({
            main: "./tests/cloudflare-worker.ts",
            miniflare: {
              compatibilityDate: "2026-06-27",
              durableObjects: {
                FIXTURE_DATABASE: {
                  className: "FixtureDatabase",
                  useSQLite: true,
                },
              },
            },
          }),
        ],
        test: {
          name: "cloudflare",
          include: ["tests/cloudflare.test.ts"],
        },
      },
    ],
  },
});
