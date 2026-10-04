import { defineConfig } from "vite-plus";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { cloudflare } from "@cloudflare/vite-plugin";
import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import type { PluginOption } from "vite-plus";
export default defineConfig(({ mode }) => ({
  // First test cold-starts the Durable Object, which loads Effect module-by-module.
  test: { testTimeout: 15_000 },
  plugins: [
    svelte(),
    ...(mode === "test"
      ? [cloudflareTest({ wrangler: { configPath: "./wrangler.jsonc" } })]
      : [cloudflare()]),
  ] as PluginOption[],
}));
