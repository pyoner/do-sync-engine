import { defineConfig } from "vite-plus";

export default defineConfig({
  staged: {
    "*": "vp check --fix",
  },
  fmt: {
    ignorePatterns: [".repos/**"],
  },
  lint: {
    ignorePatterns: [".repos/**"],
    jsPlugins: [{ name: "vite-plus", specifier: "vite-plus/oxlint-plugin" }],
    rules: { "vite-plus/prefer-vite-plus-imports": "error" },
    options: { typeAware: true, typeCheck: true },
  },
  test: {
    exclude: [
      ".repos/**",
      "**/node_modules/**",
      "apps/todo-demo/test/**",
      "packages/sql-regex-adapter/tests/cloudflare.test.ts",
      "packages/durable-object-websocket/tests/index.test.ts",
    ],
  },
  run: {
    cache: true,
  },
});
