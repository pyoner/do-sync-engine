import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    dts: {
      tsgo: true,
    },
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
});
