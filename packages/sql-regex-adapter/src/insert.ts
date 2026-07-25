import { extractTables } from "./rules.ts";

const identifier = '[A-Za-z_$][\\w$]*|"[^"]+"|`[^`]+`|\\[[^\\]]+\\]';
const target = new RegExp(
  `\\binsert\\s+into\\s+((?:${identifier})(?:\\s*\\.\\s*(?:${identifier}))*)`,
  "i",
);
const sources = new RegExp(
  `\\b(?:from|join)\\s+((?:${identifier})(?:\\s*\\.\\s*(?:${identifier}))*)`,
  "gi",
);
export function insertTables(sql: string): string[] {
  return extractTables(sql, [target, sources]);
}
