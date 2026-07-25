import { extractTables, identifier } from "./rules.ts";

const target = new RegExp(
  `\\bdelete\\s+from\\s+((?:${identifier})(?:\\s*\\.\\s*(?:${identifier}))*)`,
  "i",
);
const sources = new RegExp(
  `\\b(?:using|join)\\s+((?:${identifier})(?:\\s*\\.\\s*(?:${identifier}))*)`,
  "gi",
);
export function deleteTables(sql: string): string[] {
  return extractTables(sql, [target, sources]);
}
