import { extractTables, identifier } from "./rules.ts";

const target = new RegExp(
  `\\binsert\\s+into\\s+((?:${identifier})(?:\\s*\\.\\s*(?:${identifier}))*)`,
  "i",
);
export function insertTables(sql: string): string[] {
  return extractTables(sql, [target]);
}
