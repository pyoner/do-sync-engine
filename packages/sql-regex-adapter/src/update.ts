import { extractTables, identifier } from "./rules.ts";

const target = new RegExp(`\\bupdate\\s+((?:${identifier})(?:\\s*\\.\\s*(?:${identifier}))*)`, "i");
const sources = new RegExp(
  `\\b(?:from|join)\\s+((?:${identifier})(?:\\s*\\.\\s*(?:${identifier}))*)`,
  "gi",
);
export function updateTables(sql: string): string[] {
  return extractTables(sql, [target, sources]);
}
