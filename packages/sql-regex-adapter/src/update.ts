import { extractTables, identifier } from "./rules.ts";

const target = new RegExp(`\\bupdate\\s+((?:${identifier})(?:\\s*\\.\\s*(?:${identifier}))*)`, "i");
export function updateTables(sql: string): string[] {
  return extractTables(sql, [target]);
}
