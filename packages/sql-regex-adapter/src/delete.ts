import { extractTables, identifier } from "./rules";

const target = new RegExp(
  `\\bdelete\\s+from\\s+((?:${identifier})(?:\\s*\\.\\s*(?:${identifier}))*)`,
  "i",
);
export function deleteTables(sql: string): string[] {
  return extractTables(sql, [target]);
}
