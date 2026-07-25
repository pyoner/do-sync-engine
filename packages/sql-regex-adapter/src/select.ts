import { extractTables } from "./rules.ts";

const identifier = '[A-Za-z_$][\\w$]*|"[^"]+"|`[^`]+`|\\[[^\\]]+\\]';
const from = new RegExp(`\\bfrom\\s+((?:${identifier})(?:\\s*\\.\\s*(?:${identifier}))*)`, "gi");
const join = new RegExp(`\\bjoin\\s+((?:${identifier})(?:\\s*\\.\\s*(?:${identifier}))*)`, "gi");
export function selectTables(sql: string): string[] {
  return extractTables(sql, [from, join]);
}
