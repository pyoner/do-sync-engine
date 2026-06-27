import { Parser } from "node-sql-parser";
import type { SqlAnalysis } from "./types.js";

const parser = new Parser();
const OPT = { database: "SQLite" } as const;

export function analyzeSql(sql: string): SqlAnalysis {
  const { tableList, ast } = parser.parse(sql, OPT);

  const readTables = new Set<string>();
  const writtenTables = new Set<string>();
  let statementType: SqlAnalysis["statementType"] = "select";

  for (const entry of tableList) {
    const [action, , table] = entry.split("::");
    if (action === "select") {
      readTables.add(table);
    } else if (action === "insert" || action === "replace") {
      writtenTables.add(table);
      statementType = "insert";
    } else if (action === "update") {
      writtenTables.add(table);
      statementType = "update";
    } else if (action === "delete") {
      writtenTables.add(table);
      statementType = "delete";
    }
  }

  return { statementType, readTables, writtenTables, ast };
}
