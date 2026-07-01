import { Parser } from "node-sql-parser";
import type { Selector, Mutator } from "./types";

const parser = new Parser();
const OPT = { database: "SQLite" } as const;

type SupportedOperation = Selector["operation"] | Mutator["operation"];

export const SUPPORTED_OPERATIONS: SupportedOperation[] = [
  "select",
  "insert",
  "update",
  "delete",
] as const;

function getSupportedOperation(
  name: string,
  ast: { type?: string } | { type?: string }[],
): SupportedOperation {
  if (Array.isArray(ast)) {
    throw new Error(`SQL "${name}" must contain exactly one statement`);
  }
  const t = ast.type;

  if (t && SUPPORTED_OPERATIONS.includes(t as unknown as SupportedOperation)) {
    return t as SupportedOperation;
  }

  throw new Error(
    `SQL "${name}" must be a ${SUPPORTED_OPERATIONS.join(", ").toUpperCase()} statement, got ${(t ?? "UNKNOWN").toUpperCase()}`,
  );
}

export function analyzeSql(name: string, sql: string): Selector | Mutator {
  const { tableList, ast } = parser.parse(sql, OPT);
  const operation = getSupportedOperation(name, ast);
  const tables = new Set<string>();

  for (const entry of tableList) {
    const [action, , table] = entry.split("::");
    if (operation === action) {
      tables.add(table);
    }
  }

  const normalizedSql = parser.sqlify(ast, OPT);
  return { name, sql: normalizedSql, operation, tables };
}
