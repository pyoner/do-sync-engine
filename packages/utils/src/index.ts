export type SqlValue = string | number | boolean | null | bigint | Uint8Array;
export type SqlRow = Record<string, SqlValue>;

export interface MutationMetadata {
  rowsAffected: number;
  lastInsertRowid: number | bigint | null;
}

export interface SqlDatabase {
  query(sql: string, ...params: SqlValue[]): SqlRow[];
  execute(sql: string, ...params: SqlValue[]): MutationMetadata;
}

const READ_TABLES_PATTERN = /\b(?:from|join)\s+([a-z_][a-z0-9_]*)/gi;
const WRITE_TABLE_PATTERN =
  /^\s*(?:insert\s+into|replace\s+into|update|delete\s+from)\s+([a-z_][a-z0-9_]*)/i;

export function readTablesFromSql<Table extends string>(sql: string): Set<Table> {
  return new Set(
    Array.from(sql.matchAll(READ_TABLES_PATTERN), (match) => match[1].toLowerCase() as Table),
  );
}

export function writeTablesFromSql<Table extends string>(sql: string): Set<Table> {
  const match = WRITE_TABLE_PATTERN.exec(sql);
  return new Set(match ? [match[1].toLowerCase() as Table] : []);
}
