export type SqlValue = string | number | boolean | null | bigint | Uint8Array;
export type SqlRow = Record<string, SqlValue>;

export interface SqlAnalysis {
  statementType: "select" | "insert" | "update" | "delete";
  readTables: Set<string>;
  writtenTables: Set<string>;
  ast: unknown;
}

export interface Selector {
  name: string;
  sql: string;
  readTables: Set<string>;
  ast: unknown;
}

export interface Mutator {
  name: string;
  sql: string;
  operation: "insert" | "update" | "delete";
  writtenTables: Set<string>;
  readTables: Set<string>;
  ast: unknown;
}

export interface MutationMetadata {
  rowsAffected: number;
  lastInsertRowid: number | bigint | null;
}

export interface MutationResult {
  mutatorName: string;
  metadata: MutationMetadata;
  recomputedSelectors: string[];
  recomputeResults: Record<string, SqlRow[]>;
}

export interface SyncStorage {
  query(sql: string, ...params: SqlValue[]): SqlRow[];
  execute(sql: string, ...params: SqlValue[]): MutationMetadata;
}
