export type SqlValue = string | number | boolean | null | bigint | Uint8Array;
export type SqlRow = Record<string, SqlValue>;

export interface SqlOperation<Operation extends string = string> {
  name: string;
  sql: string;
  operation: Operation;
  tables: Set<string>;
}

export type Selector = SqlOperation<"select">;

export type Mutator = SqlOperation<"insert" | "update" | "delete">;

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
