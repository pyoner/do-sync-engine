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
