export interface MutationMetadata {
  rowsAffected: number;
  lastInsertRowid: number;
}

export type SqlValue = string | number | boolean | null | bigint | Uint8Array;
export type SqlRow = Record<string, SqlValue>;

export class DurableObjectSqlStorage {
  private sql: SqlStorage;

  constructor(sql: SqlStorage) {
    this.sql = sql;
  }

  query(sql: string, ...params: SqlValue[]): SqlRow[] {
    return this.sql.exec(sql, ...params).toArray() as SqlRow[];
  }

  execute(sql: string, ...params: SqlValue[]): MutationMetadata {
    this.sql.exec(sql, ...params);
    const meta = this.sql
      .exec<{ rowsAffected: number; lastInsertRowid: number }>(
        "SELECT changes() AS rowsAffected, last_insert_rowid() AS lastInsertRowid",
      )
      .one();
    return {
      rowsAffected: Number(meta.rowsAffected),
      lastInsertRowid: Number(meta.lastInsertRowid),
    };
  }
}
