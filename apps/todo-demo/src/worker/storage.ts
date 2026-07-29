import { Effect } from "effect";
import type { Table } from "@do-sync-engine/core";
import {
  SqlAdapterError,
  type MutationMetadata,
  type SqlAdapter,
  type SqlOperation,
  type SqlRow,
  type SqlValue,
} from "@do-sync-engine/sql-regex-adapter";

export class DurableObjectSqlStorage {
  private readonly sql: SqlStorage;
  private readonly adapter: SqlAdapter;

  constructor(sql: SqlStorage, adapter: SqlAdapter) {
    this.sql = sql;
    this.adapter = adapter;
  }
  tables(statement: string): Effect.Effect<Set<Table>, SqlAdapterError> {
    return Effect.map(this.adapter(statement), (operation: SqlOperation) => operation.tables);
  }

  query(sql: string, ...params: SqlValue[]): Effect.Effect<SqlRow[], SqlAdapterError> {
    return Effect.try({
      try: () => this.sql.exec(sql, ...params).toArray() as SqlRow[],
      catch: (cause) => SqlAdapterError.make({ cause, operation: "query" }),
    });
  }

  execute(sql: string, ...params: SqlValue[]): Effect.Effect<MutationMetadata, SqlAdapterError> {
    return Effect.try({
      try: () => {
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
      },
      catch: (cause) => SqlAdapterError.make({ cause, operation: "execute" }),
    });
  }
}
