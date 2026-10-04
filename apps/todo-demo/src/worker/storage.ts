import { toTables } from "@do-sync-engine/core";
import { Effect } from "effect";
import {
  createAdapter,
  type MutationMetadata,
  type SqlDatabase,
  type SqlRow,
  type SqlValue,
} from "@do-sync-engine/sql-regex-adapter";

export class DurableObjectSqlStorage implements SqlDatabase {
  private sql: SqlStorage;

  constructor(sql: SqlStorage) {
    this.sql = sql;
  }

  tables(statement: string) {
    const operation = Effect.runSync(
      createAdapter(this.sql).pipe(
        Effect.flatMap((adapt) => adapt(statement)),
        Effect.mapError((cause) => new Error("Invalid static SQL", { cause })),
      ),
    );
    return toTables([...operation.tables]);
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
