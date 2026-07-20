import { DatabaseSync } from "node:sqlite";
import type { MutationMetadata, SqlDatabase, SqlRow, SqlValue } from "@do-sync-engine/utils";

export class NodeSqliteStorage implements SqlDatabase {
  private db: DatabaseSync;

  constructor(path: string = ":memory:") {
    this.db = new DatabaseSync(path);
  }

  exec(sql: string): SqlRow[] {
    this.db.exec(sql);
    return [];
  }

  query(sql: string, ...params: SqlValue[]): SqlRow[] {
    return this.db.prepare(sql).all(...(params as never[])) as SqlRow[];
  }

  execute(sql: string, ...params: SqlValue[]): MutationMetadata {
    const result = this.db.prepare(sql).run(...(params as never[]));
    return {
      rowsAffected: Number(result.changes),
      lastInsertRowid: Number(result.lastInsertRowid),
    };
  }

  close(): void {
    this.db.close();
  }
}
