import type { Table } from "./types";

export function toTables(names: readonly string[]): ReadonlySet<Table> {
  return new Set(names as readonly Table[]);
}
