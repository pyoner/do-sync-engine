import { Table } from "./types";

export function toTables(names: readonly string[]): ReadonlySet<Table> {
  return new Set(names.map(Table));
}
