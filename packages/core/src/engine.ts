import { analyzeSql } from "./analyze.js";
import type { Selector, Mutator, MutationResult, SqlRow, SqlValue, SyncStorage } from "./types.js";

export class SyncEngine {
  private storage: SyncStorage;
  private selectors = new Map<string, Selector>();
  private mutators = new Map<string, Mutator>();
  private lastParams = new Map<string, SqlValue[]>();
  private tableToSelectors = new Map<string, Set<string>>();

  constructor(storage: SyncStorage) {
    this.storage = storage;
  }

  registerSelector(name: string, sql: string): Selector {
    const analysis = analyzeSql(sql);
    if (analysis.statementType !== "select") {
      throw new Error(`Selector "${name}" must be a SELECT, got ${analysis.statementType}`);
    }
    this.unregisterSelector(name);

    const selector: Selector = {
      name,
      sql,
      readTables: analysis.readTables,
      ast: analysis.ast,
    };
    this.selectors.set(name, selector);

    for (const table of analysis.readTables) {
      let set = this.tableToSelectors.get(table);
      if (!set) {
        set = new Set();
        this.tableToSelectors.set(table, set);
      }
      set.add(name);
    }
    return selector;
  }

  private unregisterSelector(name: string): void {
    const old = this.selectors.get(name);
    if (!old) return;
    for (const table of old.readTables) {
      const set = this.tableToSelectors.get(table);
      set?.delete(name);
      if (set && set.size === 0) this.tableToSelectors.delete(table);
    }
    this.selectors.delete(name);
  }

  registerMutator(name: string, sql: string): Mutator {
    const analysis = analyzeSql(sql);
    if (analysis.writtenTables.size === 0) {
      throw new Error(`Mutator "${name}" must be an INSERT/UPDATE/DELETE`);
    }
    const mutator: Mutator = {
      name,
      sql,
      operation: analysis.statementType as "insert" | "update" | "delete",
      writtenTables: analysis.writtenTables,
      readTables: analysis.readTables,
      ast: analysis.ast,
    };
    this.mutators.set(name, mutator);
    return mutator;
  }

  query(name: string, ...params: SqlValue[]): SqlRow[] {
    const selector = this.selectors.get(name);
    if (!selector) throw new Error(`Selector "${name}" not registered`);
    this.lastParams.set(name, params);
    return this.storage.query(selector.sql, ...params);
  }

  mutate(name: string, ...params: SqlValue[]): MutationResult {
    const mutator = this.mutators.get(name);
    if (!mutator) throw new Error(`Mutator "${name}" not registered`);

    const metadata = this.storage.execute(mutator.sql, ...params);

    const affected = new Set<string>();
    for (const table of mutator.writtenTables) {
      const selectorSet = this.tableToSelectors.get(table);
      if (selectorSet) for (const s of selectorSet) affected.add(s);
    }

    const recomputeResults: Record<string, SqlRow[]> = {};
    for (const selectorName of affected) {
      const p = this.lastParams.get(selectorName) ?? [];
      recomputeResults[selectorName] = this.storage.query(
        this.selectors.get(selectorName)!.sql,
        ...p,
      );
    }

    return {
      mutatorName: name,
      metadata,
      recomputedSelectors: [...affected],
      recomputeResults,
    };
  }
}
