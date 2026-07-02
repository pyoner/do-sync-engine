import type { MutationResult, Mutator, Selector, TableKey } from "./types";

interface TrackedSelectorCall<Table, Result = unknown> {
  selector: Selector<unknown[], Result, Table>;
  params: readonly unknown[];
  tables: readonly Table[];
}

export class SyncEngine<Table = TableKey> {
  private selectorIds = new WeakMap<object, number>();
  private nextSelectorId = 1;
  private trackedSelectors = new Map<string, TrackedSelectorCall<Table>>();

  async query<Params extends unknown[], Result>(
    selector: Selector<Params, Result, Table>,
    ...params: Params
  ): Promise<Result> {
    const selectorId = this.getSelectorId(selector);
    const paramsHash = await this.hashParams(params);
    const result = await selector.run(...params);

    this.trackedSelectors.set(`${selectorId}:${paramsHash}`, {
      selector: selector as Selector<unknown[], Result, Table>,
      params: [...params],
      tables: [...selector.tables],
    });

    return result;
  }

  async mutate<Params extends unknown[], Metadata>(
    mutator: Mutator<Params, Metadata, Table>,
    ...params: Params
  ): Promise<MutationResult<Metadata, unknown, Table>> {
    const metadata = await mutator.run(...params);
    const tables = [...new Set(mutator.tables)];
    const touchedTables = new Set(tables);
    const recomputedSelectors: MutationResult<Metadata, unknown, Table>["recomputedSelectors"] = [];

    for (const tracked of this.trackedSelectors.values()) {
      const isAffected = tracked.tables.some((table) => touchedTables.has(table));
      if (!isAffected) {
        continue;
      }

      const result = await tracked.selector.run(...tracked.params);
      recomputedSelectors.push({
        selector: tracked.selector,
        params: tracked.params,
        tables: tracked.tables,
        result,
      });
    }

    return {
      metadata,
      tables,
      recomputedSelectors,
    };
  }

  private getSelectorId(selector: object): number {
    const existingId = this.selectorIds.get(selector);
    if (existingId !== undefined) {
      return existingId;
    }

    const nextId = this.nextSelectorId;
    this.nextSelectorId += 1;
    this.selectorIds.set(selector, nextId);
    return nextId;
  }

  private async hashParams(params: readonly unknown[]): Promise<string> {
    const input = new TextEncoder().encode(this.stableStringify(params));
    const digest = await globalThis.crypto.subtle.digest("SHA-256", input);
    return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join(
      "",
    );
  }

  private stableStringify(value: unknown): string {
    const seen = new Set<object>();

    const visit = (current: unknown): string => {
      if (current === null) {
        return "null";
      }

      switch (typeof current) {
        case "string":
          return JSON.stringify(current);
        case "boolean":
          return current ? "true" : "false";
        case "number":
          if (!Number.isFinite(current)) {
            throw new TypeError("Selector params must be serializable");
          }
          return Object.is(current, -0) ? "0" : String(current);
        case "bigint":
          return `{"$type":"bigint","value":${JSON.stringify(current.toString())}}`;
        case "undefined":
        case "function":
        case "symbol":
          throw new TypeError("Selector params must be serializable");
        case "object": {
          if (current instanceof Date) {
            if (Number.isNaN(current.getTime())) {
              throw new TypeError("Selector params must be serializable");
            }
            return `{"$type":"date","value":${JSON.stringify(current.toISOString())}}`;
          }

          if (current instanceof Uint8Array) {
            return `{"$type":"uint8array","value":[${Array.from(current, (byte) => String(byte)).join(",")}]}`;
          }

          if (Array.isArray(current)) {
            if (seen.has(current)) {
              throw new TypeError("Selector params must be serializable");
            }
            seen.add(current);
            const serialized = `[${current.map((entry) => visit(entry)).join(",")}]`;
            seen.delete(current);
            return serialized;
          }

          const prototype = Object.getPrototypeOf(current);
          if (prototype !== Object.prototype && prototype !== null) {
            throw new TypeError("Selector params must be serializable");
          }

          if (seen.has(current)) {
            throw new TypeError("Selector params must be serializable");
          }
          seen.add(current);
          const keys = Object.keys(current).sort();
          const serialized = `{${keys
            .map(
              (key) => `${JSON.stringify(key)}:${visit((current as Record<string, unknown>)[key])}`,
            )
            .join(",")}}`;
          seen.delete(current);
          return serialized;
        }
        default:
          throw new TypeError("Selector params must be serializable");
      }
    };

    return visit(value);
  }
}
