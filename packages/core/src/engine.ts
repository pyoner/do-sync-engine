import type { Broker, Mutator, Selector, TableKey, Unsubscribe } from "./types";

interface TrackedSelectorCall<Table, Result = unknown> {
  selector: Selector<unknown[], Result, Table>;
  params: readonly unknown[];
  tables: readonly Table[];
}

export class SyncEngine<Table = TableKey> implements Broker<Table> {
  private subscriptions = new Set<TrackedSelectorCall<Table>>();

  subscribe<Params extends unknown[], Result>(
    selector: Selector<Params, Result, Table>,
    ...params: Params
  ): Unsubscribe {
    const subscription: TrackedSelectorCall<Table> = {
      selector: selector as Selector<unknown[], unknown, Table>,
      params: [...params],
      tables: [...selector.tables],
    };
    this.subscriptions.add(subscription);
    return () => {
      this.subscriptions.delete(subscription);
    };
  }

  async publish<Params extends unknown[], Metadata>(
    mutator: Mutator<Params, Metadata, Table>,
    ...params: Params
  ): Promise<void> {
    await mutator.run(...params);
    const tables = [...new Set(mutator.tables)];
    const touchedTables = new Set(tables);

    // oxlint-disable-next-line unicorn/no-useless-spread -- snapshot required for unsubscribe-safe publish iteration
    for (const subscription of [...this.subscriptions]) {
      if (!this.subscriptions.has(subscription)) {
        continue;
      }

      const isAffected = subscription.tables.some((table) => touchedTables.has(table));
      if (!isAffected) {
        continue;
      }

      const result = await subscription.selector.run(...subscription.params);
      await subscription.selector.callback(result);
    }
  }
}
