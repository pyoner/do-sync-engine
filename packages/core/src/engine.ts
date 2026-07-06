import type { Broker, Mutator, Selector, SubscribeCallback, TableKey, Unsubscribe } from "./types";

interface TrackedSelectorCall<Table> {
  selector: Selector<unknown[], unknown, Table>;
  params: readonly unknown[];
  tables: readonly Table[];
  callback?: SubscribeCallback<unknown[], unknown, Table>;
}

export class SyncEngine<Table = TableKey> implements Broker<Table> {
  private subscriptions = new Set<TrackedSelectorCall<Table>>();

  subscribe<Result>(
    selector: Selector<[], Result, Table>,
    params?: [],
    callback?: SubscribeCallback<[], Result, Table>,
  ): Unsubscribe;
  subscribe<Params extends [unknown, ...unknown[]], Result>(
    selector: Selector<Params, Result, Table>,
    params: Params,
    callback?: SubscribeCallback<Params, Result, Table>,
  ): Unsubscribe;
  subscribe<Params extends unknown[], Result>(
    selector: Selector<Params, Result, Table>,
    ...rest: Params extends []
      ? [params?: [], callback?: SubscribeCallback<[], Result, Table>]
      : [params: Params, callback?: SubscribeCallback<Params, Result, Table>]
  ): Unsubscribe {
    const [params, callback] = rest as [
      Params | undefined,
      SubscribeCallback<Params, Result, Table> | undefined,
    ];
    const subscription: TrackedSelectorCall<Table> = {
      selector: selector as Selector<unknown[], unknown, Table>,
      params: [...(params ?? [])],
      tables: [...selector.tables],
    };
    if (callback !== undefined) {
      subscription.callback = callback as SubscribeCallback<unknown[], unknown, Table>;
    }
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
      await subscription.callback?.(result, subscription.selector, subscription.params);
    }
  }
}
