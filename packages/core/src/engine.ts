import type {
  Broker,
  Mutator,
  Selector,
  SubscribeCallback,
  SubscriptionId,
  TableKey,
} from "./types";

interface TrackedSelectorCall<Table> {
  selector: Selector<unknown[], unknown, Table>;
  params: readonly unknown[];
  tables: readonly Table[];
  callback?: SubscribeCallback<unknown[], unknown, Table>;
}

export class SyncEngine<Table = TableKey> implements Broker<Table> {
  private nextSubscriptionId: SubscriptionId = 1;
  private subscriptions = new Map<SubscriptionId, TrackedSelectorCall<Table>>();

  subscribe<Result>(
    selector: Selector<[], Result, Table>,
    params?: [],
    callback?: SubscribeCallback<[], Result, Table>,
  ): SubscriptionId;
  subscribe<Params extends [unknown, ...unknown[]], Result>(
    selector: Selector<Params, Result, Table>,
    params: Params,
    callback?: SubscribeCallback<Params, Result, Table>,
  ): SubscriptionId;
  subscribe<Params extends unknown[], Result>(
    selector: Selector<Params, Result, Table>,
    ...rest: Params extends []
      ? [params?: [], callback?: SubscribeCallback<[], Result, Table>]
      : [params: Params, callback?: SubscribeCallback<Params, Result, Table>]
  ): SubscriptionId {
    const [params, callback] = rest as [
      Params | undefined,
      SubscribeCallback<Params, Result, Table> | undefined,
    ];
    const subscriptionId = this.nextSubscriptionId;
    this.nextSubscriptionId += 1;
    const subscription: TrackedSelectorCall<Table> = {
      selector: selector as Selector<unknown[], unknown, Table>,
      params: [...(params ?? [])],
      tables: [...selector.tables],
    };
    if (callback !== undefined) {
      subscription.callback = callback as SubscribeCallback<unknown[], unknown, Table>;
    }
    this.subscriptions.set(subscriptionId, subscription);
    return subscriptionId;
  }

  unsubscribe(subscriptionId: SubscriptionId): boolean {
    return this.subscriptions.delete(subscriptionId);
  }

  async publish<Params extends unknown[], Metadata>(
    mutator: Mutator<Params, Metadata, Table>,
    ...params: Params
  ): Promise<void> {
    await mutator.run(...params);
    const tables = [...new Set(mutator.tables)];
    const touchedTables = new Set(tables);

    // oxlint-disable-next-line unicorn/no-useless-spread -- snapshot required for unsubscribe-safe publish iteration
    for (const [subscriptionId, subscription] of [...this.subscriptions]) {
      if (!this.subscriptions.has(subscriptionId)) {
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
