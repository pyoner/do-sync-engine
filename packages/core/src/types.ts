export type MaybePromise<T> = T | PromiseLike<T>;
export type TableKey = string;
export type SubscriptionId = number;
export type OperationKey = string;

export interface Selector<
  Params extends unknown[] = unknown[],
  Result = unknown,
  Table = TableKey,
> {
  tables: readonly Table[];
  run(...params: Params): MaybePromise<Result>;
}

export interface Mutator<
  Params extends unknown[] = unknown[],
  Metadata = unknown,
  Table = TableKey,
> {
  tables: readonly Table[];
  run(...params: Params): MaybePromise<Metadata>;
}

export type SelectorRegistry<Table = TableKey> = Record<string, Selector<any[], unknown, Table>>;

export type MutatorRegistry<Table = TableKey> = Record<string, Mutator<any[], unknown, Table>>;

export interface SubscriptionState<SelectorName extends string = string> {
  id: SubscriptionId;
  selector: SelectorName;
  params: readonly unknown[];
}

export interface BrokerSnapshot<SelectorName extends string = string> {
  nextSubscriptionId: SubscriptionId;
  subscriptions: readonly SubscriptionState<SelectorName>[];
}

export interface SelectionResult<SelectorName extends string = string, Result = unknown> {
  subscriptionId: SubscriptionId;
  selector: SelectorName;
  params: readonly unknown[];
  result: Result;
}

export interface PublishResult<
  Metadata = unknown,
  SelectorName extends string = string,
  Result = unknown,
> {
  metadata: Metadata;
  selections: readonly SelectionResult<SelectorName, Result>[];
}

export interface SyncEngineOptions<Table = TableKey> {
  selectors: SelectorRegistry<Table>;
  mutators: MutatorRegistry<Table>;
  snapshot?: BrokerSnapshot;
}

export interface Broker {
  subscribe(selector: OperationKey, params?: readonly unknown[]): SubscriptionId;
  unsubscribe(subscriptionId: SubscriptionId): boolean;
  publish(mutator: OperationKey, ...params: unknown[]): Promise<PublishResult>;
  snapshot(): BrokerSnapshot;
}
