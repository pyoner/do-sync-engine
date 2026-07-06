export type MaybePromise<T> = T | PromiseLike<T>;
export type TableKey = string;

export interface Selector<
  Params extends unknown[] = unknown[],
  Result = unknown,
  Table = TableKey,
> {
  tables: readonly Table[];
  run(...params: Params): MaybePromise<Result>;
}

export type SubscribeCallback<
  Params extends unknown[] = unknown[],
  Result = unknown,
  Table = TableKey,
> = (
  result: Result,
  selector: Selector<Params, Result, Table>,
  params: readonly [...Params],
) => MaybePromise<void>;

export interface Mutator<
  Params extends unknown[] = unknown[],
  Metadata = unknown,
  Table = TableKey,
> {
  tables: readonly Table[];
  run(...params: Params): MaybePromise<Metadata>;
}

export type Unsubscribe = () => void;
export interface Broker<Table = TableKey> {
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
  publish<Params extends unknown[], Metadata>(
    mutator: Mutator<Params, Metadata, Table>,
    ...params: Params
  ): Promise<void>;
}
