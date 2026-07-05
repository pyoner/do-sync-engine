export type MaybePromise<T> = T | PromiseLike<T>;
export type TableKey = string;

export interface Selector<
  Params extends unknown[] = unknown[],
  Result = unknown,
  Table = TableKey,
> {
  tables: readonly Table[];
  run(...params: Params): MaybePromise<Result>;
  callback(result: Result): MaybePromise<void>;
}

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
  subscribe<Params extends unknown[], Result>(
    selector: Selector<Params, Result, Table>,
    ...params: Params
  ): Unsubscribe;
  publish<Params extends unknown[], Metadata>(
    mutator: Mutator<Params, Metadata, Table>,
    ...params: Params
  ): Promise<void>;
}
