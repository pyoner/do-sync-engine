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

export interface Mutator<
  Params extends unknown[] = unknown[],
  Metadata = unknown,
  Table = TableKey,
> {
  tables: readonly Table[];
  run(...params: Params): MaybePromise<Metadata>;
}

export interface RecomputedSelector<Result = unknown, Table = TableKey> {
  selector: object;
  params: readonly unknown[];
  tables: readonly Table[];
  result: Result;
}

export interface MutationResult<Metadata = unknown, Result = unknown, Table = TableKey> {
  metadata: Metadata;
  tables: readonly Table[];
  recomputedSelectors: RecomputedSelector<Result, Table>[];
}
