import type {
  Mutation,
  MutationMap,
  OperationParams,
  OperationResult,
  Query,
  QueryMap,
  QueryResult,
  Snapshot,
  StringKey,
  Subscription,
  SubscriptionId,
  SyncEngineInterface,
  SyncEngineMutationResult,
  SyncEngineOptions,
} from "./types";

function cloneOrThrow<T>(value: T, label: string): T {
  try {
    return structuredClone(value);
  } catch (cause) {
    throw new TypeError(`${label} must support structuredClone`, { cause });
  }
}

interface ValidatedSnapshot<QueryName extends string = string> {
  nextSubscriptionId: SubscriptionId;
  subscriptions: Subscription<QueryName>[];
}

function validateSnapshot<QueryName extends string = string>(
  snapshot: Snapshot<QueryName>,
  knownQueries: Set<string>,
): ValidatedSnapshot<QueryName> {
  const cloned = cloneOrThrow(snapshot, "Snapshot");

  if (!Number.isInteger(cloned.nextSubscriptionId) || cloned.nextSubscriptionId < 1) {
    throw new TypeError("Snapshot nextSubscriptionId must be a positive integer");
  }

  if (!Array.isArray(cloned.subscriptions)) {
    throw new TypeError("Snapshot subscriptions must be an array");
  }

  const seenIds = new Set<SubscriptionId>();
  for (const sub of cloned.subscriptions) {
    if (!Number.isInteger(sub.id) || sub.id < 1) {
      throw new TypeError("Subscription id must be a positive integer");
    }
    if (seenIds.has(sub.id)) {
      throw new TypeError(`Duplicate subscription id: ${sub.id}`);
    }
    seenIds.add(sub.id);

    if (!Array.isArray(sub.params)) {
      throw new TypeError("Subscription params must be an array");
    }

    if (typeof sub.query !== "string") {
      throw new TypeError("Subscription query must be a string");
    }

    if (!knownQueries.has(sub.query)) {
      throw new ReferenceError(`Unknown query: ${sub.query}`);
    }
  }

  return cloned as ValidatedSnapshot<QueryName>;
}

export class SyncEngine<
  Queries extends QueryMap<Queries> = QueryMap,
  Mutations extends MutationMap<Mutations> = MutationMap,
> implements SyncEngineInterface<Queries, Mutations> {
  private readonly queries: ReadonlyMap<string, Query<unknown[], unknown>>;
  private readonly mutations: ReadonlyMap<string, Mutation<unknown[], unknown>>;
  private nextSubscriptionId: SubscriptionId;
  private subscriptions: Subscription<StringKey<Queries>>[] = [];

  constructor(options: SyncEngineOptions<Queries, Mutations>) {
    this.queries = new Map(
      Object.entries(options.queries) as [string, Query<unknown[], unknown>][],
    );
    this.mutations = new Map(
      Object.entries(options.mutations) as [string, Mutation<unknown[], unknown>][],
    );

    if (options.snapshot === undefined) {
      this.nextSubscriptionId = 1;
      this.subscriptions = [];
    } else {
      const knownQueryNames = new Set(this.queries.keys());
      const snap = validateSnapshot(options.snapshot, knownQueryNames);

      const highestId = snap.subscriptions.reduce((max, sub) => Math.max(max, sub.id), 0);
      this.nextSubscriptionId = Math.max(snap.nextSubscriptionId, highestId + 1, 1);
      this.subscriptions = snap.subscriptions;
    }
  }

  subscribe<Name extends StringKey<Queries>>(
    query: Name,
    ...params: OperationParams<Queries[Name]>
  ): SubscriptionId {
    if (!this.queries.has(query)) {
      throw new ReferenceError(`Unknown query: ${query}`);
    }

    const clonedParams = cloneOrThrow([...params], "Subscription params");

    const id = this.nextSubscriptionId++;
    this.subscriptions.push({ id, query, params: clonedParams });
    return id;
  }

  unsubscribe(subscriptionId: SubscriptionId): boolean {
    const index = this.subscriptions.findIndex((sub) => sub.id === subscriptionId);
    if (index === -1) return false;
    this.subscriptions.splice(index, 1);
    return true;
  }

  snapshot(): Snapshot<StringKey<Queries>> {
    const snap: Snapshot<StringKey<Queries>> = {
      nextSubscriptionId: this.nextSubscriptionId,
      subscriptions: [...this.subscriptions],
    };
    return cloneOrThrow(snap, "Snapshot");
  }

  async mutate<Name extends StringKey<Mutations>>(
    mutation: Name,
    ...params: OperationParams<Mutations[Name]>
  ): Promise<SyncEngineMutationResult<Queries, OperationResult<Mutations[Name]>>> {
    const mutationDef = this.mutations.get(mutation);
    if (mutationDef === undefined) {
      throw new ReferenceError(`Unknown mutation: ${mutation}`);
    }

    const metadata = await mutationDef.run(...params);
    const changedTables = new Set(mutationDef.tables);

    // Shallow-copy subscriptions for safe iteration (destroyed subscriptions skipped)
    const snapshot = [...this.subscriptions];
    const results: QueryResult<StringKey<Queries>>[] = [];

    for (const sub of snapshot) {
      if (!this.subscriptions.some((s) => s.id === sub.id)) continue;

      const queryDef = this.queries.get(sub.query);
      if (queryDef === undefined) continue;
      const isAffected = queryDef.tables.some((table) => changedTables.has(table));
      if (!isAffected) continue;

      const result = await queryDef.run(...sub.params);
      results.push({
        subscriptionId: sub.id,
        query: sub.query,
        params: sub.params,
        result,
      });
    }

    return { metadata, results } as SyncEngineMutationResult<
      Queries,
      OperationResult<Mutations[Name]>
    >;
  }
}
