import { SyncEngineBase } from "./types";
import type {
  Mutation,
  MutationMap,
  OperationParams,
  Query,
  QueryMap,
  Snapshot,
  StringKey,
  Subscription,
  SubscriptionId,
  SyncEngineOptions,
  SyncEngineQueryResult,
  SyncEngineSyncResult,
} from "./types";

function cloneOrThrow<T>(value: T, label: string): T {
  try {
    return structuredClone(value);
  } catch (cause) {
    throw new TypeError(`${label} must support structuredClone`, { cause });
  }
}
function assertKnownQuery(query: string, knownQueries: { has(query: string): boolean }): void {
  if (!knownQueries.has(query)) {
    throw new ReferenceError(`Unknown query: ${query}`);
  }
}

function hasSubscription<QueryName extends string>(
  subscriptions: readonly Subscription<QueryName>[],
  subscriptionId: SubscriptionId,
): boolean {
  return subscriptions.some((subscription) => subscription.id === subscriptionId);
}

function* activeSubscriptions<QueryName extends string>(
  subscriptions: readonly Subscription<QueryName>[],
): Iterable<Subscription<QueryName>> {
  const subscriptionSnapshot = Array.from(subscriptions);
  for (const subscription of subscriptionSnapshot) {
    if (hasSubscription(subscriptions, subscription.id)) yield subscription;
  }
}

interface ValidatedSnapshot<QueryName extends string = string> {
  nextSubscriptionId: SubscriptionId;
  subscriptions: Subscription<QueryName>[];
}

function validateSnapshot<QueryName extends string = string>(
  snapshot: Snapshot<QueryName>,
  knownQueryNames: Set<string>,
): ValidatedSnapshot<QueryName> {
  const snapshotCopy = cloneOrThrow(snapshot, "Snapshot");

  if (!Number.isInteger(snapshotCopy.nextSubscriptionId) || snapshotCopy.nextSubscriptionId < 1) {
    throw new TypeError("Snapshot nextSubscriptionId must be a positive integer");
  }

  if (!Array.isArray(snapshotCopy.subscriptions)) {
    throw new TypeError("Snapshot subscriptions must be an array");
  }

  const seenSubscriptionIds = new Set<SubscriptionId>();
  for (const subscription of snapshotCopy.subscriptions) {
    if (!Number.isInteger(subscription.id) || subscription.id < 1) {
      throw new TypeError("Subscription id must be a positive integer");
    }
    if (seenSubscriptionIds.has(subscription.id)) {
      throw new TypeError(`Duplicate subscription id: ${subscription.id}`);
    }
    seenSubscriptionIds.add(subscription.id);

    if (!Array.isArray(subscription.params)) {
      throw new TypeError("Subscription params must be an array");
    }

    if (typeof subscription.query !== "string") {
      throw new TypeError("Subscription query must be a string");
    }

    assertKnownQuery(subscription.query, knownQueryNames);
  }

  return snapshotCopy as ValidatedSnapshot<QueryName>;
}

export class SyncEngine<
  Queries extends QueryMap<Queries> = QueryMap,
  Mutations extends MutationMap<Mutations> = MutationMap,
> extends SyncEngineBase<Queries, Mutations> {
  private readonly queries: ReadonlyMap<string, Query<unknown[], unknown>>;
  private readonly mutations: ReadonlyMap<string, Mutation<unknown[], unknown>>;
  private nextSubscriptionId: SubscriptionId = 1;
  private subscriptions: Subscription<StringKey<Queries>>[] = [];

  constructor(options: SyncEngineOptions<Queries, Mutations>) {
    super();
    this.queries = new Map(
      Object.entries(options.queries) as [string, Query<unknown[], unknown>][],
    );
    this.mutations = new Map(
      Object.entries(options.mutations) as [string, Mutation<unknown[], unknown>][],
    );

    if (options.snapshot !== undefined) {
      const knownQueryNames = new Set(this.queries.keys());
      const validatedSnapshot = validateSnapshot(options.snapshot, knownQueryNames);
      const highestSubscriptionId = validatedSnapshot.subscriptions.reduce(
        (max, subscription) => Math.max(max, subscription.id),
        0,
      );
      this.nextSubscriptionId = Math.max(
        validatedSnapshot.nextSubscriptionId,
        highestSubscriptionId + 1,
      );
      this.subscriptions = validatedSnapshot.subscriptions;
    }
  }

  subscribe<Name extends StringKey<Queries>>(
    query: Name,
    ...params: OperationParams<Queries[Name]>
  ): SubscriptionId {
    assertKnownQuery(query, this.queries);

    const subscriptionParams = cloneOrThrow(params, "Subscription params");

    const subscriptionId = this.nextSubscriptionId++;
    this.subscriptions.push({ id: subscriptionId, query, params: subscriptionParams });
    return subscriptionId;
  }

  unsubscribe(subscriptionId: SubscriptionId): boolean {
    const subscriptionIndex = this.subscriptions.findIndex(
      (subscription) => subscription.id === subscriptionId,
    );
    if (subscriptionIndex === -1) return false;
    this.subscriptions.splice(subscriptionIndex, 1);
    return true;
  }

  snapshot(): Snapshot<StringKey<Queries>> {
    const engineSnapshot: Snapshot<StringKey<Queries>> = {
      nextSubscriptionId: this.nextSubscriptionId,
      subscriptions: this.subscriptions,
    };
    return cloneOrThrow(engineSnapshot, "Snapshot");
  }

  async mutate<Name extends StringKey<Mutations>>(
    mutation: Name,
    ...params: OperationParams<Mutations[Name]>
  ): Promise<readonly string[]> {
    const mutationDefinition = this.mutations.get(mutation);
    if (mutationDefinition === undefined) {
      throw new ReferenceError(`Unknown mutation: ${mutation}`);
    }

    await mutationDefinition.run(...params);
    return [...mutationDefinition.tables];
  }

  protected publish(
    subscriptionId: SubscriptionId,
    value: unknown,
  ): SyncEngineQueryResult<Queries> | undefined {
    const subscription = this.subscriptions.find(({ id }) => id === subscriptionId);
    if (subscription === undefined) return undefined;

    return {
      subscriptionId,
      query: subscription.query,
      params: subscription.params,
      result: value,
    } as SyncEngineQueryResult<Queries>;
  }

  async sync<Name extends StringKey<Mutations>>(
    mutation: Name,
    ...params: OperationParams<Mutations[Name]>
  ): Promise<SyncEngineSyncResult<Queries>> {
    const affectedTables = await this.mutate(mutation, ...params);
    const changedTables = new Set(affectedTables);

    const results: SyncEngineQueryResult<Queries>[] = [];

    for (const subscription of activeSubscriptions(this.subscriptions)) {
      const queryDefinition = this.queries.get(subscription.query);
      if (queryDefinition === undefined) continue;
      const touchesChangedTable = queryDefinition.tables.some((table) => changedTables.has(table));
      if (!touchesChangedTable) continue;

      const value = await queryDefinition.run(...subscription.params);

      const result = this.publish(subscription.id, value);
      if (result !== undefined) {
        results.push(result);
      }
    }

    return { affectedTables, results } as SyncEngineSyncResult<Queries>;
  }
}
