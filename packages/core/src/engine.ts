import type {
  Broker,
  BrokerSnapshot,
  MutatorRegistry,
  OperationKey,
  PublishResult,
  SelectionResult,
  SelectorRegistry,
  SubscriptionId,
  SubscriptionState,
  SyncEngineOptions,
  TableKey,
} from "./types";

function cloneOrThrow<T>(value: T, label: string): T {
  try {
    return structuredClone(value);
  } catch (cause) {
    throw new TypeError(`${label} must support structuredClone`, { cause });
  }
}

interface ValidatedSnapshot {
  nextSubscriptionId: SubscriptionId;
  subscriptions: SubscriptionState[];
}

function validateSnapshot(
  snapshot: BrokerSnapshot,
  knownSelectors: Set<string>,
): ValidatedSnapshot {
  const cloned = cloneOrThrow(snapshot, "Broker snapshot");

  if (!Number.isInteger(cloned.nextSubscriptionId) || cloned.nextSubscriptionId < 1) {
    throw new TypeError("Broker snapshot nextSubscriptionId must be a positive integer");
  }

  if (!Array.isArray(cloned.subscriptions)) {
    throw new TypeError("Broker snapshot subscriptions must be an array");
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

    if (typeof sub.selector !== "string") {
      throw new TypeError("Subscription selector must be a string");
    }

    if (!knownSelectors.has(sub.selector)) {
      throw new ReferenceError(`Unknown selector: ${sub.selector}`);
    }
  }

  return cloned as ValidatedSnapshot;
}

export class SyncEngine<Table = TableKey> implements Broker {
  private readonly knownSelectors: ReadonlyMap<OperationKey, SelectorRegistry<Table>[OperationKey]>;
  private readonly knownMutators: ReadonlyMap<OperationKey, MutatorRegistry<Table>[OperationKey]>;
  private nextSubscriptionId: SubscriptionId;
  private subscriptions: SubscriptionState[] = [];

  constructor(options: SyncEngineOptions<Table>) {
    this.knownSelectors = new Map(Object.entries(options.selectors));
    this.knownMutators = new Map(Object.entries(options.mutators));

    if (options.snapshot === undefined) {
      this.nextSubscriptionId = 1;
      this.subscriptions = [];
    } else {
      const knownSelectorNames = new Set(this.knownSelectors.keys());
      const snap = validateSnapshot(options.snapshot, knownSelectorNames);

      const highestId = snap.subscriptions.reduce((max, sub) => Math.max(max, sub.id), 0);
      this.nextSubscriptionId = Math.max(snap.nextSubscriptionId, highestId + 1, 1);
      this.subscriptions = snap.subscriptions;
    }
  }

  subscribe(selector: OperationKey, params?: readonly unknown[]): SubscriptionId {
    if (!this.knownSelectors.has(selector)) {
      throw new ReferenceError(`Unknown selector: ${selector}`);
    }

    const clonedParams = cloneOrThrow(params ?? [], "Subscription params");

    const id = this.nextSubscriptionId++;
    this.subscriptions.push({ id, selector, params: clonedParams });
    return id;
  }

  unsubscribe(subscriptionId: SubscriptionId): boolean {
    const index = this.subscriptions.findIndex((sub) => sub.id === subscriptionId);
    if (index === -1) return false;
    this.subscriptions.splice(index, 1);
    return true;
  }

  snapshot(): BrokerSnapshot {
    const snap: BrokerSnapshot = {
      nextSubscriptionId: this.nextSubscriptionId,
      subscriptions: [...this.subscriptions],
    };
    return cloneOrThrow(snap, "Broker snapshot");
  }

  async publish(mutator: OperationKey, ...params: unknown[]): Promise<PublishResult> {
    const mutatorDef = this.knownMutators.get(mutator);
    if (mutatorDef === undefined) {
      throw new ReferenceError(`Unknown mutator: ${mutator}`);
    }

    const metadata = await mutatorDef.run(...params);
    // Intentionally erased by MutatorRegistry type; safe because registry key was already validated
    const mutatorTables: readonly unknown[] = mutatorDef.tables;
    const touchedTables = new Set(mutatorTables);

    // Shallow-copy subscriptions for safe iteration (destroyed subscriptions skipped)
    const snapshot = [...this.subscriptions];
    const selections: SelectionResult[] = [];

    for (const sub of snapshot) {
      if (!this.subscriptions.some((s) => s.id === sub.id)) continue;

      const selectorDef = this.knownSelectors.get(sub.selector);
      if (selectorDef === undefined) continue;

      // Intentionally erased by SelectorRegistry type; safe because registry key was already validated
      const selectorTables: readonly unknown[] = selectorDef.tables;
      const isAffected = selectorTables.some((table) => touchedTables.has(table));
      if (!isAffected) continue;

      // Registry erases param types intentionally; subscription stores only clone-safe data
      const selectorParams: readonly unknown[] = sub.params;
      const result = await selectorDef.run(...(selectorParams as never[]));
      selections.push({
        subscriptionId: sub.id,
        selector: sub.selector,
        params: sub.params,
        result,
      });
    }

    return { metadata, selections };
  }
}
