import { Context, Data, Effect, Layer } from "effect";
import type { Table, Topic, TopicHash } from "./types";

export class TopicBuildError extends Data.TaggedError("TopicBuildError")<{
  readonly cause: unknown;
  readonly operation: "clone" | "serialize" | "hash";
}> {}

export class UnknownQueryError extends Data.TaggedError("UnknownQueryError")<{
  readonly query: string;
}> {}

export function toTables(names: readonly string[]): Set<Table> {
  return new Set(names as readonly Table[]);
}

export function cloneOrThrow<T>(value: T, label: string): T {
  try {
    return structuredClone(value);
  } catch (cause) {
    throw new TypeError(`${label} must support structuredClone`, { cause });
  }
}

export const clone = <T>(value: T) =>
  Effect.try({
    try: () => structuredClone(value),
    catch: (cause) => new TopicBuildError({ cause, operation: "clone" }),
  });

export function assertKnownQuery(
  query: string,
  knownQueries: { has(query: string): boolean },
): void {
  if (!knownQueries.has(query)) {
    throw new ReferenceError(`Unknown query: ${query}`);
  }
}

export const assertKnownQueryEffect = (
  query: string,
  knownQueries: { has(query: string): boolean },
) =>
  knownQueries.has(query) ? Effect.succeed(query) : Effect.fail(new UnknownQueryError({ query }));

export class TopicHasher extends Context.Service<
  TopicHasher,
  {
    readonly hash: (input: string) => Effect.Effect<TopicHash, TopicBuildError>;
  }
>()("@do-sync-engine/core/TopicHasher") {}

export const TopicHasherLive = Layer.succeed(TopicHasher, {
  hash: (input) =>
    Effect.tryPromise({
      try: () => globalThis.crypto.subtle.digest("SHA-256", new TextEncoder().encode(input)),
      catch: (cause) => new TopicBuildError({ cause, operation: "hash" }),
    }).pipe(
      Effect.map(
        (digest) =>
          Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join(
            "",
          ) as TopicHash,
      ),
    ),
});

export function buildTopic<Name extends string, Params extends readonly unknown[]>(
  name: Name,
  params: Params,
): Effect.Effect<Topic<Name, Params>, TopicBuildError, TopicHasher> {
  return Effect.gen(function* () {
    const serialized = yield* Effect.try({
      try: () => JSON.stringify({ name, params }),
      catch: (cause) => new TopicBuildError({ cause, operation: "serialize" }),
    });
    const hasher = yield* TopicHasher;
    const hash = yield* hasher.hash(serialized);
    return { name, params, hash };
  });
}

export function validateTopic(
  topic: unknown,
  knownQueryNames: { has(query: string): boolean },
): Topic<string, readonly unknown[]> {
  if (typeof topic !== "object" || topic === null) {
    throw new TypeError("Topic must be an object");
  }

  const candidate = topic as { name?: unknown; params?: unknown; hash?: unknown };
  if (typeof candidate.name !== "string") {
    throw new TypeError("Topic name must be a string");
  }
  assertKnownQuery(candidate.name, knownQueryNames);
  if (!Array.isArray(candidate.params)) {
    throw new TypeError("Topic params must be an array");
  }
  if (typeof candidate.hash !== "string" || !/^[0-9a-f]{64}$/.test(candidate.hash)) {
    throw new TypeError("Topic hash must be 64 lowercase hexadecimal characters");
  }

  return candidate as Topic<string, readonly unknown[]>;
}
