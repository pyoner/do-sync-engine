import { Context, Effect, Layer, Schema } from "effect";
import {
  TableSchema,
  TopicHashSchema,
  TopicSchema,
  type Table,
  type Topic,
  type TopicHash,
} from "./types";

export class TopicBuildError extends Schema.TaggedErrorClass<TopicBuildError>()("TopicBuildError", {
  cause: Schema.Unknown,
  operation: Schema.Union([
    Schema.Literal("clone"),
    Schema.Literal("serialize"),
    Schema.Literal("hash"),
  ]),
}) {}

export class UnknownQueryError extends Schema.TaggedErrorClass<UnknownQueryError>()(
  "UnknownQueryError",
  {
    query: Schema.String,
  },
) {}
export function toTables(names: readonly string[]): Set<Table> {
  return new Set(names.map((name) => TableSchema.make(name)));
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
    catch: (cause) => TopicBuildError.make({ cause, operation: "clone" }),
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
  knownQueries.has(query) ? Effect.succeed(query) : Effect.fail(UnknownQueryError.make({ query }));

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
      catch: (cause) => TopicBuildError.make({ cause, operation: "hash" }),
    }).pipe(
      Effect.map((digest) =>
        TopicHashSchema.make(
          Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join(""),
        ),
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
      catch: (cause) => TopicBuildError.make({ cause, operation: "serialize" }),
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
  let candidate: typeof TopicSchema.Type;
  try {
    candidate = Schema.decodeUnknownSync(TopicSchema)(topic);
  } catch {
    if (typeof topic !== "object" || topic === null) {
      throw new TypeError("Topic must be an object");
    }
    const value = topic as { name?: unknown; params?: unknown; hash?: unknown };
    if (typeof value.name !== "string") {
      throw new TypeError("Topic name must be a string");
    }
    if (!Array.isArray(value.params)) {
      throw new TypeError("Topic params must be an array");
    }
    throw new TypeError("Topic hash must be 64 lowercase hexadecimal characters");
  }
  assertKnownQuery(candidate.name, knownQueryNames);
  return candidate;
}
