import { Effect, Schema } from "effect";
import { TableSchema, Topic, TopicSchema, type Table } from "./types";

export class TopicBuildError extends Schema.TaggedErrorClass<TopicBuildError>()("TopicBuildError", {
  cause: Schema.Unknown,
  operation: Schema.Union([Schema.Literal("clone"), Schema.Literal("serialize")]),
}) {}

export class UnknownQueryError extends Schema.TaggedErrorClass<UnknownQueryError>()(
  "UnknownQueryError",
  {
    query: Schema.String,
  },
) {}
export class UnknownMutationError extends Schema.TaggedErrorClass<UnknownMutationError>()(
  "UnknownMutationError",
  { mutation: Schema.String },
) {}

export class TopicValidationError extends Schema.TaggedErrorClass<TopicValidationError>()(
  "TopicValidationError",
  { cause: Schema.Unknown },
) {}
export function toTables(names: readonly string[]): Set<Table> {
  return new Set(names.map((name) => TableSchema.make(name)));
}

export function assertSupportedParams(value: unknown, seen = new WeakSet<object>()): void {
  if (value === null || typeof value === "string" || typeof value === "boolean") return;
  if (typeof value === "number") {
    if (Number.isFinite(value) && !Object.is(value, -0)) return;
    throw new TypeError("Topic params must contain only JSON-safe values");
  }
  if (typeof value !== "object") {
    throw new TypeError("Topic params must contain only JSON-safe values");
  }
  if (seen.has(value)) {
    throw new TypeError("Topic params must not contain cycles or shared references");
  }
  seen.add(value);
  if (Array.isArray(value)) {
    if (
      Object.getOwnPropertySymbols(value).length > 0 ||
      Object.keys(value).length !== value.length
    ) {
      throw new TypeError("Topic params must contain only dense arrays");
    }
    for (const child of value) assertSupportedParams(child, seen);
    return;
  }
  if (Object.getPrototypeOf(value) !== Object.prototype) {
    throw new TypeError("Topic params must contain only plain objects and arrays");
  }
  if (Reflect.ownKeys(value).length !== Object.keys(value).length) {
    throw new TypeError("Topic params must contain only enumerable string properties");
  }
  for (const child of Object.values(value)) assertSupportedParams(child, seen);
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

export function buildTopic<Name extends string, Params extends readonly unknown[]>(
  name: Name,
  params: Params,
): Effect.Effect<Topic<Name, Params>, TopicBuildError> {
  return Effect.gen(function* () {
    yield* Effect.try({
      try: () => JSON.stringify({ name, params }),
      catch: (cause) => TopicBuildError.make({ cause, operation: "serialize" }),
    });
    return new Topic<Name, Params>({ name, params });
  });
}

export function validateTopic(
  topic: unknown,
  knownQueryNames: { has(query: string): boolean },
): Effect.Effect<Topic<string, readonly unknown[]>, TopicValidationError | UnknownQueryError> {
  return Effect.gen(function* () {
    const candidate = yield* Schema.decodeUnknownEffect(TopicSchema)(topic).pipe(
      Effect.mapError((cause) => TopicValidationError.make({ cause })),
    );
    yield* Effect.try({
      try: () => assertSupportedParams(candidate.params),
      catch: (cause) => TopicValidationError.make({ cause }),
    });
    yield* assertKnownQueryEffect(candidate.name, knownQueryNames);
    return candidate;
  });
}
