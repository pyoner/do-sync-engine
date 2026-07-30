import { Effect, Schema } from "effect";
import { TableSchema, type Table } from "./types";

export class UnknownQueryError extends Schema.TaggedErrorClass<UnknownQueryError>()(
  "UnknownQueryError",
  { query: Schema.String },
) {}

export class UnknownMutationError extends Schema.TaggedErrorClass<UnknownMutationError>()(
  "UnknownMutationError",
  { mutation: Schema.String },
) {}

export function toTables(names: readonly string[]): Set<Table> {
  return new Set(names.map((name) => TableSchema.make(name)));
}

export const assertKnownQueryEffect = (
  query: string,
  knownQueries: { has(query: string): boolean },
): Effect.Effect<string, UnknownQueryError> =>
  knownQueries.has(query) ? Effect.succeed(query) : Effect.fail(UnknownQueryError.make({ query }));
