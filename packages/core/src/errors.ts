import * as errore from "errore";

export class UnknownQueryError extends errore.createTaggedError({
  name: "UnknownQueryError",
  message: "Unknown query: $query",
}) {}

export class UnknownMutationError extends errore.createTaggedError({
  name: "UnknownMutationError",
  message: "Unknown mutation: $mutation",
}) {}

export class QueryExecutionError extends errore.createTaggedError({
  name: "QueryExecutionError",
  message: "Query execution failed",
}) {}

export class MutationExecutionError extends errore.createTaggedError({
  name: "MutationExecutionError",
  message: "Mutation execution failed",
}) {}
