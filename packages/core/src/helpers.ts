import { UnknownQueryError } from "./errors";
import type { BaseParams, Table, Topic } from "./types";

export function toTables(names: readonly string[]): Set<Table> {
  return new Set(names as readonly Table[]);
}

export function assertKnownQuery(
  query: string,
  knownQueries: { has(query: string): boolean },
): undefined | UnknownQueryError {
  if (!knownQueries.has(query)) return new UnknownQueryError({ query });
}

export function createTopic<Name extends string, Params extends BaseParams>(
  name: Name,
  params: Params,
): Topic<Name, Params> {
  return { name, params };
}
