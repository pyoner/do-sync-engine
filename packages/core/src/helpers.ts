import * as errore from "errore";
import { CloneError, InvalidTopicError, UnknownQueryError } from "./errors";
import type { BaseParams, Table, Topic } from "./types";

export function toTables(names: readonly string[]): Set<Table> {
  return new Set(names as readonly Table[]);
}

export function cloneOrThrow<T>(value: T, label: string): T | CloneError {
  return errore.try({
    try: () => structuredClone(value),
    catch: (cause) => new CloneError({ label, cause }),
  });
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
  knownQueryNames: { has(query: string): boolean },
): Topic<Name, Params> | CloneError | InvalidTopicError | UnknownQueryError {
  const knownQuery = assertKnownQuery(name, knownQueryNames);
  if (knownQuery instanceof Error) return knownQuery;
  const topicParams = cloneOrThrow(params, "Topic params");
  if (topicParams instanceof Error) return topicParams;
  const topic = { name, params: topicParams };
  return validateTopic<Name, Params>(topic);
}

export function validateTopic<Name extends string, Params extends BaseParams>(
  topic: unknown,
): Topic<Name, Params> | InvalidTopicError {
  if (typeof topic !== "object" || topic === null)
    return new InvalidTopicError({ reason: "Topic must be an object" });

  const candidate = topic as { name?: unknown; params?: unknown };
  if (typeof candidate.name !== "string")
    return new InvalidTopicError({ reason: "Topic name must be a string" });
  if (!Array.isArray(candidate.params))
    return new InvalidTopicError({ reason: "Topic params must be an array" });

  return candidate as Topic<Name, Params>;
}
