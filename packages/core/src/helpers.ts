import type { Table, Topic, TopicHash } from "./types";

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

export function assertKnownQuery(
  query: string,
  knownQueries: { has(query: string): boolean },
): void {
  if (!knownQueries.has(query)) {
    throw new ReferenceError(`Unknown query: ${query}`);
  }
}

export async function buildTopic<Name extends string, Params extends readonly unknown[]>(
  name: Name,
  params: Params,
): Promise<Topic<Name, Params>> {
  const input = { name, params };
  const serialized = JSON.stringify(input);
  const bytes = new TextEncoder().encode(serialized);
  const digest = await globalThis.crypto.subtle.digest("SHA-256", bytes);
  const hash = Array.from(new Uint8Array(digest), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("") as TopicHash;
  return { name, params, hash };
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
