import { evictDurableObject } from "cloudflare:test";
import { env, exports } from "cloudflare:workers";
import { describe, expect, it } from "vite-plus/test";

const worker = exports as unknown as {
  default: { fetch(request: Request): Promise<Response> };
};

type ResponseMessage = {
  id: string | null;
  result?: unknown;
  error?: { code?: number; message?: string };
};

async function connect() {
  const response = await worker.default.fetch(
    new Request("https://example.com", { headers: { Upgrade: "websocket" } }),
  );
  const socket = response.webSocket!;
  socket.accept();
  const messages: unknown[] = [];
  socket.addEventListener("message", (event: MessageEvent) =>
    messages.push(JSON.parse(String(event.data))),
  );
  let requestId = 0;
  const call = (method: string, ...params: unknown[]) => {
    const id = String(++requestId);
    socket.send(JSON.stringify({ jsonrpc: "2.0", id, method, params }));
    return id;
  };
  return { socket, messages, call };
}

async function waitFor<T>(
  messages: unknown[],
  predicate: (value: unknown) => value is T,
): Promise<T> {
  for (;;) {
    const value = messages.find(predicate);
    if (value !== undefined) return value;
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

describe("Durable Object typed-rpc WebSocket transport", () => {
  it("rejects non-WebSocket requests", async () => {
    const response = await worker.default.fetch(new Request("https://example.com"));
    expect([response.status, await response.text()]).toEqual([
      400,
      "This endpoint only accepts WebSocket requests.",
    ]);
  });

  it("supports one callback per topic and aggregates sync values", async () => {
    const { socket, messages, call } = await connect();
    try {
      const topic = { name: "counter", params: ["alpha"] };
      const subscribeId = call("subscribe", topic);
      const subscribeResponse = await waitFor(
        messages,
        (value): value is { id: string; result?: unknown } =>
          typeof value === "object" && value !== null && "id" in value && value.id === subscribeId,
      );
      expect(subscribeResponse.result).toBeNull();
      await waitFor(
        messages,
        (value): value is Record<string, unknown> =>
          typeof value === "object" &&
          value !== null &&
          "method" in value &&
          value.method === "listener",
      );
      call("subscribe", topic);
      call("sync", "increment", ["alpha", 2]);
      const event = await waitFor(
        messages,
        (value): value is { jsonrpc: string; method: string; params: [Record<string, unknown>] } =>
          typeof value === "object" &&
          value !== null &&
          "method" in value &&
          value.method === "listener" &&
          "params" in value &&
          Array.isArray(value.params) &&
          value.params[0]?.value?.value === 2,
      );
      expect(event.params[0]).toMatchObject({ value: { key: "alpha", value: 2 } });
      call("unsubscribe", topic);
      const syncId = call("sync", "increment", ["alpha", 1]);
      await waitFor(
        messages,
        (value): value is { id: string; result?: unknown } =>
          typeof value === "object" && value !== null && "id" in value && value.id === syncId,
      );
      expect(
        messages.filter(
          (value) =>
            typeof value === "object" &&
            value !== null &&
            "method" in value &&
            value.method === "listener",
        ),
      ).toHaveLength(2);
    } finally {
      socket.close();
    }
  });

  it("returns unknown queries as RPC errors", async () => {
    const { socket, messages, call } = await connect();
    try {
      const id = call("subscribe", { name: "unknown", params: [] });
      const response = await waitFor(
        messages,
        (value): value is ResponseMessage =>
          typeof value === "object" && value !== null && (value as ResponseMessage).id === id,
      );
      expect(response.error).toBeDefined();
    } finally {
      socket.close();
    }
  });

  it("returns parse errors for malformed JSON", async () => {
    const { socket, messages } = await connect();
    try {
      socket.send("{");
      const response = await waitFor(
        messages,
        (value): value is ResponseMessage =>
          typeof value === "object" &&
          value !== null &&
          "id" in value &&
          value.id === null &&
          "error" in value &&
          JSON.stringify(value).includes('"code":-32700'),
      );
      expect(response.error?.code).toBe(-32700);
    } finally {
      socket.close();
    }
  });
  it("restores subscriptions after Durable Object eviction", async () => {
    const { socket, messages, call } = await connect();
    try {
      call("subscribe", { name: "counter", params: ["restore"] });
      await waitFor(
        messages,
        (value): value is Record<string, unknown> =>
          typeof value === "object" &&
          value !== null &&
          "method" in value &&
          value.method === "listener",
      );
      await evictDurableObject(env.FIXTURE_SYNC_OBJECT.getByName("default"));
      call("sync", "increment", ["restore", 3]);
      const event = await waitFor(
        messages,
        (value): value is { jsonrpc: string; method: string; params: [Record<string, unknown>] } =>
          typeof value === "object" &&
          value !== null &&
          "method" in value &&
          value.method === "listener" &&
          "params" in value &&
          Array.isArray(value.params) &&
          value.params[0]?.value?.value === 3,
      );
      expect(event.params[0]).toMatchObject({ value: { key: "restore", value: 3 } });
    } finally {
      socket.close();
    }
  });
});
