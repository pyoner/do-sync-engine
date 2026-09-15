import { exports } from "cloudflare:workers";
import { describe, expect, it } from "vite-plus/test";
import { newWebSocketRpcSession } from "@do-sync-engine/durable-object-websocket/client";
import type { TodoMutations, TodoQueries, TodoSummary } from "../src/todo-protocol.ts";

describe("TodoStore Capnweb WebSocket transport", () => {
  it("accepts a WebSocket connection", async () => {
    const response = await exports.default.fetch(
      new Request("https://example.com/api/todos", { headers: { Upgrade: "websocket" } }),
    );
    expect(response.status).toBe(101);
    response.webSocket?.accept();
    response.webSocket?.close();
  });

  it("subscribes allTodos, adds a unique todo, observes it, and cleans up", async () => {
    const response = await exports.default.fetch(
      new Request("https://example.com/api/todos", { headers: { Upgrade: "websocket" } }),
    );
    expect(response.status).toBe(101);
    const socket = response.webSocket!;
    socket.accept();
    const client = newWebSocketRpcSession<TodoQueries, TodoMutations>(socket);

    try {
      const topic = await client.createTopic("allTodos", []);
      if (topic instanceof Error) throw topic;

      const events: Array<{ value: TodoSummary[] }> = [];
      const waiters = new Set<() => void>();
      const listener = (event: { value: TodoSummary[] }) => {
        events.push(event);
        for (const waiter of [...waiters]) waiter();
      };

      const waitFor = (predicate: () => boolean): Promise<void> => {
        if (predicate()) return Promise.resolve();
        return new Promise<void>((resolve) => {
          const check = () => {
            if (!predicate()) return;
            waiters.delete(check);
            resolve();
          };
          waiters.add(check);
        });
      };

      const subId = await client.subscribe(topic, listener);
      if (subId instanceof Error) throw subId;
      expect(typeof subId).toBe("string");

      await waitFor(() => events.length >= 1);
      const uniqueTitle = `round-trip-${crypto.randomUUID()}`;
      const syncResult = await client.sync("addTodo", [uniqueTitle]);
      expect(syncResult).toBeUndefined();

      await waitFor(() => events.some((e) => e.value.some((todo) => todo.title === uniqueTitle)));

      const added = events.flatMap((e) => e.value).find((todo) => todo.title === uniqueTitle);
      expect(added).toBeDefined();

      if (added !== undefined) {
        await client.sync("deleteTodo", [added.id]);
        await waitFor(
          () =>
            events.length > 0 &&
            !events[events.length - 1]!.value.some((todo) => todo.title === uniqueTitle),
        );
      }

      const unsubResult = await client.unsubscribe(subId);
      expect(unsubResult).toBeUndefined();
    } finally {
      client[Symbol.dispose]();
      socket.close();
    }
  });
});
