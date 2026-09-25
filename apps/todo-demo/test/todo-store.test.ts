import { exports } from "cloudflare:workers";
import { newWebSocketRpcSession, RpcStub } from "capnweb";
import { describe, expect, it, vi } from "vite-plus/test";
import { createSyncStore } from "@do-sync-engine/xstate-store";
import type { Service } from "@do-sync-engine/durable-object-websocket";
import type { Topics } from "@do-sync-engine/core";
import type { TodoMutations, TodoQueries, TodoSummary } from "../src/todo-protocol.ts";

type StoreContext = {
  status: string;
  topics: Record<string, unknown>;
  error: Error | null;
};
type StoreView = {
  getSnapshot: () => { context: StoreContext };
  subscribe: (listener: (snapshot: { context: StoreContext }) => void) => {
    unsubscribe: () => void;
  };
};

function waitForContext(store: StoreView, predicate: (context: StoreContext) => boolean) {
  const context = store.getSnapshot().context;
  if (predicate(context)) return Promise.resolve(context);

  let unsubscribe = () => {};
  const promise = new Promise<StoreContext>((resolve, reject) => {
    const subscription = store.subscribe(({ context }) => {
      if (context.error !== null) reject(context.error);
      else if (predicate(context)) resolve(context);
    });
    unsubscribe = () => subscription.unsubscribe();
  });
  void promise.then(
    () => unsubscribe(),
    () => unsubscribe(),
  );
  return promise;
}
describe("TodoStore Capnweb WebSocket transport", () => {
  it("subscribes allTodos, adds a unique todo, observes it, and cleans up", async () => {
    const response = await exports.default.fetch(
      new Request("https://example.com/api/todos", { headers: { Upgrade: "websocket" } }),
    );
    expect(response.status).toBe(101);
    const socket = response.webSocket!;
    socket.accept();
    const client = newWebSocketRpcSession<Service<TodoQueries, TodoMutations>>(socket);

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

      const listenerStub = new RpcStub(listener);
      let subscribeResult: void | Error;
      try {
        subscribeResult = await client.subscribe(topic as never, listenerStub as never);
      } finally {
        listenerStub[Symbol.dispose]();
      }
      if (subscribeResult instanceof Error) throw subscribeResult;
      expect(subscribeResult).toBeUndefined();

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

      const unsubResult = await client.unsubscribe(topic);
      expect(unsubResult).toBeUndefined();
    } finally {
      client[Symbol.dispose]();
      socket.close();
    }
  });

  it("connects through createSyncStore, subscribes, syncs, and unsubscribes", async () => {
    const response = await exports.default.fetch(
      new Request("https://example.com/api/todos", { headers: { Upgrade: "websocket" } }),
    );
    expect(response.status).toBe(101);
    const socket = response.webSocket!;
    socket.accept();
    class FakeWebSocket {
      static CLOSING = WebSocket.CLOSING;

      constructor() {
        queueMicrotask(() => socket.dispatchEvent(new Event("open")));
        return socket as unknown as FakeWebSocket;
      }
    }
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const client = createSyncStore<TodoQueries, TodoMutations>({
      url: "ws://example.com/api/todos",
    });
    const allTopic: Topics<TodoQueries> = { name: "allTodos", params: [] };
    const syncedEvents: Array<{ key: string; value: unknown }> = [];
    client.store.on("synced", (event) => syncedEvents.push(event));

    try {
      client.subscribe(allTopic, "allTodos");
      await waitForContext(client.store, ({ status }) => status === "ready");

      const firstTitle = `store-${crypto.randomUUID()}`;
      expect(await client.sync("addTodo", [firstTitle])).toBeUndefined();
      const updatedAll = (
        await waitForContext(
          client.store,
          ({ topics }) =>
            Array.isArray(topics.allTodos) &&
            topics.allTodos.some(
              (todo) =>
                typeof todo === "object" &&
                todo !== null &&
                "title" in todo &&
                todo.title === firstTitle,
            ),
        )
      ).topics.allTodos as TodoSummary[];
      expect(syncedEvents).toContainEqual({ type: "synced", key: "allTodos", value: updatedAll });
      const firstTodo = updatedAll.find((todo) => todo.title === firstTitle);
      expect(firstTodo).toBeDefined();
      if (firstTodo === undefined) throw new Error("Added todo was not returned");

      expect(await client.sync("deleteTodo", [firstTodo.id])).toBeUndefined();
      await waitForContext(
        client.store,
        ({ topics }) =>
          Array.isArray(topics.allTodos) &&
          !topics.allTodos.some(
            (todo) =>
              typeof todo === "object" && todo !== null && "id" in todo && todo.id === firstTodo.id,
          ),
      );
      client.unsubscribe(allTopic);
      expect(client.store.getSnapshot().context.status).toBe("disconnected");
      expect(await client.sync("deleteTodo", [firstTodo.id])).toBeInstanceOf(Error);
    } finally {
      client.unsubscribe(allTopic);
      socket.close();
      vi.unstubAllGlobals();
    }
  });
});
