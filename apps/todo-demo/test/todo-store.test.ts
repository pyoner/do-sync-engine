import { exports } from "cloudflare:workers";
import { newWebSocketRpcSession } from "capnweb";
import { describe, expect, it } from "vite-plus/test";
import type { ServerAPI } from "@do-sync-engine/durable-object-websocket";
import type { TodoMutations, TodoQueries, TodoQueryName } from "../src/todo-protocol";

type Event = { topic: { name: TodoQueryName }; value: unknown };

describe("TodoStore Cap’n Web transport", () => {
  it("uses RPC subscriptions and mutations", async () => {
    const response = await exports.default.fetch(
      new Request("https://example.com/api/todos", { headers: { Upgrade: "websocket" } }),
    );
    expect(response.status).toBe(101);
    const socket = response.webSocket!;
    socket.accept();
    const api = newWebSocketRpcSession<ServerAPI<TodoQueries, TodoMutations>>(socket);

    try {
      const topic = await api.createTopic("allTodos", []);
      if (topic instanceof Error) throw topic;
      const events: Event[] = [];
      const listener = (event: Event) => {
        events.push(event);
      };
      const subscribed = await api.subscribe(topic, listener);
      if (subscribed instanceof Error) throw subscribed;
      await expect.poll(() => events[0]?.topic.name).toBe("allTodos");

      const synced = await api.sync("addTodo", ["shared"]);
      if (synced instanceof Error) throw synced;
      await expect
        .poll(() => events.at(-1)?.value)
        .toMatchObject([{ title: "shared", completed: 0 }]);

      await api.unsubscribe(topic, listener);
    } finally {
      socket.close();
    }
  });
  it("switches filter subscriptions without retaining the old topic", async () => {
    const response = await exports.default.fetch(
      new Request("https://example.com/api/todos", { headers: { Upgrade: "websocket" } }),
    );
    expect(response.status).toBe(101);
    const socket = response.webSocket!;
    socket.accept();
    const api = newWebSocketRpcSession<ServerAPI<TodoQueries, TodoMutations>>(socket);

    try {
      const allTopic = await api.createTopic("allTodos", []);
      const activeTopic = await api.createTopic("incompleteTodos", []);
      if (allTopic instanceof Error) throw allTopic;
      if (activeTopic instanceof Error) throw activeTopic;
      const allEvents: Event[] = [];
      const activeEvents: Event[] = [];
      const allListener = (event: Event) => allEvents.push(event);
      const activeListener = (event: Event) => activeEvents.push(event);

      const allSubscribed = await api.subscribe(allTopic, allListener);
      if (allSubscribed instanceof Error) throw allSubscribed;
      await expect.poll(() => allEvents.length).toBe(1);
      await api.unsubscribe(allTopic, allListener);

      const activeSubscribed = await api.subscribe(activeTopic, activeListener);
      if (activeSubscribed instanceof Error) throw activeSubscribed;
      await expect.poll(() => activeEvents.length).toBe(1);

      const synced = await api.sync("addTodo", ["switch lifecycle"]);
      if (synced instanceof Error) throw synced;
      await expect
        .poll(() => activeEvents.at(-1)?.value)
        .toMatchObject([{ title: "shared" }, { title: "switch lifecycle" }]);
      expect(allEvents).toHaveLength(1);
    } finally {
      socket.close();
    }
  });
});
