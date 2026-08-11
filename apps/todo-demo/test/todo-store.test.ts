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
});
