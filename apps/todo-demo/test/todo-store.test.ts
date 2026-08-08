import { exports } from "cloudflare:workers";
import { newWebSocketRpcSession, type RpcStub } from "capnweb";
import { describe, expect, it } from "vite-plus/test";
import type { TodoQueryName } from "../src/todo-protocol";

type Event = { topic: { name: TodoQueryName }; value: unknown };
type Api = {
  subscribe(
    query: TodoQueryName,
    params: [],
    listener: (event: Event) => void,
  ): Promise<{ unsubscribe(): boolean }>;
  sync(
    mutation: "addTodo" | "toggleTodo" | "deleteTodo" | "clearCompleted",
    params: unknown[],
  ): Promise<void>;
};

describe("TodoStore Cap’n Web transport", () => {
  it("uses RPC subscriptions and mutations", async () => {
    const response = await exports.default.fetch(
      new Request("https://example.com/api/todos", { headers: { Upgrade: "websocket" } }),
    );
    expect(response.status).toBe(101);
    const socket = response.webSocket!;
    socket.accept();
    const api = newWebSocketRpcSession<Api>(socket) as RpcStub<Api>;
    const events: Event[] = [];
    const subscription = await api.subscribe("allTodos", [], (event) => void events.push(event));
    expect(events[0]?.topic.name).toBe("allTodos");
    await api.sync("addTodo", ["shared"]);
    expect(events.at(-1)?.value).toMatchObject([{ title: "shared", completed: 0 }]);
    expect(await subscription.unsubscribe()).toBe(true);
    socket.close();
  });
});
