import { Effect } from "effect";
import { exports } from "cloudflare:workers";
import { describe, expect, it } from "vite-plus/test";
import { parseServerMessage } from "../src/todo-protocol";
import type { ServerMessage } from "../src/todo-protocol";

describe("TodoStore WebSocket transport", () => {
  it("uses the shared subscribe and sync protocol", async () => {
    const response = await exports.default.fetch(
      new Request("https://example.com/api/todos", { headers: { Upgrade: "websocket" } }),
    );
    expect(response.status).toBe(101);
    const socket = response.webSocket!;
    socket.accept();
    const messages: ServerMessage[] = [];
    const waiters: Array<(message: ServerMessage) => void> = [];
    let parsing = Promise.resolve();
    socket.addEventListener("message", (event) => {
      parsing = parsing.then(() =>
        Effect.runPromise(parseServerMessage(String(event.data))).then((message) => {
          const waiter = waiters.shift();
          if (waiter) waiter(message);
          else messages.push(message);
        }),
      );
    });
    const next = () =>
      messages.length > 0
        ? Promise.resolve(messages.shift()!)
        : new Promise<ServerMessage>((resolve) => waiters.push(resolve));
    socket.send(
      JSON.stringify({ type: "subscribe", requestId: "sub", query: "allTodos", params: [] }),
    );
    const subscribed = await next();
    expect(subscribed.type).toBe("queryResult");
    if (subscribed.type === "queryResult") expect(subscribed.topic.name).toBe("allTodos");
    const queryResult = next();
    const syncedResult = next();
    socket.send(
      JSON.stringify({ type: "sync", requestId: "add", mutation: "addTodo", params: ["shared"] }),
    );
    expect(await queryResult).toMatchObject({ type: "queryResult", value: [{ title: "shared" }] });
    expect(await syncedResult).toEqual({ type: "synced", requestId: "add" });
    socket.close();
  });
});
