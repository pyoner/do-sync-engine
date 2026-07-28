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
    const next = () =>
      new Promise<ServerMessage>((resolve) =>
        socket.addEventListener(
          "message",
          (event) => resolve(parseServerMessage(String(event.data))),
          { once: true },
        ),
      );
    socket.send(
      JSON.stringify({ type: "subscribe", requestId: "sub", query: "allTodos", params: [] }),
    );
    const subscribed = await next();
    expect(subscribed.type).toBe("queryResult");
    if (subscribed.type === "queryResult") expect(subscribed.topic.name).toBe("allTodos");
    socket.send(
      JSON.stringify({ type: "sync", requestId: "add", mutation: "addTodo", params: ["shared"] }),
    );
    expect(await next()).toMatchObject({ type: "queryResult", value: [{ title: "shared" }] });
    expect(await next()).toEqual({ type: "synced", requestId: "add" });
    socket.close();
  });
});
