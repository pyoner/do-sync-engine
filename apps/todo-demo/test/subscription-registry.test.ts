import { describe, expect, it } from "vite-plus/test";
import { SyncEngine, toTables } from "@do-sync-engine/core";
import { SubscriptionRegistry } from "../src/worker/subscription-registry";
import type { TodoMutations, TodoQueries, TodoQueryName } from "../src/todo-protocol";

function fakeWebSocket() {
  let attachment: unknown;
  const ws = {
    readyState: WebSocket.OPEN,
    serializeAttachment: (value: unknown) => {
      attachment = value;
    },
    deserializeAttachment: () => attachment,
  } as unknown as WebSocket;
  return { ws, getAttachment: () => attachment };
}

function setup() {
  const todos = [{ id: 1, title: "a", completed: 0, created_at: 0 }];
  const queries = {
    allTodos: { tables: toTables(["todos"]), run: () => todos },
  } as unknown as TodoQueries;
  const mutations = {
    addTodo: {
      tables: toTables(["todos"]),
      run: (title: string) => {
        todos.push({ id: todos.length + 1, title, completed: 0, created_at: 0 });
        return { rowsAffected: 1, lastInsertRowid: todos.length };
      },
    },
  } as unknown as TodoMutations;
  const engine = new SyncEngine({ queries, mutations });
  const sent: { name: TodoQueryName; result: unknown }[] = [];
  const registry = new SubscriptionRegistry(engine, queries, (_ws, name, result) => {
    sent.push({ name, result });
  });
  return { engine, registry, sent };
}

describe("SubscriptionRegistry", () => {
  it("sends the initial result, persists the selector, and fans out on sync", async () => {
    const { engine, registry, sent } = setup();
    const { ws, getAttachment } = fakeWebSocket();

    await registry.subscribe(ws, ["allTodos"]);

    expect(sent).toEqual([
      { name: "allTodos", result: [{ id: 1, title: "a", completed: 0, created_at: 0 }] },
    ]);
    expect(getAttachment()).toEqual({ selectors: ["allTodos"] });

    engine.sync("addTodo", ["b"]);
    expect(sent).toHaveLength(2);
    expect(sent[1].name).toBe("allTodos");
  });

  it("unsubscribe stops events and clears the persisted selector", async () => {
    const { engine, registry, sent } = setup();
    const { ws, getAttachment } = fakeWebSocket();

    await registry.subscribe(ws, ["allTodos"]);
    registry.unsubscribe(ws, ["allTodos"]);

    expect(getAttachment()).toEqual({ selectors: [] });
    engine.sync("addTodo", ["b"]);
    expect(sent).toHaveLength(1);
  });

  it("restores subscriptions from the attachment after hibernation", async () => {
    const { engine, registry, sent } = setup();
    const { ws } = fakeWebSocket();
    ws.serializeAttachment({ selectors: ["allTodos"] });

    await registry.restore(ws);
    expect(sent).toHaveLength(1);

    engine.sync("addTodo", ["b"]);
    expect(sent).toHaveLength(2);
  });

  it("clear detaches every listener and empties the attachment", async () => {
    const { engine, registry, sent } = setup();
    const { ws, getAttachment } = fakeWebSocket();

    await registry.subscribe(ws, ["allTodos"]);
    registry.clear(ws);

    expect(getAttachment()).toEqual({ selectors: [] });
    engine.sync("addTodo", ["b"]);
    expect(sent).toHaveLength(1);
  });
});
