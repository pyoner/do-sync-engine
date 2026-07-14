import { exports, env } from "cloudflare:workers";
import { evictDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vite-plus/test";
import { parseServerMessage } from "../src/todo-protocol";
import type { ClientMessage, QueryResultMessage, ServerMessage } from "../src/todo-protocol";

type MessagePredicate = (message: ServerMessage) => boolean;

type MessageWaiter = {
  predicate: MessagePredicate;
  resolve: (message: ServerMessage) => void;
  reject: (error: Error) => void;
};

class TestSocket {
  readonly messages: ServerMessage[] = [];
  readonly socket: WebSocket;
  private readonly waiters: MessageWaiter[] = [];

  private constructor(socket: WebSocket) {
    this.socket = socket;
    socket.addEventListener("message", (event) => {
      if (typeof event.data !== "string") {
        throw new Error("Expected a text WebSocket message");
      }

      const message = parseServerMessage(event.data);
      this.messages.push(message);

      for (let index = this.waiters.length - 1; index >= 0; index -= 1) {
        const waiter = this.waiters[index];
        if (!waiter.predicate(message)) {
          continue;
        }
        this.waiters.splice(index, 1);
        waiter.resolve(message);
      }
    });
  }

  static async connect(): Promise<TestSocket> {
    const response = await exports.default.fetch(
      new Request("https://example.com/api/todos", {
        headers: { Upgrade: "websocket" },
      }),
    );
    expect(response.status).toBe(101);
    expect(response.webSocket).not.toBeNull();

    const socket = new TestSocket(response.webSocket as WebSocket);
    socket.socket.accept();
    return socket;
  }

  waitFor(predicate: MessagePredicate, startAt = 0): Promise<ServerMessage> {
    for (const message of this.messages.slice(startAt)) {
      if (predicate(message)) {
        return Promise.resolve(message);
      }
    }

    return new Promise<ServerMessage>((resolve, reject) => {
      this.waiters.push({ predicate, resolve, reject });
    });
  }

  send(message: ClientMessage): void {
    this.socket.send(JSON.stringify(message));
  }

  close(): void {
    for (const waiter of this.waiters) {
      waiter.reject(new Error("Test socket closed while waiting for a message"));
    }
    this.waiters.length = 0;
    this.socket.close();
  }
}

function isQueryResult<Name extends QueryResultMessage["query"]>(
  message: ServerMessage,
  query: Name,
): message is Extract<QueryResultMessage, { query: Name }> {
  return message.type === "queryResult" && message.query === query;
}

describe("TodoStore hibernation", () => {
  it("restores attachment-backed subscriptions after eviction", async () => {
    let first: TestSocket | undefined;
    let second: TestSocket | undefined;

    try {
      first = await TestSocket.connect();
      second = await TestSocket.connect();

      const firstAllTodos = first.waitFor(
        (message) => isQueryResult(message, "allTodos") && message.result.length === 0,
      );
      const firstTodoCount = first.waitFor(
        (message) => isQueryResult(message, "todoCount") && message.result[0]?.total_count === 0,
      );
      first.send({
        type: "subscribe",
        requestId: "first-subscribe",
        queries: ["allTodos", "todoCount"],
      });
      await Promise.all([firstAllTodos, firstTodoCount]);

      const secondAllTodos = second.waitFor(
        (message) => isQueryResult(message, "allTodos") && message.result.length === 0,
      );
      second.send({
        type: "subscribe",
        requestId: "second-subscribe",
        queries: ["allTodos"],
      });
      await secondAllTodos;

      const firstAfterEviction = first.messages.length;
      const secondAfterEviction = second.messages.length;
      await evictDurableObject(env.TODO_STORE.getByName("default"), {
        webSockets: "hibernate",
      });

      const firstTodoAfterEviction = first.waitFor(
        (message) =>
          isQueryResult(message, "allTodos") &&
          message.result.length === 1 &&
          message.result[0]?.title === "survives hibernation",
        firstAfterEviction,
      );
      const firstCountAfterEviction = first.waitFor(
        (message) => isQueryResult(message, "todoCount") && message.result[0]?.total_count === 1,
        firstAfterEviction,
      );
      const mutationAcknowledgement = first.waitFor(
        (message) =>
          message.type === "mutation" &&
          message.requestId === "after-hibernation" &&
          message.mutation.affectedTables.length === 1 &&
          message.mutation.affectedTables[0] === "todos",
        firstAfterEviction,
      );
      const secondTodoAfterEviction = second.waitFor(
        (message) =>
          isQueryResult(message, "allTodos") &&
          message.result.length === 1 &&
          message.result[0]?.title === "survives hibernation",
        secondAfterEviction,
      );

      first.send({
        type: "addTodo",
        requestId: "after-hibernation",
        title: "survives hibernation",
      });

      const [firstTodo, firstCount, acknowledgement, secondTodo] = await Promise.all([
        firstTodoAfterEviction,
        firstCountAfterEviction,
        mutationAcknowledgement,
        secondTodoAfterEviction,
      ]);

      expect(firstTodo).toMatchObject({
        type: "queryResult",
        query: "allTodos",
        result: [{ id: 1, title: "survives hibernation", completed: 0 }],
      });
      expect(isQueryResult(firstTodo, "allTodos")).toBe(true);
      if (isQueryResult(firstTodo, "allTodos")) {
        expect(typeof firstTodo.result[0]?.created_at).toBe("number");
      }
      expect(firstCount).toEqual({
        type: "queryResult",
        query: "todoCount",
        result: [{ total_count: 1 }],
      });
      expect(acknowledgement).toEqual({
        type: "mutation",
        requestId: "after-hibernation",
        mutation: { affectedTables: ["todos"] },
      });
      expect(secondTodo).toMatchObject({
        type: "queryResult",
        query: "allTodos",
        result: [{ id: 1, title: "survives hibernation", completed: 0 }],
      });
      expect(isQueryResult(secondTodo, "allTodos")).toBe(true);
      if (isQueryResult(secondTodo, "allTodos")) {
        expect(typeof secondTodo.result[0]?.created_at).toBe("number");
      }

      const firstPostEvictionQueries = first.messages
        .slice(firstAfterEviction)
        .filter((message): message is QueryResultMessage => message.type === "queryResult");
      const secondPostEvictionQueries = second.messages
        .slice(secondAfterEviction)
        .filter((message): message is QueryResultMessage => message.type === "queryResult");
      expect(
        firstPostEvictionQueries
          .filter(
            (message) =>
              (message.query === "allTodos" && message.result.length === 1) ||
              (message.query === "todoCount" && message.result[0]?.total_count === 1),
          )
          .map((message) => message.query)
          .sort(),
      ).toEqual(["allTodos", "todoCount"]);
      expect(secondPostEvictionQueries.every((message) => message.query === "allTodos")).toBe(true);
    } finally {
      first?.close();
      second?.close();
    }
  });
});
