import { Cause, Effect, Exit, Fiber, Option, Queue, Schema, Scope, Stream } from "effect";
import { exports, env } from "cloudflare:workers";
import { evictDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vite-plus/test";
import { makeWebSocketRpcClient } from "@do-sync-engine/durable-object-websocket";
import type { WebSocketRpcClient } from "@do-sync-engine/durable-object-websocket";
import type { ListenerId } from "@do-sync-engine/core";
import {
  todoCountSchema,
  todoSchema,
  todoSummarySchema,
  type Todo,
  type TodoCount,
  type TodoQueries,
  type TodoSummary,
} from "../src/todo-protocol";
const openSocket = async () => {
  const response = await exports.default.fetch(
    new Request("https://example.com/api/todos", { headers: { Upgrade: "websocket" } }),
  );
  expect(response.status).toBe(101);
  const socket = response.webSocket;
  if (!socket) throw new Error("Worker did not return a WebSocket");
  socket.accept();
  return socket;
};
type DecodedSubscription<T> = {
  readonly queue: Queue.Queue<T>;
  readonly fiber: Fiber.Fiber<void, unknown>;
  readonly listenerId: () => ListenerId | undefined;
};

const subscribeDecoded = <S extends Schema.Decoder<readonly unknown[], never>>(
  client: WebSocketRpcClient,
  query: keyof TodoQueries,
  schema: S,
): Effect.Effect<DecodedSubscription<S["Type"]>, never, Scope.Scope> =>
  Effect.gen(function* () {
    const queue = yield* Queue.unbounded<S["Type"]>();
    let id: ListenerId | undefined;
    const decodedStream = client.subscribe({ query, params: [] }).pipe(
      Stream.tap((event) =>
        Effect.sync(() => {
          id = event.listenerId;
        }),
      ),
      Stream.mapEffect((event) => Schema.decodeUnknownEffect(schema)(event.value)),
    );
    const fiber = yield* Effect.forkScoped(
      Stream.runForEach(decodedStream, (value) => Queue.offer(queue, value)).pipe(
        Effect.catchCauseIf(Cause.hasInterrupts, () => Effect.void),
      ),
    );
    return { queue, fiber, listenerId: () => id };
  });
const uniqueTitle = () => `todo-${crypto.randomUUID()}`;

const takeFour = (subscriptions: {
  readonly allTodos: DecodedSubscription<readonly Todo[]>;
  readonly incompleteTodos: DecodedSubscription<readonly TodoSummary[]>;
  readonly completedTodos: DecodedSubscription<readonly TodoSummary[]>;
  readonly todoCount: DecodedSubscription<readonly TodoCount[]>;
}) =>
  Effect.all(
    [
      Queue.take(subscriptions.allTodos.queue),
      Queue.take(subscriptions.incompleteTodos.queue),
      Queue.take(subscriptions.completedTodos.queue),
      Queue.take(subscriptions.todoCount.queue),
    ],
    { concurrency: "unbounded" },
  );

describe("TodoStore WebSocket RPC", () => {
  it("enforces the Worker route boundaries", async () => {
    const notFound = await exports.default.fetch(new Request("https://example.com/not-todos"));
    expect(notFound.status).toBe(404);

    const missingUpgrade = await exports.default.fetch(
      new Request("https://example.com/api/todos"),
    );
    expect(missingUpgrade.status).toBe(426);

    const socket = await openSocket();
    socket.close();
  });

  it("runs the complete add-toggle-delete lifecycle through four RPC subscriptions", async () => {
    const socket = await openSocket();
    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const client = yield* makeWebSocketRpcClient(socket);
            const subscriptions = {
              allTodos: yield* subscribeDecoded(client, "allTodos", Schema.Array(todoSchema)),
              incompleteTodos: yield* subscribeDecoded(
                client,
                "incompleteTodos",
                Schema.Array(todoSummarySchema),
              ),
              completedTodos: yield* subscribeDecoded(
                client,
                "completedTodos",
                Schema.Array(todoSummarySchema),
              ),
              todoCount: yield* subscribeDecoded(
                client,
                "todoCount",
                Schema.Array(todoCountSchema),
              ),
            };

            yield* Queue.take(subscriptions.allTodos.queue);
            yield* Queue.take(subscriptions.incompleteTodos.queue);
            yield* Queue.take(subscriptions.completedTodos.queue);
            const initialCount = yield* Queue.take(subscriptions.todoCount.queue);
            const initialCountRow = initialCount[0];
            if (!initialCountRow) throw new Error("Todo count query returned no row");
            const initialTotal = initialCountRow.total_count;
            const title = uniqueTitle();

            yield* client.sync({ mutation: "addTodo", params: [title] });
            const [afterAdd, incompleteAfterAdd, completedAfterAdd, countAfterAdd] =
              yield* takeFour(subscriptions);
            const addedTodo = afterAdd.find((todo) => todo.title === title);
            expect(addedTodo).toBeDefined();
            if (!addedTodo) throw new Error("Added todo was not returned by allTodos");
            expect(incompleteAfterAdd.some((todo) => todo.id === addedTodo.id)).toBe(true);
            expect(completedAfterAdd.some((todo) => todo.id === addedTodo.id)).toBe(false);
            expect(countAfterAdd[0]?.total_count).toBe(initialTotal + 1);

            yield* client.sync({ mutation: "toggleTodo", params: [addedTodo.id] });
            const [afterToggle, incompleteAfterToggle, completedAfterToggle, countAfterToggle] =
              yield* takeFour(subscriptions);
            expect(afterToggle.find((todo) => todo.id === addedTodo.id)?.completed).toBe(1);
            expect(incompleteAfterToggle.some((todo) => todo.id === addedTodo.id)).toBe(false);
            expect(completedAfterToggle.some((todo) => todo.id === addedTodo.id)).toBe(true);
            expect(countAfterToggle[0]?.total_count).toBe(initialTotal + 1);

            yield* client.sync({ mutation: "deleteTodo", params: [addedTodo.id] });
            const [afterDelete, incompleteAfterDelete, completedAfterDelete, countAfterDelete] =
              yield* takeFour(subscriptions);
            expect(afterDelete.some((todo) => todo.id === addedTodo.id)).toBe(false);
            expect(incompleteAfterDelete.some((todo) => todo.id === addedTodo.id)).toBe(false);
            expect(completedAfterDelete.some((todo) => todo.id === addedTodo.id)).toBe(false);
            expect(countAfterDelete[0]?.total_count).toBe(initialTotal);
          }),
        ),
      );
    } finally {
      socket.close();
    }
  });

  it("converges allTodos snapshots across independent browser sockets", async () => {
    const writerSocket = await openSocket();
    const readerSocket = await openSocket();
    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const writer = yield* makeWebSocketRpcClient(writerSocket);
            const reader = yield* makeWebSocketRpcClient(readerSocket);
            const writerSubscription = yield* subscribeDecoded(
              writer,
              "allTodos",
              Schema.Array(todoSchema),
            );
            const readerSubscription = yield* subscribeDecoded(
              reader,
              "allTodos",
              Schema.Array(todoSchema),
            );
            yield* Effect.all(
              [Queue.take(writerSubscription.queue), Queue.take(readerSubscription.queue)],
              { concurrency: "unbounded" },
            );

            const title = uniqueTitle();
            yield* writer.sync({ mutation: "addTodo", params: [title] });
            const [writerUpdate, readerUpdate] = yield* Effect.all(
              [Queue.take(writerSubscription.queue), Queue.take(readerSubscription.queue)],
              { concurrency: "unbounded" },
            );
            const writerTodo = writerUpdate.find((todo) => todo.title === title);
            const readerTodo = readerUpdate.find((todo) => todo.title === title);
            expect(writerTodo).toBeDefined();
            expect(readerTodo).toBeDefined();
            if (!writerTodo || !readerTodo) throw new Error("Todo did not converge across sockets");
            expect(readerTodo).toMatchObject({ id: writerTodo.id, completed: 0 });

            yield* reader.sync({ mutation: "deleteTodo", params: [writerTodo.id] });
            const [writerCleanup, readerCleanup] = yield* Effect.all(
              [Queue.take(writerSubscription.queue), Queue.take(readerSubscription.queue)],
              { concurrency: "unbounded" },
            );
            expect(writerCleanup.some((todo) => todo.id === writerTodo.id)).toBe(false);
            expect(readerCleanup.some((todo) => todo.id === writerTodo.id)).toBe(false);
          }),
        ),
      );
    } finally {
      writerSocket.close();
      readerSocket.close();
    }
  });

  it("finishes an active stream after an explicit unsubscribe", async () => {
    const socket = await openSocket();
    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const client = yield* makeWebSocketRpcClient(socket);
            const subscription = yield* subscribeDecoded(
              client,
              "allTodos",
              Schema.Array(todoSchema),
            );
            yield* Queue.take(subscription.queue);
            const listenerId = subscription.listenerId();
            if (!listenerId) throw new Error("Subscription listener ID was not received");
            expect(yield* client.unsubscribe({ listenerId })).toBe(true);
            yield* Effect.yieldNow;
            const exit = yield* Fiber.await(subscription.fiber);
            expect(Exit.isSuccess(exit)).toBe(true);
            expect(yield* client.unsubscribe({ listenerId })).toBe(false);
          }),
        ),
      );
    } finally {
      socket.close();
    }
  });

  it("keeps the original decoded stream alive across Durable Object hibernation", async () => {
    const socket = await openSocket();
    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const client = yield* makeWebSocketRpcClient(socket);
            const subscription = yield* subscribeDecoded(
              client,
              "allTodos",
              Schema.Array(todoSchema),
            );
            yield* Queue.take(subscription.queue);
            const listenerId = subscription.listenerId();
            if (!listenerId) throw new Error("Subscription listener ID was not received");
            const title = uniqueTitle();

            yield* client.sync({ mutation: "addTodo", params: [title] });
            const added = (yield* Queue.take(subscription.queue)).find(
              (todo) => todo.title === title,
            );
            expect(added).toBeDefined();
            if (!added) throw new Error("Hibernation todo was not returned");

            yield* Effect.promise(() =>
              evictDurableObject(env.TODO_STORE.getByName("default"), { webSockets: "hibernate" }),
            );
            yield* client.sync({ mutation: "toggleTodo", params: [added.id] });
            const toggled = (yield* Queue.take(subscription.queue)).find(
              (todo) => todo.id === added.id,
            );
            expect(toggled?.completed).toBe(1);
            yield* Effect.yieldNow;
            yield* Effect.yieldNow;
            expect(yield* Queue.poll(subscription.queue)).toEqual(Option.none());

            yield* client.sync({ mutation: "deleteTodo", params: [added.id] });
            const afterDelete = yield* Queue.take(subscription.queue);
            expect(afterDelete.some((todo) => todo.id === added.id)).toBe(false);
            expect(yield* client.unsubscribe({ listenerId })).toBe(true);
          }),
        ),
      );
    } finally {
      socket.close();
    }
  });

  it("returns declared RPC failures and interrupts a request when the socket closes", async () => {
    const socket = await openSocket();
    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const client = yield* makeWebSocketRpcClient(socket);
            const unknownMutation = yield* Effect.exit(
              client.sync({ mutation: "missing", params: [] }),
            );
            const unknownQuery = yield* Effect.exit(
              Stream.runDrain(client.subscribe({ query: "missing", params: [] })),
            );
            const mutationError = Exit.findErrorOption(unknownMutation);
            const queryError = Exit.findErrorOption(unknownQuery);
            expect(Option.isSome(mutationError)).toBe(true);
            expect(Option.isSome(queryError)).toBe(true);
            if (Option.isSome(mutationError)) {
              expect(mutationError.value).toMatchObject({
                _tag: "UnknownMutationError",
                mutation: "missing",
              });
            }
            if (Option.isSome(queryError)) {
              expect(queryError.value).toMatchObject({
                _tag: "UnknownQueryError",
                query: "missing",
              });
            }
          }),
        ),
      );
    } finally {
      socket.close();
    }

    const closingSocket = await openSocket();
    const result = await Effect.runPromiseExit(
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* makeWebSocketRpcClient(closingSocket);
          closingSocket.close();
          return yield* client.sync({ mutation: "toggleTodo", params: [-1] });
        }),
      ),
    );
    expect(Exit.hasInterrupts(result)).toBe(true);
  });
});
