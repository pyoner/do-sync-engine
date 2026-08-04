import { Effect, Exit, Fiber, Option, Queue, Result, Schema, Stream } from "effect";
import { exports, env } from "cloudflare:workers";
import { evictDurableObject } from "cloudflare:test";
import { expect, it } from "vite-plus/test";
import { Rpc, RpcGroup } from "effect/unstable/rpc";
import { SyncEngine, Topic, toTables, UnknownMutationError } from "@do-sync-engine/core";
import type { Mutation, Query, SyncEngineInterface } from "@do-sync-engine/core";
import { RpcOperationError, Subscribe, Unsubscribe } from "../src/protocol.ts";
import { makeWebSocketRpcClient } from "../src/client.ts";
import { makeWebSocketRpcClientFor } from "../src/client-transport.ts";
import { SubscriptionRegistry } from "../src/subscriptions.ts";
import type { FixtureSyncObject } from "./cloudflare-worker.ts";

const worker = exports as unknown as { default: { fetch(request: Request): Promise<Response> } };
const fixtureEnv = env as typeof env & {
  FIXTURE_SYNC_OBJECT: DurableObjectNamespace<FixtureSyncObject>;
};

const openSocket = async () => {
  const response = await worker.default.fetch(
    new Request("https://example.com", { headers: { Upgrade: "websocket" } }),
  );
  expect(response.status).toBe(101);
  const socket = response.webSocket!;
  socket.accept();
  return socket;
};

const InvalidSync = Rpc.make("sync", {
  payload: Schema.Struct({ mutation: Schema.String, params: Schema.Unknown }),
  success: Schema.Void,
  error: Schema.Union([UnknownMutationError, RpcOperationError]),
});
const InvalidRpc = RpcGroup.make(Subscribe, Unsubscribe, InvalidSync);

const registryEngine = () =>
  new SyncEngine({
    queries: {
      counter: {
        tables: toTables(["counters"]),
        run: () => Effect.succeed({ value: 0 }),
      },
    },
    mutations: {
      increment: {
        tables: toTables(["counters"]),
        run: () => Effect.succeed(undefined),
      },
    },
  });

it("shares equivalent subscriptions and persists one canonical topic", async () => {
  const engine = registryEngine();
  let attachment: unknown = { id: 7, topics: [] };
  const socket = {
    deserializeAttachment: () => attachment,
    serializeAttachment: (value: unknown) => {
      attachment = value;
    },
  } as WebSocket;
  const registry = new SubscriptionRegistry(
    socket,
    engine as unknown as SyncEngineInterface<
      { counter: Query<[string], { value: number }> },
      { increment: Mutation<[string, number], void> }
    >,
  );

  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const firstEvents = yield* Queue.unbounded<unknown>();
        const secondEvents = yield* Queue.unbounded<unknown>();
        const first = yield* Effect.forkScoped(
          Stream.runForEach(
            registry.subscribeStream(new Topic({ name: "counter", params: ["same"] })),
            (event) => Queue.offer(firstEvents, event),
          ),
        );
        yield* Effect.forkScoped(
          Stream.runForEach(
            registry.subscribeStream(new Topic({ name: "counter", params: ["same"] })),
            (event) => Queue.offer(secondEvents, event),
          ),
        );
        const firstEvent = yield* Queue.take(firstEvents);
        const secondEvent = yield* Queue.take(secondEvents);
        expect(firstEvent).toEqual(secondEvent);
        expect(firstEvent).toMatchObject({
          topic: { name: "counter", params: ["same"] },
          value: { value: 0 },
        });
        if (
          typeof attachment === "object" &&
          attachment !== null &&
          "id" in attachment &&
          "topics" in attachment
        ) {
          expect(typeof attachment.id).toBe("number");
          expect(attachment.topics).toHaveLength(1);
        }
        const backing = (engine as unknown as { registry: { backing: Map<unknown, unknown> } })
          .registry;
        expect(backing.backing.size).toBe(1);

        yield* engine.sync("increment", []);
        expect(yield* Queue.take(firstEvents)).toEqual(secondEvent);
        expect(yield* Queue.take(secondEvents)).toEqual(secondEvent);

        const topic = firstEvent as { readonly topic: Topic };
        yield* registry.unsubscribe(topic.topic);
        if (
          typeof attachment === "object" &&
          attachment !== null &&
          "id" in attachment &&
          "topics" in attachment
        ) {
          expect(typeof attachment.id).toBe("number");
          expect(attachment.topics).toHaveLength(0);
        }
        expect(Exit.isSuccess(yield* Fiber.await(first))).toBe(true);
      }),
    ),
  );
});

it("resets malformed and old attachments with a fresh numeric ID", async () => {
  for (const raw of [
    undefined,
    { version: 1, subscriptions: [] },
    { id: "not a number", topics: [] },
    { id: 4, topics: [{ name: "counter", params: "invalid" }] },
  ]) {
    const engine = registryEngine();
    let attachment: unknown = raw;
    const socket = {
      deserializeAttachment: () => attachment,
      serializeAttachment: (value: unknown) => {
        attachment = value;
      },
    } as WebSocket;
    const registry = new SubscriptionRegistry(
      socket,
      engine as unknown as SyncEngineInterface<
        { counter: Query<[string], { value: number }> },
        { increment: Mutation<[string, number], void> }
      >,
    );
    await Effect.runPromise(registry.restore());
    expect(attachment).toMatchObject({ topics: [] });
    if (typeof attachment === "object" && attachment !== null && "id" in attachment)
      expect(typeof attachment.id).toBe("number");
  }
});

it("cleans up every listener when restoring a topic fails", async () => {
  const engine = new SyncEngine({
    queries: {
      first: {
        tables: toTables(["counters"]),
        run: () => Effect.succeed({ value: 0 }),
      },
      second: {
        tables: toTables(["counters"]),
        run: () => Effect.fail(new Error("snapshot failed")),
      },
    },
    mutations: {},
  });
  const firstTopic = await Effect.runPromise(engine.createTopic("first", []));
  const secondTopic = await Effect.runPromise(engine.createTopic("second", []));
  let attachment: unknown = { id: 11, topics: [firstTopic, secondTopic] };
  const socket = {
    deserializeAttachment: () => attachment,
    serializeAttachment: (value: unknown) => {
      attachment = value;
    },
  } as WebSocket;
  const registry = new SubscriptionRegistry(
    socket,
    engine as unknown as SyncEngineInterface<
      { first: Query<[], { value: number }>; second: Query<[], { value: number }> },
      Record<string, never>
    >,
  );

  const result = Effect.runSyncExit(registry.restore());
  expect(Exit.isFailure(result)).toBe(true);
  expect(attachment).toEqual({ id: 11, topics: [] });
});

it("runs typed RPC requests over the real Durable Object socket", async () => {
  const socket = await openSocket();
  try {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* makeWebSocketRpcClient(socket);
          yield* client.sync({ mutation: "increment", params: ["typed", 2] });
          const result = yield* Stream.runHead(
            client.subscribe(new Topic({ name: "counter", params: ["typed"] })),
          );
          expect(result._tag).toBe("Some");
          if (result._tag === "Some")
            expect(result.value.value).toEqual({ key: "typed", value: 2 });
        }),
      ),
    );
  } finally {
    socket.close();
  }
});

it("returns each declared typed operation failure", async () => {
  const socket = await openSocket();
  try {
    const result = await Effect.runPromiseExit(
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* makeWebSocketRpcClient(socket);
          const unknownMutation = yield* Effect.exit(
            client.sync({ mutation: "missing", params: [] }),
          );
          const unknownQuery = yield* Effect.exit(
            Stream.runHead(client.subscribe(new Topic({ name: "missing", params: [] }))),
          );
          const failedMutation = yield* Effect.exit(client.sync({ mutation: "fail", params: [] }));
          return { unknownMutation, unknownQuery, failedMutation };
        }),
      ),
    );
    expect(Exit.isSuccess(result)).toBe(true);
    if (Exit.isSuccess(result)) {
      const unknownMutation = Exit.findErrorOption(result.value.unknownMutation);
      const unknownQuery = Exit.findErrorOption(result.value.unknownQuery);
      const failedMutation = Exit.findErrorOption(result.value.failedMutation);
      expect(Option.isSome(unknownMutation)).toBe(true);
      expect(Option.isSome(unknownQuery)).toBe(true);
      expect(Option.isSome(failedMutation)).toBe(true);
      if (Option.isSome(unknownMutation))
        expect(unknownMutation.value).toMatchObject({
          _tag: "UnknownMutationError",
          mutation: "missing",
        });
      if (Option.isSome(unknownQuery))
        expect(unknownQuery.value).toMatchObject({ _tag: "UnknownQueryError", query: "missing" });
      if (Option.isSome(failedMutation))
        expect(failedMutation.value).toMatchObject({
          _tag: "RpcOperationError",
          message: "fixture mutation failed",
        });
    }
  } finally {
    socket.close();
  }
});

it("correlates malformed payload defects without poisoning other requests", async () => {
  const socket = await openSocket();
  try {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* makeWebSocketRpcClientFor(socket, InvalidRpc);
          const events = yield* Queue.unbounded<{
            readonly topic: Topic;
            readonly value: unknown;
          }>();
          const stream = client
            .subscribe(new Topic({ name: "counter", params: ["schema"] }))
            .pipe(Stream.runForEach((event) => Queue.offer(events, event)));
          const fiber = yield* Effect.forkScoped(stream);
          const first = yield* Queue.take(events);
          expect(first.value).toEqual({ key: "schema", value: 0 });

          const malformed = yield* Effect.exit(
            client.sync({ mutation: "increment", params: "invalid" }),
          );
          expect(Exit.hasDies(malformed)).toBe(true);
          const defect = Exit.findDefect(malformed);
          expect(Result.isSuccess(defect)).toBe(true);
          if (Result.isSuccess(defect)) expect(String(defect.success)).toMatch(/array/i);

          yield* client.sync({ mutation: "increment", params: ["schema", 1] });
          expect((yield* Queue.take(events)).value).toEqual({ key: "schema", value: 1 });
          yield* client.unsubscribe(first.topic);
          yield* Fiber.await(fiber);
        }),
      ),
    );
  } finally {
    socket.close();
  }
});

it("keeps a malformed-frame socket open for typed RPC", async () => {
  const socket = await openSocket();
  try {
    const frames: unknown[] = [];
    let resolveDefect!: (frame: unknown) => void;
    const defect = new Promise<unknown>((resolve) => {
      resolveDefect = resolve;
    });
    const listener = (event: MessageEvent) => {
      const frame = JSON.parse(event.data);
      frames.push(frame);
      if (typeof frame === "object" && frame !== null && "_tag" in frame && frame._tag === "Defect")
        resolveDefect(frame);
    };
    socket.addEventListener("message", listener);
    socket.send("not valid json");
    expect(await defect).toMatchObject({ _tag: "Defect" });
    expect(frames).toHaveLength(1);
    socket.removeEventListener("message", listener);
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* makeWebSocketRpcClient(socket);
          yield* client.sync({ mutation: "increment", params: ["framing", 1] });
        }),
      ),
    );
  } finally {
    socket.close();
  }
});
it("resubscribes explicitly after Durable Object hibernation", async () => {
  const socket = await openSocket();
  try {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* makeWebSocketRpcClient(socket);
          yield* client.sync({ mutation: "increment", params: ["hibernation", 2] });
          const staleEvents = yield* Queue.unbounded<{
            readonly topic: Topic;
            readonly value: unknown;
          }>();
          const stale = yield* Effect.forkScoped(
            Stream.runForEach(
              client.subscribe(new Topic({ name: "counter", params: ["hibernation"] })),
              (event) => Queue.offer(staleEvents, event),
            ),
          );
          const first = yield* Queue.take(staleEvents);
          yield* Effect.promise(() =>
            evictDurableObject(fixtureEnv.FIXTURE_SYNC_OBJECT.getByName("default"), {
              webSockets: "hibernate",
            }),
          );
          yield* Fiber.interrupt(stale);
          const replacementEvents = yield* Queue.unbounded<{
            readonly topic: Topic;
            readonly value: unknown;
          }>();
          const replacement = yield* Effect.forkScoped(
            Stream.runForEach(
              client.subscribe(new Topic({ name: "counter", params: ["hibernation"] })),
              (event) => Queue.offer(replacementEvents, event),
            ),
          );
          const replay = yield* Queue.take(replacementEvents);
          expect(replay.value).toEqual(first.value);
          yield* client.sync({ mutation: "increment", params: ["hibernation", 1] });
          expect((yield* Queue.take(replacementEvents)).value).toEqual({
            key: "hibernation",
            value: 3,
          });
          yield* client.unsubscribe(replay.topic);
          yield* Fiber.await(replacement);
        }),
      ),
    );
  } finally {
    socket.close();
  }
});

it("interrupts a typed RPC when the socket closes", async () => {
  const socket = await openSocket();
  const result = await Effect.runPromiseExit(
    Effect.scoped(
      Effect.gen(function* () {
        const client = yield* makeWebSocketRpcClient(socket);
        socket.close();
        return yield* client.sync({ mutation: "increment", params: ["closed", 1] });
      }),
    ),
  );
  expect(result._tag).toBe("Failure");
});
