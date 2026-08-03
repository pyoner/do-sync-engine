import { Effect, Exit, Fiber, Option, Queue, Result, Schema, Stream } from "effect";
import { exports, env } from "cloudflare:workers";
import { evictDurableObject } from "cloudflare:test";
import { expect, it } from "vite-plus/test";
import { Rpc, RpcGroup } from "effect/unstable/rpc";
import { SyncEngine, toTables, UnknownMutationError } from "@do-sync-engine/core";
import type { Mutation, Query, SyncEngineInterface } from "@do-sync-engine/core";
import { RpcOperationError, Subscribe, Unsubscribe } from "../src/protocol.ts";
import { makeWebSocketRpcClient, Topic } from "../src/client.ts";
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

it("restores duplicate sessions with one listener and filters invalid IDs", async () => {
  const engine = registryEngine();
  let attachment: unknown = {
    version: 1,
    subscriptions: [
      { requestId: "first", query: "counter", params: ["same"], headers: [] },
      { requestId: "first", query: "counter", params: ["duplicate"], headers: [] },
      { requestId: 42, query: "counter", params: ["same"], headers: [["x-test", "yes"]] },
      { requestId: {}, query: "counter", params: ["invalid"], headers: [] },
      { requestId: "malformed", query: "counter", params: "invalid", headers: [] },
    ],
  };
  const socket = {
    deserializeAttachment: () => attachment,
    serializeAttachment: (value: unknown) => {
      attachment = value;
    },
  } as WebSocket;
  const registry = new SubscriptionRegistry(
    engine as unknown as SyncEngineInterface<
      { counter: Query<[string], { value: number }> },
      { increment: Mutation<[string, number], void> }
    >,
  );

  const restored = await Effect.runPromise(registry.restore(socket));
  expect(restored.map(({ requestId }) => requestId)).toEqual(["first", 42]);
  expect(attachment).toEqual({
    version: 1,
    subscriptions: [
      { requestId: "first", query: "counter", params: ["same"], headers: [] },
      { requestId: 42, query: "counter", params: ["same"], headers: [["x-test", "yes"]] },
    ],
  });
  const backing = (engine as unknown as { registry: { backing: Map<unknown, unknown> } }).registry;
  expect(backing.backing.size).toBe(1);
  await Effect.runPromise(registry.clear(socket));
});

it("rolls back every listener when restoring snapshots fails", async () => {
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
  let attachment: unknown = {
    version: 1,
    subscriptions: [
      { requestId: "first", query: "first", params: [], headers: [] },
      { requestId: "second", query: "second", params: [], headers: [] },
    ],
  };
  const socket = {
    deserializeAttachment: () => attachment,
    serializeAttachment: (value: unknown) => {
      attachment = value;
    },
  } as WebSocket;
  const registry = new SubscriptionRegistry(
    engine as unknown as SyncEngineInterface<
      { first: Query<[], { value: number }>; second: Query<[], { value: number }> },
      Record<string, never>
    >,
  );

  const result = Effect.runSyncExit(registry.restore(socket));
  expect(Exit.isFailure(result)).toBe(true);
  const backing = (engine as unknown as { registry: { backing: Map<unknown, unknown> } }).registry;
  expect(backing.backing.size).toBe(0);
  expect(attachment).toEqual({ version: 1, subscriptions: [] });
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
            client.subscribe({ query: "counter", params: ["typed"] }),
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
            Stream.runHead(client.subscribe({ query: "missing", params: [] })),
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
          const events = yield* Queue.unbounded<unknown>();
          const stream = client
            .subscribe({ query: "counter", params: ["schema"] })
            .pipe(Stream.runForEach((event) => Queue.offer(events, event.value)));
          const fiber = yield* Effect.forkScoped(stream);
          expect(yield* Queue.take(events)).toEqual({ key: "schema", value: 0 });

          const malformed = yield* Effect.exit(
            client.sync({ mutation: "increment", params: "invalid" }),
          );
          expect(Exit.hasDies(malformed)).toBe(true);
          const defect = Exit.findDefect(malformed);
          expect(Result.isSuccess(defect)).toBe(true);
          if (Result.isSuccess(defect)) expect(String(defect.success)).toMatch(/array/i);

          yield* client.sync({ mutation: "increment", params: ["schema", 1] });
          expect(yield* Queue.take(events)).toEqual({ key: "schema", value: 1 });
          yield* client.unsubscribe({ topic: new Topic({ name: "counter", params: ["schema"] }) });
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

it("ends an active stream after typed unsubscribe", async () => {
  const socket = await openSocket();
  try {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* makeWebSocketRpcClient(socket);
          const events = yield* Queue.unbounded<unknown>();
          const fiber = yield* Effect.forkScoped(
            Stream.runForEach(
              client.subscribe({ query: "counter", params: ["unsubscribe"] }),
              (event) => Queue.offer(events, event.value),
            ),
          );
          expect(yield* Queue.take(events)).toEqual({ key: "unsubscribe", value: 0 });
          const removed = yield* client.unsubscribe({
            topic: new Topic({ name: "counter", params: ["unsubscribe"] }),
          });
          expect(removed).toBe(true);
          yield* Fiber.await(fiber);
        }),
      ),
    );
  } finally {
    socket.close();
  }
});

it("resumes the original stream across Durable Object hibernation", async () => {
  const socket = await openSocket();
  try {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* makeWebSocketRpcClient(socket);
          yield* client.sync({ mutation: "increment", params: ["hibernation", 2] });
          const events = yield* Queue.unbounded<unknown>();
          const fiber = yield* Effect.forkScoped(
            Stream.runForEach(
              client.subscribe({ query: "counter", params: ["hibernation"] }),
              (event) => Queue.offer(events, event.value),
            ),
          );
          expect(yield* Queue.take(events)).toEqual({ key: "hibernation", value: 2 });

          yield* Effect.promise(() =>
            evictDurableObject(fixtureEnv.FIXTURE_SYNC_OBJECT.getByName("default"), {
              webSockets: "hibernate",
            }),
          );
          yield* client.sync({ mutation: "increment", params: ["hibernation", 1] });
          expect(yield* Queue.take(events)).toEqual({ key: "hibernation", value: 3 });
          yield* Effect.yieldNow;
          yield* Effect.yieldNow;
          expect(yield* Queue.poll(events)).toEqual(Option.none());

          const removed = yield* client.unsubscribe({
            topic: new Topic({ name: "counter", params: ["hibernation"] }),
          });
          expect(removed).toBe(true);
          yield* Fiber.await(fiber);
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
