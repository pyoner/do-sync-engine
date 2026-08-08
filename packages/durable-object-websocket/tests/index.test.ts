import { Effect, Exit, Fiber, Stream } from "effect";
import { exports } from "cloudflare:workers";
import { expect, it } from "vite-plus/test";
import { Topic } from "@do-sync-engine/core";
import { makeWebSocketRpcSession } from "../src/client";

const worker = exports as unknown as { default: { fetch(request: Request): Promise<Response> } };

it("runs Cap’n Web RPC over a Durable Object WebSocket", async () => {
  const response = await worker.default.fetch(
    new Request("https://example.com", { headers: { Upgrade: "websocket" } }),
  );
  expect(response.status).toBe(101);
  const socket = response.webSocket;
  if (!socket) throw new Error("Worker did not return a WebSocket");
  socket.accept();
  try {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* makeWebSocketRpcSession(socket);
          yield* session.client.sync({ mutation: "increment", params: ["capnweb", 1] });
          const event = yield* Stream.runHead(
            session.client.subscribe(new Topic({ name: "counter", params: ["capnweb"] })),
          );
          expect(event._tag).toBe("Some");
          if (event._tag === "Some")
            expect(event.value.value).toEqual({ key: "capnweb", value: 1 });
        }),
      ),
    );
  } finally {
    socket.close();
  }
});

it("releases pending subscriptions when the session closes", async () => {
  const response = await worker.default.fetch(
    new Request("https://example.com", { headers: { Upgrade: "websocket" } }),
  );
  const socket = response.webSocket;
  if (!socket) throw new Error("Worker did not return a WebSocket");
  socket.accept();
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const session = yield* makeWebSocketRpcSession(socket);
        const pending = yield* Effect.forkScoped(
          Stream.runDrain(
            session.client.subscribe(new Topic({ name: "counter", params: ["pending"] })),
          ),
        );
        session.close();
        const exit = yield* Fiber.await(pending);
        expect(Exit.isFailure(exit)).toBe(true);
      }),
    ),
  );
});
