import { Effect, Stream } from "effect";
import { newWebSocketRpcSession } from "capnweb";
import type { RpcStub } from "capnweb";
import { Topic } from "@do-sync-engine/core";
import type { QueryEvent as WireEvent, Subscription, WebSocketRpcApi } from "./protocol";

export { Topic };
export type WebSocketRpcClient = {
  readonly subscribe: (
    topic: Topic,
  ) => Stream.Stream<{ readonly topic: Topic; readonly value: unknown }, unknown>;
  readonly unsubscribe: (topic: Topic) => Effect.Effect<void, unknown>;
  readonly sync: (request: {
    readonly mutation: string;
    readonly params: readonly unknown[];
  }) => Effect.Effect<void, unknown>;
};
export interface WebSocketRpcSession {
  readonly client: WebSocketRpcClient;
  readonly restored: Stream.Stream<void>;
  readonly close: () => void;
}

export const makeWebSocketRpcSession = (socket: WebSocket): Effect.Effect<WebSocketRpcSession> =>
  Effect.sync(() => {
    const remote = newWebSocketRpcSession<WebSocketRpcApi>(socket);
    const subscriptions = new Map<string, RpcStub<Subscription>>();
    const key = (topic: Topic) => JSON.stringify([topic.name, topic.params]);
    const client: WebSocketRpcClient = {
      subscribe: (topic) =>
        Stream.fromAsyncIterable(
          (async function* () {
            const subscription = await remote.subscribe({
              name: topic.name,
              params: topic.params,
            });
            subscriptions.set(key(topic), subscription);
            try {
              while (true) {
                const event = await subscription.next();
                if (event === null) return;
                const wireEvent = event as WireEvent;
                yield { topic: new Topic(wireEvent.topic), value: wireEvent.value };
              }
            } finally {
              subscriptions.delete(key(topic));
              try {
                await subscription.close();
              } finally {
                subscription[Symbol.dispose]();
              }
            }
          })(),
          (error) => error,
        ),
      unsubscribe: (topic) =>
        Effect.promise(async () => {
          const subscription = subscriptions.get(key(topic));
          if (!subscription) return;
          subscriptions.delete(key(topic));
          await subscription.close();
          subscription[Symbol.dispose]();
        }),
      sync: (request) => Effect.promise(() => remote.sync(request)),
    };
    return { client, restored: Stream.empty, close: () => remote[Symbol.dispose]() };
  });
