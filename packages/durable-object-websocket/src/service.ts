import type {
  ListenerEvent,
  MutationMap,
  OperationParams,
  OperationResult,
  QueryMap,
  StringKey,
  SyncEngineInterface,
  Topic,
} from "@do-sync-engine/core";
import * as errore from "errore";
import type { JsonRpcRequest } from "typed-rpc/server";

export type QueryTopic<Queries extends QueryMap<Queries>> = {
  [Name in StringKey<Queries>]: Topic<Name, OperationParams<Queries[Name]>>;
}[StringKey<Queries>];

export type SubscriptionEvent<Queries extends QueryMap<Queries>> = {
  [Name in StringKey<Queries>]: ListenerEvent<
    Name,
    OperationParams<Queries[Name]>,
    OperationResult<Queries[Name]>
  >;
}[StringKey<Queries>];

export interface Service<
  Queries extends QueryMap<Queries>,
  Mutations extends MutationMap<Mutations>,
> {
  subscribe(topic: QueryTopic<Queries>): void | Error;
  unsubscribe(topic: QueryTopic<Queries>): void | Error;
  sync<Name extends StringKey<Mutations>>(
    name: Name,
    params: OperationParams<Mutations[Name]>,
  ): void | Error;
}

export const SYNC_METHOD = "sync";

type StoredTopic<Q extends QueryMap<Q>> = Topic<StringKey<Q>, OperationParams<Q[StringKey<Q>]>>;

function rpcResult<T>(result: T | Error): T {
  if (result instanceof Error) throw result;
  return result;
}

function syncNotification(event: unknown): JsonRpcRequest {
  return { jsonrpc: "2.0", method: SYNC_METHOD, params: [event] } satisfies JsonRpcRequest;
}

function topicKey(topic: unknown): string | undefined {
  return JSON.stringify(topic);
}

export class SocketService<Q extends QueryMap<Q>, M extends MutationMap<M>> {
  constructor(
    private readonly socket: WebSocket,
    private readonly engine: SyncEngineInterface<WebSocket, Q, M>,
  ) {}

  static restore<Q extends QueryMap<Q>, M extends MutationMap<M>>(
    socket: WebSocket,
    engine: SyncEngineInterface<WebSocket, Q, M>,
  ): void {
    const service = new SocketService(socket, engine);
    for (const topic of service.topics()) {
      const valid = engine.createTopic(topic.name, topic.params);
      if (valid instanceof Error) {
        console.warn("Failed to restore WebSocket subscription", valid);
        continue;
      }
      const result = service.listen(valid, false);
      if (result instanceof Error) console.warn("Failed to restore WebSocket subscription", result);
    }
  }

  subscribe<Name extends StringKey<Q>>(topic: Topic<Name, OperationParams<Q[Name]>>): null {
    const valid = rpcResult(this.engine.createTopic(topic.name, topic.params));
    const previous = this.topics();
    const key = topicKey(valid);
    if (previous.some((item) => topicKey(item) === key)) return null;
    this.socket.serializeAttachment([...previous, valid]);
    const result = this.listen(valid, true);
    if (!(result instanceof Error)) return null;
    this.engine.unsubscribe(valid, this.socket);
    this.socket.serializeAttachment(previous);
    throw result;
  }

  unsubscribe<Name extends StringKey<Q>>(topic: Topic<Name, OperationParams<Q[Name]>>): null {
    const valid = rpcResult(this.engine.createTopic(topic.name, topic.params));
    const key = topicKey(valid);
    this.socket.serializeAttachment(this.topics().filter((item) => topicKey(item) !== key));
    this.engine.unsubscribe(valid, this.socket);
    return null;
  }

  sync<Name extends StringKey<M>>(mutation: Name, params: OperationParams<M[Name]>): null {
    rpcResult(this.engine.sync(mutation, params));
    return null;
  }

  private listen<Name extends StringKey<Q>>(
    topic: Topic<Name, OperationParams<Q[Name]>>,
    notifyInitial: boolean,
  ): WebSocket | Error {
    let active = notifyInitial;
    const result = this.engine.subscribe(
      topic,
      (event) => {
        if (active) this.socket.send(JSON.stringify(syncNotification(event)));
      },
      this.socket,
    );
    active = true;
    return result;
  }

  private topics(): Array<StoredTopic<Q>> {
    const attachment = errore.try({
      try: () => this.socket.deserializeAttachment() as unknown,
      catch: (cause) => new Error("Failed to deserialize WebSocket subscriptions", { cause }),
    });
    if (attachment instanceof Error) {
      console.warn(attachment.message, attachment);
      return [];
    }
    if (attachment === null) return [];
    if (Array.isArray(attachment)) return attachment as Array<StoredTopic<Q>>;
    console.warn("Invalid WebSocket subscription attachment", attachment);
    return [];
  }
}
