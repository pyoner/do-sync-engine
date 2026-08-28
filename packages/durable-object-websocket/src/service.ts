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

export type QueryTopic<Queries extends QueryMap> = {
  [Name in StringKey<Queries>]: Topic<Name, OperationParams<Queries[Name]>>;
}[StringKey<Queries>];

export type SubscriptionEvent<Queries extends QueryMap> = {
  [Name in StringKey<Queries>]: ListenerEvent<
    Name,
    OperationParams<Queries[Name]>,
    OperationResult<Queries[Name]>
  >;
}[StringKey<Queries>];

export interface Service<Queries extends QueryMap, Mutations extends MutationMap> {
  subscribe(topic: QueryTopic<Queries>): null;
  unsubscribe(topic: QueryTopic<Queries>): null;
  sync<Name extends StringKey<Mutations>>(
    name: Name,
    params: OperationParams<Mutations[Name]>,
  ): null;
}

export const SYNC_METHOD = "sync";

function rpcResult<T>(result: T | Error): T {
  if (result instanceof Error) throw result;
  return result;
}

function topicKey(topic: unknown): string | undefined {
  return JSON.stringify(topic);
}

export class SocketService<Q extends QueryMap, M extends MutationMap> implements Service<Q, M> {
  constructor(
    private readonly socket: WebSocket,
    private readonly engine: SyncEngineInterface<WebSocket, Q, M>,
  ) {}

  static restore<Q extends QueryMap, M extends MutationMap>(
    socket: WebSocket,
    engine: SyncEngineInterface<WebSocket, Q, M>,
  ): void {
    const service = new SocketService(socket, engine);
    for (const topic of service.topics()) {
      const valid = engine.createTopic(topic.name, topic.params);
      const result = valid instanceof Error ? valid : service.listen(valid, false);
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
    if (result instanceof Error) {
      this.engine.unsubscribe(valid, this.socket);
      this.socket.serializeAttachment(previous);
      throw result;
    }
    return null;
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
  ): void | Error {
    let active = notifyInitial;
    const result = this.engine.subscribe(
      topic,
      (event) => {
        if (active)
          this.socket.send(
            JSON.stringify({ jsonrpc: "2.0", method: SYNC_METHOD, params: [event] }),
          );
      },
      this.socket,
    );
    active = true;
    return result instanceof Error ? result : undefined;
  }

  private topics(): Array<QueryTopic<Q>> {
    const attachment = errore.try({
      try: () => this.socket.deserializeAttachment() as unknown,
      catch: (cause) => new Error("Failed to deserialize WebSocket subscriptions", { cause }),
    });
    if (attachment instanceof Error) {
      console.warn(attachment.message, attachment);
      return [];
    }
    if (attachment === null) return [];
    if (Array.isArray(attachment)) return attachment as Array<QueryTopic<Q>>;
    console.warn("Invalid WebSocket subscription attachment", attachment);
    return [];
  }
}
