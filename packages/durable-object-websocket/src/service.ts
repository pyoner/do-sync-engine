import type { JsonRpcRequest } from "typed-rpc";

import type {
  MutationMap,
  OperationParams,
  QueryMap,
  StringKey,
  SyncEngineInterface,
  Topic,
} from "@do-sync-engine/core";
import { WebsocketStorage } from "./websocket-storage";

export type QueryTopic<Queries extends QueryMap> = {
  [Name in StringKey<Queries>]: Topic<Name, OperationParams<Queries[Name]>>;
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

function rpcNotification(method: string, params: unknown[]) {
  return { jsonrpc: "2.0", method, params } satisfies JsonRpcRequest;
}

function rpcResult<T>(result: T | Error): T {
  if (result instanceof Error) throw result;
  return result;
}

export class SocketService<Q extends QueryMap, M extends MutationMap> implements Service<Q, M> {
  constructor(
    private readonly socket: WebSocket,
    private readonly engine: SyncEngineInterface<WebSocket, Q, M>,
    private readonly storage: WebsocketStorage,
    private readonly topicKey: string,
  ) {}

  static restore<Q extends QueryMap, M extends MutationMap>(
    socket: WebSocket,
    engine: SyncEngineInterface<WebSocket, Q, M>,
    storage: WebsocketStorage,
    topicKey: string,
  ): void {
    const service = new SocketService(socket, engine, storage, topicKey);
    const stored = storage.get<unknown>(topicKey);
    if (stored === null) return;
    if (!Array.isArray(stored)) {
      console.warn("Invalid WebSocket subscription attachment", stored);
      return;
    }
    for (const topic of stored as Array<QueryTopic<Q>>) service.subscribe(topic);
  }

  subscribe<Name extends StringKey<Q>>(topic: Topic<Name, OperationParams<Q[Name]>>): null {
    const validTopic = rpcResult(this.engine.createTopic(topic.name, topic.params));
    rpcResult(
      this.engine.subscribe(
        validTopic,
        (event) => {
          this.socket.send(JSON.stringify(rpcNotification(SYNC_METHOD, [event])));
        },
        this.socket,
      ),
    );
    this.storage.set(this.topicKey, this.topics());
    return null;
  }

  unsubscribe<Name extends StringKey<Q>>(topic: Topic<Name, OperationParams<Q[Name]>>): null {
    const valid = rpcResult(this.engine.createTopic(topic.name, topic.params));
    this.engine.unsubscribe(valid, this.socket);
    this.storage.set(this.topicKey, this.topics());
    return null;
  }

  sync<Name extends StringKey<M>>(mutation: Name, params: OperationParams<M[Name]>): null {
    rpcResult(this.engine.sync(mutation, params));
    return null;
  }

  private topics(): Array<QueryTopic<Q>> {
    const topics: Array<QueryTopic<Q>> = [];
    for (const { id, topic } of this.engine.subscriptions()) {
      if (id === this.socket) topics.push(topic as QueryTopic<Q>);
    }
    return topics;
  }
}
