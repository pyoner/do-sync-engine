import { RpcTarget } from "capnweb";

export type Topic = {
  readonly name: string;
  readonly params: readonly unknown[];
};

export type QueryEvent = {
  readonly topic: Topic;
  readonly value: unknown;
};

export abstract class Subscription extends RpcTarget {
  abstract next(): Promise<QueryEvent | null>;
  abstract close(): Promise<void>;
}

export interface WebSocketRpcApi {
  subscribe(topic: Topic): Promise<Subscription>;
  unsubscribe(subscription: Subscription): Promise<void>;
  sync(request: { readonly mutation: string; readonly params: readonly unknown[] }): Promise<void>;
}
