import { RpcClient, RpcGroup } from "effect/unstable/rpc";
import { Topic } from "@do-sync-engine/core";
import { RpcOperationError, WebSocketRpc } from "./protocol.ts";
import { makeWebSocketRpcClientFor } from "./client-transport.ts";

export { RpcOperationError, Topic };
export type WebSocketRpcClient = RpcClient.RpcClient<RpcGroup.Rpcs<typeof WebSocketRpc>>;

export const makeWebSocketRpcClient = (socket: WebSocket) =>
  makeWebSocketRpcClientFor(socket, WebSocketRpc);
