import { RpcClient, RpcGroup } from "effect/unstable/rpc";
import { Topic } from "@do-sync-engine/core";
import { RpcOperationError, WebSocketRpc } from "./protocol";
import { makeWebSocketRpcSessionFor } from "./client-transport";
import type { WebSocketRpcSession as GenericWebSocketRpcSession } from "./client-transport";

export { RpcOperationError, Topic };
export type WebSocketRpcClient = RpcClient.RpcClient<RpcGroup.Rpcs<typeof WebSocketRpc>>;
export type WebSocketRpcSession = GenericWebSocketRpcSession<RpcGroup.Rpcs<typeof WebSocketRpc>>;

export const makeWebSocketRpcSession = (socket: WebSocket) =>
  makeWebSocketRpcSessionFor(socket, WebSocketRpc);
