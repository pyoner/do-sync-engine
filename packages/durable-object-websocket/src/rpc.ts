import * as errore from "errore";
import type { JsonRpcRequest, JsonRpcResponse } from "typed-rpc";
import { handleRpc, isJsonRpcRequest, type RpcService } from "typed-rpc/server";

export type RpcWireMessage = string | ArrayBuffer;
export type RestoredAttachment = Error | { readonly value: unknown };

export interface RpcSessionContext {
  readonly attachment: RestoredAttachment;
  notify(request: JsonRpcRequest): void | Error;
  persist(attachment: unknown): void | Error;
  fail(error: Error): void;
}

export interface RpcSession<Service extends RpcService<Service, unknown>> {
  readonly service: Service;
  start(): void | Error;
  close(): void;
}

export type RpcSessionFactory<Service extends RpcService<Service, unknown>> = (
  context: RpcSessionContext,
) => RpcSession<Service>;

export interface RpcBinding<Service extends RpcService<Service, unknown>> {
  readonly createSession: RpcSessionFactory<Service>;
}

const PARSE_ERROR_RESPONSE: JsonRpcResponse = {
  jsonrpc: "2.0",
  id: null,
  error: { code: -32700, message: "Parse error" },
};

function encode(value: unknown): string | Error {
  const message = errore.try({
    try: () => JSON.stringify(value),
    catch: (cause) => new Error("Failed to serialize RPC message", { cause }),
  });
  if (message instanceof Error) return message;
  if (message === undefined) return new Error("Failed to serialize RPC message");
  return message;
}

function decode(raw: RpcWireMessage): Error | { readonly value: unknown } {
  const text = typeof raw === "string" ? raw : new TextDecoder().decode(raw);
  const value = errore.try({
    try: () => JSON.parse(text) as unknown,
    catch: (cause) => new Error("Failed to parse RPC message", { cause }),
  });
  if (value instanceof Error) return value;
  return { value };
}

export function encodeRpcMessage(message: JsonRpcRequest): string | Error {
  return encode(message);
}

export async function handleRpcFrame<Service extends RpcService<Service, unknown>>({
  raw,
  service,
}: {
  raw: RpcWireMessage;
  service: Service;
}): Promise<string | Error | null> {
  const decoded = decode(raw);
  if (decoded instanceof Error) return encode(PARSE_ERROR_RESPONSE);

  const request = decoded.value;
  const notification = isJsonRpcRequest(request) && !("id" in request);
  const response = await handleRpc<Service, unknown>(request, service, {
    onError: (error) => console.error("WebSocket RPC method failed", error),
  });
  if (notification) return null;
  return encode(response);
}
