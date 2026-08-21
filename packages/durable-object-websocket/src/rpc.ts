import * as errore from "errore";
import type { JsonRpcRequest, JsonRpcResponse, RpcTranscoder } from "typed-rpc";
import { handleRpc, isJsonRpcRequest, type RpcService } from "typed-rpc/server";

export type RpcWireMessage = string | ArrayBuffer;

export interface WireRpcTranscoder extends RpcTranscoder<unknown> {
  serialize(data: unknown): RpcWireMessage | null;
  deserialize(data: unknown): unknown;
}

export type RpcTranscoderFactory = () => WireRpcTranscoder;
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
  readonly createTranscoder: RpcTranscoderFactory;
}

class RpcParseError extends errore.createTaggedError({
  name: "RpcParseError",
  message: "Failed to parse RPC message",
}) {}

class RpcTranscodeError extends errore.createTaggedError({
  name: "RpcTranscodeError",
  message: "Failed to transcode RPC message",
}) {}

const PARSE_ERROR_RESPONSE: JsonRpcResponse = {
  jsonrpc: "2.0",
  id: null,
  error: { code: -32700, message: "Parse error" },
};

function encode({
  transcoder,
  value,
}: {
  transcoder: WireRpcTranscoder;
  value: unknown;
}): RpcWireMessage | Error | null {
  return errore.try({
    try: () => transcoder.serialize(value),
    catch: (cause) => new RpcTranscodeError({ cause }),
  });
}

export function createJsonRpcTranscoder(): WireRpcTranscoder {
  let notification = false;

  return {
    deserialize: (raw) => {
      const text =
        typeof raw === "string"
          ? raw
          : raw instanceof ArrayBuffer
            ? new TextDecoder().decode(raw)
            : new RpcParseError({ cause: new TypeError("Unsupported RPC message type") });
      if (text instanceof Error) throw text;

      const request = errore.try({
        try: () => JSON.parse(text) as unknown,
        catch: (cause) => new RpcParseError({ cause }),
      });
      if (request instanceof Error) throw request;

      notification = isJsonRpcRequest(request) && !("id" in request);
      return request;
    },
    serialize: (response) => {
      if (notification) return null;

      const message = errore.try({
        try: () => JSON.stringify(response),
        catch: (cause) => new RpcTranscodeError({ cause }),
      });
      if (message instanceof Error) throw message;
      if (message === undefined) throw new RpcTranscodeError();
      return message;
    },
  };
}

export function encodeRpcMessage({
  message,
  createTranscoder,
}: {
  message: JsonRpcRequest;
  createTranscoder: RpcTranscoderFactory;
}): RpcWireMessage | Error {
  const encoded = encode({ transcoder: createTranscoder(), value: message });
  if (encoded === null) return new RpcTranscodeError();
  return encoded;
}

export async function handleRpcFrame<Service extends RpcService<Service, unknown>>({
  raw,
  service,
  createTranscoder,
}: {
  raw: RpcWireMessage;
  service: Service;
  createTranscoder: RpcTranscoderFactory;
}): Promise<RpcWireMessage | Error | null> {
  const transcoder = createTranscoder();
  // typed-rpc declares JsonRpcResponse, but returns transcoder.serialize() at runtime.
  return await (
    handleRpc<Service, unknown>(raw, service, {
      transcoder,
      onError: (error) => console.error("WebSocket RPC method failed", error),
    }) as unknown as Promise<RpcWireMessage | null>
  ).catch((cause) => {
    if (RpcParseError.is(cause)) return encode({ transcoder, value: PARSE_ERROR_RESPONSE });
    return new RpcTranscodeError({ cause });
  });
}
