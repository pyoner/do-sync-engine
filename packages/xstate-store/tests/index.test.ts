import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import type { Mutation, Query, Topics } from "@do-sync-engine/core";
import { createSyncStore } from "../src/index.ts";

type Queries = { count: Query<[], number>; other: Query<[], number> };
type Mutations = { increment: Mutation<[], void> };

function installFakeWebSocket() {
  class FakeWebSocket extends EventTarget {
    static CLOSING = 2;

    readyState = 1;
    closed = false;

    constructor() {
      super();
      sockets.push(this);
    }

    close() {
      this.closed = true;
      this.readyState = 3;
    }
  }
  const sockets: FakeWebSocket[] = [];
  vi.stubGlobal("WebSocket", FakeWebSocket);
  return sockets;
}

const countTopic: Topics<Queries> = { name: "count", params: [] };
const status = (client: { store: { getSnapshot: () => { context: { status: string } } } }) =>
  client.store.getSnapshot().context.status;

afterEach(() => vi.unstubAllGlobals());

describe("createSyncStore", () => {
  it("walks idle -> connecting -> ready -> disconnected", () => {
    const sockets = installFakeWebSocket();
    using client = createSyncStore<Queries, Mutations>({ url: "ws://localhost" });
    const types: string[] = [];
    client.store.on("*", (event) => types.push(event.type));

    expect(status(client)).toBe("idle");
    expect(client.store.can.disconnect()).toBe(false);
    client.connect();
    expect(status(client)).toBe("connecting");
    expect(client.store.can.connect()).toBe(false);

    sockets[0]?.dispatchEvent(new Event("open"));
    expect(status(client)).toBe("ready");
    expect(client.store.getSnapshot().context.transport).toBe(sockets[0]);

    client.disconnect();
    expect(status(client)).toBe("disconnected");
    expect(sockets[0]?.closed).toBe(true);
    expect(client.store.getSnapshot().context.transport).toBeNull();
    expect(types).toEqual(["connecting", "opened", "disconnected"]);
  });

  it("only allows subscribe, unsubscribe and sync when ready", () => {
    const sockets = installFakeWebSocket();
    using client = createSyncStore<Queries, Mutations>({ url: "ws://localhost" });
    const can = () => [
      client.store.can.subscribe({ topic: countTopic, key: "count" }),
      client.store.can.unsubscribe({ topic: countTopic, key: "count" }),
      client.store.can.sync({ mutation: "increment", params: [] }),
    ];

    expect(can()).toEqual([false, false, false]);
    expect(client.subscribe(countTopic)).toBeUndefined();
    expect(client.sync("increment", [])).toBeUndefined();
    expect(client.store.getSnapshot().context).toMatchObject({ status: "idle", error: null });

    client.connect();
    expect(can()).toEqual([false, false, false]);
    sockets[0]?.dispatchEvent(new Event("open"));
    expect(can()).toEqual([true, true, true]);

    sockets[0]?.dispatchEvent(new Event("error"));
    expect(can()).toEqual([false, false, false]);
  });

  it("fails the connection, clears topics, and ignores stale socket events", () => {
    const sockets = installFakeWebSocket();
    using client = createSyncStore<Queries, Mutations>({ url: "ws://localhost" });
    const errors: Error[] = [];
    client.store.on("closed", ({ error }) => errors.push(error));

    client.connect();
    client.disconnect();
    client.connect();
    sockets[0]?.dispatchEvent(new Event("error"));
    expect(status(client)).toBe("connecting");
    expect(errors).toHaveLength(0);

    sockets[1]?.dispatchEvent(new Event("error"));
    expect(status(client)).toBe("disconnected");
    expect(errors[0]?.message).toBe("WebSocket connection failed");
    expect(client.store.getSnapshot().context.error).toBe(errors[0]);
    expect(sockets[1]?.closed).toBe(true);
    expect(client.store.getSnapshot().context.topics).toEqual({});
  });

  it("closes the failed socket before a closed listener reconnects", () => {
    const sockets = installFakeWebSocket();
    using client = createSyncStore<Queries, Mutations>({ url: "ws://localhost" });
    client.store.on("closed", () => client.connect());

    client.connect();
    sockets[0]?.dispatchEvent(new Event("error"));
    expect(sockets).toHaveLength(2);
    expect(sockets[0]?.closed).toBe(true);
    expect(sockets[1]?.closed).toBe(false);
    expect(status(client)).toBe("connecting");
  });

  it("disposal disconnects", () => {
    const sockets = installFakeWebSocket();
    const client = createSyncStore<Queries, Mutations>({ url: "ws://localhost" });
    client.connect();
    client[Symbol.dispose]();
    expect(sockets[0]?.closed).toBe(true);
    expect(status(client)).toBe("disconnected");
  });
});
