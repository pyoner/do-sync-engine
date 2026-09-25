import { describe, expect, it, vi } from "vite-plus/test";
import type { Mutation, Query, Topics } from "@do-sync-engine/core";
import {
  AppLifetimeConnectionStrategy,
  createSyncStore,
  IdleTimeoutConnectionStrategy,
  ManualConnectionStrategy,
} from "../src/index.ts";

type Queries = { count: Query<[], number>; other: Query<[], number> };
type Mutations = { increment: Mutation<[], void> };

function installFakeWebSocket() {
  class FakeWebSocket extends EventTarget {
    static CLOSING = 2;

    readyState = 1;
    closed = false;

    close() {
      this.closed = true;
      this.readyState = 3;
    }
  }
  const sockets: FakeWebSocket[] = [];
  class TrackedWebSocket extends FakeWebSocket {
    constructor() {
      super();
      sockets.push(this);
    }
  }
  vi.stubGlobal("WebSocket", TrackedWebSocket);
  return sockets;
}

const countTopic: Topics<Queries> = { name: "count", params: [] };
const otherTopic: Topics<Queries> = { name: "other", params: [] };

describe("createSyncStore connection strategies", () => {
  it("connects for subscriptions and disconnects after the last one", () => {
    const sockets = installFakeWebSocket();
    try {
      const client = createSyncStore<Queries, Mutations>({ url: "ws://localhost" });

      expect(sockets).toHaveLength(0);
      client.subscribe(countTopic, "count");
      expect(client.store.getSnapshot().context.status).toBe("connecting");
      expect(sockets).toHaveLength(1);

      client.subscribe(otherTopic, "other");
      client.subscribe(otherTopic, "other");
      client.unsubscribe(countTopic);
      expect(client.store.getSnapshot().context.status).toBe("connecting");
      expect(sockets[0]?.closed).toBe(false);
      client.unsubscribe(otherTopic);
      expect(sockets[0]?.closed).toBe(false);
      client.unsubscribe(otherTopic);
      expect(client.store.getSnapshot().context.status).toBe("disconnected");
      expect(sockets[0]?.closed).toBe(true);

      client.subscribe(countTopic, "count");
      sockets[1]?.dispatchEvent(new Event("error"));
      expect(client.store.getSnapshot().context.status).toBe("disconnected");
      expect(sockets[1]?.closed).toBe(true);
      client.unsubscribe(countTopic);
      client.subscribe(countTopic, "count");
      expect(sockets).toHaveLength(3);
      client.unsubscribe(countTopic);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("connects and disconnects only when explicitly requested", () => {
    const sockets = installFakeWebSocket();
    try {
      const client = createSyncStore<Queries, Mutations>({
        url: "ws://localhost",
        strategy: new ManualConnectionStrategy(),
      });

      expect(sockets).toHaveLength(0);
      expect(client.subscribe(countTopic, "count")).toMatchObject({
        message: "WebSocket RPC session is not ready",
      });
      expect(sockets).toHaveLength(0);
      client.connect();
      expect(sockets).toHaveLength(1);
      client.subscribe(countTopic, "count");
      client.unsubscribe(countTopic);
      expect(sockets[0]?.closed).toBe(false);
      client.disconnect();
      expect(client.store.getSnapshot().context.status).toBe("disconnected");
      expect(sockets[0]?.closed).toBe(true);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("connects for the store lifetime and closes on disposal", () => {
    const sockets = installFakeWebSocket();
    try {
      const client = createSyncStore<Queries, Mutations>({
        url: "ws://localhost",
        strategy: new AppLifetimeConnectionStrategy(),
      });
      expect(sockets).toHaveLength(1);
      client.subscribe(countTopic, "count");
      client.unsubscribe(countTopic);
      expect(sockets[0]?.closed).toBe(false);
      client[Symbol.dispose]();
      expect(client.store.getSnapshot().context.status).toBe("disconnected");
      expect(sockets[0]?.closed).toBe(true);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("holds the connection through the idle timeout and cancels it on resubscribe", () => {
    vi.useFakeTimers();
    const sockets = installFakeWebSocket();
    try {
      const client = createSyncStore<Queries, Mutations>({
        url: "ws://localhost",
        strategy: new IdleTimeoutConnectionStrategy(100),
      });
      client.subscribe(countTopic, "count");
      client.unsubscribe(countTopic);
      vi.advanceTimersByTime(50);
      client.subscribe(countTopic, "count");
      vi.advanceTimersByTime(100);
      expect(sockets).toHaveLength(1);
      expect(sockets[0]?.closed).toBe(false);

      client.unsubscribe(countTopic);
      vi.advanceTimersByTime(100);
      expect(client.store.getSnapshot().context.status).toBe("disconnected");
      expect(sockets[0]?.closed).toBe(true);
    } finally {
      vi.useRealTimers();
      vi.unstubAllGlobals();
    }
  });

  it("clears the idle timer and detaches the controller on disposal", () => {
    vi.useFakeTimers();
    const sockets = installFakeWebSocket();
    const strategy = new IdleTimeoutConnectionStrategy(100);
    try {
      const client = createSyncStore<Queries, Mutations>({
        url: "ws://localhost",
        strategy,
      });
      client.subscribe(countTopic, "count");
      client.unsubscribe(countTopic);
      client[Symbol.dispose]();
      expect(vi.getTimerCount()).toBe(0);
      client[Symbol.dispose]();
      vi.advanceTimersByTime(100);
      strategy.connect();
      expect(client.store.getSnapshot().context.status).toBe("disconnected");
      expect(sockets).toHaveLength(1);
      expect(sockets[0]?.closed).toBe(true);
    } finally {
      vi.useRealTimers();
      vi.unstubAllGlobals();
    }
  });
});
