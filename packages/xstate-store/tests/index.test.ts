import { describe, expect, it } from "vite-plus/test";
import type { Mutation, Query, Topics } from "@do-sync-engine/core";
import { createSyncStore } from "../src/index.ts";

type Queries = { count: Query<[], number> };
type Mutations = { increment: Mutation<[], void> };

describe("createSyncStore", () => {
  it("keeps the store disconnected until connected and reports unavailable operations", async () => {
    const client = createSyncStore<Queries, Mutations>({
      url: "ws://localhost",
      autoconnect: false,
    });
    const topic: Topics<Queries> = { name: "count", params: [] };

    expect(client.store.getSnapshot().context.status).toBe("disconnected");
    client.subscribe(topic, "count");
    expect(client.store.getSnapshot().context.error?.message).toBe(
      "WebSocket RPC session is not ready",
    );
    client.unsubscribe(topic);
    const result = await client.sync("increment", []);
    expect(result).toBeInstanceOf(Error);
    expect(result).toMatchObject({ message: "WebSocket RPC session is not ready" });
    expect(client.store.getSnapshot().context.status).toBe("disconnected");
  });
});
