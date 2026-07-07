import { expect, test } from "vite-plus/test";
import { SyncEngine } from "../src/index.js";
import type { Broker, Selector, SubscriptionId } from "../src/index.js";

test("exports Broker contract through SyncEngine", () => {
  const selectors = {
    numbers: {
      tables: ["numbers"],
      run: () => 1,
    } satisfies Selector<[], number>,
  };
  const mutators = {
    noop: {
      tables: [],
      run: () => ({}),
    } satisfies Selector<[], Record<string, never>>,
  };
  const broker: Broker = new SyncEngine({ selectors, mutators });

  const subscriptionId: SubscriptionId = broker.subscribe("numbers");

  expect(subscriptionId).toBeTypeOf("number");

  expect(Object.getOwnPropertyNames(SyncEngine.prototype).sort()).toEqual([
    "constructor",
    "publish",
    "snapshot",
    "subscribe",
    "unsubscribe",
  ]);

  expect(broker.unsubscribe(subscriptionId)).toBe(true);
  expect(broker.unsubscribe(subscriptionId)).toBe(false);
});
