import { expect, test } from "vite-plus/test";
import { SyncEngine } from "../src/index.js";
import type { Broker, Selector, Unsubscribe } from "../src/index.js";

test("exports Broker contract through SyncEngine", () => {
  const broker: Broker = new SyncEngine();
  const selector: Selector<[], number> = {
    tables: ["numbers"],
    run: () => 1,
    callback: () => {},
  };

  const unsubscribe: Unsubscribe = broker.subscribe(selector);

  expect(unsubscribe).toBeTypeOf("function");

  expect(Object.getOwnPropertyNames(SyncEngine.prototype).sort()).toEqual([
    "constructor",
    "publish",
    "subscribe",
  ]);

  unsubscribe();
});
