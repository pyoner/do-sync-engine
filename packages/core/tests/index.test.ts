import { expect, test } from "vite-plus/test";
import { SyncEngine } from "../src/index.js";
import type { Broker, Selector, SubscribeCallback, Unsubscribe } from "../src/index.js";

test("exports Broker contract through SyncEngine", () => {
  const broker: Broker = new SyncEngine();
  const selector: Selector<[], number> = {
    tables: ["numbers"],
    run: () => 1,
  };
  const subscribeCallback: SubscribeCallback<[], number> = (result, subscribedSelector, params) => {
    void result;
    void subscribedSelector;
    void params;
  };

  const unsubscribe: Unsubscribe = broker.subscribe(selector, [], subscribeCallback);

  expect(unsubscribe).toBeTypeOf("function");

  expect(Object.getOwnPropertyNames(SyncEngine.prototype).sort()).toEqual([
    "constructor",
    "publish",
    "subscribe",
  ]);

  unsubscribe();
});
