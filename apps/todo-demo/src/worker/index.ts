import { TODO_WS_PATH } from "../todo-protocol";
import type { Env } from "./todo-store";

export { TodoStore } from "./todo-store";

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname !== TODO_WS_PATH) {
      return new Response("Not Found", { status: 404 });
    }

    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const stub = env.TODO_STORE.getByName("default");
    return stub.fetch(request);
  },
} satisfies ExportedHandler<Env>;
