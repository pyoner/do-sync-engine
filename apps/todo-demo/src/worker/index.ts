import type { Env } from "./todo-store.js";

export { TodoStore } from "./todo-store.js";

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    try {
      const method = request.method;
      const url = new URL(request.url);
      const match = /^\/api\/todos(?:\/(\d+))?$/.exec(url.pathname);

      if (!match) return json({ error: "Not Found" }, 404);

      const stub = env.TODO_STORE.getByName("default");
      const id = match[1] !== undefined ? Number(match[1]) : null;

      if (url.pathname === "/api/todos" && method === "GET") {
        return json({ todos: await stub.getAllTodos() });
      }
      if (url.pathname === "/api/todos" && method === "POST") {
        const { title } = (await request.json()) as { title: string };
        if (!title?.trim()) return json({ error: "title required" }, 400);
        return json(await stub.addTodo(title.trim()));
      }
      if (id !== null && method === "PATCH") {
        return json(await stub.toggleTodo(id));
      }
      if (id !== null && method === "DELETE") {
        return json(await stub.deleteTodo(id));
      }
      if (url.pathname === "/api/todos" && method === "DELETE") {
        return json(await stub.clearCompleted());
      }
      return json({ error: "Method Not Allowed" }, 405);
    } catch (err) {
      return json({ error: String(err) }, 500);
    }
  },
} satisfies ExportedHandler<Env>;

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}
