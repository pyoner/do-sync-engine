import * as errore from "errore";

export class WebsocketStorage {
  constructor(private readonly socket: WebSocket) {}

  get<Value>(key: string): Value | null {
    const attachment = this.attachment();
    return Object.hasOwn(attachment, key) ? (attachment[key] as Value) : null;
  }

  set(key: string, value: unknown): void {
    this.socket.serializeAttachment({ ...this.attachment(), [key]: value });
  }

  remove(key: string): void {
    const attachment = { ...this.attachment() };
    delete attachment[key];
    this.socket.serializeAttachment(attachment);
  }

  private attachment(): Record<string, unknown> {
    const attachment = errore.try({
      try: () => this.socket.deserializeAttachment() as unknown,
      catch: (cause) => new Error("Failed to deserialize WebSocket attachment", { cause }),
    });
    if (attachment instanceof Error) {
      console.warn(attachment.message, attachment);
      return {};
    }
    if (attachment === null) return {};
    if (typeof attachment === "object" && !Array.isArray(attachment)) {
      return attachment as Record<string, unknown>;
    }
    console.warn("Invalid WebSocket attachment", attachment);
    return {};
  }
}
