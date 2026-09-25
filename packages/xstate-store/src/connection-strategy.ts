export type ConnectionController = { connect: () => void; disconnect: () => void };

export abstract class ConnectionStrategy {
  #controller: ConnectionController | undefined;

  attach(controller: ConnectionController): void {
    this.#controller = controller;
    this.attached();
  }

  connect(): void {
    this.#controller?.connect();
  }

  disconnect(): void {
    this.#controller?.disconnect();
  }

  [Symbol.dispose](): void {
    this.disconnect();
    this.#controller = undefined;
  }

  subscribed(): void {}
  unsubscribed(): void {}
  failed(): void {}

  protected attached(): void {}

  protected connectSocket(): void {
    this.#controller?.connect();
  }

  protected disconnectSocket(): void {
    this.#controller?.disconnect();
  }
}

export class ManualConnectionStrategy extends ConnectionStrategy {}

export class AppLifetimeConnectionStrategy extends ConnectionStrategy {
  protected override attached(): void {
    this.connectSocket();
  }
}

export class SubscriptionOwnedConnectionStrategy extends ConnectionStrategy {
  protected count = 0;

  override subscribed(): void {
    if (++this.count === 1) this.connectSocket();
  }

  override unsubscribed(): void {
    if (this.count === 0) return;
    if (--this.count === 0) this.empty();
  }

  override disconnect(): void {
    this.count = 0;
    super.disconnect();
  }

  override failed(): void {
    this.count = 0;
  }

  override [Symbol.dispose](): void {
    this.count = 0;
    super[Symbol.dispose]();
  }

  protected empty(): void {
    this.disconnectSocket();
  }
}

export class IdleTimeoutConnectionStrategy extends SubscriptionOwnedConnectionStrategy {
  #timer: ReturnType<typeof setTimeout> | undefined;

  constructor(readonly delayMs: number) {
    super();
  }

  override subscribed(): void {
    clearTimeout(this.#timer);
    this.#timer = undefined;
    super.subscribed();
  }

  override disconnect(): void {
    clearTimeout(this.#timer);
    this.#timer = undefined;
    super.disconnect();
  }

  protected override empty(): void {
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      if (this.count === 0) this.disconnectSocket();
    }, this.delayMs);
  }

  override failed(): void {
    clearTimeout(this.#timer);
    this.#timer = undefined;
    super.failed();
  }

  override [Symbol.dispose](): void {
    clearTimeout(this.#timer);
    this.#timer = undefined;
    super[Symbol.dispose]();
  }
}
