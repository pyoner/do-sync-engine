import { Schema } from "effect";

export class UnknownQueryError extends Schema.TaggedError<UnknownQueryError>()(
  "UnknownQueryError",
  { query: Schema.String },
) {
  override get message(): string {
    return `Unknown query: ${this.query}`;
  }
}

export class UnknownMutationError extends Schema.TaggedError<UnknownMutationError>()(
  "UnknownMutationError",
  { mutation: Schema.String },
) {
  override get message(): string {
    return `Unknown mutation: ${this.mutation}`;
  }
}

export class MissingSubscriptionIdError extends Schema.TaggedError<MissingSubscriptionIdError>()(
  "MissingSubscriptionIdError",
  {},
) {
  override get message(): string {
    return "Missing subscription id";
  }
}
