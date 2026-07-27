import { parse } from "yaml";
import deleteYaml from "./fixtures/delete.yaml?raw";
import insertYaml from "./fixtures/insert.yaml?raw";
import selectYaml from "./fixtures/select.yaml?raw";
import updateYaml from "./fixtures/update.yaml?raw";

export const operations = ["select", "update", "insert", "delete"] as const;
export type Operation = (typeof operations)[number];
export type SqlParameter = string | number | null;
export type Fixture = {
  setup: { database: string[]; seed: string[] };
  testData: { sql: string; params?: SqlParameter[]; tables: string[] }[];
};

const sources: Record<Operation, string> = {
  select: selectYaml,
  update: updateYaml,
  insert: insertYaml,
  delete: deleteYaml,
};

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string");
}
function isSqlParameterArray(value: unknown): value is SqlParameter[] {
  return (
    Array.isArray(value) &&
    value.every((item) => item === null || typeof item === "string" || typeof item === "number")
  );
}

function isFixture(value: unknown): value is Fixture {
  if (
    typeof value !== "object" ||
    value === null ||
    !("setup" in value) ||
    !("testData" in value) ||
    typeof value.setup !== "object" ||
    value.setup === null ||
    !("database" in value.setup) ||
    !("seed" in value.setup)
  )
    return false;
  return (
    isStringArray(value.setup.database) &&
    isStringArray(value.setup.seed) &&
    Array.isArray(value.testData) &&
    value.testData.every((item) => {
      if (typeof item !== "object" || item === null || !("sql" in item) || !("tables" in item))
        return false;
      return (
        typeof item.sql === "string" &&
        isStringArray(item.tables) &&
        (!("params" in item) || isSqlParameterArray(item.params))
      );
    })
  );
}

export function fixtures(name: Operation): Fixture {
  const value: unknown = parse(sources[name]);
  if (!isFixture(value)) throw new TypeError(`Invalid ${name} fixture`);
  return value;
}
