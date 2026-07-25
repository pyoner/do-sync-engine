import { readFileSync } from "node:fs";
import { describe, expect, test } from "vite-plus/test";
import { adapter } from "../src/index.ts";

type Fixture = { sql: string; tables: string[] };
function fixtures(name: string): Fixture[] {
  const lines = readFileSync(new URL(`./fixtures/${name}.yaml`, import.meta.url), "utf8").split(
    "\n",
  );
  return lines.reduce<Fixture[]>((items, line) => {
    if (line.startsWith("- sql: ")) items.push({ sql: line.slice(7), tables: [] });
    else if (line.trimStart().startsWith("tables: "))
      items.at(-1)!.tables = line.slice(line.indexOf("[") + 1, -1).split(", ");
    return items;
  }, []);
}

for (const operation of ["select", "update", "insert", "delete"] as const) {
  describe(`${operation} SQL`, () => {
    for (const fixture of fixtures(operation)) {
      test(fixture.sql, () => {
        expect(adapter(fixture.sql).tables).toEqual(new Set(fixture.tables));
      });
    }
  });
}

test("rejects unsupported SQL", () => {
  expect(() => adapter("CREATE TABLE users (id integer)")).toThrow(TypeError);
});
