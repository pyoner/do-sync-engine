export const identifier = '[A-Za-z_$][\\w$]*|"[^"]+"|`[^`]+`|\\[[^\\]]+\\]';

export function extractTables(sql: string, patterns: RegExp[]): string[] {
  const names: string[] = [];
  const ctes = new Set(
    [...sql.matchAll(/(?:\bwith|,)\s*(?:recursive\s+)?([A-Za-z_$][\w$]*)\s+as\s*\(/gi)].map(
      (match) => match[1]?.toLowerCase(),
    ),
  );
  for (const pattern of patterns) {
    pattern.lastIndex = 0;
    let match = pattern.exec(sql);
    while (match) {
      const name = match[1]
        ?.split(".")
        .map((part) => part.trim().replace(/^(?:["`[])(.*?)["`\]]$/, "$1"))
        .join(".");
      if (name && !ctes.has(name.toLowerCase()) && !names.includes(name)) names.push(name);
      if (!pattern.global) break;
      match = pattern.exec(sql);
    }
  }
  return names;
}

export function operationOf(sql: string): "select" | "update" | "insert" | "delete" | undefined {
  let depth = 0;
  let quote = "";
  for (let index = 0; index < sql.length; index += 1) {
    const char = sql[index];
    if (quote) {
      if (char === quote && sql[index - 1] !== "\\") quote = "";
      continue;
    }
    if (char === "'" || char === '"' || char === "`") {
      quote = char;
      continue;
    }
    if (char === "(") {
      depth += 1;
      continue;
    }
    if (char === ")") {
      depth -= 1;
      continue;
    }
    if (depth === 0 && /[A-Za-z]/.test(char)) {
      const match = sql.slice(index).match(/^(select|update|insert|delete)\b/i);
      if (match) return match[1].toLowerCase() as "select" | "update" | "insert" | "delete";
    }
  }
}
