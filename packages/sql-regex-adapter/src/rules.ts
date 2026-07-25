function withoutSqlNoise(sql: string): string {
  return sql.replace(/--[^\r\n]*|\/\*[\s\S]*?\*\/|'(?:''|[^'])*'/g, (match) =>
    match.replace(/[^\r\n]/g, " "),
  );
}

export const identifier = '[A-Za-z_$][\\w$]*|"[^"]+"|`[^`]+`|\\[[^\\]]+\\]';

export function extractTables(sql: string, patterns: RegExp[]): string[] {
  const names: string[] = [];
  const sanitized = withoutSqlNoise(sql);
  const ctes = new Set(
    [...sanitized.matchAll(/(?:\bwith|,)\s*(?:recursive\s+)?([A-Za-z_$][\w$]*)\s+as\s*\(/gi)].map(
      (match) => match[1]?.toLowerCase(),
    ),
  );
  for (const pattern of patterns) {
    pattern.lastIndex = 0;
    let match = pattern.exec(sanitized);
    while (match) {
      const name = match[1]
        ?.split(".")
        .map((part) => part.trim().replace(/^(?:["`[])(.*?)["`\]]$/, "$1"))
        .join(".");
      if (name && !ctes.has(name.toLowerCase()) && !names.includes(name)) names.push(name);
      if (!pattern.global) break;
      match = pattern.exec(sanitized);
    }
  }
  return names;
}

export function operationOf(sql: string): "select" | "update" | "insert" | "delete" | undefined {
  const sanitized = withoutSqlNoise(sql);
  let depth = 0;
  let quote = "";
  for (let index = 0; index < sanitized.length; index += 1) {
    const char = sanitized[index];
    if (quote) {
      if (char === quote && sanitized[index - 1] !== "\\") quote = "";
      continue;
    }
    if (char === '"' || char === "`") {
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
      const match = sanitized.slice(index).match(/^(select|update|insert|delete)\b/i);
      if (match) return match[1].toLowerCase() as "select" | "update" | "insert" | "delete";
    }
  }
}
