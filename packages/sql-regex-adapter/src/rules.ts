export function extractTables(sql: string, patterns: RegExp[]): string[] {
  const names: string[] = [];
  const ctes = new Set(
    [...sql.matchAll(/\bwith\s+(?:recursive\s+)?([A-Za-z_$][\w$]*)\s+as\s*\(/gi)].map((match) =>
      match[1]?.toLowerCase(),
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
