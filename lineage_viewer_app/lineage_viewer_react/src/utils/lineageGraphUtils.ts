/**
 * Graph node IDs must be case-normalized so edges (which use .toUpperCase())
 * attach to the same nodes as PASS 1. Labels use the original strings from lineage JSON.
 */
export function canonicalNonVolatileNodeId(tableName: string): string {
  return tableName.toUpperCase();
}

export function canonicalVolatileNodeId(scriptName: string, tableName: string): string {
  return `${scriptName}::${tableName.toUpperCase()}`;
}

/** Resolve a table key when rel.name / JSON keys may differ only by case. */
export function findTableDefinition(
  tables: Record<string, any> | undefined,
  refName: string | undefined
): { key: string; table: any } | null {
  if (!tables || refName === undefined || refName === null || refName === '') {
    return null;
  }
  if (tables[refName]) {
    return { key: refName, table: tables[refName] };
  }
  const upper = refName.toUpperCase();
  for (const key of Object.keys(tables)) {
    if (key.toUpperCase() === upper) {
      return { key, table: tables[key] };
    }
  }
  return null;
}

export function relationshipRefName(rel: { table?: string; name?: string }): string | undefined {
  const ref = rel.table ?? rel.name;
  return ref === undefined || ref === null || ref === '' ? undefined : String(ref);
}

/**
 * Map a table filter string to the spelling used as a key in lineage JSON
 * (first matching key across scripts). Used for UI labels when filters are stored uppercase.
 */
export function displayTableNameFromLineageScripts(
  scripts: Record<string, any> | undefined,
  filter: string
): string {
  if (!scripts || filter === undefined || filter === null || filter === '') {
    return filter === undefined || filter === null ? '' : String(filter);
  }
  const want = String(filter).toUpperCase();
  for (const scriptData of Object.values(scripts)) {
    const tables = (scriptData as { tables?: Record<string, unknown> }).tables || {};
    for (const key of Object.keys(tables)) {
      if (key.toUpperCase() === want) {
        return key;
      }
    }
  }
  return String(filter);
}
