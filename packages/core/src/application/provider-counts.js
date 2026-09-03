// @ts-nocheck

export function sqliteProviderRowsToChange(sqliteCounts, targetProvider) {
  let count = 0;
  for (const scope of ["sessions", "archived_sessions"]) {
    for (const [provider, providerCount] of Object.entries(sqliteCounts?.[scope] ?? {})) {
      if (provider !== targetProvider && Number.isSafeInteger(providerCount)) count += providerCount;
    }
  }
  return count;
}
