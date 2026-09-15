import path from "node:path";

export function rolloutSkip(filePath, reason, stage = "scan", id = null) {
  return { kind: "rollout", path: filePath, ...(id ? { id } : {}), reason, stage,
    retryable: ["locked", "missing", "changed", "deferred", "write-not-applied"].includes(reason) };
}

export function fileReadSkipReason(error) {
  if (error?.name === "RolloutMetadataEncodingError") return "metadata-invalid-utf8";
  if (error?.name === "RolloutMetadataLimitError") return "metadata-too-large";
  const code = error?.code === "ROLLOUT_FILE_BUSY" ? error?.cause?.code : error?.code;
  if (["EBUSY", "ETXTBSY"].includes(code) || error?.code === "ROLLOUT_FILE_BUSY") return "locked";
  if (["EACCES", "EPERM"].includes(code)) return "unreadable";
  if (code === "ENOENT") return "missing";
  if (error?.details?.fileChanged === true) return "changed";
  return null;
}

export function providerPathKey(value) {
  const normalized = path.resolve(value);
  return process.platform === "win32" ? normalized.toLowerCase() : normalized;
}

export function uniqueSkips(items) {
  const unique = new Map();
  for (const item of items ?? []) {
    const key = `${item.kind}:${item.kind === "rollout" ? providerPathKey(item.path) : String(item.id)}`;
    if (!unique.has(key)) unique.set(key, item);
  }
  return [...unique.values()];
}

export function summarizeSkips(items, unconfirmed = 0) {
  const all = uniqueSkips(items);
  const details = [];
  let detailBytes = 1024;
  for (const item of all) {
    const detail = { kind: item.kind, reason: item.reason, stage: item.stage, retryable: item.retryable,
      ...(item.kind === "rollout" && typeof item.path === "string" && item.path.length <= 32768 && !/[\u0000-\u001f]/.test(item.path) ? { path: item.path } : {}),
      ...(item.kind === "sqlite" && /^[A-Za-z0-9_.:-]{1,128}$/.test(item.id) ? { id: item.id } : {}) };
    const bytes = Buffer.byteLength(JSON.stringify(detail), "utf8") + 1;
    if (details.length >= 200 || detailBytes + bytes > 1024 * 1024) break;
    details.push(detail); detailBytes += bytes;
  }
  return { total: all.length, rolloutFiles: all.filter(item => item.kind === "rollout").length,
    sqliteRows: all.filter(item => item.kind === "sqlite").length,
    unconfirmed: unconfirmed + all.filter(item => ["association-unknown", "association-conflict"].includes(item.reason)).length,
    omitted: all.length - details.length, retryRecommended: all.some(item => item.retryable), items: details };
}

// Resolve only paths within a known rollout tree. No filename-based identity inference.
function rowRolloutPath(home, value) {
  if (typeof value !== "string" || !value || value.includes("\0")) return null;
  const absolute = path.resolve(home, value);
  const relative = path.relative(home, absolute).split(path.sep);
  if (!["sessions", "archived_sessions"].includes(relative[0]) || relative.includes("..")) return null;
  return providerPathKey(absolute);
}

export function selectProviderRows(home, scan, state, targetProvider, additionalSkips = []) {
  const skipped = uniqueSkips([...(scan.skippedItems ?? []), ...additionalSkips]);
  const skippedPaths = new Set(skipped.filter(item => item.kind === "rollout").map(item => providerPathKey(item.path)));
  const files = scan.files ?? [];
  const byId = new Map();
  const byPath = new Map(files.map(file => [providerPathKey(file.path), file]));
  for (const file of files) if (file.id) {
    const matches = byId.get(file.id) ?? [];
    matches.push(file); byId.set(file.id, matches);
  }
  for (const item of skipped) if (item.kind === "rollout" && !byPath.has(providerPathKey(item.path))) {
    const file = { path: item.path, id: item.id ?? null };
    byPath.set(providerPathKey(item.path), file);
    if (file.id) { const matches = byId.get(file.id) ?? []; matches.push(file); byId.set(file.id, matches); }
  }
  const rowMatches = new Map();
  const ownersByPath = new Map();
  const associatedBadPaths = new Set();
  const rows = state?.rows ?? [];
  for (const row of rows) {
    const idMatches = state.key === "rowid" ? [] : (byId.get(String(row.id)) ?? []);
    const pathKey = rowRolloutPath(home, row.rollout_path);
    const pathMatch = pathKey ? byPath.get(pathKey) : null;
    const matches = [...new Map([...idMatches, ...(pathMatch ? [pathMatch] : [])].map(file => [providerPathKey(file.path), file])).values()];
    const conflict = matches.length > 1 || (pathMatch?.id && String(row.id) !== pathMatch.id && state.key !== "rowid");
    rowMatches.set(row, { matches, conflict });
    for (const match of matches) {
      const key = providerPathKey(match.path);
      const owners = ownersByPath.get(key) ?? [];
      owners.push(row); ownersByPath.set(key, owners);
    }
  }
  for (const row of rows) {
    const relation = rowMatches.get(row);
    if (relation.matches.some(file => ownersByPath.get(providerPathKey(file.path)).length > 1)) relation.conflict = true;
    if (!relation.conflict) for (const file of relation.matches) {
      const key = providerPathKey(file.path);
      if (skippedPaths.has(key)) associatedBadPaths.add(key);
    }
  }
  // A trustworthy ID absent from the index still proves which row would own
  // the file. Do not manufacture that certainty for corrupt/no-ID headers.
  for (const key of skippedPaths) {
    const file = byPath.get(key);
    if (file?.id && byId.get(file.id)?.length === 1 && !ownersByPath.has(key)) associatedBadPaths.add(key);
  }
  const unknown = [...skippedPaths].some(key => !associatedBadPaths.has(key));
  const selected = [];
  const rowSkips = [];
  for (const row of rows) {
    if (row.model_provider === targetProvider) continue;
    const { matches, conflict } = rowMatches.get(row);
    let reason = conflict ? "association-conflict" : null;
    const skippedMatch = matches.find(file => skippedPaths.has(providerPathKey(file.path)));
    const fileSkip = skippedMatch ? skipped.find(item => item.kind === "rollout" && providerPathKey(item.path) === providerPathKey(skippedMatch.path)) : null;
    if (!reason && fileSkip) reason = fileSkip.reason;
    if (!reason && unknown && matches.length === 0) reason = "association-unknown";
    if (reason) rowSkips.push({ kind: "sqlite", id: String(row.id), reason, stage: "plan", retryable: !conflict && fileSkip?.retryable === true });
    else selected.push({ ...row, paths: matches.map(file => file.path) });
  }
  return { rows: selected, skippedItems: rowSkips };
}
