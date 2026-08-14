export function captureProfileOperation(profile, operation, status) {
  if (!profile?.id
      || !profile?.revision
      || status?.profileId !== profile.id
      || status?.profileRevision !== profile.revision
      || !status?.storageRevision) return null;
  return {
    ...operation,
    profile: { ...profile },
    profileId: profile.id,
    profileRevision: profile.revision,
    storageRevision: status.storageRevision,
    status: { ...status }
  };
}

export function skippedLockedRolloutFiles(payload) {
  const files = payload?.result?.skippedLockedRolloutFiles ?? payload?.skippedLockedRolloutFiles ?? [];
  return Array.isArray(files) ? files.filter(Boolean) : [];
}

export function operationToast(payload, { successTitle, partialTitle, message }) {
  const skipped = skippedLockedRolloutFiles(payload);
  const partial = payload?.result?.outcome === "partial" || payload?.outcome === "partial" || skipped.length > 0;
  if (!partial) return { tone: "success", title: successTitle, message };
  const skippedDetail = skipped.length
    ? `已跳过 ${skipped.length} 个被占用的 rollout 文件：${skipped.join("、")}`
    : "部分项目未完成；请查看活动日志后重试。";
  return {
    tone: "warning",
    title: partialTitle,
    message: [message, skippedDetail].filter(Boolean).join("；")
  };
}

function normalizeHistoryPath(path, platform) {
  const normalized = String(path ?? "").replaceAll("\\", "/").replace(/\/{2,}/g, "/").replace(/\/$/, "");
  return platform === "win32" ? normalized.toLowerCase() : normalized;
}

export function dedupeHistorySessions(sessions = [], { platform = typeof process !== "undefined" ? process.platform : "browser" } = {}) {
  const seen = new Set();
  return sessions.filter((session, index) => {
    const threadId = session?.threadId ?? session?.id;
    const rolloutPath = session?.rolloutPath ?? session?.filePath ?? session?.rolloutFile ?? session?.path;
    const key = threadId ? `thread:${threadId}` : rolloutPath ? `rollout:${normalizeHistoryPath(rolloutPath, platform)}` : `item:${index}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

export function restoreRelocationState({ backup, profile, targetSqliteHome, restoreDatabase, restoreConfig, sqliteSupported }) {
  const sourceSqliteHome = backup?.metadata?.sqliteHome;
  const explicitSqliteHome = profile?.sqliteHome?.trim() ?? "";
  const normalize = (value) => String(value ?? "").replace(/[\\/]+$/, "").replaceAll("\\", "/").toLowerCase();
  const requiresRelocation = Boolean(
    restoreDatabase
    && sourceSqliteHome
    && targetSqliteHome
    && normalize(sourceSqliteHome) !== normalize(targetSqliteHome)
  );
  const missingExplicitTarget = requiresRelocation && !explicitSqliteHome;
  const configRestoreConflict = requiresRelocation && restoreConfig;
  return {
    requiresRelocation,
    missingExplicitTarget,
    configRestoreConflict,
    canSubmit: Boolean(sqliteSupported) && !missingExplicitTarget && !configRestoreConflict
  };
}
