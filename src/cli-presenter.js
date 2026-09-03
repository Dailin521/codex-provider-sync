// Human-readable CLI presentation is deliberately outside the Core public
// API. These helpers have no storage or mutation authority.

export function formatCounts(counts) {
  return Object.entries(counts ?? {})
    .map(([provider, count]) => `${provider}: ${count}`)
    .join(", ") || "(none)";
}

export function formatBytes(bytes) {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = bytes;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  return unitIndex === 0 ? `${bytes} B` : `${value.toFixed(value >= 10 ? 1 : 2).replace(/\.0$/, "")} ${units[unitIndex]}`;
}

export function renderStatus(status) {
  const lines = [
    `Codex home: ${status.codexHome}`,
    `SQLite home: ${status.sqliteHome} (source: ${status.sqliteHomeSource})`,
    `Current provider: ${status.currentProvider}${status.currentProviderImplicit ? " (implicit default)" : ""}`,
    `Configured providers: ${status.configuredProviders.join(", ")}`,
    `Backups: ${status.backupSummary.count} (${formatBytes(status.backupSummary.totalBytes)})`,
    `Backup root: ${status.backupRoot}`
  ];

  if (status.pendingTransactions?.length) {
    lines.push("");
    lines.push("Recovery required:");
    for (const transaction of status.pendingTransactions) {
      lines.push(`  ${transaction.state}: ${transaction.backupDir}`);
    }
    lines.push("  Run restore with the listed backup before the next write operation.");
  }

  lines.push("");
  lines.push("Rollout files:");
  lines.push(`  sessions: ${formatCounts(status.rolloutCounts.sessions)}`);
  lines.push(`  archived_sessions: ${formatCounts(status.rolloutCounts.archived_sessions)}`);
  if (status.encryptedContentCounts) {
    lines.push(`  encrypted_content sessions: ${formatCounts(status.encryptedContentCounts.sessions)}`);
    lines.push(`  encrypted_content archived_sessions: ${formatCounts(status.encryptedContentCounts.archived_sessions)}`);
  }
  if (status.encryptedContentWarning) {
    lines.push(`  ${status.encryptedContentWarning}`);
  }
  if (status.lockedRolloutFiles?.length) {
    lines.push(`  Locked rollout files skipped during status scan: ${status.lockedRolloutFiles.length}`);
  }

  lines.push("");
  lines.push("SQLite state:");
  if (!status.sqliteAccess?.supported) {
    lines.push(`  ${status.sqliteAccess.message}`);
    return lines.join("\n");
  }
  if (status.stateDbLocation) {
    const legacyNote = status.stateDbLocation.source === "legacy-root" ? " (legacy root)" : "";
    lines.push(`  database: ${status.stateDbLocation.path}${legacyNote}`);
  } else {
    lines.push(`  database: not found (checked ${status.checkedStateDbPaths.join(", ")})`);
  }
  if (status.sqliteCounts?.unreadable) {
    lines.push(`  ${status.sqliteCounts.error ?? "state_5.sqlite is malformed or unreadable"}`);
  } else if (!status.sqliteCounts) {
    lines.push("  state_5.sqlite not found");
  } else {
    lines.push(`  sessions: ${formatCounts(status.sqliteCounts.sessions)}`);
    lines.push(`  archived_sessions: ${formatCounts(status.sqliteCounts.archived_sessions)}`);
    if (status.sqliteRepairStats?.userEventRowsNeedingRepair) {
      lines.push(`  user-event flags needing repair: ${status.sqliteRepairStats.userEventRowsNeedingRepair}`);
    }
    if (status.sqliteRepairStats?.cwdRowsNeedingRepair) {
      lines.push(`  cwd paths needing repair: ${status.sqliteRepairStats.cwdRowsNeedingRepair}`);
    }
  }

  if (status.projectThreadVisibility?.length) {
    lines.push("");
    lines.push("Project visibility:");
    for (const project of status.projectThreadVisibility) {
      const providers = formatCounts(project.providerCounts);
      const rankText = project.rankPreview || "(none)";
      lines.push(
        `  ${project.root}: interactive ${project.interactiveThreads}, first page ${project.firstPageThreads}/50, ranks ${rankText}, exact cwd ${project.exactCwdMatches}/${project.interactiveThreads}, verbatim cwd ${project.verbatimCwdRows}, providers ${providers}`
      );
    }
  }

  return lines.join("\n");
}
