import i18next, { type i18n } from "i18next";
import type { SupportedLocale } from "@codex-provider-sync/design-system";

export const resources = {
  en: {
    translation: {
      brand: {
        desktop: {
          label: "Desktop",
          subtitle: "V1 primary desktop candidate · .NET post-handoff Legacy target"
        },
        web: {
          label: "Web",
          subtitle: "Local Web companion"
        }
      },
      a11y: {
        skipToContent: "Skip to content",
        profile: "Profile",
        primaryNavigation: "Primary navigation"
      },
      nav: {
        overview: "Overview",
        sync: "Sync",
        switchProvider: "Switch Provider",
        backupsRestore: "Backups / Restore",
        history: "History",
        profiles: "Profiles",
        diagnostics: "Diagnostics",
        settings: "Settings"
      },
      common: {
        refresh: "Refresh",
        loading: "Loading…",
        cancel: "Cancel",
        confirm: "Confirm and apply",
        save: "Save",
        delete: "Delete",
        close: "Close",
        yes: "Yes",
        no: "No",
        none: "None",
        unknown: "Unknown",
        current: "Current",
        provider: "Provider",
        model: "Model",
        status: "Status",
        warnings: "Warnings",
        retry: "Retry"
      },
      global: {
        ready: "Local service ready",
        busy: "Operation in progress",
        recovery: "Recovery required. Writes are disabled until the pending transaction is resolved.",
        stale: "Protected state changed. Prepare the operation again.",
        unexpected: "The page encountered an unexpected error.",
        partial: "Completed with locked rollout files skipped.",
        completed: "Operation completed.",
        cancelled: "Operation cancelled.",
        profileChanged: "Profile changed.",
        profileChangedHint: "Review the current profile and prepare the operation again.",
        failed: "Operation failed."
      },
      overview: {
        title: "Provider metadata overview",
        subtitle: "Compare rollout files, the SQLite thread index, and the selected profile.",
        alignment: "Alignment",
        aligned: "Aligned",
        notAligned: "Needs attention",
        rollout: "Rollout metadata",
        sqlite: "SQLite metadata",
        codexHomeSource: "Codex Home source",
        sqliteHomeSource: "SQLite Home source",
        snapshot: "Snapshot",
        backupCount: "Managed backups",
        locked: "Locked rollouts"
      },
      sync: {
        title: "Sync current Provider",
        subtitle: "Use the selected profile's root model_provider and align rollout and SQLite metadata.",
        keep: "Backups to keep",
        prepare: "Prepare sync"
      },
      switchPage: {
        title: "Switch Provider",
        subtitle: "Update root model_provider and synchronize history in one protected operation.",
        provider: "Provider ID",
        modelMode: "Model handling",
        providerDefault: "Use provider default",
        keepModel: "Keep root model",
        explicitModel: "Set explicit model",
        model: "Model name",
        prepare: "Prepare switch"
      },
      backups: {
        title: "Backups and Restore",
        subtitle: "Only managed backup IDs can be restored.",
        empty: "No managed backups.",
        restoreConfig: "Restore config.toml",
        restoreDatabase: "Restore State DB",
        restoreSessions: "Restore rollout files",
        relocation: "Confirm SQLite Home relocation",
        targetProfile: "Relocation target profile",
        prepare: "Prepare restore",
        pruneKeep: "Keep newest backups",
        prune: "Prune older backups",
        readOnly: "This build lists managed backups read-only; Restore and Prune are not exposed."
      },
      history: {
        title: "History",
        subtitle: "Session bodies load only after you explicitly open a session.",
        empty: "No sessions found.",
        untitled: "Untitled session",
        open: "Open session",
        back: "Back to sessions",
        messages: "messages",
        archived: "Archived",
        active: "Active",
        pagination: "History pagination",
        pageSummary: "Page {{page}} · {{total}} sessions",
        previous: "Previous",
        next: "Next",
        roles: {
          user: "You",
          assistant: "Assistant"
        }
      },
      profiles: {
        title: "Profiles",
        subtitle: "The host resolves paths; Core requests receive only profile IDs and revisions.",
        id: "Profile ID",
        name: "Name",
        codexHome: "Codex Home",
        sqliteHome: "SQLite Home (optional)",
        create: "Create profile",
        update: "Update profile",
        defaultManaged: "The default profile is managed by startup flags.",
        pathManaged: {
          desktop: "Storage paths are retained by the trusted desktop Host.",
          web: "Storage paths are retained by the local Web Host."
        },
        readOnly: "This build exposes profile IDs and revisions only; profile editing is not enabled."
      },
      diagnostics: {
        title: "Diagnostics",
        subtitle: "Read-only, redacted runtime and safety state.",
        runtime: "Runtime",
        storage: "Storage",
        provider: "Provider",
        safety: "Safety",
        items: "{{count}} items",
        fieldsAvailable: "{{count}} redacted fields",
        technicalDetails: "Show technical details",
        fields: {
          arch: "Architecture",
          node: "Node.js",
          platform: "Platform",
          sqliteHomeSource: "SQLite Home source",
          sqliteSupported: "SQLite supported",
          stateDbFound: "State DB found",
          configured: "Configured Providers",
          current: "Current Provider",
          implicit: "Implicit Provider",
          rolloutCounts: "Rollout distribution",
          sqliteCounts: "SQLite distribution",
          lockedRolloutCount: "Locked rollouts",
          operationInProgress: "Operation in progress",
          pendingRecovery: "Recovery required",
          pendingTransactions: "Pending transactions",
          projectThreadVisibilityAvailable: "Project visibility available",
          rolloutScanComplete: "Rollout scan complete",
          storageRevision: "Storage revision"
        },
        export: "Export redacted bundle",
        exporting: "Exporting…",
        exportCreated: "Redacted diagnostics bundle created.",
        exportCancelled: "Diagnostics export cancelled.",
        exportFailed: "Diagnostics export failed."
      },
      settings: {
        title: "Settings",
        subtitle: {
          desktop: "Language and theme preferences stay on this device.",
          web: "Language and theme preferences stay in this browser; pairing remains managed by the local Web Host."
        },
        language: "Language",
        theme: "Theme",
        system: "System",
        light: "Light",
        dark: "Dark",
        watch: "Watch",
        watchStart: "Start watch",
        watchStop: "Stop watch",
        watchRecoveryBlocked: "Resolve the pending recovery before starting Watch.",
        update: "Updates",
        updateStatus: {
          disabled: "Unavailable",
          idle: "Ready to check",
          checking: "Checking",
          available: "Update available",
          downloading: "Downloading",
          downloaded: "Ready to install",
          "not-available": "Up to date",
          error: "Update failed",
          installing: "Restarting to install"
        },
        updateReason: {
          "not-packaged": "Update checks are available only in a packaged build.",
          "not-configured": "No release update channel is configured.",
          "unsupported-target": "Updates are not supported for this platform target.",
          "check-failed": "The update check failed without affecting Core operations.",
          "download-failed": "The update download failed without affecting Core operations.",
          "install-failed": "The installer could not be started; the current version remains active."
        },
        updateBlocked: {
          "write-in-progress": "An update cannot be installed while a protected operation is running.",
          "watch-active": "Stop Watch before installing an update.",
          "pending-recovery": "An update cannot be installed while a transaction requires recovery.",
          "recovery-unverified": "Recovery state could not be verified; installation remains blocked."
        },
        updateVersion: "Version {{version}}",
        updateProgress: "{{percent}}% downloaded",
        updateCheck: "Check for updates",
        updateDownload: "Download update",
        updateInstall: "Restart and install",
        forget: "Forget this browser",
        englishFallback: "English fallback",
        forgetHint: "Pairing credentials are removed by the local host."
      },
      plan: {
        title: "Review plan",
        operations: {
          sync: "Sync Provider metadata",
          switch: "Switch Provider",
          restore: "Restore backup",
          operation: "Protected operation"
        },
        modelModes: {
          "provider-default": "Use Provider default model",
          "keep-root-model": "Keep root model",
          explicit: "Use explicit model"
        },
        fields: {
          modelMode: "Model handling",
          restoreConfig: "Restore config.toml",
          restoreDatabase: "Restore State DB",
          restoreSessions: "Restore rollout files",
          relocation: "SQLite Home relocation",
          rolloutFiles: "Rollout files affected",
          sqliteRows: "SQLite rows affected",
          workspaceRoots: "Workspace roots affected",
          stateDbFiles: "State DB files affected",
          configFiles: "Config files affected",
          lockedRollouts: "Currently locked rollouts"
        },
        stages: {
          scan_rollout_files: "Scan rollout files",
          check_locked_rollout_files: "Check locked rollouts",
          create_backup: "Create managed backup",
          rewrite_rollout_files: "Update rollout files",
          update_sqlite: "Update SQLite metadata",
          update_config: "Update config.toml",
          clean_backups: "Clean old backups",
          create_restore_pre_snapshot: "Create pre-restore snapshot",
          persist_restore_journal: "Persist Restore journal",
          apply_restore_targets: "Restore selected targets",
          commit_restore: "Commit Restore",
          acknowledge_restore_commit: "Acknowledge Restore commit",
          rollback_restore: "Roll back Restore"
        },
        statuses: {
          start: "Starting",
          progress: "In progress",
          complete: "Completed"
        },
        target: "Target",
        impact: "Impact",
        expires: "Expires",
        items: "{{count}} items",
        backupExpected: "A backup will be created before writes.",
        exactApply: "Apply sends only this one-time plan ID.",
        writeBlocked: "Another protected operation or recovery state currently blocks confirmation.",
        technicalDetails: "Technical details",
        progress: "Operation progress",
        starting: "Starting protected operation…",
        cancelOperation: "Cancel operation",
        cancelling: "Cancelling…",
        cancelPending: "Cancellation will take effect at the next safe point."
      },
      operationResult: {
        title: "Operation result",
        operationId: "Operation ID",
        backupId: "Managed backup ID",
        skippedRollouts: "Skipped locked rollout files",
        resolveBeforeClose: "Resolve the pending recovery before closing this result.",
        fields: {
          targetProvider: "Target Provider",
          targetModel: "Target model",
          modelSource: "Model source",
          restoreOperationId: "Restore operation ID",
          preRestoreSnapshotId: "Pre-restore snapshot ID",
          restoreJournalState: "Restore journal state",
          backupDurationMs: "Backup duration (ms)",
          changedSessionFiles: "Rollout files changed",
          sqliteRowsUpdated: "SQLite rows updated",
          sqliteProviderRowsUpdated: "Provider rows updated",
          sqliteUserEventRowsUpdated: "User-event rows updated",
          sqliteCwdRowsUpdated: "Workspace rows updated",
          updatedWorkspaceRoots: "Workspace roots updated",
          savedWorkspaceRootCount: "Saved workspace roots",
          restoreVersion: "Restore format version",
          resolvedOperationCount: "Resolved operations",
          commitAcknowledgementRecovered: "Commit acknowledgement recovered"
        },
        completed: {
          title: "Completed",
          description: "The protected operation reached a durable completed state."
        },
        partial: {
          title: "Partially completed",
          description: "Committed changes are durable, but one or more locked rollout files were skipped."
        },
        failedRolledBack: {
          title: "Failed and rolled back",
          description: "The operation failed, and the previous state was restored successfully."
        },
        recoveryRequired: {
          title: "Recovery required",
          description: "A durable journal remains unresolved. Further writes stay blocked until recovery is completed."
        },
        cancelled: {
          title: "Cancelled",
          description: "The operation stopped at a safe cancellation point."
        },
        stale: {
          title: "Plan became stale",
          description: "Protected state changed after planning. Review a newly prepared plan before retrying."
        }
      },
      validation: {
        required: "This field is required.",
        keep: "Use a whole number from 1 to 1000.",
        provider: "Enter a valid Provider ID.",
        model: "Enter a model name for explicit mode.",
        restore: "Select at least one item to restore.",
        profileId: "Use letters, numbers, dots, underscores, or hyphens.",
        path: "Enter an absolute path."
      }
    }
  },
  "zh-CN": {
    translation: {
      brand: {
        desktop: {
          label: "桌面端",
          subtitle: "V1 新版主桌面端候选 · .NET 交接后 Legacy fallback 目标"
        },
        web: {
          label: "Web",
          subtitle: "本地 Web companion"
        }
      },
      a11y: {
        skipToContent: "跳到主要内容",
        profile: "存储配置",
        primaryNavigation: "主导航"
      },
      nav: {
        overview: "概览",
        sync: "同步",
        switchProvider: "切换 Provider",
        backupsRestore: "备份 / 恢复",
        history: "聊天记录",
        profiles: "存储配置",
        diagnostics: "诊断",
        settings: "设置"
      },
      common: {
        refresh: "刷新",
        loading: "正在加载…",
        cancel: "取消",
        confirm: "确认并执行",
        save: "保存",
        delete: "删除",
        close: "关闭",
        yes: "是",
        no: "否",
        none: "无",
        unknown: "未知",
        current: "当前",
        provider: "Provider",
        model: "模型",
        status: "状态",
        warnings: "警告",
        retry: "重试"
      },
      global: {
        ready: "本地服务就绪",
        busy: "操作执行中",
        recovery: "存在待恢复事务；在明确恢复前已禁用写操作。",
        stale: "受保护状态已变化，请重新生成计划。",
        unexpected: "页面遇到未预期错误。",
        partial: "操作完成，但跳过了仍被锁定的 rollout 文件。",
        completed: "操作已完成。",
        cancelled: "操作已取消。",
        profileChanged: "存储配置已变化。",
        profileChangedHint: "请检查当前存储配置，然后重新生成操作计划。",
        failed: "操作失败。"
      },
      overview: {
        title: "Provider 元数据总览",
        subtitle: "比较 rollout 文件、SQLite 线程索引与当前存储配置。",
        alignment: "对齐状态",
        aligned: "已对齐",
        notAligned: "需要处理",
        rollout: "Rollout 元数据",
        sqlite: "SQLite 元数据",
        codexHomeSource: "Codex Home 来源",
        sqliteHomeSource: "SQLite Home 来源",
        snapshot: "快照时间",
        backupCount: "受管备份",
        locked: "锁定的 rollout"
      },
      sync: {
        title: "同步当前 Provider",
        subtitle: "读取当前配置的根 model_provider，并对齐 rollout 与 SQLite 元数据。",
        keep: "保留备份数量",
        prepare: "生成同步计划"
      },
      switchPage: {
        title: "切换 Provider",
        subtitle: "在一次受保护操作中修改根 model_provider 并同步历史。",
        provider: "Provider ID",
        modelMode: "模型处理方式",
        providerDefault: "使用 Provider 默认模型",
        keepModel: "保留根模型",
        explicitModel: "显式设置模型",
        model: "模型名称",
        prepare: "生成切换计划"
      },
      backups: {
        title: "备份与恢复",
        subtitle: "恢复只能使用服务端管理的 backupId。",
        empty: "暂无受管备份。",
        restoreConfig: "恢复 config.toml",
        restoreDatabase: "恢复 State DB",
        restoreSessions: "恢复 rollout 文件",
        relocation: "确认 SQLite Home 迁移",
        targetProfile: "迁移目标配置",
        prepare: "生成恢复计划",
        pruneKeep: "保留最新备份数",
        prune: "清理旧备份",
        readOnly: "此构建仅只读列出受管备份；未开放恢复和清理。"
      },
      history: {
        title: "聊天记录",
        subtitle: "只有在你明确打开会话后才加载消息正文。",
        empty: "没有找到会话。",
        untitled: "未命名会话",
        open: "打开会话",
        back: "返回会话列表",
        messages: "条消息",
        archived: "已归档",
        active: "活动",
        pagination: "聊天记录分页",
        pageSummary: "第 {{page}} 页 · 共 {{total}} 个会话",
        previous: "上一页",
        next: "下一页",
        roles: {
          user: "你",
          assistant: "助手"
        }
      },
      profiles: {
        title: "存储配置",
        subtitle: "路径由 Host 可信解析；Core 请求只携带配置 ID 与 revision。",
        id: "配置 ID",
        name: "名称",
        codexHome: "Codex Home",
        sqliteHome: "SQLite Home（可选）",
        create: "新建配置",
        update: "更新配置",
        defaultManaged: "默认配置由启动参数管理。",
        pathManaged: {
          desktop: "存储路径仅由可信桌面 Host 持有。",
          web: "存储路径仅由本地 Web Host 持有。"
        },
        readOnly: "此构建只公开配置 ID 与 revision；未开放配置编辑。"
      },
      diagnostics: {
        title: "诊断",
        subtitle: "只读展示经脱敏的运行时与安全状态。",
        runtime: "运行时",
        storage: "存储",
        provider: "Provider",
        safety: "安全状态",
        items: "{{count}} 项",
        fieldsAvailable: "{{count}} 个脱敏字段",
        technicalDetails: "显示技术详情",
        fields: {
          arch: "架构",
          node: "Node.js",
          platform: "平台",
          sqliteHomeSource: "SQLite Home 来源",
          sqliteSupported: "SQLite 支持状态",
          stateDbFound: "State DB 是否存在",
          configured: "已配置 Provider",
          current: "当前 Provider",
          implicit: "隐式 Provider",
          rolloutCounts: "Rollout 分布",
          sqliteCounts: "SQLite 分布",
          lockedRolloutCount: "锁定的 rollout",
          operationInProgress: "执行中的操作",
          pendingRecovery: "需要恢复",
          pendingTransactions: "待处理事务",
          projectThreadVisibilityAvailable: "项目可见性可用",
          rolloutScanComplete: "Rollout 扫描完成",
          storageRevision: "存储 revision"
        },
        export: "导出脱敏诊断包",
        exporting: "正在导出…",
        exportCreated: "脱敏诊断包已创建。",
        exportCancelled: "已取消诊断导出。",
        exportFailed: "诊断导出失败。"
      },
      settings: {
        title: "设置",
        subtitle: {
          desktop: "语言和主题偏好仅保存在此设备。",
          web: "语言和主题偏好仅保存在此浏览器；配对仍由本地 Web Host 管理。"
        },
        language: "语言",
        theme: "主题",
        system: "跟随系统",
        light: "浅色",
        dark: "深色",
        watch: "监视",
        watchStart: "启动监视",
        watchStop: "停止监视",
        watchRecoveryBlocked: "请先解决待恢复事务，再启动监视。",
        update: "更新",
        updateStatus: {
          disabled: "不可用",
          idle: "可检查更新",
          checking: "正在检查",
          available: "发现新版本",
          downloading: "正在下载",
          downloaded: "可安装",
          "not-available": "已是最新版本",
          error: "更新失败",
          installing: "正在重启安装"
        },
        updateReason: {
          "not-packaged": "仅打包后的应用可检查更新。",
          "not-configured": "尚未配置正式 Release 更新通道。",
          "unsupported-target": "当前平台目标不支持应用内更新。",
          "check-failed": "检查更新失败，不会影响 Core 操作。",
          "download-failed": "下载更新失败，不会影响 Core 操作。",
          "install-failed": "无法启动安装程序，当前版本仍保持可用。"
        },
        updateBlocked: {
          "write-in-progress": "受保护操作运行期间不能安装更新。",
          "watch-active": "请先停止监视，再安装更新。",
          "pending-recovery": "存在待恢复事务时不能安装或重启更新。",
          "recovery-unverified": "无法确认所有 Profile 的恢复状态，已阻止安装。"
        },
        updateVersion: "版本 {{version}}",
        updateProgress: "已下载 {{percent}}%",
        updateCheck: "检查更新",
        updateDownload: "下载更新",
        updateInstall: "重启并安装",
        forget: "忘记此浏览器",
        englishFallback: "英文为兜底语言",
        forgetHint: "配对凭据将由本地 Host 删除。"
      },
      plan: {
        title: "审核计划",
        operations: {
          sync: "同步 Provider 元数据",
          switch: "切换 Provider",
          restore: "恢复备份",
          operation: "受保护操作"
        },
        modelModes: {
          "provider-default": "使用 Provider 默认模型",
          "keep-root-model": "保留根模型",
          explicit: "使用显式模型"
        },
        fields: {
          modelMode: "模型处理方式",
          restoreConfig: "恢复 config.toml",
          restoreDatabase: "恢复 State DB",
          restoreSessions: "恢复 rollout 文件",
          relocation: "SQLite Home 迁移",
          rolloutFiles: "受影响的 rollout 文件",
          sqliteRows: "受影响的 SQLite 行",
          workspaceRoots: "受影响的工作区根目录",
          stateDbFiles: "受影响的 State DB 文件",
          configFiles: "受影响的配置文件",
          lockedRollouts: "当前锁定的 rollout"
        },
        stages: {
          scan_rollout_files: "扫描 rollout 文件",
          check_locked_rollout_files: "检查锁定的 rollout",
          create_backup: "创建受管备份",
          rewrite_rollout_files: "更新 rollout 文件",
          update_sqlite: "更新 SQLite 元数据",
          update_config: "更新 config.toml",
          clean_backups: "清理旧备份",
          create_restore_pre_snapshot: "创建恢复前快照",
          persist_restore_journal: "持久化 Restore journal",
          apply_restore_targets: "恢复所选目标",
          commit_restore: "提交恢复",
          acknowledge_restore_commit: "确认恢复提交",
          rollback_restore: "回滚恢复"
        },
        statuses: {
          start: "正在开始",
          progress: "执行中",
          complete: "已完成"
        },
        target: "目标",
        impact: "影响",
        expires: "失效时间",
        items: "{{count}} 项",
        backupExpected: "写入前会先创建备份。",
        exactApply: "执行时只提交此一次性 planId。",
        writeBlocked: "当前存在其他受保护操作或待恢复状态，暂不能确认执行。",
        technicalDetails: "技术详情",
        progress: "操作进度",
        starting: "正在启动受保护操作…",
        cancelOperation: "取消操作",
        cancelling: "正在取消…",
        cancelPending: "取消将在下一个安全点生效。"
      },
      operationResult: {
        title: "操作结果",
        operationId: "操作 ID",
        backupId: "受管备份 ID",
        skippedRollouts: "跳过的锁定 rollout 文件",
        resolveBeforeClose: "请先完成待处理的恢复，再关闭此结果。",
        fields: {
          targetProvider: "目标 Provider",
          targetModel: "目标模型",
          modelSource: "模型来源",
          restoreOperationId: "恢复操作 ID",
          preRestoreSnapshotId: "恢复前快照 ID",
          restoreJournalState: "恢复 journal 状态",
          backupDurationMs: "备份耗时（毫秒）",
          changedSessionFiles: "已修改 rollout 文件",
          sqliteRowsUpdated: "已更新 SQLite 行",
          sqliteProviderRowsUpdated: "已更新 Provider 行",
          sqliteUserEventRowsUpdated: "已更新用户事件行",
          sqliteCwdRowsUpdated: "已更新工作区行",
          updatedWorkspaceRoots: "已更新工作区根目录",
          savedWorkspaceRootCount: "已保存工作区根目录",
          restoreVersion: "恢复格式版本",
          resolvedOperationCount: "已解决操作数",
          commitAcknowledgementRecovered: "已恢复提交确认"
        },
        completed: {
          title: "已完成",
          description: "受保护操作已进入耐久的完成状态。"
        },
        partial: {
          title: "部分完成",
          description: "已提交的更改已持久化，但仍有一个或多个锁定的 rollout 文件被跳过。"
        },
        failedRolledBack: {
          title: "失败并已回滚",
          description: "操作失败，且先前状态已成功恢复。"
        },
        recoveryRequired: {
          title: "需要恢复",
          description: "仍有未解决的耐久 journal；完成恢复前将继续阻止写操作。"
        },
        cancelled: {
          title: "已取消",
          description: "操作已在安全取消点停止。"
        },
        stale: {
          title: "计划已失效",
          description: "生成计划后受保护状态发生变化；重试前请重新生成并审核计划。"
        }
      },
      validation: {
        required: "此项必填。",
        keep: "请输入 1 到 1000 的整数。",
        provider: "请输入有效的 Provider ID。",
        model: "显式模式必须填写模型名称。",
        restore: "至少选择一种恢复内容。",
        profileId: "只能使用字母、数字、点、下划线或连字符。",
        path: "请输入绝对路径。"
      }
    }
  }
} as const;

export async function createAppI18n(locale: SupportedLocale): Promise<i18n> {
  const instance = i18next.createInstance();
  await instance.init({
    resources,
    lng: locale,
    fallbackLng: "en",
    interpolation: { escapeValue: false },
    returnNull: false
  });
  return instance;
}

export function resourcesHaveMatchingKeys(): boolean {
  const flatten = (value: Record<string, unknown>, prefix = ""): string[] => Object.entries(value).flatMap(([key, entry]) => {
    const path = prefix ? `${prefix}.${key}` : key;
    return entry && typeof entry === "object"
      ? flatten(entry as Record<string, unknown>, path)
      : [path];
  });
  const english = flatten(resources.en.translation as unknown as Record<string, unknown>).sort();
  const chinese = flatten(resources["zh-CN"].translation as unknown as Record<string, unknown>).sort();
  return english.length === chinese.length && english.every((key, index) => key === chinese[index]);
}
