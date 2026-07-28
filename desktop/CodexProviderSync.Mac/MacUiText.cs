namespace CodexProviderSync.Mac;

internal static class MacUiText
{
    public const string English = "en";
    public const string Chinese = "zh-Hans";

    public static string NormalizeLanguage(string? language)
    {
        return string.Equals(language, Chinese, StringComparison.Ordinal) ? Chinese : English;
    }

    public static bool IsChinese(string language)
    {
        return string.Equals(NormalizeLanguage(language), Chinese, StringComparison.Ordinal);
    }

    public static string Get(string language, string key)
    {
        bool zh = IsChinese(language);
        return key switch
        {
            "language" => zh ? "语言" : "Language",
            "sqliteHome" => "SQLite Home",
            "sqliteHomePlaceholder" => zh ? "留空时按配置自动解析" : "Resolved automatically when empty",
            "recent" => zh ? "最近使用" : "Recent",
            "browse" => zh ? "浏览" : "Browse",
            "refresh" => zh ? "刷新" : "Refresh",
            "status" => zh ? "当前状态" : "Status",
            "providers" => zh ? "Provider 列表" : "Providers",
            "actions" => zh ? "执行" : "Actions",
            "executionLog" => zh ? "执行日志" : "Execution Log",
            "addProvider" => zh ? "添加 provider id" : "Add provider id",
            "add" => zh ? "添加" : "Add",
            "removeManual" => zh ? "删除手动项" : "Remove Manual",
            "noProvider" => zh ? "未选择 provider" : "No provider selected",
            "switchConfig" => zh ? "切换 config.toml 并同步" : "Switch config.toml and sync",
            "syncMetadata" => zh ? "仅同步 Metadata" : "Sync Metadata Only",
            "switchSync" => zh ? "切换配置并同步" : "Switch Config + Sync",
            "restoreConfig" => zh ? "恢复 config.toml" : "Restore config.toml",
            "restoreSqlite" => zh ? "恢复 SQLite" : "Restore SQLite",
            "restoreRollout" => zh ? "恢复 rollout metadata" : "Restore rollout metadata",
            "restoreBackup" => zh ? "恢复备份" : "Restore Backup",
            "openBackupFolder" => zh ? "打开备份目录" : "Open Backup Folder",
            "cleanBackups" => zh ? "清理旧备份" : "Clean Old Backups",
            "ready" => zh ? "就绪" : "Ready",
            "targetProvider" => zh ? "目标 Provider" : "Target provider",
            "restoreContents" => zh ? "恢复内容" : "Restore contents",
            "keepBackups" => zh ? "保留备份数" : "Keep backups",
            "writeWarning" => zh ? "执行写操作前，请先关闭 Codex CLI、Codex App、app-server 和相关终端。" : "Close Codex CLI, Codex App, app-server, and related terminals before executing write actions.",
            "refreshing" => zh ? "刷新中..." : "Refreshing...",
            "executing" => zh ? "执行中..." : "Executing...",
            "restoring" => zh ? "恢复中..." : "Restoring...",
            "cleaning" => zh ? "清理中..." : "Cleaning backups...",
            "loadedSettings" => zh ? "已加载设置" : "Loaded settings",
            "languageChanged" => zh ? "语言已切换" : "Language changed",
            "enterProvider" => zh ? "请输入要添加的 provider id。" : "Enter a provider id before adding it.",
            "selectProvider" => zh ? "请先选择一个 provider。" : "Select a provider first.",
            "selectTargetProvider" => zh ? "请先选择目标 provider。" : "Select a target provider first.",
            "addedManualProvider" => zh ? "已添加手动 provider" : "Added manual provider",
            "removedManualProvider" => zh ? "已删除手动 provider" : "Removed manual provider",
            "confirmSyncMode" => zh ? "仅同步 metadata" : "sync metadata only",
            "confirmSwitchMode" => zh ? "切换 config.toml 并同步 metadata" : "switch config.toml and sync metadata",
            "confirmWriteMessage" => zh ? "将为 provider \"{1}\" 执行：{0}。\n\n请先关闭 Codex CLI、Codex App、app-server 和相关终端。\n\n修改 metadata 前会先创建备份。是否继续？" : "This will {0} for provider \"{1}\".\n\nClose Codex CLI, Codex App, app-server, and related terminals first.\n\nA backup will be created before metadata is changed. Continue?",
            "executionFinished" => zh ? "执行完成" : "Execution finished",
            "switchedAndSynced" => zh ? "已切换并同步" : "Switched and synced",
            "synced" => zh ? "已同步" : "Synced",
            "chooseRestoreTarget" => zh ? "请至少选择一种要恢复的内容。" : "Choose at least one restore target.",
            "restoreMessage" => zh ? "确认恢复以下备份？\n\n{0}\n\n将覆盖当前的: {1}。\n请先关闭 Codex。是否继续？" : "Restore this backup?\n\n{0}\n\nThis will overwrite: {1}.\nClose Codex first. Continue?",
            "restoreFinished" => zh ? "恢复完成" : "Restore finished",
            "cleanMessage" => zh ? "只保留最新 {0} 份本工具管理的备份。\n\n已删除的备份无法从本 App 恢复。是否继续？" : "Keep only the newest {0} managed backup(s).\n\nDeleted backup folders cannot be restored from this app. Continue?",
            "backupCleanupFinished" => zh ? "备份清理完成" : "Backup cleanup finished",
            "refreshed" => zh ? "已刷新" : "Refreshed",
            "chooseCodexFolder" => zh ? "选择 .codex 目录" : "Choose .codex folder",
            "chooseSqliteFolder" => zh ? "选择包含 state_5.sqlite 的目录" : "Choose the folder containing state_5.sqlite",
            "chooseBackupFolder" => zh ? "选择备份目录" : "Choose backup folder",
            "continue" => zh ? "继续" : "Continue",
            "cancel" => zh ? "取消" : "Cancel",
            "ok" => zh ? "确定" : "OK",
            "error" => zh ? "错误" : "Error",
            "confirmWriteTitle" => zh ? "确认写操作" : "Confirm Write Action",
            "restoreTitle" => zh ? "恢复备份" : "Restore Backup",
            "relocationTitle" => zh ? "确认 SQLite Home 迁移" : "Confirm SQLite Home Relocation",
            "relocationMessage" => zh
                ? "备份中的 SQLite Home 与当前目标不同。\n\n备份来源: {0}\n恢复目标: {1}\n\n确认将数据库恢复到当前目标吗？"
                : "The backup SQLite Home differs from the current target.\n\nBackup source: {0}\nRestore target: {1}\n\nRestore the database to the current target?",
            "cleanTitle" => zh ? "清理旧备份" : "Clean Old Backups",
            "current" => zh ? "当前" : "current",
            "manual" => zh ? "手动" : "manual",
            "saved" => zh ? "已保存" : "saved",
            _ => key
        };
    }
}
