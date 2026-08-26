import i18next, { type i18n } from "i18next";
import type { SupportedLocale } from "@codex-provider-sync/design-system";

export const resources = {
  en: {
    translation: {
      brandSubtitle: "Local metadata alignment",
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
        readOnly: "Desktop Alpha lists managed backups read-only; Restore and Prune are not exposed."
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
        active: "Active"
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
        pathManaged: "Storage paths are retained by the trusted desktop host.",
        readOnly: "Desktop Alpha exposes profile IDs and revisions only; profile editing is not enabled."
      },
      diagnostics: {
        title: "Diagnostics",
        subtitle: "Read-only, redacted runtime and safety state.",
        runtime: "Runtime",
        storage: "Storage",
        provider: "Provider",
        safety: "Safety"
      },
      settings: {
        title: "Settings",
        subtitle: "Only language and theme preferences are stored in this browser.",
        language: "Language",
        theme: "Theme",
        system: "System",
        light: "Light",
        dark: "Dark",
        watch: "Watch",
        watchStart: "Start watch",
        watchStop: "Stop watch",
        forget: "Forget this browser",
        englishFallback: "English fallback",
        forgetHint: "Pairing credentials are removed by the local host."
      },
      plan: {
        title: "Review plan",
        target: "Target",
        impact: "Impact",
        expires: "Expires",
        backupExpected: "A backup will be created before writes.",
        exactApply: "Apply sends only this one-time plan ID.",
        progress: "Operation progress",
        starting: "Starting protected operation…",
        cancelOperation: "Cancel operation",
        cancelling: "Cancelling…",
        cancelPending: "Cancellation will take effect at the next safe point."
      },
      validation: {
        required: "This field is required.",
        keep: "Use a whole number from 0 to 1000.",
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
      brandSubtitle: "本机元数据一致性工具",
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
        readOnly: "桌面 Alpha 仅只读列出受管备份；未开放恢复和清理。"
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
        active: "活动"
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
        pathManaged: "存储路径仅由可信桌面 Host 持有。",
        readOnly: "桌面 Alpha 只公开配置 ID 与 revision；尚未开放配置编辑。"
      },
      diagnostics: {
        title: "诊断",
        subtitle: "只读展示经脱敏的运行时与安全状态。",
        runtime: "运行时",
        storage: "存储",
        provider: "Provider",
        safety: "安全状态"
      },
      settings: {
        title: "设置",
        subtitle: "此浏览器只持久化语言和主题偏好。",
        language: "语言",
        theme: "主题",
        system: "跟随系统",
        light: "浅色",
        dark: "深色",
        watch: "监视",
        watchStart: "启动监视",
        watchStop: "停止监视",
        forget: "忘记此浏览器",
        englishFallback: "英文为兜底语言",
        forgetHint: "配对凭据将由本地 Host 删除。"
      },
      plan: {
        title: "审核计划",
        target: "目标",
        impact: "影响",
        expires: "失效时间",
        backupExpected: "写入前会先创建备份。",
        exactApply: "执行时只提交此一次性 planId。",
        progress: "操作进度",
        starting: "正在启动受保护操作…",
        cancelOperation: "取消操作",
        cancelling: "正在取消…",
        cancelPending: "取消将在下一个安全点生效。"
      },
      validation: {
        required: "此项必填。",
        keep: "请输入 0 到 1000 的整数。",
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
