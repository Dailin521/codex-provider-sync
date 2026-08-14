import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  PairingRequiredError,
  ProfileRevisionError,
  apiRequest,
  forgetThisBrowser,
  getActivity,
  getHistory,
  getHistorySession,
  getProfiles,
  initializeAccess
} from "./api.js";
import { usePersistentState } from "./hooks.js";
import { createLatestRequestGate, scheduleDebounced } from "./history-requests.js";
import { captureProfileOperation, dedupeHistorySessions, operationToast, resolveRestoreTargetSqliteHome, restoreRelocationState } from "./operation-state.js";
import { createProfileRefresh, storagePayload } from "./profile-refresh.js";
import {
  ActivityIcon,
  AlertIcon,
  CheckIcon,
  ChevronIcon,
  DatabaseIcon,
  FolderIcon,
  HistoryIcon,
  OverviewIcon,
  RefreshIcon,
  ShieldIcon,
  XIcon
} from "./icons.jsx";

const NAV_ITEMS = [
  { id: "overview", label: "概览", icon: OverviewIcon },
  { id: "history", label: "聊天记录", icon: HistoryIcon },
  { id: "backups", label: "备份", icon: HistoryIcon },
  { id: "activity", label: "活动", icon: ActivityIcon }
];

const EMPTY_BACKUPS = { backupRoot: "", backups: [] };

function formatNumber(value) {
  return new Intl.NumberFormat("zh-CN").format(Number(value) || 0);
}

function formatBytes(bytes) {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = Number(bytes) || 0;
  let index = 0;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  return index === 0 ? `${value} B` : `${value.toFixed(value >= 10 ? 1 : 2).replace(/\.0$/, "")} ${units[index]}`;
}

function formatDate(value) {
  if (!value) return "未知时间";
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  }).format(new Date(value));
}

function renderInlineMarkdown(text, keyPrefix) {
  const parts = text.split(/(`[^`]+`|\*\*[^*]+\*\*|\*[^*]+\*)/g);
  return parts.map((part, index) => {
    if (part.startsWith("`") && part.endsWith("`")) return <code key={`${keyPrefix}-code-${index}`}>{part.slice(1, -1)}</code>;
    if (part.startsWith("**") && part.endsWith("**")) return <strong key={`${keyPrefix}-strong-${index}`}>{part.slice(2, -2)}</strong>;
    if (part.startsWith("*") && part.endsWith("*")) return <em key={`${keyPrefix}-em-${index}`}>{part.slice(1, -1)}</em>;
    return <React.Fragment key={`${keyPrefix}-text-${index}`}>{part}</React.Fragment>;
  });
}

function SafeMarkdown({ text }) {
  const blocks = String(text ?? "").split(/(```[^\n]*\n[\s\S]*?```)/g);
  return blocks.map((block, index) => {
    if (block.startsWith("```") && block.endsWith("```")) {
      const lines = block.slice(3, -3).replace(/^\w*\n/, "");
      return <pre key={`block-${index}`}><code>{lines}</code></pre>;
    }
    return block.split("\n").map((line, lineIndex, lines) => <React.Fragment key={`line-${index}-${lineIndex}`}>{renderInlineMarkdown(line, `${index}-${lineIndex}`)}{lineIndex < lines.length - 1 ? <br /> : null}</React.Fragment>);
  });
}

function providersFromStatus(status) {
  if (!status) return [];
  const sources = new Map();
  const add = (values, source) => {
    for (const value of values ?? []) {
      if (!value || value === "(missing)") continue;
      const bucket = sources.get(value) ?? new Set();
      bucket.add(source);
      sources.set(value, bucket);
    }
  };
  add(status.configuredProviders, "config");
  add(Object.keys(status.rolloutCounts?.sessions ?? {}), "rollout");
  add(Object.keys(status.rolloutCounts?.archived_sessions ?? {}), "rollout");
  add(Object.keys(status.sqliteCounts?.sessions ?? {}), "sqlite");
  add(Object.keys(status.sqliteCounts?.archived_sessions ?? {}), "sqlite");
  add([status.currentProvider], "config");
  return [...sources.entries()]
    .map(([id, providerSources]) => ({
      id,
      sources: [...providerSources],
      configured: status.configuredProviders?.includes(id),
      current: id === status.currentProvider
    }))
    .sort((left, right) => Number(right.current) - Number(left.current) || left.id.localeCompare(right.id));
}

function StatusDot({ tone = "neutral" }) {
  return <span className={`status-dot status-dot--${tone}`} aria-hidden="true" />;
}

function AppHeader({ status, busy, onRefresh }) {
  const healthy = status?.sqliteAccess?.supported !== false && !status?.sqliteCounts?.unreadable;
  return (
    <header className="app-header">
      <div className="brand">
        <div className="brand-mark"><DatabaseIcon size={19} /></div>
        <div>
          <div className="brand-name">Codex Provider Sync</div>
          <div className="brand-subtitle">本机元数据一致性工具</div>
        </div>
      </div>
      <div className="header-actions">
        <div className="service-state">
          <StatusDot tone={busy ? "warning" : healthy ? "success" : "danger"} />
          <span>{busy ? "操作执行中" : healthy ? "本地服务就绪" : "需要检查"}</span>
        </div>
        <button className="button button--secondary button--compact" type="button" onClick={onRefresh} disabled={busy}>
          <RefreshIcon size={16} />
          刷新
        </button>
      </div>
    </header>
  );
}

function Sidebar({ view, setView, status, onForgetBrowser }) {
  return (
    <aside className="sidebar">
      <nav className="navigation" aria-label="主导航">
        {NAV_ITEMS.map((item) => {
          const Icon = item.icon;
          return (
            <button
              className={`nav-item ${view === item.id ? "nav-item--active" : ""}`}
              key={item.id}
              type="button"
              onClick={() => setView(item.id)}
            >
              <Icon size={17} />
              <span>{item.label}</span>
            </button>
          );
        })}
      </nav>
      <div className="sidebar-foot">
        <div className="sidebar-provider-label">当前 Provider</div>
        <div className="sidebar-provider-value">
          <StatusDot tone="success" />
          <span>{status?.currentProvider ?? "未读取"}</span>
        </div>
        <div className="sidebar-version">Web UI · localhost only</div>
        <button className="button button--quiet button--compact" type="button" onClick={onForgetBrowser}>忘记此浏览器</button>
      </div>
    </aside>
  );
}

function StorageBar({ profiles, profileId, setProfileId, status, onAddProfile, onDeleteProfile, onRefresh, loading, profileSwitchDisabled }) {
  return (
    <section className="storage-bar" aria-label="存储位置">
      <label className="path-field path-field--wide">
        <span>存储配置</span>
        <div className="path-input-wrap">
          <FolderIcon size={16} />
          <select value={profileId} onChange={(event) => setProfileId(event.target.value)} disabled={profileSwitchDisabled}>
            {profiles.map((profile) => <option key={profile.id} value={profile.id}>{profile.name}</option>)}
          </select>
        </div>
      </label>
      <label className="path-field">
        <span>当前路径 <small>由服务端解析</small></span>
        <div className="path-input-wrap">
          <DatabaseIcon size={16} />
          <input value={status?.codexHome ?? ""} readOnly placeholder="读取状态后显示" />
        </div>
      </label>
      <button className="button button--secondary storage-refresh" type="button" onClick={onAddProfile}>新增配置</button>
      {profileId !== "default" ? <button className="button button--quiet storage-refresh" type="button" onClick={onDeleteProfile}>删除配置</button> : null}
      <button className="button button--secondary storage-refresh" type="button" onClick={onRefresh} disabled={loading}>
        <RefreshIcon size={16} className={loading ? "spin" : ""} />
        读取状态
      </button>
    </section>
  );
}

function ProviderBars({ title, counts, currentProvider }) {
  const entries = Object.entries(counts ?? {}).sort((left, right) => right[1] - left[1]);
  const total = entries.reduce((sum, [, count]) => sum + count, 0);
  return (
    <div className="distribution-block">
      <div className="distribution-heading">
        <span>{title}</span>
        <strong>{formatNumber(total)}</strong>
      </div>
      <div className="distribution-list">
        {entries.length === 0 ? <div className="empty-inline">无记录</div> : entries.map(([provider, count]) => (
          <div className="distribution-row" key={provider}>
            <div className="distribution-meta">
              <span className="provider-name">{provider}</span>
              <span>{formatNumber(count)}</span>
            </div>
            <div className="bar-track">
              <span
                className={provider === currentProvider ? "bar-fill bar-fill--current" : "bar-fill"}
                style={{ width: `${Math.max(5, total ? (count / total) * 100 : 0)}%` }}
              />
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function StatusPanel({ status, loading }) {
  const rolloutTotal = Object.values(status?.rolloutCounts?.sessions ?? {}).reduce((sum, value) => sum + value, 0)
    + Object.values(status?.rolloutCounts?.archived_sessions ?? {}).reduce((sum, value) => sum + value, 0);
  const sqliteTotal = Object.values(status?.sqliteCounts?.sessions ?? {}).reduce((sum, value) => sum + value, 0)
    + Object.values(status?.sqliteCounts?.archived_sessions ?? {}).reduce((sum, value) => sum + value, 0);
  const aligned = status?.alignment?.aligned;
  return (
    <section className="status-panel">
      <div className="section-title-row">
        <div>
          <h2>状态总览</h2>
          <p>比较 rollout 文件、SQLite 线程索引和当前 Provider。</p>
        </div>
        <div className={`alignment-state ${aligned ? "alignment-state--success" : "alignment-state--warning"}`}>
          {loading ? <RefreshIcon size={16} className="spin" /> : aligned ? <CheckIcon size={16} /> : <AlertIcon size={16} />}
          <span>{loading ? "正在检查" : aligned ? "元数据已对齐" : "发现不一致"}</span>
        </div>
      </div>

      <div className="summary-strip">
        <div className="summary-item">
          <span>当前 Provider</span>
          <strong>{status?.currentProvider ?? "—"}</strong>
          <small>{status?.currentProviderImplicit ? "隐式默认" : "config.toml 根级配置"}</small>
        </div>
        <div className="summary-item">
          <span>Rollout 文件</span>
          <strong>{formatNumber(rolloutTotal)}</strong>
          <small>sessions + archived</small>
        </div>
        <div className="summary-item">
          <span>SQLite threads</span>
          <strong>{status?.sqliteCounts?.unreadable ? "不可读" : formatNumber(sqliteTotal)}</strong>
          <small>{status?.stateDbLocation?.source ?? "未定位数据库"}</small>
        </div>
        <div className="summary-item">
          <span>托管备份</span>
          <strong>{formatNumber(status?.backupSummary?.count)}</strong>
          <small>{formatBytes(status?.backupSummary?.totalBytes)}</small>
        </div>
      </div>

      <div className="distribution-grid">
        <div className="distribution-column">
          <div className="column-label"><FolderIcon size={16} /> Rollout files</div>
          <ProviderBars title="sessions" counts={status?.rolloutCounts?.sessions} currentProvider={status?.currentProvider} />
          <ProviderBars title="archived_sessions" counts={status?.rolloutCounts?.archived_sessions} currentProvider={status?.currentProvider} />
        </div>
        <div className="distribution-divider" />
        <div className="distribution-column">
          <div className="column-label"><DatabaseIcon size={16} /> SQLite state</div>
          <ProviderBars title="sessions" counts={status?.sqliteCounts?.sessions} currentProvider={status?.currentProvider} />
          <ProviderBars title="archived_sessions" counts={status?.sqliteCounts?.archived_sessions} currentProvider={status?.currentProvider} />
        </div>
      </div>
    </section>
  );
}

function Warnings({ status }) {
  const items = [];
  if (status?.sqliteAccess?.supported === false) {
    items.push({ tone: "danger", title: "SQLite 路径不可安全访问", detail: status.sqliteAccess.message });
  }
  if (status?.sqliteCounts?.unreadable) {
    items.push({ tone: "danger", title: "SQLite 当前不可读", detail: status.sqliteCounts.error });
  }
  if (status?.lockedRolloutFiles?.length) {
    items.push({ tone: "warning", title: `${status.lockedRolloutFiles.length} 个 rollout 文件正在使用`, detail: "同步会跳过这些活跃文件；会话结束后可再次执行。" });
  }
  if (status?.encryptedContentWarning) {
    items.push({ tone: "warning", title: "检测到 encrypted_content", detail: status.encryptedContentWarning });
  }
  const repairs = status?.sqliteRepairStats;
  if (repairs?.userEventRowsNeedingRepair || repairs?.cwdRowsNeedingRepair) {
    items.push({ tone: "info", title: "SQLite 有待修复字段", detail: `user-event ${repairs.userEventRowsNeedingRepair ?? 0}，cwd ${repairs.cwdRowsNeedingRepair ?? 0}` });
  }
  if (items.length === 0) return null;
  return (
    <section className="warning-stack" aria-label="诊断信息">
      {items.map((item, index) => (
        <div className={`warning-row warning-row--${item.tone}`} key={`${item.title}-${index}`}>
          <AlertIcon size={17} />
          <div><strong>{item.title}</strong><span>{item.detail}</span></div>
        </div>
      ))}
    </section>
  );
}

function ProjectVisibility({ projects = [] }) {
  return (
    <section className="data-section">
      <div className="section-title-row section-title-row--compact">
        <div><h2>项目可见性</h2><p>检查 Desktop 项目路径、全局排序和首屏 50 条命中。</p></div>
      </div>
      <div className="table-scroll">
        <table className="data-table">
          <thead><tr><th>项目根目录</th><th>交互会话</th><th>首屏</th><th>Ranks</th><th>CWD 精确匹配</th><th>Provider</th></tr></thead>
          <tbody>
            {projects.length === 0 ? (
              <tr><td colSpan="6" className="table-empty">没有可显示的项目诊断。</td></tr>
            ) : projects.map((project) => (
              <tr key={project.root}>
                <td className="path-cell">{project.root}</td>
                <td>{project.interactiveThreads}</td>
                <td>{project.firstPageThreads}/50</td>
                <td>{project.rankPreview || "—"}</td>
                <td>{project.exactCwdMatches}/{project.interactiveThreads}</td>
                <td>{Object.entries(project.providerCounts ?? {}).map(([provider, count]) => `${provider} ${count}`).join(" · ") || "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function SegmentedControl({ value, onChange, disabled }) {
  return (
    <div className="segmented" role="radiogroup" aria-label="执行模式">
      <button type="button" className={value === "sync" ? "segmented-option segmented-option--active" : "segmented-option"} onClick={() => onChange("sync")} disabled={disabled}>
        <span>仅同步元数据</span><small>不修改 config.toml</small>
      </button>
      <button type="button" className={value === "switch" ? "segmented-option segmented-option--active" : "segmented-option"} onClick={() => onChange("switch")} disabled={disabled}>
        <span>切换 Provider 并同步</span><small>更新根级配置</small>
      </button>
    </div>
  );
}

function ExecutionPanel({ status, providers, selectedProvider, setSelectedProvider, onAddManualProvider, onRemoveManualProvider, onRequestExecute, busy }) {
  const [mode, setMode] = usePersistentState("cps.web.mode", "sync");
  const [modelMode, setModelMode] = usePersistentState("cps.web.modelMode", "auto");
  const [customModel, setCustomModel] = usePersistentState("cps.web.customModel", "");
  const [keepCount, setKeepCount] = usePersistentState("cps.web.keepCount", 5);
  const [manualProvider, setManualProvider] = useState("");
  const selectedOption = providers.find((provider) => provider.id === selectedProvider);
  const switchAllowed = selectedOption?.configured;
  const validManualProvider = /^[A-Za-z0-9_.-]+$/.test(manualProvider.trim());
  const sqliteUnsupported = status?.sqliteAccess?.supported === false;
  const executeDisabled = busy || !selectedProvider || sqliteUnsupported || status?.sqliteCounts?.unreadable;

  return (
    <section className="execution-panel">
      <div className="execution-head">
        <div><h2>执行同步</h2><p>所有写操作都会先创建备份。执行前请关闭 Codex CLI、App 和 app-server。</p></div>
        <ShieldIcon size={22} />
      </div>
      <SegmentedControl value={mode} onChange={setMode} disabled={busy} />
      <div className="form-grid">
        <label className="form-field">
          <span>目标 Provider</span>
          <select value={selectedProvider} onChange={(event) => setSelectedProvider(event.target.value)} disabled={busy}>
            {providers.map((provider) => <option key={provider.id} value={provider.id}>{provider.id}{provider.current ? "（当前）" : ""}</option>)}
          </select>
          {mode === "switch" && !switchAllowed ? <small className="field-error">切换模式要求该 Provider 已定义在 config.toml。</small> : null}
        </label>
        <label className="form-field form-field--short">
          <span>保留备份数</span>
          <input type="number" min="1" max="100000" value={keepCount} onChange={(event) => setKeepCount(Number(event.target.value))} disabled={busy} />
          <small>同步后自动清理</small>
        </label>
      </div>
      <div className="manual-provider-row">
        <input
          type="text"
          value={manualProvider}
          onChange={(event) => setManualProvider(event.target.value)}
          placeholder="手动添加 Provider ID"
          spellCheck="false"
          disabled={busy}
        />
        <button
          className="button button--quiet button--compact"
          type="button"
          disabled={busy || !validManualProvider}
          onClick={() => {
            onAddManualProvider(manualProvider.trim());
            setManualProvider("");
          }}
        >
          添加
        </button>
        {selectedOption?.manual ? <button className="manual-remove" type="button" onClick={() => onRemoveManualProvider(selectedProvider)} disabled={busy}>删除当前手动项</button> : null}
      </div>

      {mode === "switch" ? (
        <fieldset className="model-options" disabled={busy}>
          <legend>根级 model</legend>
          <label><input type="radio" name="model-mode" checked={modelMode === "auto"} onChange={() => setModelMode("auto")} /><span><strong>跟随 Provider 配置</strong><small>采用 `[model_providers.{selectedProvider}]` 中的 model</small></span></label>
          <label><input type="radio" name="model-mode" checked={modelMode === "keep"} onChange={() => setModelMode("keep")} /><span><strong>保留当前根级 model</strong><small>只切换 model_provider</small></span></label>
          <label className="custom-model-option"><input type="radio" name="model-mode" checked={modelMode === "custom"} onChange={() => setModelMode("custom")} /><span><strong>自定义 model</strong><input type="text" value={customModel} onFocus={() => setModelMode("custom")} onChange={(event) => setCustomModel(event.target.value)} placeholder="例如 MiniMax-M3" /></span></label>
        </fieldset>
      ) : null}

      {sqliteUnsupported ? <div className="modal-callout modal-callout--danger"><AlertIcon size={18} /><div><strong>此 SQLite 布局仅供诊断</strong><span>{status?.sqliteAccess?.message || "当前 SQLite 路径不可由 Web UI 安全写入，因此已禁用执行同步。"}</span></div></div> : null}

      <div className="backup-assurance"><CheckIcon size={16} /><span>修改前创建 metadata v2 备份，并记录 SQLite Home</span></div>
      <button
        className="button button--primary execute-button"
        type="button"
        disabled={executeDisabled || (mode === "switch" && !switchAllowed) || (mode === "switch" && modelMode === "custom" && !customModel.trim())}
        onClick={() => onRequestExecute({ mode, modelMode, model: customModel.trim(), keepCount })}
      >
        {busy ? <RefreshIcon size={17} className="spin" /> : <ShieldIcon size={17} />}
        {busy ? "正在执行…" : mode === "switch" ? "切换并同步" : "执行同步"}
      </button>
    </section>
  );
}

function RecentBackups({ backups, onViewAll, onRestore, restoreDisabled }) {
  return (
    <section className="recent-backups">
      <div className="section-title-row section-title-row--compact">
        <div><h2>最近备份</h2><p>{backups.backupRoot || "同步后将在 Codex Home 下创建备份"}</p></div>
        <button type="button" className="text-button" onClick={onViewAll}>查看全部 <ChevronIcon size={14} /></button>
      </div>
      <div className="backup-rows">
        {backups.backups.length === 0 ? <div className="empty-state">尚无由本工具创建的备份。</div> : backups.backups.slice(0, 3).map((backup) => (
          <div className="backup-row" key={backup.id}>
            <div className="backup-icon"><HistoryIcon size={17} /></div>
            <div className="backup-main"><strong>{formatDate(backup.metadata.createdAt)}</strong><span>{backup.metadata.targetProvider} · {backup.metadata.changedSessionFiles ?? 0} 个 rollout</span></div>
            <div className="backup-size">{formatBytes(backup.sizeBytes)}</div>
            <button className="button button--quiet button--compact" type="button" disabled={restoreDisabled} title={restoreDisabled ? "当前 SQLite 路径仅供诊断，不能恢复" : undefined} onClick={() => onRestore(backup)}>恢复</button>
          </div>
        ))}
      </div>
    </section>
  );
}

function Overview({ status, backups, providers, selectedProvider, setSelectedProvider, onAddManualProvider, onRemoveManualProvider, onExecute, onRestore, setView, busy, loading }) {
  return (
    <div className="view-content">
      <StatusPanel status={status} loading={loading} />
      <Warnings status={status} />
      <div className="overview-lower-grid">
        <div className="overview-main-column">
          <ProjectVisibility projects={status?.projectThreadVisibility ?? []} />
          <RecentBackups backups={backups} onViewAll={() => setView("backups")} onRestore={onRestore} restoreDisabled={status?.sqliteAccess?.supported === false} />
        </div>
        <ExecutionPanel
          status={status}
          providers={providers}
          selectedProvider={selectedProvider}
          setSelectedProvider={setSelectedProvider}
          onAddManualProvider={onAddManualProvider}
          onRemoveManualProvider={onRemoveManualProvider}
          onRequestExecute={onExecute}
          busy={busy}
        />
      </div>
    </div>
  );
}

function BackupsView({ backups, status, busy, onRestore, onPrune }) {
  const [keepCount, setKeepCount] = usePersistentState("cps.web.keepCount", 5);
  const restoreDisabled = status?.sqliteAccess?.supported === false;
  return (
    <div className="view-content">
      <section className="page-intro">
        <div><h1>备份</h1><p>管理当前 Codex Home 下由本工具创建的 metadata v2 备份。</p></div>
        <div className="prune-control"><label>保留最近 <input type="number" min="0" max="100000" value={keepCount} onChange={(event) => setKeepCount(Number(event.target.value))} /> 份</label><button className="button button--secondary" type="button" disabled={busy} onClick={() => onPrune(keepCount)}>清理旧备份</button></div>
      </section>
      <section className="backup-list-section">
        <div className="backup-root-line"><FolderIcon size={16} /><span>{backups.backupRoot || status?.backupRoot || "—"}</span><strong>{backups.backups.length} 份 · {formatBytes(backups.backups.reduce((sum, backup) => sum + backup.sizeBytes, 0))}</strong></div>
        <div className="full-backup-list">
          {backups.backups.length === 0 ? <div className="large-empty"><HistoryIcon size={26} /><strong>还没有备份</strong><span>执行一次同步或切换后，备份会显示在这里。</span></div> : backups.backups.map((backup) => (
            <article className="full-backup-row" key={backup.id}>
              <div className="backup-date"><strong>{formatDate(backup.metadata.createdAt)}</strong><span>{backup.id}</span></div>
              <div className="backup-facts"><span>Provider <strong>{backup.metadata.targetProvider}</strong></span><span>Rollout <strong>{backup.metadata.changedSessionFiles ?? 0}</strong></span><span>SQLite <strong>{backup.metadata.sqliteDbFiles?.length ? "已包含" : "未包含"}</strong></span></div>
              <div className="backup-source"><span>SQLite Home</span><code>{backup.metadata.sqliteHome ?? "旧版 metadata 未记录"}</code></div>
              <div className="backup-row-actions"><span>{formatBytes(backup.sizeBytes)}</span><button className="button button--secondary button--compact" type="button" disabled={busy || restoreDisabled} title={restoreDisabled ? "当前 SQLite 路径仅供诊断，不能恢复" : undefined} onClick={() => onRestore(backup)}>恢复</button></div>
            </article>
          ))}
        </div>
      </section>
    </div>
  );
}

function ActivityView({ activity, activeOperation }) {
  return (
    <div className="view-content">
      <section className="page-intro"><div><h1>活动日志</h1><p>当前 Web UI 会话中的状态刷新、同步阶段和操作结果。</p></div>{activeOperation ? <div className="live-operation"><RefreshIcon size={16} className="spin" /> {activeOperation.kind}</div> : null}</section>
      <section className="activity-console">
        <div className="console-head"><span>Activity log</span><span>{activity.length} entries</span></div>
        <div className="console-body">
          {activity.length === 0 ? <div className="console-empty">等待操作…</div> : activity.map((entry) => (
            <div className={`console-row console-row--${entry.level}`} key={entry.id}>
              <time>{formatDate(entry.timestamp)}</time>
              <span className="console-level">{entry.level}</span>
              <span className="console-message">{entry.message}</span>
              {typeof entry.detail === "string" ? <span className="console-detail">{entry.detail}</span> : null}
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}

function HistoryView({ profileId, status }) {
  const [query, setQuery] = useState("");
  const [committedQuery, setCommittedQuery] = useState("");
  const [provider, setProvider] = useState("");
  const [project, setProject] = useState("");
  const [archived, setArchived] = useState("all");
  const [page, setPage] = useState(1);
  const [history, setHistory] = useState({ sessions: [], total: 0, pageSize: 50, hasNextPage: false });
  const [selectedId, setSelectedId] = useState("");
  const [detail, setDetail] = useState(null);
  const [loading, setLoading] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError] = useState("");
  const listRequest = useRef(null);
  const detailRequest = useRef(null);
  if (!listRequest.current) listRequest.current = createLatestRequestGate();
  if (!detailRequest.current) detailRequest.current = createLatestRequestGate();
  const providers = providersFromStatus(status);
  const projects = [...new Set((status?.projectThreadVisibility ?? []).map((item) => item.root))];

  useEffect(() => {
    listRequest.current.cancel();
    detailRequest.current.cancel();
    setQuery("");
    setCommittedQuery("");
    setProvider("");
    setProject("");
    setArchived("all");
    setPage(1);
    setHistory({ sessions: [], total: 0, pageSize: 50, hasNextPage: false });
    setSelectedId("");
    setDetail(null);
    setError("");
  }, [profileId]);

  useEffect(() => {
    return scheduleDebounced(() => setCommittedQuery(query), 300, window);
  }, [query]);

  const loadList = useCallback(async () => {
    const { controller, sequence } = listRequest.current.begin();
    setLoading(true); setError("");
    try {
      const payload = await getHistory({ ...storagePayload(profileId), page, pageSize: 50, query: committedQuery, provider, project, archived }, { signal: controller.signal });
      if (!listRequest.current.isLatest(sequence)) return;
      const nextHistory = { ...payload.history, sessions: dedupeHistorySessions(payload.history?.sessions) };
      setHistory(nextHistory);
      setSelectedId((current) => nextHistory.sessions.some((session) => session.id === current) ? current : nextHistory.sessions[0]?.id ?? "");
    } catch (requestError) {
      if (requestError.name !== "AbortError" && listRequest.current.isLatest(sequence)) setError(requestError.message);
    } finally {
      if (listRequest.current.isLatest(sequence)) setLoading(false);
    }
  }, [profileId, page, committedQuery, provider, project, archived]);

  useEffect(() => {
    loadList();
    return () => listRequest.current.cancel();
  }, [loadList]);
  useEffect(() => {
    detailRequest.current.cancel();
    if (!selectedId) { setDetail(null); setDetailLoading(false); return undefined; }
    const { controller, sequence } = detailRequest.current.begin();
    setDetail(null);
    setDetailLoading(true);
    setError("");
    getHistorySession({ ...storagePayload(profileId), sessionId: selectedId }, { signal: controller.signal })
      .then((payload) => { if (detailRequest.current.isLatest(sequence)) setDetail(payload.history); })
      .catch((requestError) => { if (requestError.name !== "AbortError" && detailRequest.current.isLatest(sequence)) setError(requestError.message); })
      .finally(() => { if (detailRequest.current.isLatest(sequence)) setDetailLoading(false); });
    return () => controller.abort();
  }, [profileId, selectedId]);

  const updateFilter = (setter) => (event) => { setter(event.target.value); setPage(1); };
  return (
    <div className="view-content history-view">
      <section className="page-intro"><div><h1>聊天记录</h1><p>从 rollout 文件读取历史会话，只读查看，不修改本地数据。</p></div><button className="button button--secondary" type="button" onClick={loadList} disabled={loading}><RefreshIcon size={16} className={loading ? "spin" : ""} />刷新</button></section>
      <section className="history-toolbar">
        <input value={query} onChange={updateFilter(setQuery)} onKeyDown={(event) => { if (event.key === "Enter") { setCommittedQuery(query); setPage(1); } }} placeholder="搜索标题、项目、Provider 或消息内容" />
        <select value={provider} onChange={updateFilter(setProvider)}><option value="">全部 Provider</option>{providers.map((item) => <option key={item.id} value={item.id}>{item.id}</option>)}</select>
        <select value={project} onChange={updateFilter(setProject)}><option value="">全部项目</option>{projects.map((item) => <option key={item} value={item}>{item}</option>)}</select>
        <select value={archived} onChange={updateFilter(setArchived)}><option value="all">全部会话</option><option value="active">活跃会话</option><option value="archived">已归档</option></select>
      </section>
      {error ? <div className="history-error"><AlertIcon size={16} />{error}</div> : null}
      <section className="history-layout">
        <div className="history-list-panel">
          <div className="history-list-head"><strong>{history.total} 个会话</strong><span>第 {history.page} 页</span></div>
          <div className="history-list">
            {loading && !history.sessions.length ? <div className="large-empty">读取中…</div> : null}
            {!loading && !history.sessions.length ? <div className="large-empty"><HistoryIcon size={24} /><strong>没有匹配的会话</strong></div> : null}
            {history.sessions.map((session) => <button type="button" key={session.id} className={`history-session-row ${session.id === selectedId ? "history-session-row--selected" : ""}`} onClick={() => setSelectedId(session.id)}><div className="history-session-top"><strong>{session.title}</strong><time>{formatDate(session.updatedAt)}</time></div><p>{session.firstUserMessage || "没有可读的用户消息"}</p><div className="history-session-meta"><span>{session.provider}</span><span>{session.messageCount} 条消息</span><span>{session.archived ? "已归档" : "活跃"}</span></div></button>)}
          </div>
          <div className="history-pagination"><button className="button button--quiet button--compact" type="button" disabled={page <= 1 || loading} onClick={() => setPage((value) => value - 1)}>上一页</button><span>{page}</span><button className="button button--quiet button--compact" type="button" disabled={!history.hasNextPage || loading} onClick={() => setPage((value) => value + 1)}>下一页</button></div>
        </div>
        <div className="history-detail-panel">
          {detailLoading ? <div className="large-empty"><RefreshIcon size={24} className="spin" /><strong>正在读取会话</strong></div> : !detail ? <div className="large-empty"><HistoryIcon size={28} /><strong>选择一个会话</strong><span>聊天内容将在这里显示。</span></div> : <><div className="history-detail-head"><div><h2>{detail.session.title}</h2><p>{detail.session.cwd || "未知项目"} · {detail.session.provider} · {detail.session.messageCount} 条消息</p></div><span>{detail.session.archived ? "已归档" : "活跃"}</span></div>{detail.truncated ? <div className="history-truncated"><AlertIcon size={15} />仅显示最近 {detail.returnedMessageCount} 条消息。</div> : null}<div className="message-stream">{detail.messages.map((message) => <article className={`chat-message chat-message--${message.role}`} key={`${message.sequence}-${message.timestamp}`}><div className="chat-message-label">{message.role === "user" ? "你" : "Codex"}<time>{formatDate(message.timestamp)}</time></div><div className="chat-message-body"><SafeMarkdown text={message.text} /></div></article>)}</div></>}
        </div>
      </section>
    </div>
  );
}

function Modal({ title, children, confirmLabel, onConfirm, onCancel, tone = "primary", confirmDisabled = false }) {
  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={(event) => event.target === event.currentTarget && onCancel()}>
      <section className="modal" role="dialog" aria-modal="true" aria-labelledby="modal-title">
        <div className="modal-head"><h2 id="modal-title">{title}</h2><button type="button" className="icon-button" onClick={onCancel} aria-label="关闭"><XIcon size={18} /></button></div>
        <div className="modal-body">{children}</div>
        <div className="modal-actions"><button className="button button--secondary" type="button" onClick={onCancel}>取消</button><button className={`button button--${tone}`} type="button" disabled={confirmDisabled} onClick={onConfirm}>{confirmLabel}</button></div>
      </section>
    </div>
  );
}

function ExecuteModal({ plan, status, selectedProvider, onCancel, onConfirm }) {
  return (
    <Modal title={plan.mode === "switch" ? "确认切换并同步" : "确认同步元数据"} confirmLabel={plan.mode === "switch" ? "确认切换并同步" : "确认执行同步"} onCancel={onCancel} onConfirm={onConfirm}>
      <div className="modal-callout"><AlertIcon size={18} /><div><strong>请确认 Codex 已完全关闭</strong><span>关闭 Codex CLI、Codex App、app-server 及相关终端，避免 SQLite 或 rollout 被占用。</span></div></div>
      <dl className="operation-scope">
        <div><dt>Codex Home</dt><dd>{status?.codexHome}</dd></div>
        <div><dt>SQLite Home</dt><dd>{status?.sqliteHome} <small>({status?.sqliteHomeSource})</small></dd></div>
        <div><dt>当前 Provider</dt><dd>{status?.currentProvider}</dd></div>
        <div><dt>目标 Provider</dt><dd>{selectedProvider}</dd></div>
        <div><dt>配置变更</dt><dd>{plan.mode === "switch" ? "更新 config.toml 根级 model_provider" : "不修改 config.toml"}</dd></div>
        <div><dt>Model 策略</dt><dd>{plan.mode !== "switch" ? "跟随当前根级 model" : plan.modelMode === "auto" ? "跟随目标 Provider 配置" : plan.modelMode === "keep" ? "保留当前根级 model" : `设置为 ${plan.model}`}</dd></div>
        <div><dt>备份策略</dt><dd>修改前创建备份，保留最近 {plan.keepCount} 份</dd></div>
      </dl>
    </Modal>
  );
}

function RestoreModal({ backup, status, profile, onCancel, onConfirm }) {
  const [restoreConfig, setRestoreConfig] = useState(false);
  const [restoreDatabase, setRestoreDatabase] = useState(true);
  const [restoreSessions, setRestoreSessions] = useState(true);
  const targetSqliteHome = resolveRestoreTargetSqliteHome(status, backup);
  const relocation = restoreRelocationState({
    backup,
    profile,
    targetSqliteHome,
    restoreDatabase,
    restoreConfig,
    sqliteSupported: status?.sqliteAccess?.supported !== false,
    pathComparisonCaseInsensitive: status?.pathComparisonCaseInsensitive === true
  });
  return (
    <Modal
      title="恢复备份"
      confirmLabel="覆盖当前元数据"
      tone="danger"
      onCancel={onCancel}
      confirmDisabled={(!restoreConfig && !restoreDatabase && !restoreSessions) || !relocation.canSubmit}
      onConfirm={() => onConfirm({ restoreConfig, restoreDatabase, restoreSessions, allowSqliteHomeRelocation: relocation.requiresRelocation })}
    >
      <div className="restore-summary"><HistoryIcon size={20} /><div><strong>{formatDate(backup.metadata.createdAt)} · {backup.metadata.targetProvider}</strong><code>{backup.path}</code></div></div>
      <fieldset className="restore-options">
        <legend>选择要覆盖的内容</legend>
        <label><input type="checkbox" checked={restoreConfig} onChange={(event) => setRestoreConfig(event.target.checked)} /><span><strong>config.toml 与 global state</strong><small>恢复 Provider 配置和 Desktop workspace roots</small></span></label>
        <label><input type="checkbox" checked={restoreDatabase} onChange={(event) => setRestoreDatabase(event.target.checked)} /><span><strong>SQLite 线程数据库</strong><small>恢复 state_5.sqlite 及备份中的 WAL/SHM</small></span></label>
        <label><input type="checkbox" checked={restoreSessions} onChange={(event) => setRestoreSessions(event.target.checked)} /><span><strong>Rollout 元数据</strong><small>恢复 session_meta 和被修改的 turn_context.model</small></span></label>
      </fieldset>
      {status?.sqliteAccess?.supported === false ? <div className="modal-callout modal-callout--danger"><AlertIcon size={18} /><div><strong>当前 SQLite 路径仅供诊断</strong><span>{status.sqliteAccess.message || "不能从 Web UI 执行恢复。"}</span></div></div> : null}
      {relocation.requiresRelocation ? <div className={`modal-callout ${relocation.missingExplicitTarget || relocation.configRestoreConflict ? "modal-callout--danger" : "modal-callout--warning"}`}><AlertIcon size={18} /><div><strong>SQLite Home 与备份来源不同</strong><span>来源：{backup.metadata.sqliteHome}<br />目标：{targetSqliteHome}<br />{relocation.missingExplicitTarget ? "当前 Profile 未明确配置 SQLite Home，不能提交数据库迁移恢复。" : relocation.configRestoreConflict ? "迁移数据库时不能同时恢复旧 config.toml。" : "确认后数据库将恢复到当前 Profile 明确配置的目标位置。"}</span></div></div> : null}
      <div className="modal-callout"><AlertIcon size={18} /><div><strong>恢复前请关闭 Codex</strong><span>该操作将覆盖所选的当前元数据；请确认 Codex CLI、App 和 app-server 已关闭。</span></div></div>
    </Modal>
  );
}

function PruneModal({ keepCount, backups, onCancel, onConfirm }) {
  const deleteCount = Math.max(0, backups.backups.length - keepCount);
  return (
    <Modal title="清理旧备份" confirmLabel={`删除 ${deleteCount} 份旧备份`} tone="danger" onCancel={onCancel} onConfirm={onConfirm} confirmDisabled={deleteCount === 0}>
      <div className="modal-callout modal-callout--warning"><AlertIcon size={18} /><div><strong>被删除的备份无法直接恢复</strong><span>只处理当前 Codex Home 下由本工具管理的备份目录。</span></div></div>
      <dl className="operation-scope"><div><dt>当前备份</dt><dd>{backups.backups.length} 份</dd></div><div><dt>保留</dt><dd>最近 {keepCount} 份</dd></div><div><dt>将删除</dt><dd>{deleteCount} 份</dd></div></dl>
    </Modal>
  );
}

function ProfileModal({ onCancel, onConfirm }) {
  const [profileId, setProfileId] = useState("");
  const [name, setName] = useState("");
  const [codexHome, setCodexHome] = useState("");
  const [sqliteHome, setSqliteHome] = useState("");
  const valid = /^[A-Za-z0-9_.-]{1,80}$/.test(profileId) && name.trim() && codexHome.trim();
  return (
    <Modal title="新增存储配置" confirmLabel="保存配置" onCancel={onCancel} confirmDisabled={!valid} onConfirm={() => onConfirm({ profileId, name, codexHome, sqliteHome })}>
      <div className="profile-form">
        <label><span>配置 ID</span><input value={profileId} onChange={(event) => setProfileId(event.target.value)} placeholder="work" spellCheck="false" /></label>
        <label><span>显示名称</span><input value={name} onChange={(event) => setName(event.target.value)} placeholder="工作环境" /></label>
        <label><span>Codex Home</span><input value={codexHome} onChange={(event) => setCodexHome(event.target.value)} placeholder="/home/user/.codex" spellCheck="false" /></label>
        <label><span>SQLite Home（可选）</span><input value={sqliteHome} onChange={(event) => setSqliteHome(event.target.value)} placeholder="留空时由服务端按配置解析" spellCheck="false" /></label>
      </div>
    </Modal>
  );
}

function Toast({ toast, onClose }) {
  useEffect(() => {
    if (!toast) return undefined;
    const timer = window.setTimeout(onClose, 6000);
    return () => window.clearTimeout(timer);
  }, [toast, onClose]);
  if (!toast) return null;
  return <div className={`toast toast--${toast.tone}`}><div>{toast.tone === "success" ? <CheckIcon size={18} /> : <AlertIcon size={18} />}<span><strong>{toast.title}</strong>{toast.message ? <small>{toast.message}</small> : null}</span></div><button type="button" onClick={onClose} aria-label="关闭通知"><XIcon size={16} /></button></div>;
}

export default function App() {
  const [view, setView] = useState("overview");
  const [accessState, setAccessState] = useState("checking");
  const [accessMessage, setAccessMessage] = useState("");
  const [profileId, setProfileId] = usePersistentState("cps.web.profileId", "default");
  const [profiles, setProfiles] = useState([]);
  const [manualProviders, setManualProviders] = usePersistentState("cps.web.manualProviders", []);
  const [status, setStatus] = useState(null);
  const [backups, setBackups] = useState(EMPTY_BACKUPS);
  const [selectedProvider, setSelectedProvider] = useState("");
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [modal, setModal] = useState(null);
  const [toast, setToast] = useState(null);
  const [activity, setActivity] = useState([]);
  const [activeOperation, setActiveOperation] = useState(null);
  const lastActivityId = useRef(0);

  const providers = useMemo(() => {
    const detected = providersFromStatus(status);
    const detectedIds = new Set(detected.map((provider) => provider.id));
    return [
      ...detected.map((provider) => ({
        ...provider,
        manual: manualProviders.includes(provider.id)
      })),
      ...manualProviders
        .filter((provider) => !detectedIds.has(provider))
        .map((id) => ({ id, sources: ["manual"], configured: false, current: false, manual: true }))
    ];
  }, [status, manualProviders]);
  const selectedProfile = profiles.find((profile) => profile.id === profileId) ?? null;

  const profileRefreshRef = useRef(null);
  if (!profileRefreshRef.current) {
    profileRefreshRef.current = createProfileRefresh({
      fetchStatus: (storage, options) => apiRequest("/api/status", storage, options),
      fetchBackups: (storage, options) => apiRequest("/api/backups", storage, options)
    });
  }

  // Only the latest request for the current profile may update status,
  // backups, selectedProvider, loading, and error toasts. Switching profiles
  // starts a newer request, which aborts and invalidates the older one even
  // if the older one finishes last.
  const refresh = useCallback(async ({ quiet = false } = {}) => {
    await profileRefreshRef.current({
      profileId,
      showLoading: !quiet,
      onLoading: setLoading,
      onResult: ({ status: nextStatus, backups: nextBackups }) => {
        setStatus(nextStatus);
        setBackups(nextBackups);
        setSelectedProvider((current) => current && providersFromStatus(nextStatus).some((provider) => provider.id === current)
          ? current
          : nextStatus.currentProvider);
      },
      onError: (error) => {
        setToast({ tone: "error", title: "状态读取失败", message: error.message });
      }
    });
  }, [profileId]);

  const refreshProfiles = useCallback(async () => {
    const payload = await getProfiles();
    setProfiles(payload.profiles);
    setProfileId((current) => payload.profiles.some((profile) => profile.id === current) ? current : "default");
  }, [setProfileId]);

  const handleProfileConflict = useCallback(async () => {
    setModal(null);
    try {
      await refreshProfiles();
      await refresh({ quiet: true });
    } catch {
      // The user can still retry after the next explicit refresh.
    }
    setToast({ tone: "warning", title: "配置已变更，请重新确认", message: "已刷新存储配置和当前状态；未自动重试原操作。" });
  }, [refresh, refreshProfiles]);

  const openProfileOperation = useCallback((operation) => {
    const captured = captureProfileOperation(selectedProfile, operation, status);
    if (!captured) {
      setToast({ tone: "warning", title: "配置需要刷新", message: "没有可用的配置版本，请刷新后重新确认操作。" });
      refreshProfiles().catch(() => {});
      return;
    }
    setModal(captured);
  }, [refreshProfiles, selectedProfile, status]);

  useEffect(() => {
    let cancelled = false;
    initializeAccess()
      .then(async (paired) => {
        if (!paired) throw new PairingRequiredError();
        await refreshProfiles();
        if (!cancelled) setAccessState("ready");
      })
      .catch((error) => {
        if (cancelled) return;
        setAccessState(error instanceof PairingRequiredError ? "required" : "error");
        setAccessMessage(error.message);
      });
    return () => { cancelled = true; };
  }, [refreshProfiles]);

  useEffect(() => {
    if (accessState === "ready") refresh();
  }, [accessState, profileId]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    const requirePairing = (event) => {
      setAccessState("required");
      setAccessMessage(event.detail || "设备凭证已失效。请重新运行 codex-provider web。");
    };
    window.addEventListener("cps:pairing-required", requirePairing);
    return () => window.removeEventListener("cps:pairing-required", requirePairing);
  }, []);

  useEffect(() => {
    if (accessState !== "ready") return undefined;
    let stopped = false;
    const poll = async () => {
      try {
        const payload = await getActivity(lastActivityId.current);
        if (stopped) return;
        if (payload.activity?.length) {
          lastActivityId.current = payload.activity[payload.activity.length - 1].id;
          setActivity((current) => [...current, ...payload.activity].slice(-250));
        }
        setActiveOperation(payload.activeOperation ?? null);
      } catch {
        // A transient poll failure should not interrupt a running operation.
      }
    };
    poll();
    const timer = window.setInterval(poll, 900);
    return () => { stopped = true; window.clearInterval(timer); };
  }, [accessState]);

  const execute = useCallback(async () => {
    const plan = modal.plan;
    const targetProfileId = modal.profileId;
    setModal(null);
    setBusy(true);
    setView("activity");
    try {
      const common = { ...storagePayload(targetProfileId), profileRevision: modal.profileRevision, storageRevision: modal.storageRevision, provider: modal.selectedProvider, keepCount: plan.keepCount };
      const payload = plan.mode === "switch"
        ? await apiRequest("/api/switch", { ...common, model: plan.modelMode === "custom" ? plan.model : undefined, keepRootModel: plan.modelMode === "keep" })
        : await apiRequest("/api/sync", common);
      setToast(operationToast(payload, {
        successTitle: plan.mode === "switch" ? "切换并同步完成" : "同步完成",
        partialTitle: plan.mode === "switch" ? "切换并同步部分完成" : "同步部分完成",
        message: `备份：${payload.result?.backupDir ?? "已创建"}`
      }));
      await refresh({ quiet: true });
    } catch (error) {
      if (error instanceof ProfileRevisionError) {
        await handleProfileConflict();
        return;
      }
      setToast({ tone: "error", title: "操作失败", message: error.message });
    } finally {
      setBusy(false);
    }
  }, [handleProfileConflict, modal, refresh]);

  const restore = useCallback(async (options) => {
    const backup = modal.backup;
    const targetProfileId = modal.profileId;
    setModal(null);
    setBusy(true);
    setView("activity");
    try {
      const payload = await apiRequest("/api/restore", { ...storagePayload(targetProfileId), profileRevision: modal.profileRevision, storageRevision: modal.storageRevision, backupId: backup.id, ...options });
      setToast(operationToast(payload, { successTitle: "备份恢复完成", partialTitle: "备份恢复部分完成", message: backup.id }));
      await refresh({ quiet: true });
    } catch (error) {
      if (error instanceof ProfileRevisionError) {
        await handleProfileConflict();
        return;
      }
      setToast({ tone: "error", title: "恢复失败", message: error.message });
    } finally {
      setBusy(false);
    }
  }, [handleProfileConflict, modal, refresh]);

  const prune = useCallback(async () => {
    const keepCount = modal.keepCount;
    const targetProfileId = modal.profileId;
    setModal(null);
    setBusy(true);
    try {
      const payload = await apiRequest("/api/prune", { ...storagePayload(targetProfileId), profileRevision: modal.profileRevision, storageRevision: modal.storageRevision, keepCount });
      setToast(operationToast(payload, {
        successTitle: "旧备份清理完成",
        partialTitle: "旧备份清理部分完成",
        message: `删除 ${payload.result?.deletedCount ?? 0} 份，释放 ${formatBytes(payload.result?.freedBytes)}`
      }));
      await refresh({ quiet: true });
    } catch (error) {
      if (error instanceof ProfileRevisionError) {
        await handleProfileConflict();
        return;
      }
      setToast({ tone: "error", title: "备份清理失败", message: error.message });
    } finally {
      setBusy(false);
    }
  }, [handleProfileConflict, modal, refresh]);

  const closeToast = useCallback(() => setToast(null), []);
  const saveProfile = useCallback(async (profile) => {
    try {
      const payload = await apiRequest("/api/profiles/save", profile.revision ? { ...profile, profileRevision: profile.revision } : profile);
      setModal(null);
      await refreshProfiles();
      setProfileId(payload.profile.id);
      setToast({ tone: "success", title: "存储配置已保存", message: payload.profile.name });
    } catch (error) {
      if (error instanceof ProfileRevisionError) {
        await handleProfileConflict();
        return;
      }
      setToast({ tone: "error", title: "配置保存失败", message: error.message });
    }
  }, [handleProfileConflict, refreshProfiles, setProfileId]);
  const deleteProfile = useCallback(async () => {
    try {
      await apiRequest("/api/profiles/delete", { profileId, profileRevision: selectedProfile?.revision });
      setProfileId("default");
      await refreshProfiles();
    } catch (error) {
      if (error instanceof ProfileRevisionError) {
        await handleProfileConflict();
        return;
      }
      setToast({ tone: "error", title: "配置删除失败", message: error.message });
    }
  }, [handleProfileConflict, profileId, refreshProfiles, selectedProfile?.revision, setProfileId]);
  const forgetBrowser = useCallback(async () => {
    await forgetThisBrowser().catch(() => {});
    setAccessState("required");
    setAccessMessage("此浏览器的设备凭证已失效。重新运行 codex-provider web 即可自动配对。");
  }, []);
  const addManualProvider = useCallback((provider) => {
    setManualProviders((current) => [...new Set([...current, provider])].sort());
    setSelectedProvider(provider);
  }, [setManualProviders]);
  const removeManualProvider = useCallback((provider) => {
    setManualProviders((current) => current.filter((item) => item !== provider));
    setSelectedProvider(status?.currentProvider ?? "");
  }, [setManualProviders, status?.currentProvider]);

  if (accessState !== "ready") {
    return <div className="access-gate"><ShieldIcon size={32} /><h1>{accessState === "checking" ? "正在完成安全配对" : "需要重新配对"}</h1><p>{accessState === "checking" ? "请稍候…" : accessMessage || "请重新运行 codex-provider web。"}</p>{accessState !== "checking" ? <code>codex-provider web</code> : null}</div>;
  }

  return (
    <div className="app-shell">
      <AppHeader status={status} busy={busy || Boolean(activeOperation)} onRefresh={() => refresh()} />
      <Sidebar view={view} setView={setView} status={status} onForgetBrowser={forgetBrowser} />
      <main className="main-area">
      <StorageBar profiles={profiles} profileId={profileId} setProfileId={setProfileId} status={status} onAddProfile={() => setModal({ type: "profile" })} onDeleteProfile={deleteProfile} onRefresh={() => refresh()} loading={loading} profileSwitchDisabled={busy || Boolean(modal)} />
        {view === "overview" ? <Overview status={status} backups={backups} providers={providers} selectedProvider={selectedProvider} setSelectedProvider={setSelectedProvider} onAddManualProvider={addManualProvider} onRemoveManualProvider={removeManualProvider} onExecute={(plan) => openProfileOperation({ type: "execute", plan, selectedProvider })} onRestore={(backup) => openProfileOperation({ type: "restore", backup })} setView={setView} busy={busy} loading={loading} /> : null}
        {view === "history" ? <HistoryView profileId={profileId} status={status} /> : null}
        {view === "backups" ? <BackupsView backups={backups} status={status} busy={busy} onRestore={(backup) => openProfileOperation({ type: "restore", backup })} onPrune={(keepCount) => openProfileOperation({ type: "prune", keepCount })} /> : null}
        {view === "activity" ? <ActivityView activity={activity} activeOperation={activeOperation} /> : null}
      </main>
      {modal?.type === "execute" ? <ExecuteModal plan={modal.plan} status={modal.status} selectedProvider={modal.selectedProvider} onCancel={() => setModal(null)} onConfirm={execute} /> : null}
      {modal?.type === "restore" ? <RestoreModal backup={modal.backup} status={modal.status} profile={modal.profile} onCancel={() => setModal(null)} onConfirm={restore} /> : null}
      {modal?.type === "prune" ? <PruneModal keepCount={modal.keepCount} backups={backups} onCancel={() => setModal(null)} onConfirm={prune} /> : null}
      {modal?.type === "profile" ? <ProfileModal onCancel={() => setModal(null)} onConfirm={saveProfile} /> : null}
      <Toast toast={toast} onClose={closeToast} />
    </div>
  );
}
