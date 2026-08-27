# vNext Electron 桌面端候选说明

> **发行状态：未发布候选。**当前公开发布的桌面入口仍是 Windows .NET GUI。本页用于代码审查和内部验收，不提供 Stable 下载，不授权 tag、npm/GitHub Release、签名、公证或更新通道发布。

vNext 桌面端位于 `apps/desktop`，在 Windows x64、macOS x64/arm64 和 Linux x64 上共享 React 界面与 Node Core。达到 Phase 6 退出门槛后，Electron 才会成为主桌面端，保留的 .NET GUI 才会标记为 Legacy fallback；当前不能提前这样宣称。

## 能力

- Overview：Provider/model 分布、SQLite/Codex Home 来源、备份、pending、operation 与 locked rollout 状态。
- Sync、Switch Provider：先 Prepare 展示计划，再用同一 `planId` 确认 Apply；Switch 支持 Provider 默认模型、保留根模型和显式模型。
- Backups/Restore：只接受受管 `backupId`；Restore 在任何目标写入前创建恢复前快照，并按耐久 journal 完成确认、补偿或进入 recovery required。
- History：仅在用户明确打开后加载列表，详情延迟读取；消息正文不进入日志、缓存、诊断包或全量导出。
- Profiles：当前桌面 Host 只公开受管 Profile 标识和 revision，不让 Renderer 提交任意路径。
- Diagnostics、Settings：Main 选择诊断目标并签发可信 token；主题支持 system/light/dark，语言支持 `zh-CN` 和 `en`。
- Watch 与 Update：Watch 每次 Apply 都重新获取双层锁并让位于人工操作；更新只由 Main 管理，写操作、Watch 或未解决 journal 存在时禁止安装。

## 安全数据流

```text
Electron Renderer
  → DesktopCoreClient
  → Preload 窄 IPC
  → Main（窗口、生命周期、选择器、更新、监管）
  → Utility Process
  → Node Core 公共 API
  → Codex 原始存储
```

`BrowserWindow` 固定启用 context isolation、sandbox 和 web security，并关闭 Node integration；本地协议使用严格 CSP。Renderer 不能访问 Node、文件系统、任意 IPC channel 或任意路径。Utility Process 在任何业务调用前完成 app/core/protocol handshake；崩溃会拒绝 pending 请求，并在检查未完成 journal 后最多自动重启一次。

写操作保持 backup-first、Codex Home → State DB 固定顺序双层锁、事务 journal、回滚/恢复和 WSL UNC 仅诊断边界。应用不读取或输出认证数据，也不修改消息正文、会话标题或 `updated_at`。

## 内部验收

现代 workspace 和 Electron 构建使用 Node 24。自动化测试只使用临时目录和脱敏 fixture；不要拿真实用户 Codex Home 做开发测试。

```powershell
npm ci
npm run desktop:test
$env:CPS_DESKTOP_WINDOW_DISPLAY = "hidden"
npm run desktop:test:e2e
```

隐藏策略不会显示或占用主屏窗口。需要人工可视验收时，必须显式改用受控测试环境；自动化仍保持 hidden。

C9 候选矩阵固定为 Windows x64 NSIS/portable ZIP、macOS x64/arm64 DMG/ZIP、Linux x64 AppImage/deb。每个平台必须在原生 runner 上完成打包、ASAR/native SQLite 审计、最终容器 Status、Sync→Restore、正常退出以及 checksum/SBOM 验证，再由四目标 aggregate 和 C10 脱敏 evidence bundle 收口。

## 发布与交接边界

- `1.0.0-alpha|beta|rc.<run>` 只用于 CI 候选，构建始终 `--publish never`。
- required CI、四目标 aggregate 和最终 C10 证据未全绿前，不写 source `1.0.0`，不把 Electron 称为 Beta、Stable、默认入口或已发布产品。
- 当前候选 unsigned、not notarized，未配置正式 Release 更新通道。
- 公开发布、签名、公证、更新 metadata 和跨版本升级验证都需要另行授权。
- .NET 删除不属于本 PR；即使 Electron 正式接管，也至少保留到稳定版发布后的两个维护周期，再单独立项。

实现与阶段状态以 [Accepted 架构基线](VNEXT_ELECTRON_NODE_ARCHITECTURE_ZH.md) 和 [vNext 迁移执行索引](migration/VNEXT_MIGRATION_EXECUTION_INDEX_ZH.md) 为准。
