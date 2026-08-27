# C8 Restore / Watch / Diagnostics / Update 证据（2026-08-27）

状态：候选实现的本地 Windows x64 门禁通过；C7 及其前置 C5/C6 的 required CI、真实 WSL UNC、远端 Windows/macOS/Linux C8 CI 与最终 PR 合入均未闭合。因此 C8/Phase 5 仍为 Pending，不构成发布、稳定版或 .NET 替代声明。输入 checkpoint 为 `1ec27a5`；C8 输出 commit SHA 在本 checkpoint 提交后及 C10 evidence bundle 中索引。

## 已实现边界

- DesktopCoreClient、Preload、Main IPC、Supervisor 与 Utility 只增加 `prepareRestore/applyRestore`、`pruneBackups`、`startWatch/stopWatch/getWatchStatus` 和固定 Diagnostics/Update bridge。Main 持有 Restore Plan、Watch ID、诊断目标 capability 与更新 controller；Renderer 只能提交受管 `backupId`、受限 Restore options、`keepCount`、有限 Watch 输入或无参数 Update 请求。
- `RECOVERY_REQUIRED` 继续阻断 Sync、Switch 与 `startWatch`；Restore、Prune 是 recovery-safe 操作，`stopWatch/getWatchStatus` 保持可用。Restore Apply 完成后使 Status preflight 失效并强制重新读取。
- Restore v2 在任何目标 mutation 前创建受管恢复前 snapshot 和独立 journal，覆盖 `prepared`、`applying`、`committing`、`committed-pending-ack`、`completed`、`rollback-pending`、`rolled-back`、`recovery-required`。`committed-pending-ack` 只能按完整目标 Hash 向前确认，不得补偿已提交目标。
- snapshot manifest 与 durable `prepared` event 全量绑定 schema/protocol、operation、source backup、storage、required target kinds、resolver IDs、ordered targets 和 snapshot 稳定物理目录；即使 manifest hash 被同步重算，任一业务字段不一致也在 compensation/ack 前 fail closed。
- journal 持久化 `codexHomePhysical`。pending、completed resolver 与当前已加锁 Home 必须匹配同一稳定物理 identity；junction/reparse 换接后不能用可变 lexical Home 的当前 realpath 隐藏历史 pending。
- Watch 每次 apply 都重新 Prepare/Apply 并取得 Home→State DB 双锁；人工 Plan 优先，重复事件合并为一次 follow-up，首次遇到 `RECOVERY_REQUIRED/PENDING_TRANSACTION` 即停止自动写。弃置 Plan 与人工 intent 由不阻止进程退出的单一最早到期 timer 自治清理。
- Diagnostics 的目标文件只由 Main 原生选择器产生，并转换为 5 分钟、单次消费 capability。ZIP 仅含固定、二次 schema 校验的脱敏条目，排除 `auth.json`、凭据、token、路径、rollout/SQLite、消息正文和 `encrypted_content`。
- Update 仅由 Main 的受控 `electron-updater` controller 管理，禁止 `setFeedURL`，关闭自动下载与退出时安装。安装前 Supervisor 同步关闭 restart gate，计入并排空已经 admission 但尚未 dispatch 的写，拒绝后续写，再强制刷新全部 Profile、复核 active Watch/write/pending recovery；installer 异常或任一状态无法验证时不退出并重新开放 gate。
- 当前 C8 只提供受控状态机和门禁。Desktop 版本为 `0.0.0`、非 packaged 或目标不受支持时 Update 为 disabled，不进行网络检查；真实版本注入、签名、更新 metadata、下载和跨版本 packaged smoke 属于 C9/C10。

## Restore v2 与跨运行时证据

- Node Restore 状态机覆盖 prepared/applying/committing/rollback-pending 真实进程终止、snapshot 失败、observer 异常、中途失败补偿、forward-only commit acknowledgement、unknown schema、truncated journal、SQLite online snapshot、State DB physical identity 复核和 reparse swap。
- Node 与仍受支持的 .NET Core 对同一 v2 journal、source backup、snapshot manifest、terminal 与 resolver projection 采用相同 fail-closed 语义。跨运行时 harness 覆盖双向 Backup/Restore、legacy foreign pending、Restore v2 crash matrix、foreign pending、unknown schema、forward-only ack、manifest/prepared mismatch 和 persisted physical Home mismatch。
- foreign pending、绑定不完整或未知 evidence 均在新 snapshot/journal/mutation 前阻断。completed resolver 不改写旧 raw journal；Prune 继续保护旧 journal、source backup 与 pre-restore snapshot。
- 独立 Restore journal 的首个格式就是 v2；历史 protocol v1 `transaction-journal.jsonl` 是 Sync/Switch transaction journal，不被伪装为 standalone Restore v1。

## 本地验证

环境：Windows 11 Pro x64 `10.0.26200`，Node `v24.11.1`，npm `11.10.0`，.NET SDK `10.0.400`；输入 SHA `1ec27a5`。所有 Electron E2E 使用 `CPS_DESKTOP_WINDOW_DISPLAY=hidden` 后台运行，没有显示或占用主屏窗口。所有 Core/Restore/Watch/Diagnostics 用例只使用临时 Fixture，没有读取真实用户 Codex Home、认证数据或消息正文。

| 命令 | 结果 |
| --- | --- |
| `npm run desktop:test` | Desktop security/IPC/profile/runtime/diagnostics/updater contracts：56 passed，0 failed/skipped |
| `npm run desktop:test:e2e` | hidden test-build Electron E2E：15 passed，0 failed，1 skipped；Restore/Prune/Watch/Diagnostics/Update surface、Sync/Switch、Busy/Partial/Cancel 与六窗口 crash matrix 通过；唯一 Skip 为损坏的本机 WSL |
| `npm run desktop:build` + `npm run desktop:verify-production-bundle` + `npm run desktop:test:e2e:production` | production 构建/边界通过，hidden E2E 2/2 passed；真实 SQLite Status 与 Sync，无 test bridge/fault gate |
| `npm run desktop:pack:dir` + `npm run desktop:test:e2e:packaged` | 最新 Windows x64 unpacked app 重新构建；hidden 可执行文件 smoke 2/2 passed。该目录包不是 C9 发行产物 |
| `node --test test/restore-v2-state-machine.test.js` | 22/22 passed，含 manifest/prepared 与 persisted physical Home 回归 |
| `.NET RestoreJournalServiceTests + RestoreV2IntegrationTests` | 12/12 passed |
| `npm run fixtures:cross-runtime` | Node↔.NET 9/9 passed，含新增 manifest/prepared 和 persisted physical Home 双向拒绝 |
| `npm run workspaces:check` | 9 个 workspace 共 97 passed，0 failed/skipped；TypeScript/checkJs、依赖、导入与 root publish 边界通过 |
| `npm test` | 408 passed，0 failed/skipped |
| `npm run web:build` + `npm run web:test:e2e` | Web production build 成功，2034 modules；Chromium 2/2 passed，共享 UI 未回归 |
| `dotnet test CodexProviderSync.sln --configuration Release --no-restore` | Legacy .NET：411 passed，0 failed，1 skipped；Skip 同为不可运行的本机 WSL 实机场景 |
| `npm run package:smoke` + `npm run package:smoke:lifecycle` | Node 24 根 tarball 内容、安装生命周期、CLI/SQLite smoke 通过；Node 16.20.2 安装态由 required CI 继续验证 |
| `npm audit --omit=dev --audit-level=moderate` / `npm audit --audit-level=high` | 均为 0 vulnerabilities |
| `js-yaml` parse workflows / builder YAML | `ci.yml`、`publish.yml`、`publish-npm.yml` 与 `electron-builder.yml` 均解析成功 |
| `git diff --check` | 通过；仅 Git 的既有 CRLF 转换提示 |

独立安全复核在新增三项补丁后重读 Node/.NET Restore binding 与 Electron restart admission gate，未发现新增或遗留的 P0/P1/P2。

## 未闭合项与后续 TODO

- `wsl.exe --list --quiet` 可见 Ubuntu，但启动返回 `Wsl/Service/CreateInstance/MountDisk/HCS/ERROR_FILE_NOT_FOUND`，发行版 `ext4.vhdx` 缺失。真实 WSL UNC 场景是明确原因的 expected skip，不是通过；修复后或远端 Windows runner 必须重跑全部受保护 Hash 不变验证。
- 本地 Windows 不能替代远端 Windows/macOS/Linux required CI；macOS/Linux Runtime、锁语义和 unpacked app 证据仍为 Pending。C5/C6/C7 前置 checkpoint 也尚未因 required CI/最终合入而闭合，因此 C8/Phase 5 不标记 In Progress 或 Completed。
- C9 仍负责 Windows x64 NSIS/portable ZIP、macOS x64/arm64 DMG/ZIP、Linux x64 AppImage/deb、Electron ABI native SQLite fallback、`asarUnpack`、SBOM/checksum、最终包扫描和各平台安装/解包 smoke。当前 `--dir` 不能替代这些证据。
- 当前 Update controller 单测和门禁通过不等于更新通道已上线。真实 metadata、签名、公证、下载、重启升级和发布授权仍未发生。
- 本 checkpoint 未创建 tag，未发布 npm/GitHub Release，未签名、公证或写更新通道；.NET 保持可构建且未删除，不能据跨运行时 9/9 宣称 Legacy 已可移除。
