# C7 Electron Sync/Switch 证据（2026-08-26）

> 历史 checkpoint 快照：本文记录 ADR-0016 之前 Electron Sync/Switch 的双锁、普通 journal 与自动补偿行为。当前候选以 ADR-0016 和最新 C2/C3 轻量化证据为准。

状态：候选实现的本地 Windows x64 门禁通过；真实 WSL UNC、C5/C6 required CI、远端 Windows/macOS/Linux C7 CI 与最终 PR 合入未闭合，因此 C7/Phase 4 仍为 Pending。输入 checkpoint 为 `6820858`；C7 输出 commit SHA 在本 checkpoint 提交后及 C10 evidence bundle 中索引。

## 已实现边界

- Electron 的写能力只开放 `prepareSync/applySync/prepareSwitch/applySwitch`。Restore、Prune、Watch、Diagnostics 落盘和 Update 仍不在 Desktop allowlist；Renderer 只能提交 profile、Provider 和三种 model intent，Apply 只能提交 `{schemaVersion:1, planId}`。
- Switch 明确实现 `provider-default`、`keep-root-model`、`explicit` 三种模式；未在 `config.toml` 声明的自定义 Provider 在 Prepare 阶段返回 `INVALID_INPUT`，不创建 Plan、Backup 或业务 mutation。
- Main 为每个 Plan 绑定 sender、Apply 方法、可信 profile/revision、Utility generation、TTL 和单次消费状态；篡改、跨方法、过期、重放或 generation 漂移均返回 `PLAN_EXPIRED`。未消费 Plan 会按 TTL/generation 清理，并以 256 条上限防止 Renderer 累积内存。
- Utility runtime protocol v2 使用 Main 生成的 `dispatchId`，并同时核对 generation、requestId 与 operationId。Progress/operation-started envelope 严格、无路径；重复 requestId、未知 event、迟到 response 或 forged early cancel 都 fail closed。
- Cancel 只经 sender-bound 的窄 IPC 进入对应 AbortController。备份前取消返回 `OPERATION_CANCELLED`；观察到 mutation 后取消必须完成补偿并保留 `SYNC_FAILED_ROLLED_BACK`/`RECOVERY_REQUIRED`，不能用“已取消”掩盖事务结果。
- 写请求 timeout 被归类为 Runtime crash，不伪装为取消；Utility crash 拒绝全部 pending。下一次请求只重启一次并先检查 pending journal，非 terminal journal 继续阻断写入。
- `StatusSnapshot` 在 Utility 崩溃持锁且没有 last-complete cache 时仍返回有效 fail-closed DTO：`sqliteHomeSource="unknown"` 是“尚未可靠解析来源”的 sentinel，必须与 `rolloutScanComplete:false`、`LOCK_UNVERIFIABLE` 和 `alignment.aligned=false` 一起消费，不能解释为健康存储。

## Backup、Restore 与故障矩阵

- Sync/Switch 保持 SQLite 可写预检、Home→State DB 双层锁、Backup-first、journal、rollback 和 locked rollout partial 语义。真实 SQLite writer 在 Backup 前返回 `SQLITE_BUSY`；Windows `FileShare.None` 锁定 rollout 不被覆盖，其他安全目标提交并返回 partial。
- Electron 首次 Sync 生成的受管 Backup 通过 `@codex-provider-sync/core` 公共 facade 执行 Prepare/Apply Restore。Config、global state 与 rollout 按字节恢复；SQLite online backup/restore 按完整 schema、rows 与 `user_version` 比较语义一致，不把 SQLite 物理页布局误写成跨实现逐字节合同。
- provider-only Backup 也保存每个可解析 `turn_context` 的 line index/model 元数据，不保存消息正文；在之后发生 model switch 时，恢复旧 Backup 仍能回到一致的原 provider/model 字节状态。
- Restore 在首行 Provider 已恢复、预期 `turn_context` 又发生并发变化时返回 `ROLLOUT_CHANGED` 路径的失败，不再静默报告 completed。Apply 在 rollout mutation 后收到 Abort 也会完整回滚并保留原事务错误语义。
- test-only fault gate 只进入 `electron-vite --mode test` 的 Utility chunk；正式 Core `.d.ts`/facade/Runtime host control 不公开或转发故障注入。production verifier 拒绝 test bridge、E2E gate、fault marker 与 crash channel；打包命令自身强制重新生成并验证 production bundle，不能把残留 test `out/` 打包。
- crash matrix 覆盖 `before_backup`、config mutation、rollout mutation、SQLite commit/ack、transaction journal commit/ack 和 transaction commit 六个窗口；每个场景验证 generation 仅增加一次、journal durable state、pending write 阻断和目标 Hash。

## 本地验证

环境：Windows 11 x64，Node `v24.11.1`，npm `11.10.0`；输入 SHA `6820858`。所有 Electron E2E 使用 `CPS_DESKTOP_WINDOW_DISPLAY=hidden` 后台运行，不显示或占用主屏窗口。

| 命令 | 结果 |
| --- | --- |
| `npm run desktop:test` | Desktop security/IPC/profile/runtime/protocol contracts：35 passed，0 failed/skipped |
| `npm run desktop:test:e2e` | test-build 真实 UI/Core：15 passed，0 failed，1 skipped；三种 model intent、stale/unknown provider/tampered/replay、SQLite Busy、真实 rollout lock、两类 Cancel、六窗口 crash matrix 均通过；唯一 Skip 为损坏的本机 WSL |
| `npm run desktop:build` + `npm run desktop:verify-production-bundle` + `npm run desktop:test:e2e:production` | production 构建/边界通过，2/2 passed；真实 SQLite Status 与 Sync，无 test bridge/fault gate |
| `npm run desktop:pack:dir` + `npm run desktop:test:e2e:packaged` | 命令先覆盖 test residue、验证 production bundle，再生成 Windows x64 unpacked app；真实可执行文件 2/2 passed |
| `npm run workspaces:check` | 9 个 workspace 共 75 passed，0 failed/skipped；TypeScript/checkJs、依赖、导入与 root publish 边界通过 |
| `npm test` | 357 passed，0 failed/skipped |
| `npm run web:build` + `npm run web:test:e2e` | Web production build 成功，2034 modules；Chromium 2/2 passed，共享 UI 的 History lazy-load、opaque Apply 与全局状态未回归 |
| `npm run fixtures:cross-runtime` | Node↔.NET 双向 Backup/Restore 与 foreign pending：2/2 passed |
| `dotnet test CodexProviderSync.sln --configuration Release --no-restore` | Legacy .NET：399 passed，0 failed，1 skipped；Skip 同为不可运行的本机 WSL 实机场景 |
| `npm run package:smoke` + `npm run package:smoke:lifecycle` | Node 24 根 tarball 内容、安装生命周期、CLI/SQLite smoke 通过；Node 16.20.2 安装态由 required CI 继续验证 |
| `npm audit --omit=dev --audit-level=moderate` / `npm audit --audit-level=high` | 均为 0 vulnerabilities |
| `js-yaml` parse CI / builder YAML | `.github/workflows/ci.yml` 与 `apps/desktop/electron-builder.yml` 均解析成功 |
| `git diff --check` | 通过；仅 Git 的既有 CRLF 转换提示 |

## 未闭合项与后续 TODO

- `wsl.exe --list --quiet` 可见 Ubuntu，但启动返回 `Wsl/Service/CreateInstance/MountDisk/HCS/ERROR_FILE_NOT_FOUND`，其发行版 `ext4.vhdx` 缺失。根据 Fixture 合同，本机 C7 E2E 明确 Skip，不用伪造 UNC 路径代替真实 WSL；修复 WSL 后或远端 Windows runner 必须重跑并验证所有受保护 Hash 不变。
- 本地未替代远端 Windows/macOS/Linux required CI；macOS/Linux 的 Electron Runtime、文件锁和 unpacked app 证据仍为 Pending。C5/C6 前置阶段也尚未因远端 CI/最终合入而 Completed，所以 C7 不标记 In Progress/Completed，也不宣称 Beta。
- C8 才开放 Restore UI、Restore v2 独立 snapshot/journal/crash recovery、Watch、Diagnostics 目标文件、Update 和 Recovery action。C7 的 Restore 只作为受管 Backup 回环验证，不经 Renderer 暴露。
- C9 才完成 NSIS/portable ZIP、DMG/ZIP、AppImage/deb、native SQLite fallback/ABI/asar、checksum/SBOM、包内容扫描与全平台安装/卸载 smoke；当前 `--dir` 不是发行产物。
- 本 checkpoint 未创建 tag，未发布 npm/GitHub Release，未签名、公证或写更新通道；.NET 实现保持可构建且未删除。
