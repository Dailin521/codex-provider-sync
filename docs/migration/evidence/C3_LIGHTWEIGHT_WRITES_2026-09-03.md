# C3 轻量普通写与重试收敛证据（2026-09-03）

状态：C3 checkpoint `c09d8a2` 已完成；职责抽取、actual-target UndoBackup、跨运行时兼容修正和本地候选门禁已完成，等待当前 head 的远端 CI。

## 已实现边界

- 新 Sync、Switch、Repair 只获取 Codex Home 锁；不再获取 Node State DB 资源锁，不创建普通 transaction journal，也不自动全量回滚。SQLite 竞争由原生事务裁决。
- Apply 在 Home 锁内重新读取 config 并验证 Plan，扫描实际可写 session；没有实际写入时直接 completed/noop，不创建备份。
- 只要本次存在 config、session、workspace roots 或 SQLite mutation，现有 State DB 都会在备份前执行原生可写性预检；竞争、不可读或备份失败均发生在业务 mutation 前。
- UndoBackup 只覆盖实际目标。备份完成后、首次 mutation 前仍接受取消；一旦 config、session、workspace roots 或 SQLite 开始实际 mutation，取消关闭，操作继续或返回可重试 partial。
- 写入顺序固定为 config（仅 Switch）、可写 session、workspace roots（仅 Repair）、SQLite transaction。被占用 session 会被跳过，其余目标继续，结果为 `partial`。
- mutation 后失败不做普通自动回滚，返回 `partial`、`backupId`、`failedStage`、`failureCode`、`retryRecommended`；再次执行相同操作负责收敛，用户也可显式 Restore UndoBackup。
- Restore 继续独立使用恢复前 snapshot、完整 manifest/hash、durable journal、确认与补偿，但同样只获取 Home 锁。普通写路径不能创建 Restore recovery 状态。
- 旧 v0.5 Sync/Switch journal 继续只读显示在 Diagnostics，并由 Restore/Prune 识别关联证据；它不再阻断新的 Sync/Switch/Repair 或 Electron cold-start 普通写。
- Electron Utility Process 崩溃会拒绝 pending 请求；下一次请求重启一次并检查 Restore recovery。测试 fault marker 跨 generation 保持单次触发，重试不会重复注入同一崩溃点。
- Windows packaged smoke 只在首次 `connectOverCDP` 握手超时、endpoint 已就绪且旧进程已完整清理时，以全新进程重试一次；endpoint、page、Renderer、业务或清理失败不重试。

## 关键回归证据

- Plan/Apply：过期、篡改、单次消费、状态漂移、备份前取消、备份完成后取消、首个 mutation 后取消关闭。
- Preflight/Backup：SQLite 已对齐但 rollout 待写时，真实 `BEGIN IMMEDIATE` 竞争仍在备份前返回 `SQLITE_BUSY`，零 backup、零 rollout mutation。
- Partial/收敛：locked rollout、config 后、rollout 后、SQLite commit 后故障均不生成普通 journal；下一次 Sync 收敛为 completed。
- Restore：独立 relocation、snapshot/hash、旧 backup/journal 兼容和 crash matrix 保持通过。
- Status/Supervisor：旧普通 journal 可见但 `pendingRecovery=false`，不阻断普通写；只有未解决 Restore journal 关闭 recovery gate。

## 本地验证

环境：Windows x64，Node `v24.11.1`，npm `11.10.0`，.NET SDK `10.0.400`；Electron 窗口使用 `CPS_DESKTOP_WINDOW_DISPLAY=hidden`，没有抢占主屏。

| 命令 | 结果 |
| --- | --- |
| `npm test` | 427 tests：374 passed，0 failed，53 expected skipped |
| `node --test test/plan-apply.test.js` | 15 passed，0 failed/skipped；包含备份后取消与 SQLite 已对齐时 busy preflight |
| `npm run workspaces:check` | 全部 workspace build、unit/contract 与依赖边界通过；Desktop 76、Core 18、CoreClient 21、Contracts 12 |
| `npm run fixtures:cross-runtime` | 11 passed，0 failed/skipped；新 Node Sync source 从 config 读取 Provider，Restore 继续兼容 Node/.NET 旧备份与 journal。旧 State DB 资源锁对抗用例随 C3 协议退役 |
| 隐藏 Electron `playwright test -c playwright.config.mjs` | 14 个未受断言修正影响的场景 passed，1 个真实 WSL 条件用例 skipped；actual-target Restore 断言修正后定向场景 1/1 passed。包含 native SQLite fallback、Repair、partial、四个 Utility crash/retry 点和 Restore relocation |
| Windows unpacked production smoke | 当前源码重建、Electron ABI rebuild 后 2/2 passed；覆盖生产只读边界与真实 Sync→Restore，窗口 hidden |
| Web production build + Playwright | production build 通过，2/2 passed；`web/dist` 已刷新为当前共享 UI |
| 根 npm tarball（本机 Node 24） | content smoke 与 install-lifecycle/SQLite Sync→Restore smoke 通过；UndoBackup 只恢复 captured rollout/SQLite，不回退未捕获 config |
| Node 16.20.2 根包门禁 | 当前 head 等待远端 Windows/Ubuntu 的隔离 production-only install、runtime verify 与两类 tarball smoke |
| `dotnet test CodexProviderSync.sln -c Release --nologo` | Legacy：426 passed，0 failed，1 个真实 WSL 条件测试 skipped |
| `npm audit --omit=dev --audit-level=moderate` | 0 vulnerabilities |
| `npm audit --audit-level=high` | 0 vulnerabilities |
| `git diff --check` | 通过；仅 Windows 工作区 CRLF 提示 |

## 未闭合项

- 本机没有可运行的真实 WSL distribution；对应 Electron 与 .NET 条件测试跳过，不能用本地结果替代 Windows+WSL strict CI。
- macOS/Linux 当前 source head 和远端 required jobs 要在 push 后重新运行；任何失败、取消或适用 job 跳过仍阻止合并。
- #90 必须保持 Open/Draft。本 checkpoint 不合并 `main`、不创建 tag、不发布、不签名/公证，也不写生产更新通道。
