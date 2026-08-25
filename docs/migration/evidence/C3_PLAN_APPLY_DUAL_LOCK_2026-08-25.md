# C3 Plan/Apply、双层锁与协调状态证据（2026-08-25）

状态：本地门禁通过，等待远端 CI。输入 checkpoint 为 `13163f5`；C3 最终 commit SHA 在本 checkpoint 提交后及 C10 evidence bundle 中索引。

## 已实现边界

- Node Core 的 Sync、Switch、Restore 已提供 `prepare*/apply*`；Plan 使用 32-byte 随机不透明 ID、schema v1、10 分钟 TTL、进程内单次消费 ledger。Apply 的公共输入严格为 `{schemaVersion: 1, planId}`，旧 `runSync/runSwitch/runRestore/runWatch` 只保留同进程兼容适配。
- Plan 绑定可信 profile、config、storage、完整 rollout inventory、SQLite main/WAL/SHM 与受管 backup revision；Apply 在取得最终锁后重新解析 storage 并精确复核，任何漂移在创建备份或修改目标前返回 `STALE_STATE`。
- 写锁顺序固定为 `Codex Home -> State DB`。State DB identity 为物理父目录 realpath、NUL 分隔的规范文件名及 Windows 大小写归一，锁路径为 `<real-db-parent>/.codex-provider-sync/locks/<sha256(identity)>.lock`。协议 v2 owner/claim 增加 `scope/resourceKey`，旧读取器兼容。
- Node 与仍受支持的 .NET Core 均实现同一 State DB 资源锁协议；真实 Node/.NET 子进程双向竞争和两个 Codex Home 共用一个 DB 的零备份败方已验证。Restore 目标父目录不能可靠解析时返回 `LOCK_UNVERIFIABLE`，不回退到 Home-only 锁。
- Status/History 保持只读。Node 与 .NET Status 都在外部 Home/State DB 写锁期间返回最后完整快照和 operation marker；无缓存或锁不可验证时返回显式不完整状态，绝不扫描中间态。锁协议观测与状态 revision 在扫描前后及最终返回前复核；.NET 缓存对所有可变嵌套 DTO defensive clone。
- Web direct-write 路由固定返回 `410 PLAN_REQUIRED` 且不调用 writer；现代 Prepare 只接收产品输入，Apply 只接收 plan ID。Web Status 直接转发 Core 快照，不混入 live profile/storage revision。
- Watch 合并 in-flight/busy 事件、不与人工操作重叠；人工操作完成后只重放一个合并 batch，停止 Watch 会取消 completion subscription。observer/progress 失败不改变事务结果。
- `OPERATION_BUSY` 携带 `busyScope`；`LOCK_UNVERIFIABLE` 携带 lock scope/resource key。Prune 仍只持有 Home 锁；本 checkpoint 未提前改变 C8 Restore journal 状态机。

## 关键行为证据

- Plan ledger：过期、篡改、跨 operation、重启失效、单次消费、并发双 Apply 仅一方执行。
- 状态漂移：config、profile、storage、rollout、SQLite main/WAL、backup manifest 漂移均在备份前拒绝；Switch 锁前/锁内候选 DB 发生变化时，锁内重新 detect 并返回 `STALE_STATE(storage)`。
- 锁：Node↔Node、Node↔.NET、不同 Home 共用 DB、Home/State busyScope、stale/malformed owner、缺失 Restore 父目录、败方零 mutation。
- Status：真实外部 Node Home 锁、真实 Node State DB 锁、两个 Home 共用 Node-locked DB、.NET Home/State 锁、malformed owner；均返回缓存或显式不完整状态，不报告中间 Provider/SQLite 行为健康。
- Watch：busy batch 保留、人工优先、合并一次、停止取消订阅、既有连续失败与 watcher rebind 行为不回归。
- Web：旧直写无副作用；Prepare/Apply、profile revision、managed backup、Restore relocation、WSL UNC、partial 与安全 CoreError DTO 均有契约测试。

## 本地验证

环境：Windows x64，Node `v24.11.1`，npm `11.10.0`，.NET SDK `10.0.400`；输入 SHA `13163f510a1ac0c245ac992f7a10027f30195300`。

| 命令 | 结果 |
| --- | --- |
| `npm test` | 339 passed，0 failed，0 skipped |
| `node --test web/src/api.test.js web/src/operation-state.test.js` | 14 passed，0 failed，0 skipped |
| `npm run web:build` | Vite 8.2.2 production build 成功，21 modules transformed |
| `npm exec --yes --package=node@16.20.2 -- node --test test/plan-ledger.test.js test/operation-revision.test.js test/state-db-lock.test.js test/public-api-contract.test.js` | 实际 Node `v16.20.2`；4 个目标文件通过，0 failed/skipped |
| `npm audit --omit=dev --audit-level=moderate` | 0 vulnerabilities |
| `npm audit --audit-level=high` | 0 vulnerabilities |
| `npm pack --dry-run --json` | 成功；C3 Core 模块、Web dist 与文档进入根 tarball，Electron 依赖不存在 |
| `dotnet build CodexProviderSync.sln --configuration Release` | 13 projects build 成功，0 warnings/errors |
| `dotnet test desktop/CodexProviderSync.Core.Tests/CodexProviderSync.Core.Tests.csproj --configuration Release --no-build` | 220 passed，1 个真实 WSL 条件测试 skipped，0 failed |
| `dotnet test desktop/CodexProviderSync.Application.Tests/CodexProviderSync.Application.Tests.csproj --configuration Release --no-build` | 49 passed，0 failed/skipped |
| `dotnet test desktop/CodexProviderSync.Automation.Tests/CodexProviderSync.Automation.Tests.csproj --configuration Release` | 27 passed，0 failed/skipped |
| `dotnet test desktop/CodexProviderSync.App.Tests/CodexProviderSync.App.Tests.csproj --configuration Release --no-build` | 67 passed，0 failed/skipped |
| `dotnet test desktop/CodexProviderSync.GuiE2E.Tests/CodexProviderSync.GuiE2E.Tests.csproj --configuration Release --no-build` | 36 passed，0 failed/skipped |
| `git diff --check` | 通过，仅有 Windows CRLF 工作区提示 |

两轮独立只读复审确认无剩余 P0/P1；复审发现并推动修复了 Web status live/cache 混合、外部锁绕过、Watch busy 丢事件、旧 Web 直写、.NET Restore Home-only 降级、.NET Status 中间态扫描、last-complete cache 可变引用污染，以及 Switch 复用锁前 State DB selection 的 TOCTOU。

## 已知未闭合项

- Windows/Ubuntu/macOS 远端矩阵、Linux Node↔.NET lock contract 与真实 WSL 场景仍由 CI/C10 汇总证明；本地的 Windows 结果不能替代其他平台证据。
- C8 的 Restore v2 独立快照、完整状态机、启动恢复与 update 安装阻断尚未实现；C3 继续使用现有安全 journal/rollback 合同。
- 当前根 npm/Web dist 仍包含既有 source map；C9 必须从生产包排除并扫描。Electron、workspace、共享 UI/CoreClient 与平台产物尚未进入本 checkpoint。
- 未创建 tag，未发布 npm/GitHub Release，未签名、公证或写更新通道；只有最终 PR 的全部必需 job 成功并合入受保护分支后，对应阶段才可标记 Completed。
