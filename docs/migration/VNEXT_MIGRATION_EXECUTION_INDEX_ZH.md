# vNext 升级改造执行索引

> **状态：阶段 0 交付完成（合入 `main` 后生效）**
>
> **日期：2026-08-24**
>
> **目标：Electron + React + TypeScript + Node 单核心的渐进迁移**
>
> **架构基线：[vNext Electron + Node 单核心架构](../VNEXT_ELECTRON_NODE_ARCHITECTURE_ZH.md)**

## 1. 索引职责

本文是迁移工作的执行入口，用于记录阶段状态、PR 依赖、进入条件和退出门槛。它不复制架构正文，也不把目标设计描述成已经实现。

权威顺序：

1. 已发布兼容行为、当前代码与测试；
2. vNext 架构基线；
3. Accepted ADR；
4. [Core 外部行为合同](../architecture/contracts/CORE_EXTERNAL_BEHAVIOR_ZH.md)、[CLI 合同](../architecture/contracts/CLI_CONTRACT_ZH.md)和 [Error Code 合同](../architecture/contracts/ERROR_CODES_ZH.md)；
5. [行为兼容 Fixture 清单](BEHAVIOR_FIXTURES_ZH.md)；
6. 本执行索引中的阶段状态。

发生冲突时必须先登记差异并裁决，不能让后提交的新实现自动成为权威。

## 2. 总体状态

| 阶段 | 名称 | 当前状态 | 核心结果 |
| --- | --- | --- | --- |
| 0 | 冻结决策与安全合同 | **Completed（合入 `main` 后）** | 架构、ADR、Core/CLI/Error 合同、Fixture 语义和执行索引 |
| 1 | 提取 Node Core，不改行为 | Pending | `src/public-api.js`，CLI/Web 只经公开入口调用 |
| 2 | Contracts 与 Core Client | Pending | `packages/core`、`packages/contracts`、`packages/core-client` 与稳定 DTO |
| 3 | Electron Read-only Alpha | Pending | 三平台只读 Status/Backup/Diagnostics，不开放写能力 |
| 4 | Sync / Switch Beta | Pending | Prepare/Apply、Progress、Cancel、Partial 与 Restore 验证 |
| 5 | Restore / Watch / 完整功能 | Pending | Recovery、Restore、Prune、Watch、Update 与诊断能力 |
| 6 | Electron Stable，替代 .NET | Pending | Electron 成为默认桌面产品，.NET 标记 Legacy |
| 7 | 清理 Legacy | Pending | 移出重复 .NET 业务代码，保留历史标签、分支和迁移说明 |

状态只能按 `Pending → In Progress → Completed` 前进。PR 分支可以把自身交付标为“合入后 Completed”，但受保护分支上的阶段只有在退出门槛全部满足并完成合并后才正式生效。

## 3. 阶段门槛

### 阶段 0：冻结决策与安全合同

进入条件：

- vNext 架构方向已经确认；
- 当前 Node、Web、.NET 行为仍由现有代码和测试证明。

退出门槛：

- ADR-0001～ADR-0010 均为 Accepted，历史 v0.4 ADR 不与 vNext 编号混淆；
- Core 外部行为、CLI、Error Code、Fixture 和本索引可互相导航；
- 每份文档明确“当前已实现”与“vNext 目标”；
- 本阶段不修改运行代码、CLI 输出、Error Class 或运行代码目录结构；
- CI 全绿并完成评审。

### 阶段 1：提取 Node Core，不改行为

进入条件：阶段 0 Completed。

退出门槛：

- 新增 `src/public-api.js`，CLI/Web 不再导入 Core 内部实现；
- 展示逻辑与业务结果分离，现有用户可见语义不变；
- PR 2～PR 5 均已完成：Public API、结构化错误、CLI `--json` 与 Prepare/Apply 已分别落地；
- CLI `--json` 是 opt-in 加法；默认 Human Mode、命令语义和 v0.5 兼容行为保持不变；
- 原 Node 测试全绿，新增 Public API Contract Test；
- 真实 Node↔.NET 对同一 Codex Home 的 protocol v2 操作锁争用通过：恰有一个写者，败方无副作用；
- 锁不确定状态 fail closed，并能区分 `OPERATION_BUSY` 与 `LOCK_UNVERIFIABLE`；
- 两个 Codex Home 共享同一 SQLite Home 的并发风险已验证并形成明确锁合同。若尚未安全，阶段 1 不得宣称跨入口写入安全，后续 Electron 写能力继续阻断。

### 阶段 2：Contracts 与 Core Client

进入条件：阶段 1 Completed，Public API 已稳定收口。

退出门槛：

- npm workspaces 和 `packages/core` 先包装既有 Public API，不混入业务重写；
- `packages/contracts` 的 Result/Event/Error/Protocol 可序列化并有版本；
- `CoreClient`、`HttpCoreClient`、`MockCoreClient` 表达相同业务语义；
- Legacy Error Adapter 不解析 message，并通过 Error Code Contract Test；
- 共享 Fixture Schema、临时复制 Runner 和差异登记格式落地；
- Node/.NET 双向 Backup Round-trip、Foreign Pending Restore 至少在受支持平台形成可重复证据；未通过的差异必须阻断后续写阶段。

### 阶段 3：Electron Read-only Alpha

进入条件：阶段 2 Completed；Core/Client/Protocol 可用。

退出门槛：

- Main、Preload、Renderer 与 Core Utility Process 边界成立；
- Renderer 无 Node、文件系统或任意 IPC 权限；
- Windows/macOS/Linux Packaged Build 可启动并完成版本握手；
- 只开放 Profile、Status、Backup List、Diagnostics 和可选只读 History；
- Runtime Crash 能拒绝 Pending Request、重启，并在下一次请求检查 Pending Journal；
- Status 与 CLI/Core 在同一 Fixture 上语义一致；
- 无 Sync、Switch、Restore、Prune 或 Watch 自动写入口。

### 阶段 4：Sync / Switch Beta

进入条件：阶段 3 Completed；所有写入前置安全门槛通过。

退出门槛：

- Prepare/Confirm/Apply、Revision、Expiry、Single-use 与 Plan Stale 完整实现；
- Electron 与 CLI 调用同一 Node Core，不解析 CLI 文本；
- SQLite Busy、locked/changing rollout、Cancel、Partial Result 和 Backup-first 通过 Fault Injection；
- `shared-sqlite-home-contention` 已证明不会出现并行数据库写者；
- `wsl-unc-unchanged-hash` 通过，阻断操作不创建 Backup、不改变任何目标；
- Journal Crash Matrix 覆盖 mutation/commit/ack 关键窗口；
- 一次 Sync/Switch 后可由受管 Backup 恢复到原状态。

### 阶段 5：Restore / Watch / 完整功能

进入条件：阶段 4 Completed，写操作 Beta 无已知数据破坏问题。

退出门槛：

- Restore、Prune、Watch、Recovery Required、Update 和诊断包完整；
- `restore-mid-failure` 不产生无证据的半恢复状态；
- Node Backup→.NET Restore、.NET Backup→Node Restore 双向通过；
- Foreign Pending Journal 可由兼容入口显式恢复并写入合法 terminal；
- Pending Journal 保护 Prune，Watch 不抢占用户写操作；
- 诊断包不含凭据、消息正文或未脱敏敏感路径。

### 阶段 6：Electron Stable，替代 .NET

进入条件：阶段 5 Completed，并完成真实 Beta 用户验证。

退出门槛：

- 三平台安装、启动、SQLite Driver、Sync+Restore、退出和清理 smoke 通过；
- 无已知数据破坏 Bug，关键功能等价，无 .NET 独占正式能力；
- Installer、签名、Notarization、更新和 Release 策略明确；
- README 默认推荐 Electron，.NET 标记 Legacy；
- 旧 Backup 可由新 CLI/Desktop 恢复；
- .NET 至少保留两个维护发布周期，不立即删除旧 Release。

### 阶段 7：清理 Legacy

进入条件：阶段 6 的兼容窗口结束，迁移证据与用户反馈满足清理条件。

退出门槛：

- .NET 停止常规 CI 并从 active source tree 移出；
- legacy tag/branch、历史 ADR、Release 和 Restore 说明仍可访问；
- 删除的只是重复业务实现，不删除 Node CLI；
- 仓库文档、依赖图、发布脚本和安全说明不再引用已移除路径。

## 4. 首批 PR 依赖

| PR | 内容 | 依赖 | 关键合入门槛 | 状态 |
| --- | --- | --- | --- | --- |
| PR 1 | 冻结架构合同和 ADR | 无 | 仅文档；阶段 0 退出门槛 | **Completed（合入 `main` 后）** |
| PR 2 | Core Public API | PR 1 | CLI/Web 仅走 Public API；现有测试全绿；锁合同验证 | Pending |
| PR 3 | 结构化错误 | PR 2 | Canonical/Legacy Adapter、`LOCK_UNVERIFIABLE`、错误合同测试 | Pending |
| PR 4 | CLI `--json` | PR 2、PR 3 | stdout 单一 JSON、stderr 日志、Exit Code 与 Schema 合同 | Pending |
| PR 5 | Prepare / Apply | PR 2、PR 3 | Revision/Plan/Apply 下沉；CLI Human Mode 兼容 | Pending |
| PR 6 | Workspace 与 Core 骨架 | PR 2～PR 5（阶段 1 Completed） | workspaces/Core/Contracts/CoreClient 骨架；不搬 UI、不改算法 | Pending |
| PR 7 | React UI 分解 | PR 6 | AppShell/Feature/HttpCoreClient；现有 Web UI 可用；阶段 2 退出门槛 | Pending |
| PR 8 | Electron Skeleton | PR 7（阶段 2 Completed） | Main/Preload/Renderer 安全基线与版本握手；无业务写 | Pending |
| PR 9 | Core Utility Process | PR 8 | Supervisor、Status、Crash/Protocol Test；Pending Journal 检查 | Pending |
| PR 10 | Read-only Preview Release | PR 9 | 三平台 package、只读 smoke、使用说明和反馈模板；阶段 3 退出门槛 | Pending |

PR 4 与 PR 5 可在 PR 2/3 后并行开发；两者都完成后才能进入 PR 6。PR 6～PR 10 按阶段门槛顺序推进，不能用“只搭骨架”绕过上一阶段的退出条件。

PR 到阶段的归属固定为：PR 1 完成阶段 0；PR 2～PR 5 完成阶段 1；PR 6～PR 7 完成阶段 2；PR 8～PR 10 完成阶段 3。阶段 4 之后的 PR 编号在 Read-only Preview 证据完成后确定。

阶段 4 之后的写入、Restore/Watch、Stable 与 Legacy 清理 PR，在 Read-only Preview 证据完成后再编号，避免提前承诺不可靠的拆分。

## 5. 跨阶段安全门槛矩阵

| 安全证据 | 最晚完成阶段 | 阻断内容 |
| --- | --- | --- |
| 真实 Node↔.NET 同 Codex Home 争锁 | 阶段 1 | 阻断“迁移期入口共享安全锁合同”的声明 |
| Busy 与不可验证锁的结构化区分 | 阶段 1/PR 3 | 阻断自动重试、自动清锁和 Electron 写入 |
| 不同 Codex Home 共享 SQLite Home 的互斥 | 阶段 4 进入前 | 阻断所有 Electron 写能力 |
| 双向 Backup Round-trip | 阶段 2 建证、阶段 5 全通过 | 阻断 .NET Legacy 替代 |
| Foreign Pending Restore | 阶段 2 建证、阶段 5 全通过 | 阻断跨入口 Recovery 声明 |
| Windows WSL UNC 全 Hash 不变 | 阶段 4 | 阻断 Windows Sync/Switch Beta |
| Journal Crash Matrix | 阶段 4 | 阻断写操作 Beta |
| Restore Mid-failure | 阶段 5 | 阻断 Restore 正式开放 |

## 6. 差异登记规则

Node 与 .NET 在同一 Fixture 上不一致时，PR 必须记录：

- Fixture ID 和复现平台；
- Node 结果、.NET 结果和逐字节/SQLite 证据；
- 哪一方成为权威以及安全理由；
- Error Code、Backup、Journal 和 Restore 是否受影响；
- 修复 PR、回归测试和兼容说明。

“Node 是目标核心”只决定最终代码所有权，不自动证明任一新行为正确。

## 7. 禁止提前执行

在对应阶段退出门槛通过前，不得：

- 删除或停止维护 .NET；
- 让 Electron Renderer 访问 Node/文件系统；
- 在 Main/IPC Handler 复制业务逻辑；
- 整体把 Node JavaScript 翻译成 TypeScript；
- 用 Electron 调 CLI 并解析人类文本；
- 开放未经 Prepare/Apply 和安全 Fixture 验证的写入口；
- 声称共享 Fixture、跨运行时等价或三平台稳定已经完成。

## 8. 本 PR 完成后的下一步

阶段 0 合入并标记 Completed 后，下一项是 PR 2：新增 `src/public-api.js`，让 CLI 与 Web 经单一公开入口调用现有 Node 行为。该 PR 不移动 Core、不改同步算法，也不同时引入 `--json`、TypeScript 或 Electron。
