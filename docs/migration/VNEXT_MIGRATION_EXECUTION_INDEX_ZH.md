# vNext 升级改造执行索引

> **状态：受保护分支上的阶段 0 已完成；V1 的 C0 checkpoint 在单最终 PR 合入前不推进任何后续 Phase 状态。**
>
> **日期：2026-08-28**
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

发生冲突时必须先登记差异并裁决，不能让后提交的新实现自动成为权威。ADR-0011 的 V1 合并拓扑例外只改变 checkpoint 的承载方式，不改变本权威顺序。

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

状态只能按 `Pending → In Progress → Completed` 前进。V1 的 `C0`～`C10` 仅是内部 checkpoint：可记录“已验证”或“合入后 Completed”，但在最终 PR 合入受保护分支前，Phase 1～7 仍为 Pending 或 In Progress，不能标记 Completed。

## 2.1 V1 单最终 PR checkpoint 治理

- 本分支受 [ADR-0011](../adr/0011-v1-single-branch-single-final-pr.md) 约束：`C0`～`C10` 采用批准计划中的合并后编号；旧 PR 2～PR 10 只保留为依赖与安全意图来源。
- 每个 checkpoint 必须记录 commit SHA、范围、适用测试/Fixture、真实平台证据、未满足 gate 和上一个可回退 commit；最终 PR 审查按 checkpoint 进行。
- checkpoint 通过不自动开放下一阶段能力：Electron 写、Restore v2、Watch、公开默认桌面入口、Phase Completed 和 Legacy 清理由本索引的对应退出门槛继续阻断。V1 候选可按 C10 显示交接目标，但不得把它表述成已经发生的公开替代。
- checkpoint 无法在同步 `main` 后重放或复验时，停止在最近已验证 checkpoint；不得以合并拓扑例外跳过差异登记、Fixture 或发布验证。
- 在 V1 分支内，本索引中“进入条件：上一 Phase Completed”表示相应前序 checkpoint 的全部证据已验证；它不改变受保护分支上该 Phase 仍未 Completed 的状态。

## 3. 阶段门槛

### 阶段 0：冻结决策与安全合同

进入条件：

- vNext 架构方向已经确认；
- 当前 Node、Web、.NET 行为仍由现有代码和测试证明。

退出门槛：

- 阶段 0 的 ADR-0001～ADR-0013 均为 Accepted，后续 checkpoint ADR 不追溯改变该冻结证据，历史 v0.4 ADR 不与 vNext 编号混淆；
- Core 外部行为、CLI、Error Code、Fixture 和本索引可互相导航；
- 每份文档明确“当前已实现”与“vNext 目标”；
- 本阶段不修改运行代码、CLI 输出、Error Class 或运行代码目录结构；
- CI 全绿并完成评审。

### 阶段 1：提取 Node Core，不改行为

进入条件：阶段 0 Completed。

退出门槛：

- 新增 `src/public-api.js`，CLI/Web 不再导入 Core 内部实现；
- 展示逻辑与业务结果分离，现有用户可见语义不变；
- 最终 PR 合入前，C1～C3 的 checkpoint 门槛必须全部通过：Public API/结构化错误、CLI `--json`、Prepare/Apply 与双层锁分别落地；
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

- Restore、Prune、Watch、Recovery Required、诊断包以及 Main-only Update 状态机/安装门禁完整；真实版本 metadata、签名、下载与跨版本升级证据继续由 C9/C10 闭合；
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

## 4. V1 checkpoint 依赖

| Checkpoint | 内容 | 依赖 | 最终合入前必须保留的证据 | V1 状态 |
| --- | --- | --- | --- | --- |
| C0 | 治理、基线与依赖安全 | 阶段 0 | ADR-0011～0013、合同导航、基线测试、Vite 审计告警清零 | In Progress（V1） |
| C1 | Core Public API 与结构化错误 | C0 | CLI/Web 仅走 Public API；Canonical/Legacy Adapter；错误合同测试 | In Progress（V1，本地门禁通过） |
| C2 | CLI `--json` | C1 | stdout 单一 JSON、stderr 日志、JSON Exit Code 与 Schema 合同 | In Progress（V1，本地门禁通过） |
| C3 | Prepare/Apply、协调器与双层锁 | C1、C2 | Revision/Plan/Apply、Node/.NET 双层资源锁、真实争锁证据 | In Progress（V1，本地门禁通过） |
| C4 | Workspace、Core、Contracts、CoreClient | C1～C3（Phase 1 全部门槛已验证） | 不搬高风险算法；根 npm CLI tarball/Node 16 兼容 | In Progress（V1，本地门禁通过） |
| C5 | 共享 React UI 与 Web | C4 | AppShell/Features/HttpCoreClient；Web 安全与功能等价；阶段 2 门槛 | In Progress（V1 候选实现；`c63a403` checkpoint 的 required CI 已验证，后续 source head 以 PR 最新成功证据为准；等待最终合入） |
| C6 | Electron 安全骨架、Utility Runtime、只读能力 | C5（需 Phase 2 全部门槛闭合；受保护分支状态未满足） | 安全窗口/IPC、握手、crash recovery、三平台只读 smoke | Pending（`c63a403` checkpoint 的 Windows/macOS/Linux Electron 候选门禁已验证；后续 head、Phase 状态、真实 WSL 与最终合入未闭合） |
| C7 | Electron Sync/Switch | C6（Phase 3 全部门槛已验证） | Prepare/Apply、Busy/Partial/Cancel、Backup/Restore 回环 | Pending（`c63a403` checkpoint 的写能力、隐藏 E2E 与三平台候选 CI 已验证；后续 head、Phase 前置、真实 WSL 与最终合入未闭合） |
| C8 | Restore/Watch/Diagnostics/Update | C7（Phase 4 全部门槛已验证） | Restore v2 crash matrix、foreign pending、诊断隐私、Watch/Update | Pending（`c63a403` checkpoint 的候选门禁已验证；后续 hardening head、commit-bound 真实 WSL、获授权真实更新链、Phase 前置与最终合入未闭合） |
| C9 | 打包、CI 与发布工程 | C8（Phase 5 全部门槛已验证） | 四目标产物、native SQLite、packaged smoke、SBOM/checksums | Pending（`c63a403` checkpoint 的四目标候选证据已验证；后续 head 增加 checksum-bound hosted v0.4.1 Automation backup fixture，只有最新成功 C10 artifact 才是当前证明；签名、公证、真实更新与最终合入未闭合） |
| C10 | 最终证据与 Legacy 交接 | C9 | evidence bundle、README/Legacy、全量门禁；不自动发布 | Pending（`c63a403` checkpoint 的 source `1.0.0`、交接目标和 26/26 CI 已验证；后续 source head 的 hosted formal Release backup 兼容只以最新成功 C10 artifact 为准；真实 Beta/WSL、受保护 `main` 合入后复验、签名/公证和发布授权仍未闭合） |

`C0`～`C10` 按表中依赖和阶段门槛推进，不能用“只搭骨架”绕过上一阶段的退出条件。Checkpoint 内可以有若干小提交，但最终 evidence 必须绑定一个明确 commit。

checkpoint 到阶段的归属固定为：C0 不推进运行 Phase；C1～C3 的完整证据在最终合入后可完成阶段 1；C4～C5 可完成阶段 2；C6 可完成阶段 3；C7 可完成阶段 4；C8 可完成阶段 5；C9～C10 只能形成阶段 6 的 release-ready 证据。阶段 6 仍需真实 Beta、签名/公证与单独授权的发布验证；阶段 7 不属于本 PR。

任何 checkpoint 只有代码、合同、Fixture、CI 和适用真实平台证据同时闭合后才能标记已验证；分支内通过不等于公开发布或稳定用户验证完成。

## 5. 跨阶段安全门槛矩阵

| 安全证据 | 最晚完成阶段 | 阻断内容 |
| --- | --- | --- |
| 真实 Node↔.NET 同 Codex Home 争锁 | 阶段 1 | 阻断“迁移期入口共享安全锁合同”的声明 |
| Busy 与不可验证锁的结构化区分 | 阶段 1/C3 | 阻断自动重试、自动清锁和 Electron 写入 |
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

`c63a403688b6d148afa65fba9e1461c7ebcd3331` checkpoint 已包含 `origin/main@c7ff85218a07a8e5f14132c582cad1239c52865e`，补齐两个不同 Codex Home 共用一个 State DB 时的真实 Node writer↔.NET writer 双向争锁，并在本地跨运行时矩阵中达到 12/12。Draft PR #90 的 [CI run 33142610556](https://github.com/Dailin521/codex-provider-sync/actions/runs/33142610556) 是该 checkpoint 的历史快照：测试合并 commit `10047581a46f67993c809bb8fb3b58a89fb42d09` 上 26/26 jobs 成功，source manifest 为 `1.0.0`，候选版本为 `1.0.0-rc.204`；它不自动覆盖后续 V1 source head。

V1 候选按 C10 在界面和文档中显示“Electron 新版主桌面端候选 / .NET Legacy fallback”的交接目标；这不表示公开入口已经切换。截至本索引更新时，公开 GitHub Release 仍是 .NET `v0.4.1`，PR 仍为 Draft、未合并，Electron 没有公开下载、签名、公证或生产更新通道。没有与 source commit 绑定的健康 Windows+WSL strict 结果、真实 Beta、受保护 `main` 合入后同 SHA 复验、签名/公证、真实更新升级和独立发布授权时，Phase 1～7 仍保持 Pending/In Progress。`c63a403` 的静态证据见 [C10 最终候选证据快照（2026-08-28）](evidence/C10_FINAL_EVIDENCE_BUNDLE_2026-08-28.md)；PR #90 当前 source head 的 commit-bound 证据只以该 PR 最新成功 `c10-evidence-bundle` artifact 为准，不在静态 Markdown 中重复动态 run、merge ref 或 artifact SHA。C1 证据见 [C1 Public API 与结构化错误证据](evidence/C1_PUBLIC_API_ERRORS_2026-08-25.md)，C2 证据见 [C2 CLI JSON 合同证据](evidence/C2_CLI_JSON_2026-08-25.md)，C3 证据见 [C3 Plan/Apply 与双层锁证据](evidence/C3_PLAN_APPLY_DUAL_LOCK_2026-08-25.md)，C4 证据见 [C4 Workspace、Core 与 CoreClient 证据](evidence/C4_WORKSPACE_CORE_CLIENT_2026-08-25.md)，C5 证据见 [C5 共享 UI、Web 与跨运行时 Fixture 证据](evidence/C5_SHARED_UI_WEB_2026-08-26.md)，C6 证据见 [C6 Electron Read-only Alpha 证据](evidence/C6_ELECTRON_READONLY_2026-08-26.md)，C7 证据见 [C7 Electron Sync/Switch 证据](evidence/C7_ELECTRON_SYNC_SWITCH_2026-08-26.md)，C8 证据见 [C8 Restore / Watch / Diagnostics / Update 证据](evidence/C8_RESTORE_WATCH_DIAGNOSTICS_UPDATE_2026-08-27.md)，C9 证据见 [C9 打包、CI 与发布工程证据](evidence/C9_PACKAGING_CI_RELEASE_ENGINEERING_2026-08-27.md)。
