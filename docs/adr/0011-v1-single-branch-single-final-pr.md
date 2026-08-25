# vNext/ADR-0011：V1 单分支、单最终 PR 的受控交付例外

- Status: Accepted
- Date: 2026-08-25
- Scope: V1 delivery governance

## Context

ADR-0008 的默认拓扑是小型、可独立合入的 PR。维护者已明确批准一次受控例外：V1 在单一 `V1` 分支中推进，并只创建一个最终 PR。该授权只改变合并拓扑，不改变 vNext 的安全、兼容、验证或发布门槛。

## Decision

V1 使用本次批准计划中的不可变内部 Checkpoint `C0`～`C10`。旧执行索引中的 PR 2～PR 10 仅作为历史依赖来源，按以下方式并入新 checkpoint：Public API 与结构化错误归入 `C1`，CLI JSON 归入 `C2`，Prepare/Apply 与双层锁归入 `C3`，Workspace/Core/Contracts 归入 `C4`，共享 UI/Web 归入 `C5`，Electron Skeleton、Utility Runtime 和只读能力归入 `C6`，后续依次为 `C7` Sync/Switch、`C8` Restore/Watch/Diagnostics/Update、`C9` 打包/CI、`C10` 最终证据与 Legacy 交接。每个 checkpoint 必须有单独 commit、可重复 CI 证据、变更摘要和明确的上一个回退 commit。

最终 PR 必须保留这些 checkpoint 的线性、可审查历史。分支上的 checkpoint 可以标注“已验证”或“合入后可完成”，但在最终 PR 合入受保护分支前，不得把任一 Phase 标为 Completed、不得宣称 Electron 已替代 .NET，也不得删除 Legacy 实现或停止其关键 CI。

本 ADR 仅局部 supersede ADR-0008 的“每个主要维度必须以独立 PR 合入”这一合并拓扑。ADR-0008 的一个主要维度、可验证入口/退出条件、旧入口可用、测试/Fixture、.NET 保留、只读先行及差异登记等全部不变量继续有效。

## Invariants

- 一个 checkpoint 只改变一个主要架构维度；不得把 Core 搬迁、TypeScript 翻译、算法变更、Electron 写能力和 Legacy 清理混在同一 checkpoint。
- 每个 checkpoint 必须从干净输入重复运行其适用测试；跨运行时和三平台门槛仍以真实进程/packaged 证据为准，不能用 Mock 或分支声明替代。
- V1 开始时和最终门禁前必须合并最新 `origin/main`，不得 rebase 或 force-push；若最终合并发生冲突，解决后必须重新验证全部适用证据。其他 checkpoint 只记录基线与回退 commit，不额外制造未经计划批准的合并要求。
- Electron 写能力、Restore v2、Watch、默认桌面入口和 .NET 清理仍受执行索引对应退出门槛阻断。
- 不得从 V1 分支对真实 Codex Home、真实凭据或真实消息正文进行测试、迁移或发布验证。

## Consequences

该例外减少 PR 数量，但不减少审查单位；最终 PR 审查必须按 checkpoint 审阅。它增加主线漂移风险，因此需要 checkpoint 证据、频繁同步和可撤回的 feature gate。若真实 Beta、跨平台 package 或兼容门槛无法在最终合入前获得证据，V1 必须停在相应 checkpoint，不能用“单 PR 已获授权”绕过门槛。

## Rejected Alternatives

- **把一条长提交链当作已完成的各阶段**：未合入受保护分支时，阶段状态和发布承诺不成立。
- **用最终总 diff 代替 checkpoint 审查**：无法隔离安全回归或给出可靠回退点。
- **为缩短分支周期放宽 Fixture、锁、Restore 或 packaged 验证**：与本 ADR 的授权范围不符。

## Migration and Validation

执行索引记录 `C0`～`C10` 的目标、依赖、证据和回退点。每个 checkpoint 的验证必须更新适用合同和 Fixture；本 ADR 本身不表示任何运行时代码、错误适配、锁协议或 Restore v2 已实现。

## Related

- [渐进迁移 ADR](0008-incremental-migration-no-big-bang-rewrite.md)
- [Node Core 单一权威 ADR](0002-node-core-as-single-authority.md)
- [迁移执行索引](../migration/VNEXT_MIGRATION_EXECUTION_INDEX_ZH.md)
