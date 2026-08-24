# vNext/ADR-0009：写操作采用 Plan / Confirm / Apply

- Status: Accepted
- Date: 2026-08-24
- Scope: vNext

## Context

Sync、Switch 和 Restore 的目标状态可能在用户查看影响与真正执行之间变化。桌面 UI 不能把几秒前的扫描结果当作当前事实，也不能把任意路径和写参数直接交给 Renderer 重放。

## Decision

所有交互式写操作先由 Core 生成短期 Plan Summary，用户确认后只用 `planId` Apply。Core 在持有正式操作锁后重新校验 Revision 与目标，再创建备份并执行事务。

## Decision Drivers

- 阻止 TOCTOU 和过期确认；
- 让用户看到真实影响与警告；
- 把可执行细节留在可信 Core；
- 为 Desktop/Web 提供一致写状态机。

## Invariants

- Plan 包含 schemaVersion、planId、createdAt、expiresAt、profile/config/storage revision、目标与影响摘要；
- Plan 短期、单次使用、仅在 Core 内保存，重启即失效；
- Apply 在锁内重新解析 storage，并校验 profile、config、SQLite DB、rollout snapshot、pending transaction 和可写性；
- 变化必须返回结构化 stale/changed 错误，要求重新 Prepare；
- 备份在任何业务目标变更之前完成；
- Renderer 只能提交 planId，不能提交任意目标路径；
- CLI 可在同一进程内 Prepare 后立即 Apply，以保持现有单命令体验；
- Restore 也必须进入该模型，并在开放 Electron Restore 前补齐自身故障补偿合同。

## Consequences

写操作多一个确认阶段并需要 Plan Ledger；换来明确的影响预览、过期控制和统一并发语义。

## Rejected Alternatives

- **Renderer 提交完整执行参数**：容易篡改或过期。
- **只在 UI 比较 Revision**：CLI 或其他适配器可以绕过。
- **确认后不重新扫描**：保留 TOCTOU 风险。

## Migration and Validation

先把现有 Web Profile/Storage Revision 下沉到 Core，再实现 Prepare/Apply。需覆盖 expiry、single-use、锁内变更、foreign pending transaction、共享 SQLite Home 和所有崩溃点。

## Related

- [错误码合同](../architecture/contracts/ERROR_CODES_ZH.md)
- [无应用数据库 ADR](0006-no-application-database.md)
