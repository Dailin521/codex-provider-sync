# vNext/ADR-0006：不引入应用业务数据库

- Status: Accepted
- Date: 2026-08-24
- Scope: vNext

## Context

Codex 的 config、rollout、SQLite、global state 与 managed backups 已是业务事实来源。再复制到应用数据库会产生同步、迁移、隐私、恢复和“哪份数据权威”的新问题。

## Decision

vNext 不创建保存 Provider、历史正文、备份索引或操作结果的应用业务数据库。Core 每次从 Codex 存储和 managed backups 读取权威状态。

## Decision Drivers

- 避免双份状态和 Schema Migration；
- 缩小敏感数据持久化范围；
- 保持 CLI 与 Desktop 看到同一事实；
- 让备份/恢复仍由现有格式定义。

## Invariants

- Plan 只在 Core 内短期保存，默认有 TTL，重启后失效；
- 允许以普通配置文件保存窗口位置、主题、语言和 server-managed profile，不得把它们当业务事实；
- Backup inventory 只是可重建缓存，不是完整性证明；
- History 正文不得进入持久缓存、日志、诊断包或遥测；
- 不读取、复制、记录或修改 `auth.json`、凭据和令牌；
- 不用新数据库包装或镜像 `state_5.sqlite`。

## Consequences

状态页面可能需要重新扫描，需通过流式扫描和短期内存缓存控制性能；同时避免了新的数据迁移和隐私责任。

## Rejected Alternatives

- **SQLite 应用数据库镜像 Codex 状态**：一致性和隐私成本过高。
- **长期持久化 Operation Plan**：扩大重放和过期写风险。
- **缓存 History 正文以加速 UI**：违反最小数据原则。

## Migration and Validation

代码评审和依赖检查必须阻止新 ORM/业务数据库。性能优化先使用可丢弃内存缓存；任何持久化扩展都需要新的 ADR 和数据生命周期说明。

## Related

- [Plan/Confirm/Apply ADR](0009-plan-confirm-apply-for-writes.md)
- [Core 外部行为合同](../architecture/contracts/CORE_EXTERNAL_BEHAVIOR_ZH.md)
