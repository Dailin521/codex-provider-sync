# vNext/ADR-0008：渐进迁移，禁止 Big Bang Rewrite

- Status: Accepted
- Date: 2026-08-24
- Scope: vNext

## Context

核心包含文件锁、SQLite 在线备份、原子写、Transaction Journal、崩溃补偿、WSL 和跨运行时兼容。一次性移动目录、改语言、换 UI 并修改行为会让回归无法定位和回滚。

## Decision

迁移按阶段 0～7 和小型 PR 执行。每个 PR 只改变一个主要维度，具有可验证入口/退出门槛，并保持上一正式入口可用。

## Decision Drivers

- 控制数据破坏风险；
- 让审查、CI 和回滚有明确边界；
- 用事实解决 Node/.NET 差异；
- 避免长期不可合并的大分支。

## Invariants

- 不在同一 PR 同时大规模搬迁 Core、翻译 TypeScript 和改变算法；
- 先建立 Public API，再建立 workspaces，再逐模块迁移；
- 原测试、故障注入和契约测试必须持续通过；
- .NET 在替代门槛前不得删除或停止关键 CI；
- 不宣称跨运行时锁/备份兼容，除非真实双向进程测试通过；
- 新入口先只读，再分阶段开放 Sync/Switch、Restore/Watch；
- 每个发现的语义差异要有记录、选择和 Fixture。

## Consequences

迁移提交数量增加，短期存在适配层；但每一步可独立验证、合并、发布和回退。

## Rejected Alternatives

- **长期 rewrite 分支**：漂移大且难审查。
- **先搭完整 Electron 再补 Core 合同**：会把错误边界固化进 UI。
- **以新实现输出为准批量更新测试**：丢失既有安全证据。

## Migration and Validation

执行顺序与门槛由 [迁移执行索引](../migration/VNEXT_MIGRATION_EXECUTION_INDEX_ZH.md) 跟踪。跨阶段变更需要更新索引或新 ADR，不能在普通重构 PR 中隐式完成。

## Related

- [迁移执行索引](../migration/VNEXT_MIGRATION_EXECUTION_INDEX_ZH.md)
- [行为 Fixtures](../migration/BEHAVIOR_FIXTURES_ZH.md)
