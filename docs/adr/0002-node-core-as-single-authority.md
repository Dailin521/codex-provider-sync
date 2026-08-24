# vNext/ADR-0002：Node Core 是唯一业务权威

- Status: Accepted
- Date: 2026-08-24
- Scope: vNext

## Context

当前 Node CLI/Web 与 .NET Desktop 都能操作 Codex 数据，但重复实现会使锁、备份、恢复、SQLite 和 rollout 规则漂移。Node 路径拥有更完整的跨平台能力，并且是 npm CLI 的既有实现。

## Decision

vNext 以 Node Core 作为唯一业务权威。CLI、Local Web UI 和 Electron Desktop 必须通过稳定 Public API 调用同一 Core；.NET 仅在分阶段迁移门槛通过前作为 Legacy 实现保留。

## Decision Drivers

- 一套数据安全规则、一套错误分类和一套测试语料；
- 保留现有 CLI 和 WSL 支持；
- 消除跨 UI 的行为差异；
- 让 Electron 只承担桌面适配职责。

## Invariants

- Core 不依赖 Electron、React、DOM 或具体 Presenter；
- CLI/Web 不得绕过 Public API 深导入内部业务模块；
- .NET 在替代门槛通过前仍是当前桌面行为证据，不得提前删除；
- 迁移期差异必须记录、选择权威语义并补充 Fixture，不能静默以新实现为准；
- 锁、备份、Journal、恢复与路径边界只能由 Core 决定。

## Consequences

所有入口最终共享修复和测试。短期内需要维护 Node/.NET 对照测试，并限制 .NET 只修严重缺陷，避免继续增加重复能力。

## Rejected Alternatives

- **Node 与 .NET 双核心长期并存**：维护成本和数据风险不可接受。
- **以 Electron Main 作为新核心**：绑定运行时并阻塞可测试性。
- **直接整体翻译为 TypeScript**：把结构迁移和行为改动混在一起。

## Migration and Validation

先建立 `src/public-api.js`，再按模块迁移到 `packages/core`。每次权威转移都必须通过原测试、跨运行时锁/备份/Journal Fixtures 和对应阶段退出条件。

## Related

- [Core 外部行为合同](../architecture/contracts/CORE_EXTERNAL_BEHAVIOR_ZH.md)
- [渐进迁移 ADR](0008-incremental-migration-no-big-bang-rewrite.md)
