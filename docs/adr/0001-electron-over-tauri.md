# vNext/ADR-0001：桌面技术路线采用 Electron，而非 Tauri

- Status: Accepted
- Date: 2026-08-24
- Scope: vNext

## Context

仓库已经有经过大量故障注入、备份、锁、回滚、WSL 和路径边界测试的 Node 实现。Tauri 会要求以 Rust 重写或长期桥接这套高风险数据核心，使桌面迁移同时承担 UI、运行时和业务语义三类变化。

## Decision

vNext 桌面端采用 Electron。React + TypeScript 负责界面，Electron 负责桌面生命周期和安全 IPC，现有 Node Core 演进为所有正式入口共享的唯一业务核心。

## Decision Drivers

- 最大限度复用已有 Node 安全行为和测试证据；
- CLI、Local Web UI 与桌面端可以调用同一 Core；
- 允许以小 PR 迁移，而不是一次性替换；
- 降低 Windows、macOS、Linux 三平台业务语义漂移。

## Invariants

- 选择 Electron 不授权把业务逻辑放进 Main、Preload 或 Renderer；
- Renderer 不获得 Node 能力，见 [ADR-0004](0004-renderer-has-no-node-access.md)；
- Electron 不得解析 CLI 人类文本；
- Node CLI 继续作为独立、轻量且受支持的入口；
- 不因为桌面技术选型而改变 Codex 数据格式、备份格式或安全边界。

## Consequences

正面结果是业务核心复用和迁移可逆。代价是安装包与运行内存增加，并需要持续跟进 Electron 安全版本、原生模块 ABI 和三平台打包验证。

## Rejected Alternatives

- **Tauri + Rust Core**：当前会扩大重写风险；未来若要采用，必须有独立 ADR 和等价安全证据。
- **Electron + .NET Core**：保留双核心，不能解决行为漂移。
- **继续分别维护原生 GUI**：长期重复实现同一业务规则。

## Migration and Validation

Electron 写能力只能在 Node Public API、结构化合同和共享 Fixtures 建立后开放。三平台 packaged smoke test、Renderer 隔离和 SQLite 驱动矩阵是 Stable 的硬门槛。

## Related

- [vNext 架构基线](../VNEXT_ELECTRON_NODE_ARCHITECTURE_ZH.md)
- [迁移执行索引](../migration/VNEXT_MIGRATION_EXECUTION_INDEX_ZH.md)
