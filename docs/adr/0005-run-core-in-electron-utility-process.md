# vNext/ADR-0005：Electron 通过 Utility Process 运行 Core

- Status: Accepted
- Date: 2026-08-24
- Scope: vNext

## Context

rollout 扫描、SQLite、备份、恢复和 Watch 可能长时间运行。把它们放在 Electron Main 会阻塞窗口生命周期；放在 Renderer 会破坏安全边界。

## Decision

Electron 使用 `utilityProcess` 承载与 CLI 相同的 Node Core。Main 中的 Supervisor 负责懒启动、版本握手、请求路由、进度转发、取消、崩溃归类和优雅关闭。

Utility Process 是 Core 的执行位置，不是第二套业务核心。

## Decision Drivers

- Main 保持响应；
- 进程崩溃与 UI 崩溃相互隔离；
- 复用同一 Node Runtime 与模块；
- 建立可版本化的异步消息边界。

## Invariants

- Main 不实现同步算法、SQL、备份或恢复规则；
- Runtime 启动先握手 protocol/app/core 版本；
- 所有请求有 requestId，写操作有 operationId；
- Utility Process 崩溃时 pending 请求归类为 `CORE_RUNTIME_CRASHED`；
- 不无限自动重启；下一次用户重试至多重启一次，并先检查 Pending Transaction；
- 进度观察器失败不得改变事务结果；
- 取消只能发生在 Core 定义的安全点。

## Consequences

需要 Runtime Schema、Supervisor 状态机和崩溃 E2E，但避免 Main 阻塞，也让桌面和 CLI 共享真正的业务实现。

## Rejected Alternatives

- **在 Main 直接执行长任务**：影响窗口和更新生命周期。
- **Worker Thread 作为唯一隔离**：不能提供同等级进程崩溃隔离。
- **独立常驻本地服务**：增加安装、认证和生命周期复杂度。

## Migration and Validation

阶段 3 先开放只读调用，验证握手、并发、崩溃和关闭；阶段 4 才允许写操作。协议不兼容必须 fail closed。

## Related

- [Renderer 安全 ADR](0004-renderer-has-no-node-access.md)
- [错误码合同](../architecture/contracts/ERROR_CODES_ZH.md)
