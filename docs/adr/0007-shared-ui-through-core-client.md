# vNext/ADR-0007：通过 CoreClient 共享 UI

- Status: Accepted
- Date: 2026-08-24
- Scope: vNext

## Context

Local Web UI 与 Electron 需要共享 React 功能，但传输方式不同。若页面直接调用 `fetch`、`window` IPC 或 Core，组件会绑定平台并复制数据映射。

## Decision

共享 `packages/app-ui` 只依赖版本化的 `CoreClient` 接口。提供 `HttpCoreClient`、`DesktopCoreClient` 和 `MockCoreClient`，分别适配 Local Web、Electron 和测试。

## Decision Drivers

- 一套 Feature UI 与状态语义；
- 传输和业务模型分离；
- 页面可在无 Electron/真实 Codex Home 时测试；
- 避免 Web 与 Desktop 功能漂移。

## Invariants

- 页面和 feature 禁止直接调用 `fetch('/api/...')` 或 `window.codexProvider`；
- `app-ui` 不依赖 Electron、Node 或 DOM 之外的桌面能力；
- CoreClient DTO 与 `packages/contracts` 共享 Schema Version；
- Client 只做传输、序列化和错误映射，不复制业务规则；
- 测试通过 MockClient 和临时 Fixtures，不访问真实 `~/.codex`；
- App Shell 不承载页面专属业务状态。

## Consequences

需要维护客户端适配层和契约测试，但 React 功能可以复用、独立开发和跨入口一致验证。

## Rejected Alternatives

- **Web 与 Desktop 两套 UI**：长期漂移。
- **页面直接调用平台 API**：难以测试并绕过边界。
- **统一走 localhost HTTP**：Electron 写能力仍需额外认证和服务生命周期。

## Migration and Validation

先用 HttpCoreClient 包装现有 Web API并保持行为，再逐 feature 拆 UI；Electron Alpha 通过 DesktopCoreClient 接入同一页面。契约测试必须对两个实现运行。

## Related

- [Renderer 安全 ADR](0004-renderer-has-no-node-access.md)
- [Node Core 单一权威 ADR](0002-node-core-as-single-authority.md)
