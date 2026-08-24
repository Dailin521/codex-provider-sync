# vNext/ADR-0004：Renderer 不拥有 Node 访问能力

- Status: Accepted
- Date: 2026-08-24
- Scope: vNext

## Context

Renderer 展示来自本地 Codex 数据的内容。若页面可直接访问文件系统、进程或原始 IPC，渲染层漏洞会直接越过 Core 的路径、认证和事务边界。

## Decision

所有 BrowserWindow 必须启用 `nodeIntegration: false`、`contextIsolation: true`、`sandbox: true` 和 `webSecurity: true`。Preload 只暴露按版本命名、按用途收窄并校验 Schema 的接口。

## Decision Drivers

- 把不可信展示面与本地文件写能力隔离；
- 让 IPC 能够审计、测试和版本化；
- 防止 UI 绕过 Core 的 Plan、Lock 和 Backup；
- 避免 React 代码与 Electron 运行时耦合。

## Invariants

- Renderer 禁止导入 `node:*`、Electron 或 Core 内部模块；
- 禁止暴露原始 `ipcRenderer`、通用 `send/invoke/on` 或任意文件路径写入；
- IPC Handler 必须校验 sender、payload、长度、枚举和版本；
- 只加载本地打包内容，启用严格 CSP，导航/新窗口/权限默认拒绝；
- 消息正文仅在用户主动打开 History 时本地展示，不进入日志、遥测或诊断包；
- `auth.json`、令牌和凭据永远不通过 IPC。

## Consequences

桌面功能需要显式 Preload/IPC 合同，开发成本略增；换来边界清晰、测试可替换以及 Renderer 被攻破时更小的影响范围。

## Rejected Alternatives

- **在 Renderer 开启 Node**：风险不可接受。
- **暴露通用 IPC 包装器**：接口表面不可审计。
- **Renderer 直接调用 Core**：破坏进程隔离和运行时监督。

## Migration and Validation

Electron Alpha 必须有静态依赖检查、安全配置测试、未知 IPC 拒绝测试和 packaged E2E；写能力开放前还必须验证 Renderer 只能提交 profileId、planId 或 managed backup ID。

## Related

- [Utility Process ADR](0005-run-core-in-electron-utility-process.md)
- [Shared UI ADR](0007-shared-ui-through-core-client.md)
