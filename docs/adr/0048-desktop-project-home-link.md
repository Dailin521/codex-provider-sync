# ADR-0048：桌面端固定 GitHub 项目与发布页入口

- 状态：Accepted
- 日期：2026-09-28
- 范围：Windows Electron Desktop Host 与共享 UI；不改变 CoreFacade、Utility、Provider/Profile 操作、Web 或 Legacy。

## 背景

桌面用户需要从应用顶部快速查看本项目的公开 GitHub 主页，也需要从“设置 → 更新”直接查看发布页。两个入口均使用固定地址。

## 决策

- 顶栏状态控件旁提供独立图标按钮；中文辅助说明为“打开 GitHub 项目主页”，英文为“Open GitHub project home”。按钮不依赖 Provider 写入状态；旧 Host 不支持该能力时共享 UI 隐藏入口。
- `HostClient.openProjectHome(): Promise<void>` 仅委托 Preload 的 `project.openHome()`。Preload 只调用无输入 IPC `cps:v1:project:open-home`；Main 只接受可信顶层 sender，拒绝所有非 `null` payload，再通过 `shell.openExternal` 打开固定地址 `https://github.com/Dailin521/codex-provider-sync`，返回 `{opened:boolean}`。原生调用 resolve 后才表示成功，失败由 UI 显示。
- “设置 → 更新”提供“打开发布页”按钮；英文为“Open release page”。`HostClient.openReleasePage()` 经 `project.openReleases()` 调用无输入 IPC `cps:v1:project:open-releases`，Main 按相同 sender/payload 规则打开固定 `https://github.com/Dailin521/codex-provider-sync/releases`。此按钮不依赖更新检查结果、下载或安装状态，不改变更新状态；打开失败有反馈，旧 Host 不支持时隐藏。
- 不在启动时打开浏览器；不允许 Renderer 提供 URL、路径、channel 或其他参数。不接入 Core、Utility、Provider/Profile、操作日志或更新状态机。既有 CSP、导航、新窗口和权限默认拒绝策略保持。

## 后果与验证

这是窄的 Host 外链能力，不扩大 Renderer 的 Node、原始 IPC 或 Core 访问面。实现需覆盖 Main sender/payload/固定 URL/失败边界，UI 中英文、单次显式操作和错误反馈，以及 production 隐藏窗口中点击后由 Main 交接固定 URL、应用仍保持本地的边界；不以本 ADR 表示这些测试已运行或发布包已产出。

## 关联

- [ADR-0004：Renderer 不拥有 Node 访问能力](0004-renderer-has-no-node-access.md)
- [Core 外部行为合同](../architecture/contracts/CORE_EXTERNAL_BEHAVIOR_ZH.md)
- [行为 Fixture 清单](../migration/BEHAVIOR_FIXTURES_ZH.md)
