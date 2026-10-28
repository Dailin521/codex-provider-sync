# Contributing to codex-provider-sync

欢迎用中文或英文提交 Issue、修复、测试和文档。[English quick guide](#english-quick-guide)

## 提交一个小改动

1. 搜索现有 Issue / PR，避免重复。小修复和文档可直接提 PR；较大功能或重构建议先开 Issue 讨论。
2. Fork 仓库，从最新 `main` 创建分支，保持改动范围明确。
3. 运行与改动相关的检查，然后提交 PR。说明**改了什么、为什么、如何验证、哪些相关检查没跑**即可。

缺少某个平台或工具不妨碍先提交 PR，请注明原因，维护者会协助补充验证。UI 改动附截图有助于审阅。无需为普通修复新写 ADR，也无需准备发布证据；涉及新的架构决策或安全边界时，由维护者协助更新相应合同、ADR 和回归测试。

## 开发与验证

Node / Web / Electron 开发使用 Node.js 24，安装依赖：

```bash
npm ci
```

根 CLI 的 Node.js 16.20.2 兼容性由 CI 验证。只有修改 Legacy .NET 时才需要 .NET 10 SDK。

| 改动范围 | 本地相关检查 |
| --- | --- |
| 用户文档、翻译 | 检查链接、路径和命令，无需构建程序 |
| Node CLI | `npm test` |
| Core / 存储 / 依赖边界 | `npm run architecture:check`、`npm test` |
| Web UI | `npm run web:build`、相关测试；关键交互补浏览器 E2E |
| Electron | `npm run desktop:test`、相关 E2E 和 production bundle 检查；打包变化补对应容器验证 |
| Legacy .NET | 相关项目的 `dotnet test` / `dotnet build` |
| CI / 构建脚本 | 相关工作流回归测试和可在本地运行的命令 |

只改文档不必安装 Node 或 .NET。涉及平台句柄、SQLite、WSL 或更新安装时，补对应平台验证，或注明未运行的原因。测试通过后无需反复执行无关检查。

## 数据安全与代码约定

- 不提交真实凭据、`auth.json`、会话、数据库、备份或未脱敏日志。测试只使用临时目录或合成夹具，不操作真实 Codex Home。
- 修改写入逻辑时，保留备份优先、Home 锁、SQLite 事务、路径边界和 Restore 校验；不改消息正文或线程 `updated_at`。
- Core 开发先读 [当前架构与 Provider I/O 不变量](docs/architecture/NODE_CORE_ARCHITECTURE_ZH.md)。用户可见行为变化需同步相关说明和回归测试；翻译与文档整理可由维护者协助补齐。
- `src/` 保留成熟存储算法与兼容入口，`packages/` 放 Core / 合同 / 共享 UI，`apps/` 放 CLI / Web / Electron；`desktop/` 是 Legacy .NET。不要在界面层复制存储算法。

## PR 与发布检查

PR 使用统一的 `ci-gate`：

- **普通文档 PR**：用户指南、翻译、贡献说明等只检查文档链接，无需安装依赖；修改正式发布说明时才额外验证版本与发布说明格式。
- **界面 PR**：Web 检查工作区与 Web，桌面 Renderer 检查工作区与 Windows Electron，共享 UI 检查两端。工作区在 Linux / Node 24 验证，桌面生产包仍须通过检查。
- **其他代码 PR**：Electron 主进程、运行时或打包改动运行根测试、工作区和三平台桌面检查；纯 Legacy .NET 改动运行根测试、跨运行时恢复及 Legacy 平台检查。Core、SQLite、共享合同、依赖、CI、未知路径及跨领域混合改动保留完整代码检查。
- **main push**：额外执行四目标 Electron 候选包、候选集合校验、历史正式 Release 恢复取证和 C10 证据包。PR 无需等待这些发布取证任务，来自 Fork 的 PR 也不会因此被主动拒绝。

审查意见由维护者判断是否需要处理，GitHub 不再要求逐条关闭所有对话才能合并。Codex 审查按仓库规则仅已核实的 P0 阻断，不要求反复审查。最终发布仍须通过 main 对应 SHA 的完整验收并取得发布授权，PR 通过不代表可以发布。

精确的文档分类与检查限制见 [CI 规则](docs/WINDOWS_ELECTRON_RELEASE_ZH.md#普通文档-pr-的轻量检查)。发布由维护者按 [Electron 发布流程](docs/WINDOWS_ELECTRON_RELEASE_ZH.md) 或 [npm 发布指南](docs/NPM_PUBLISHING.md) 处理。

## English quick guide

- Small fixes, tests and documentation can go straight to a PR. Discuss larger features or refactors in an issue first.
- Fork from `main`, keep the change focused, and explain what changed, why, tests run and relevant checks not run. Missing a platform is not a reason to withhold a contribution; maintainers can help finish validation.
- Use Node.js 24 and `npm ci` for Node / Web / Electron development. .NET 10 is only needed for Legacy .NET changes. Documentation alone needs neither toolchain.
- Core changes require `npm run architecture:check` and `npm test`. Preserve the [Provider I/O invariants](docs/architecture/NODE_CORE_ARCHITECTURE_ZH.md), backup, locking, transactions and path boundaries. Use synthetic fixtures, never real Codex data or credentials.
- Ordinary documentation checks local links; formal release-note changes also validate versions and announcement metadata. Web, desktop renderer, shared UI and Legacy .NET changes run their relevant checks. UI uses Linux / Node 24 workspace checks and Windows desktop smoke; desktop runtime or packaging changes keep three-platform desktop checks. Core, dependencies, shared contracts, CI and unknown paths retain full code checks. Main pushes retain all platforms and release evidence.
- Report relevant validation only; screenshots help for UI changes. Maintainers can help with translations, contracts and documentation. Routine fixes do not require a new ADR or release evidence.
- Merge requires a PR and `ci-gate`; resolving every review conversation is no longer a GitHub requirement. Maintainers assess comments, and verified P0 Codex findings still block delivery. Publication separately requires complete acceptance on the final main SHA and explicit authorization.

## License

By contributing, you agree that your contributions will be licensed under the repository's MIT license.
