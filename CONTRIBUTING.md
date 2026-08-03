# Contributing to codex-provider-sync

感谢你愿意帮助改进 codex-provider-sync。Issues、Pull Requests 和文档可以使用中文或英文。

This project welcomes issues, pull requests, documentation, and tests in either Chinese or English.

[English quick guide](#english-quick-guide)

## 开始之前

- 小型修复、测试和文档改进可以直接提交 PR。
- 新功能、行为变化或较大的重构，请先创建 Issue，说明使用场景、预期行为和平台影响。
- 请先搜索现有 Issues 和 PR，避免重复工作。
- 请勿提交真实的 `auth.json`、`config.toml`、Codex 会话、SQLite 数据库、备份、日志、访问令牌或其他个人信息。
- 安全问题的复现材料必须经过脱敏；不要把凭据或私人会话内容公开在 Issue 或 PR 中。

## 项目结构

| 路径 | 内容 |
| --- | --- |
| `src/` | Node.js CLI 和同步逻辑 |
| `test/` | Node.js 自动化测试 |
| `desktop/CodexProviderSync.Core/` | Windows 与 macOS GUI 共用的 .NET 应用逻辑 |
| `desktop/CodexProviderSync.App/` | Windows WinForms GUI |
| `desktop/CodexProviderSync.Mac/` | macOS Avalonia GUI |
| `desktop/*Tests/` | .NET 自动化测试 |
| `scripts/` | GUI 构建和 WSL 安全验证脚本 |
| `docs/` | 用户文档和维护文档 |

## 开发环境

基础开发需要：

- Git
- Node.js 16 或更高版本；CI 同时验证 Node.js 16 和 24
- npm
- .NET 10 SDK（修改 .NET Core 或 GUI 时）
- PowerShell 7（修改或验证 Windows 打包脚本时）

安装依赖并运行 CLI 测试：

```bash
npm ci
npm test
```

运行共享 Core 和 Windows GUI 测试：

```powershell
dotnet test desktop/CodexProviderSync.Core.Tests/CodexProviderSync.Core.Tests.csproj
dotnet test desktop/CodexProviderSync.App.Tests/CodexProviderSync.App.Tests.csproj
```

验证 macOS GUI 项目：

```bash
dotnet test desktop/CodexProviderSync.Core.Tests/CodexProviderSync.Core.Tests.csproj
dotnet build desktop/CodexProviderSync.Mac/CodexProviderSync.Mac.csproj --configuration Release
```

只有修改 WSL UNC 路径或跨 Windows/WSL SQLite 行为时，才需要在 WSL 中运行：

```bash
./scripts/test-wsl-unc-safety.sh
```

## 修改原则

- 保持 PR 范围单一，避免把无关重构和功能修改混在一起。
- 优先补充能够复现问题并验证修复的自动化测试。
- 自动化测试和复现脚本必须使用临时目录或测试夹具，不得依赖、读取或改写真实用户的 `~/.codex`。人工验证时优先使用专用测试 Codex Home，并在 PR 中说明验证范围。
- 不要绕过备份、SQLite Home 解析、WSL UNC 安全阻断或跨 SQLite Home 恢复确认。
- 不要修改消息正文、认证信息、`auth.json` 或 `updated_at`。
- 修改用户可见行为、命令参数或安全边界时，更新受影响的文档；同一行为同时有中英文说明时，请保持一致。
- GUI 布局改动请附截图，并注明平台、缩放比例和是否完成真实手测。

## 按改动范围验证

| 改动范围 | 最低验证要求 |
| --- | --- |
| 文档 | 检查链接、路径和命令；同一内容有多个语言版本时保持一致 |
| Node.js CLI | `npm test` |
| 共享 .NET Core | Core Tests；涉及 CLI 时同时运行 `npm test` |
| Windows GUI | Core Tests、App Tests；布局改动附 Windows 截图或说明未手测原因 |
| macOS GUI | Core Tests、macOS Release build；真实 macOS GUI 手测无法完成时，在 PR 中明确记录 |
| WSL/SQLite 路径 | 相关自动化测试；条件允许时运行真实 WSL 安全脚本 |
| CI / GitHub Actions | 检查 YAML、权限和受影响的工作流行为；相关构建命令能在本地运行时一并验证 |

无法运行某项平台测试并不会自动阻止贡献，但必须在 PR 的 `Not run` 中写明原因和剩余风险。

## 提交 Pull Request

1. 没有本仓库 Write 权限时，先 Fork 仓库并克隆自己的 Fork。
2. 从最新的上游 `main` 创建功能分支。
3. 完成范围明确的修改和相关验证。
4. 使用简洁、可读的提交说明，并将分支推送到自己的 Fork 或有权限的远程分支。
5. 创建一个目标为本仓库 `main` 的 PR；不要直接修改 `main`。
6. 按 PR 模板填写目的、改动、测试结果、平台影响和风险。
7. 等待 CI 的 `ci-gate` 通过，并解决所有审查对话。

PR 中请特别说明：

- 为什么需要这项修改，而不只是修改了什么。
- 是否会写入 `config.toml`、rollout、SQLite 或备份。
- 是否影响 Windows、macOS、WSL 或 CLI。
- 自动化测试、真实手测和未执行项目。
- GUI 变化的前后截图。

## English quick guide

- Small fixes, tests, and documentation updates can be submitted directly as a PR. Please open an Issue before starting a large feature, behavior change, or refactor.
- If you do not have write access, fork the repository, push your branch to your fork, and open a PR against this repository's `main` branch.
- Use Node.js 16 or later and run `npm ci` followed by `npm test`. Changes to shared .NET or desktop code also require the relevant .NET 10 tests listed above.
- Automated tests and reproduction scripts must use temporary directories or fixtures and must not depend on, read, or modify a real user's `~/.codex`. Prefer a dedicated test Codex Home for manual validation and describe its scope in the PR.
- Never include unredacted credentials, `auth.json`, Codex sessions, SQLite databases, backups, logs, tokens, or personal data.
- Keep each PR focused. Explain why the change is needed, what it writes, which platforms it affects, what was tested, and what was not tested.
- GUI changes should include screenshots and the platform and display scaling used. If real macOS GUI testing is unavailable, say so clearly; it is not automatically a reason to reject the contribution.
- Update affected documentation when user-visible behavior, command options, or safety boundaries change.
- All changes go through a PR and must pass `ci-gate`.

## License

提交贡献即表示你同意按照本仓库的 [MIT License](LICENSE) 发布你的贡献。

By contributing, you agree that your contribution will be licensed under the repository's [MIT License](LICENSE).
