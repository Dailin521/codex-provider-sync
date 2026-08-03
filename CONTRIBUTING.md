# Contributing to codex-provider-sync

感谢你愿意帮助改进 codex-provider-sync。Issues、Pull Requests 和文档可以使用中文或英文。

This project welcomes issues, pull requests, documentation, and tests in either Chinese or English.

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
- PowerShell 7（运行 Windows 发布脚本时）

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
- 不要为了测试而操作真实的 `~/.codex`；使用临时目录和测试夹具。
- 不要绕过备份、SQLite Home 解析、WSL UNC 安全阻断或跨 SQLite Home 恢复确认。
- 不要修改消息正文、认证信息、`auth.json` 或 `updated_at`。
- 修改用户可见行为、命令参数或安全边界时，同步更新中文和英文文档。
- GUI 布局改动请附截图，并注明平台、缩放比例和是否完成真实手测。

## 按改动范围验证

| 改动范围 | 最低验证要求 |
| --- | --- |
| 文档 | 检查链接、路径、命令和中英文内容是否一致 |
| Node.js CLI | `npm test` |
| 共享 .NET Core | Core Tests；涉及 CLI 时同时运行 `npm test` |
| Windows GUI | Core Tests、App Tests；布局改动附 Windows 截图或说明未手测原因 |
| macOS GUI | Core Tests、macOS Release build；真实 macOS GUI 手测无法完成时，在 PR 中明确记录 |
| WSL/SQLite 路径 | 相关自动化测试；条件允许时运行真实 WSL 安全脚本 |
| 发布工作流 | 不要创建测试 `v*` Tag；通过 PR 验证 YAML、权限和构建逻辑 |

无法运行某项平台测试并不会自动阻止贡献，但必须在 PR 的 `Not run` 中写明原因和剩余风险。

## 提交 Pull Request

1. 从最新的 `main` 创建功能分支。
2. 完成范围明确的修改和相关验证。
3. 使用简洁、可读的提交说明。
4. 推送分支并创建 PR；不要直接修改 `main`。
5. 按 PR 模板填写目的、改动、测试结果、平台影响和风险。
6. 等待 CI 的 `ci-gate` 通过，并解决所有审查对话。

PR 中请特别说明：

- 为什么需要这项修改，而不只是修改了什么。
- 是否会写入 `config.toml`、rollout、SQLite 或备份。
- 是否影响 Windows、macOS、WSL 或 CLI。
- 自动化测试、真实手测和未执行项目。
- GUI 变化的前后截图。

## 发布

正式版本由维护者从受保护的 `main` 创建 `v*` Tag。贡献者不应创建、移动或删除发布 Tag，也不要为了验证工作流创建测试版本标签。

## License

提交贡献即表示你同意按照本仓库的 [MIT License](LICENSE) 发布你的贡献。
