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

当前公开发布的桌面端仍是 .NET Windows GUI；`apps/desktop` 是 vNext Electron 未发布候选。只有 Phase 6 退出门槛和单独发布授权闭合后，文档才会切换为 Electron 主入口并把 .NET 标记为 Legacy。贡献和评审中不得提前把候选描述为默认、Stable 或已发布产品。

| 路径 | 内容 |
| --- | --- |
| `src/` | 兼容 Node.js CLI、Local Web Host 与 Node Core 实现 |
| `apps/cli/` | vNext CLI workspace 边界 |
| `apps/web/` | 共享 React UI 的 Local Web 组合与浏览器 E2E |
| `apps/desktop/` | vNext Electron 候选：Main、Preload、Renderer、Utility Process、打包与 E2E |
| `packages/` | Core、Contracts、CoreClient、App UI、Design System 与脱敏 Test Fixtures |
| `web/` | 根 npm 包携带的 Local Web UI production 输出与兼容入口 |
| `test/` | Node.js 自动化测试 |
| `desktop/CodexProviderSync.Core/` | 当前发布桌面端共用的 .NET 核心；迁移期保持锁协议和兼容维护 |
| `desktop/CodexProviderSync.Application/` | 当前 Windows GUI 与 Automation 共用的应用用例 |
| `desktop/CodexProviderSync.App/` | 当前已发布的 Windows WinForms GUI |
| `desktop/CodexProviderSync.Mac/` | 迁移期保留的 macOS Avalonia 本地构建 |
| `desktop/CodexProviderSync.Automation/` | 实验性的 Windows Automation 接口 |
| `desktop/*Tests/` | .NET 自动化测试 |
| `scripts/` | GUI 构建和 WSL 安全验证脚本 |
| `docs/` | 用户文档和维护文档 |

## 开发环境

基础开发需要：

- Git
- Node.js 16.20.2 或更高版本用于根 CLI 包；现代 workspace、Web 构建和 Electron 使用 Node 24
- npm
- .NET 10 SDK（修改 .NET Core 或 GUI 时）
- PowerShell 7（修改或验证 Windows 打包脚本时）

安装依赖并运行 CLI 测试：

```bash
npm ci
npm test
npm run web:build
npm run workspaces:check
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
| Local Web UI | `npm run web:build`、`npm test`；界面改动附浏览器截图或说明未手测原因 |
| vNext Electron 候选 | `npm run desktop:test`、`npm run desktop:test:e2e`、production bundle 审计；界面自动化默认 hidden，平台打包改动还需原生候选容器 smoke |
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

## 准备发布

CLI/Web npm 包、当前 .NET Windows GitHub Release 和未来 Electron Release 是不同的受控发布路径，版本号可能不同。任何贡献或 CI 候选都不会自动授权公开发布。

### CLI / Web npm 包

按 [npm 发布维护指南](docs/NPM_PUBLISHING.md) 更新 `package.json` 与 `package-lock.json`、完成构建和测试，并从 `main` 手动运行受信发布工作流。仅发布 CLI/Web 时不创建 Git tag 或 Windows Release。

### 当前 .NET Windows GitHub Release

发布 tag 前需要：

1. 将 [中文发布说明模板](docs/release-notes/TEMPLATE-zh.md) 复制为 `docs/release-notes/v<版本>-zh.md`。
2. 填写文件顶部的 `release-title`、面向用户的升级结果、下载、升级说明、安全边界、验证结果和实际贡献者。
3. 更新 `CHANGELOG.md`，并确认 `package.json`、`package-lock.json` 和所有发布项目版本一致。
4. 运行 `node scripts/read-release-metadata.js --tag v<版本>` 和 `node scripts/verify-release-version.js --tag v<版本>`。
5. 运行完整测试和发布构建，再创建指向 `main` 中已验证提交的 tag。

发布工作流会读取与 tag 同名的中文发布说明，并生成单文件 GUI、独立 Automation ZIP、Windows 完整包和对应 SHA-256。缺少发布说明、标题与 tag 不匹配，或遗漏固定的下载、安全和限制声明时会直接停止。

这一流程只描述当前 .NET Windows Release，不授权发布 `apps/desktop` Electron。Electron 候选固定使用 `--publish never`；公开 tag、GitHub Release、签名、公证、更新 metadata 和跨版本升级验证都必须在 C10 证据闭合后另行授权。

## English quick guide

- Small fixes, tests, and documentation updates can be submitted directly as a PR. Please open an Issue before starting a large feature, behavior change, or refactor.
- If you do not have write access, fork the repository, push your branch to your fork, and open a PR against this repository's `main` branch.
- Use Node.js 16.20.2 or later for the compatible root CLI package. Modern workspaces, Web builds, and Electron use Node 24; run `npm ci`, `npm test`, `npm run workspaces:check`, and the affected Web or Electron gates. Changes to shared .NET code also require the relevant .NET 10 tests listed above.
- Automated tests and reproduction scripts must use temporary directories or fixtures and must not depend on, read, or modify a real user's `~/.codex`. Prefer a dedicated test Codex Home for manual validation and describe its scope in the PR.
- Never include unredacted credentials, `auth.json`, Codex sessions, SQLite databases, backups, logs, tokens, or personal data.
- Keep each PR focused. Explain why the change is needed, what it writes, which platforms it affects, what was tested, and what was not tested.
- GUI changes should include screenshots and the platform and display scaling used. If real macOS GUI testing is unavailable, say so clearly; it is not automatically a reason to reject the contribution.
- Update affected documentation when user-visible behavior, command options, or safety boundaries change.
- All changes go through a PR and must pass `ci-gate`.
- The CLI/Web npm package and Windows GitHub Release are independent release channels; follow `docs/NPM_PUBLISHING.md` for npm releases.
- The published desktop product is still the Windows .NET GUI. `apps/desktop` is an unreleased Electron candidate and must not be called default, Stable, or released until Phase 6 and separately authorized release validation close.

## License

提交贡献即表示你同意按照本仓库的 [MIT License](LICENSE) 发布你的贡献。

By contributing, you agree that your contribution will be licensed under the repository's [MIT License](LICENSE).
