<div align="center">

# codex-provider-sync

### 切换 Provider 后，让 Codex 历史会话重新可见

[![CI](https://github.com/Dailin521/codex-provider-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/Dailin521/codex-provider-sync/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/Dailin521/codex-provider-sync)](https://github.com/Dailin521/codex-provider-sync/releases/latest)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

[下载 Windows GUI](https://github.com/Dailin521/codex-provider-sync/releases/latest) · 中文 · [English](docs/README_EN.md)

</div>

## 什么时候需要它

Codex 切换 `model_provider` 后，旧会话可能从 Desktop 或 `/resume` 中消失。会话通常没有丢失，而是 rollout、SQLite 和项目可见性 metadata 仍指向原 Provider。

适合使用本工具：

- 在官方订阅（内部 Provider 为 `openai`）和自定义中转之间切换。
- 多个配置必须使用不同的 `model_provider`，切换后旧会话不可见。
- rollout 与 SQLite 中的 Provider 或 model 信息不一致。
- 希望在配置或 SQLite/WAL 变化后自动重新同步。

如果所有中转都能稳定复用同一个 `model_provider`，并且历史会话始终可见，那么统一 Provider ID 是更简单的方案，不需要额外同步。本工具主要用于无法统一 Provider ID，或需要在官方订阅与自定义 Provider 之间切换的场景。

本工具不负责登录、认证或切换账号；请先用原有方式完成 Provider 切换，再执行同步。

## 它会处理什么

- 同步 `~/.codex/sessions` 和 `~/.codex/archived_sessions` 中的 rollout metadata。
- 同步 Codex SQLite 线程记录，并支持 SQLite 与 `Codex Home` 分开存放。
- 修复项目可见性相关路径信息，并在需要时同步相关 model metadata。
- 每次同步前自动备份，支持恢复和清理旧备份。
- 大型 rollout 文件在满足条件时原地更新，否则自动使用完整安全重写。
- CLI `watch` 可监听 `config.toml`、SQLite 及 WAL 变化并自动同步。

## 快速使用

### Windows GUI

普通 Windows 用户建议直接从 [Releases](https://github.com/Dailin521/codex-provider-sync/releases/latest) 下载并解压：

1. 打开 `CodexProviderSync.exe`
2. 点击“刷新”
3. 选择目标 Provider
4. 点击“立即同步”

GUI 会保留备份并显示同步结果。每天首次启动会在后台检查一次稳定版更新，网络查询最多等待 10 秒；也可以随时手动检查。执行日志保存在 `%AppData%\codex-provider-sync\logs`。

Windows GUI 支持为每个 Codex Home 单独指定 Windows 文件系统中的 SQLite Home。`\\wsl.localhost\...` 和 `\\wsl$\...` 一类 WSL UNC 路径仅用于安全诊断；GUI 会显示诊断信息并禁用同步和恢复。Windows Codex Home + WSL SQLite Home 场景应在 WSL 内运行 CLI。

项目目前未做 Windows 代码签名，从浏览器下载后可能出现 SmartScreen 提示。请从本项目 Release 下载，并按需核对同版本 SHA-256。

Windows 完整说明见 [README_GUI_ZH.md](docs/README_GUI_ZH.md)。macOS 用户可自行构建 Avalonia 桌面版，分别参见 [中文说明](docs/README_MAC_GUI_ZH.md)和[英文说明](docs/README_MAC_GUI_EN.md)。

### CLI

CLI 支持 Node.js `16+`：

```bash
npm install -g git+https://github.com/Dailin521/codex-provider-sync.git
codex-provider status
codex-provider sync
```

常用命令：

| 命令 | 用途 |
| --- | --- |
| `codex-provider status` | 检查当前 Provider、rollout、SQLite 和项目可见性 |
| `codex-provider sync` | 将历史会话同步到当前 Provider，不修改登录状态 |
| `codex-provider switch <provider-id>` | 修改根级 `model_provider` 后执行同步 |
| `codex-provider restore <backup-dir>` | 从指定备份恢复 |
| `codex-provider prune-backups --keep 5` | 只保留最近 5 份托管备份 |
| `codex-provider watch` | 监听配置、SQLite 和 WAL 变化并自动同步 |
| `codex-provider watch --once` | 第一次变化并成功同步后退出 |

`switch` 支持 `--model <NAME>` 显式设置根级 model，或使用 `--keep-root-model` 只切换 Provider。所有主要命令都支持 `--codex-home <PATH>` 和 `--sqlite-home <PATH>`。

SQLite Home 按以下顺序解析：命令行 override → `config.toml` 根级 `sqlite_home` → `CODEX_SQLITE_HOME` → `<Codex Home>/sqlite`。只有最后一种默认布局会继续检查旧路径 `<Codex Home>/state_5.sqlite`；一旦显式指定 SQLite Home，就不会回退到 Codex Home 中的旧数据库。

例如 Codex App 使用 Windows 配置、app-server 与 SQLite 位于 WSL 时，可在 WSL CLI 中直接传入：

```bash
codex-provider status --codex-home /mnt/c/Users/you/.codex --sqlite-home /home/you/.codex/sqlite
codex-provider sync --codex-home /mnt/c/Users/you/.codex --sqlite-home /home/you/.codex/sqlite
```

`status` 会显示 effective SQLite Home 和来源。显式路径缺少 `state_5.sqlite` 时，状态查询只报告诊断，`sync`、`switch` 和数据库恢复不会偷偷回退到其它位置。默认布局中的数据库被删除时，`restore` 可以根据备份 metadata 在原默认位置重建数据库。

### Business Automation API（v0.4 实验性）

v0.4 Windows Release 构建同时包含 `CodexProviderSync.Automation.exe` 和 `automation-protocol-v0.4.schema.json`。这个一次性进程接口与 Windows GUI 共用同一套 Application 用例；每次调用只在 stdout 输出一份协议 `0.4` JSON，诊断信息写入 stderr。

| 命令 | 用途 |
| --- | --- |
| `describe` | 描述协议能力和安全要求 |
| `status` | 读取状态和诊断 |
| `plan --operation sync\|switch\|restore\|prune` | 为指定写操作创建计划 |
| `sync` | 规划或显式执行同步 |
| `switch` | 规划或显式执行 Provider/model 切换与同步 |
| `restore` | 规划或显式执行备份恢复 |
| `prune` | 规划或显式清理托管备份 |

所有写命令默认都是 dry-run，只返回计划，不会修改目标。实际写入必须同时提供 `--apply`、只包含 `plan` 响应中 `data` 对象的计划文件，以及该对象的精确小写 SHA-256 `digest`：

```powershell
.\CodexProviderSync.Automation.exe describe
.\CodexProviderSync.Automation.exe status --codex-home C:\isolated\.codex
.\CodexProviderSync.Automation.exe sync --codex-home C:\isolated\.codex --provider openai
$planResponse = .\CodexProviderSync.Automation.exe plan --operation sync --codex-home C:\isolated\.codex --provider openai | ConvertFrom-Json
$planResponse.data | ConvertTo-Json -Depth 100 -Compress | Set-Content -LiteralPath C:\isolated\sync-plan.json -Encoding utf8NoBOM
$planDigest = $planResponse.data.digest
.\CodexProviderSync.Automation.exe sync --codex-home C:\isolated\.codex --provider openai --apply --plan C:\isolated\sync-plan.json --plan-digest $planDigest
```

计划有有效期、绑定规范化输入和目标状态，并由持久化 ledger 保证只能使用一次；默认 ledger 位于 `<Codex Home>\tmp\provider-sync-automation-ledger`。所有路径参数必须是绝对路径，不能穿过符号链接或 reparse point，Automation 也拒绝直接指向或访问 `auth.json`。协议仍处于 pre-1.0 实验阶段，`0.4` 之外不承诺兼容。

## 安全与限制

每次 `sync` / `switch` 前都会备份到：

```text
~/.codex/backups_state/provider-sync/<timestamp>
```

- 不修改消息历史、会话标题、认证信息、`auth.json` 或 `updated_at`。
- 不在多台设备之间复制配置或会话文件；它只修复当前 Codex Home 的 metadata。
- SQLite 被占用时，需要先关闭 Codex、Codex App 和 app-server 后重试。
- Windows 进程检测到 WSL UNC SQLite Home 时会立即显示专用安全诊断并停止操作。后续操作应进入对应 WSL 发行版，并使用 `/home/...` 形式的 Linux 路径运行 CLI。
- 新备份使用 metadata v2 记录独立 SQLite Home；恢复到其它 SQLite Home 默认拒绝。CLI 需要同时传入 `--sqlite-home`、`--allow-sqlite-home-relocation` 和 `--no-config`，避免恢复后的 `config.toml` 重新指向原 SQLite Home。
- 活跃会话锁住 rollout 文件时，工具会跳过该文件并继续处理其它会话；结束活跃会话后可再次同步。
- 含 `encrypted_content` 的会话跨 Provider/account 后，可能只能恢复列表可见性，继续对话或 compact 仍可能报 `invalid_encrypted_content`。
- Codex Desktop 首屏目前只显示最近 50 条会话。若 `/resume` 可见但项目侧仍不显示，请查看状态中的 `first page` / `ranks` 诊断；本工具不会修改时间戳来绕过此限制。

## 文档

- [Windows GUI 说明](docs/README_GUI_ZH.md)
- macOS GUI 说明：[中文](docs/README_MAC_GUI_ZH.md) · [English](docs/README_MAC_GUI_EN.md)
- [v0.4.0 Release Notes（Draft）](docs/RELEASE_NOTES_V0.4.0.md)
- [v0.4 Automation 执行计划](docs/V0.4_AUTOMATION_PLAN.md)
- [English documentation](docs/README_EN.md)
- [AI / Agent 操作指南](AGENTS.md)
- [贡献指南](CONTRIBUTING.md)

## 开发

```bash
git clone https://github.com/Dailin521/codex-provider-sync.git
cd codex-provider-sync
npm test
dotnet test desktop/CodexProviderSync.Core.Tests/CodexProviderSync.Core.Tests.csproj
./scripts/test-wsl-unc-safety.sh
pwsh ./scripts/publish-gui.ps1
pwsh ./scripts/run-windows-gui-e2e.ps1
./scripts/publish-gui-macos.sh
```

`test-wsl-unc-safety.sh` 需要从 WSL 运行，并调用 Windows `dotnet.exe` 验证真实 WSL ext4 SQLite 的安全阻断。`run-windows-gui-e2e.ps1` 必须在可见、可交互的 Windows 桌面中运行。v0.4 的实现提交 `c5af7c3` 已通过该 gate：40/40 manifest 入口覆盖、53/53 必需场景通过、0 error、0 blocker，且发布 EXE 哈希、真实控件事件、原生对话框、文件/SQLite 差异、重启持久化和 GUI → Application trace 均由证据门禁核验。后续若修改相关实现必须重新运行；隐藏、跳过或直接调用 Application 不能替代真实 GUI PASS。

## License

MIT
