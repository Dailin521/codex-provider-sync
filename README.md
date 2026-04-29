<div align="center">

# codex-provider-sync

### 切换 provider 后，让 Codex 历史会话重新可见

[![CI](https://github.com/Dailin521/codex-provider-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/Dailin521/codex-provider-sync/actions/workflows/ci.yml)
[![Platform](https://img.shields.io/badge/platform-Windows-lightgrey.svg)](https://github.com/Dailin521/codex-provider-sync)
[![Node](https://img.shields.io/badge/node-24%2B-brightgreen.svg)](https://nodejs.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

[English](README_EN.md) | 中文

</div>

## 解决什么问题

Codex 切换 `model_provider` 后，旧会话可能在 Desktop 或 `/resume` 里不可见。原因通常不是会话文件丢了，而是 rollout 文件、SQLite 线程表、项目路径缓存里的 provider / 可见性 metadata 不一致。

本工具同步这些位置：

- `~/.codex/sessions`
- `~/.codex/archived_sessions`
- `~/.codex/state_5.sqlite`
- `.codex-global-state.json` 中的项目根路径缓存

## 快速使用

Windows 用户优先下载 Release 里的 `CodexProviderSync.exe`：

1. 打开 `CodexProviderSync.exe`
2. 点击 `Refresh`
3. 选择目标 Provider
4. 点击 `Execute`

macOS 等环境使用 CLI：

```bash
npm install -g github:Dailin521/codex-provider-sync
codex-provider sync
```

更多 CLI 常用命令：

```bash
codex-provider status
codex-provider sync
codex-provider sync --provider openai
codex-provider switch apigather
codex-provider restore C:\Users\you\.codex\backups_state\provider-sync\<timestamp>
codex-provider prune-backups --keep 5
```

命令含义：

- `status`：只检查当前 provider、rollout、SQLite、项目可见性诊断。
- `sync`：不切换登录状态，只把历史会话 metadata 同步到当前 provider。
- `switch <provider-id>`：修改 `config.toml` 根级 `model_provider`，然后执行同步。
- `restore <backup-dir>`：从备份恢复，支持 `--no-config`、`--no-db`、`--no-sessions`。
- `prune-backups --keep <n>`：只清理本工具创建的旧备份。

## 能力边界

本工具只修复“历史会话可见性”相关 metadata，不修改会话内容。

- 不处理登录、认证、`auth.json` 或第三方切号工具。
- 不修改消息历史、会话标题、对话内容。
- 不修改 `updated_at`，不通过改变历史排序来修复 Desktop 显示。
- 不把旧会话里的 `encrypted_content` 重新加密到另一个 provider / account。
- 含 `encrypted_content` 的旧会话跨 provider/account 后，通常只能恢复列表可见性，继续对话或 compact 仍可能报 `invalid_encrypted_content`。

## Codex Desktop 最近 50 条限制

目前 Codex Desktop 的 Recent/项目侧会话列表存在一个上游显示限制：前端首屏只拉取最近 `50` 条会话。

影响：

- CLI `/resume` 能看到的旧会话，Desktop 项目侧可能仍显示“暂无对话”。
- 旧项目会话如果排在全局最近 50 条之后，Desktop 首屏可能不会展示。
- `codex-provider-sync status` / GUI Refresh 会显示 `first page 0/50`、`ranks 64-77` 这类诊断，帮助判断是不是这个问题。

本工具不会通过修改 `updated_at` 或文件时间把旧会话强行挤进前 50。这个问题应由 Codex Desktop 上游改成按项目分页加载，或提高/开放首屏加载数量。

## 安全与排障

每次 `sync` / `switch` 前都会备份到：

```text
~/.codex/backups_state/provider-sync/<timestamp>
```

注意：

- 如果 `state_5.sqlite` 被占用，关闭 Codex / Codex App / app-server 后重试。
- 如果 `state_5.sqlite` 损坏，工具会提示 malformed/unreadable 并停止同步。
- 如果活跃会话锁住 rollout 文件，工具会跳过该文件并继续处理其它历史会话。
- 如果 EXE 双击无反应，先确认已解压，再查看 `%AppData%\codex-provider-sync\startup-error.log`，或在 PowerShell 里运行 `./CodexProviderSync.exe`。

GUI 说明见 [README_GUI_ZH.md](README_GUI_ZH.md)。AI / Agent 说明见 [AGENTS.md](AGENTS.md)。

## 开发

```bash
git clone https://github.com/Dailin521/codex-provider-sync.git
cd codex-provider-sync
npm test
dotnet test desktop/CodexProviderSync.Core.Tests/CodexProviderSync.Core.Tests.csproj
pwsh ./scripts/publish-gui.ps1
```

## License

MIT
