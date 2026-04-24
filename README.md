<div align="center">

# codex-provider-sync

### 在切换 provider 之后，让 Codex 的历史会话重新可见

[![CI](https://github.com/Dailin521/codex-provider-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/Dailin521/codex-provider-sync/actions/workflows/ci.yml)
[![Platform](https://img.shields.io/badge/platform-Windows-lightgrey.svg)](https://github.com/Dailin521/codex-provider-sync)
[![Node](https://img.shields.io/badge/node-24%2B-brightgreen.svg)](https://nodejs.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Community](https://img.shields.io/badge/community-LINUX%20DO-2ea043.svg)](https://linux.do/)

[English](README_EN.md) | 中文

</div>

## 解决什么问题

切换 `model_provider` 之后，Codex 的历史会话有时会“消失”。

常见现象：

- `codex resume` 里能看到的会话，到了 Codex App 里不一定还能看到
- 切回官方 `openai` 后，之前在中转 provider 下的历史会话像没了
- 只改 `sessions/*.jsonl` 不够，因为 SQLite 里还有一层 provider 元数据

`codex-provider-sync` 会把这两层一起同步：

- `~/.codex/sessions` 和 `~/.codex/archived_sessions`
- `~/.codex/state_5.sqlite`

## Windows 图形界面

如果你不想装 Node、也不想进终端，现在可以直接使用发布页里的 `CodexProviderSync.exe`。

GUI 版本会：

- 扫描当前 `.codex` 目录
- 展示 rollout files 和 SQLite 的 provider 分布
- 自动汇总已检测到和已保存的 provider
- 允许你选择目标 provider 后一键执行
- 可选同时改 `config.toml` 根级 `model_provider`
- 默认自动保留最近 5 份由本工具创建的备份，并支持自定义保留数
- 支持手动清理旧备份
- 支持从 backup 目录恢复

GUI 专用说明见 [README_GUI_ZH.md](README_GUI_ZH.md)。

## 安装

```bash
npm install -g github:Dailin521/codex-provider-sync
```

环境要求：

- Node.js `24+`
- 标准 `~/.codex` 目录结构
- 当前优先面向 Windows 使用场景

对普通用户来说，推荐直接下载 GUI EXE。npm CLI 会继续保留，适合脚本、自动化和高级用户。

## 快速开始

GUI 方式：

- 从 Releases 下载 `CodexProviderSync.exe`
- 打开后先点 `Refresh`
- 选择目标 Provider
- 点击 `Execute`

如果你已经用自己的方式切好了 provider 或认证状态，直接执行：

```bash
codex-provider sync
```

如果你想顺手改 `config.toml` 里的根级 `model_provider`，再同步全部历史会话：

```bash
codex-provider switch openai
codex-provider switch apigather
```

如果你想只在这一次执行里改自动保留数：

```bash
codex-provider sync --keep 5
codex-provider switch apigather --keep 10
```

先看当前状态：

```bash
codex-provider status
```

生成一个可双击的 Windows 启动器（默认放到桌面）：

```bash
codex-provider install-windows-launcher
```

从备份回滚：

```bash
codex-provider restore C:\Users\you\.codex\backups_state\provider-sync\<timestamp>
```

手动清理旧备份：

```bash
codex-provider prune-backups --keep 5
```

## AI 一键处理

如果你想直接交给 AI 助手处理，把下面这段原样发给 AI：

```text
请帮我用 codex-provider-sync 修复 Codex 历史会话在当前 provider 下不可见的问题。

步骤：
1. 先运行 `codex-provider status`。
2. 如果当前 provider 已经正确，就运行 `codex-provider sync`。
3. 如果我明确说要切到某个 provider，就改为运行 `codex-provider switch <provider-id>`。
4. 如果提示 `state_5.sqlite` 正在被占用，就提醒我先关闭 Codex / Codex App / app-server 再重试。
5. 如果 sync 跳过了被锁住的 rollout 文件，就告诉我哪些文件被跳过了，并提醒我稍后再补跑一次 `codex-provider sync`。
6. 最后总结 rollout files 和 SQLite 里的 provider 状态。
```

如果用户更倾向于 GUI，也可以让 AI 这样引导：

1. 打开 `CodexProviderSync.exe`
2. 确认顶部 `.codex` 路径
3. 点击 `Refresh`
4. 在列表中选中目标 Provider
5. 如果还要改 `config.toml` 根级 provider，再勾选右侧复选框
6. 点击 `Execute`
7. 查看底部日志区里的 backup 路径、更新数量和被跳过的锁文件

快速对应关系：

- 只想查看状态：`codex-provider status`
- 当前 provider 不变，只修复历史会话可见性：`codex-provider sync`
- 切 provider 并同步历史：`codex-provider switch openai`
- 安装桌面双击启动器：`codex-provider install-windows-launcher`
- 回滚误操作：`codex-provider restore <backup-dir>`

## 命令说明

- `codex-provider status`
  - 显示当前 provider，以及 rollout 文件和 SQLite 里的 provider 分布
- `codex-provider sync`
  - 把历史会话同步到当前 provider
  - `--provider <id>` 可手动指定目标 provider
  - 如果根级 `model_provider` 缺失，会默认按 `openai` 处理
- `codex-provider switch <provider-id>`
  - 修改 `config.toml` 根级 `model_provider`
  - 然后立即执行一次同步
  - `--keep <n>` 可覆盖这次执行后的备份保留数量
- `codex-provider prune-backups`
  - 手动清理旧备份，只保留最近 `n` 份由本工具管理的备份
- `codex-provider restore <backup-dir>`
  - 从历史备份恢复
  - 可用 `--no-config`、`--no-db`、`--no-sessions` 跳过对应恢复项
- `codex-provider install-windows-launcher`
  - 默认在桌面生成两个文件
  - `Codex Provider Sync.vbs`：双击后隐藏执行，结束后弹窗显示结果
  - `Codex Provider Sync.cmd`：可见控制台版本，方便排错
  - `--dir <path>` 可改安装目录
  - `--codex-home <path>` 可把固定的 `CODEX_HOME` 写进启动器

常见用法：

```bash
codex-provider status
codex-provider sync
codex-provider sync --keep 5
codex-provider sync --provider openai
codex-provider switch apigather
codex-provider prune-backups --keep 5
codex-provider install-windows-launcher
codex-provider install-windows-launcher --dir D:\Tools
codex-provider install-windows-launcher --codex-home C:\Users\you\.codex
codex-provider restore C:\Users\you\.codex\backups_state\provider-sync\20260319T042708906Z
```

## 安全说明

每次同步前，工具都会先备份到：

```text
~/.codex/backups_state/provider-sync/<timestamp>
```

运行期间会使用：

```text
~/.codex/tmp/provider-sync.lock
```

注意：

- 它不会替换官方 `codex`
- 它不会帮你处理 `auth.json` 或第三方切号工具
- 它不会改消息历史、标题、cwd、时间戳
- 默认自动保留最近 5 份由本工具创建的备份；GUI 保留数或 CLI `--keep <n>` 可以覆盖
- 自动清理和手动清理都只会处理 `backups_state/provider-sync` 下由本工具创建的备份
- `Codex Provider Sync.vbs` 依赖 `codex-provider` 命令本身已经可用
- 如果 `state_5.sqlite` 被占用，先关闭 Codex / Codex App / app-server 再重试
- 如果 `state_5.sqlite` 损坏，工具会在状态页提示 malformed/unreadable，并阻止同步；请先备份、修复或移除损坏数据库后再重试
- 如果当前活跃会话锁住了某个 rollout 文件，`sync` 会跳过该文件并继续处理其它历史会话
- 如果历史会话包含 `encrypted_content`，跨 provider/account 后可能只能恢复可见性，继续对话或 compact 仍可能报 `invalid_encrypted_content`；这类密文不能由本工具重新加密

## EXE 双击无反应排查

1. 先确认已经从压缩包完整解压，再运行 `CodexProviderSync.exe`。
2. 如果仍无窗口，在 PowerShell 进入 EXE 所在目录后执行 `./CodexProviderSync.exe` 查看终端输出。
3. 检查 Windows SmartScreen、Defender 或第三方杀毒软件是否拦截。
4. 查看 `%AppData%\codex-provider-sync\startup-error.log`，启动期异常会写入这个文件。

## 给 AI / Agent 的说明

更完整的机器说明见 [AGENTS.md](AGENTS.md)。

## 开发

```bash
git clone https://github.com/Dailin521/codex-provider-sync.git
cd codex-provider-sync
npm test
dotnet test desktop/CodexProviderSync.Core.Tests/CodexProviderSync.Core.Tests.csproj
pwsh ./scripts/publish-gui.ps1
node ./src/cli.js status --codex-home C:\path\to\.codex
```

## License

MIT
