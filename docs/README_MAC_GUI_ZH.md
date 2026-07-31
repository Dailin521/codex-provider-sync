# macOS GUI 使用说明

中文 · [English](README_MAC_GUI_EN.md)

`CodexProviderSync.app` 是 macOS 桌面版 GUI，使用 Avalonia 构建，复用 `desktop/CodexProviderSync.Core` 的状态、同步、切换、恢复和清理逻辑。

## 构建

需要 .NET 10 SDK。

```bash
./scripts/publish-gui-macos.sh
```

构建 Intel (x86_64) 版本：

```bash
./scripts/publish-gui-macos.sh --runtime osx-x64 --output artifacts/osx-x64
```

默认产物：

```text
artifacts/osx-arm64/CodexProviderSync.app
```

如需指定 SDK：

```bash
DOTNET=/path/to/dotnet ./scripts/publish-gui-macos.sh
```

## 功能

- 选择或输入 Codex Home，默认 `~/.codex`
- 为每个 Codex Home 单独指定 SQLite Home；留空时按配置自动解析
- `Refresh` 查看当前 provider、rollout provider counts、SQLite provider counts、备份统计、project visibility 和 `encrypted_content` 风险提示
- Provider 列表展示 `config`、`rollout`、`SQLite`、`manual` 来源
- 手动添加或删除 provider
- `Sync Metadata Only`
- `Switch config.toml and sync`
- `Restore Backup`，可选择恢复 `config.toml`、SQLite、rollout metadata
- `Clean Old Backups`
- `Open Backup Folder`
- 执行日志和错误提示

## 安全边界

- App 启动和 `Refresh` 不会修改 Codex Home 下的 session、SQLite 或 `config.toml` 元数据
- 写操作前会弹出确认
- metadata v2 备份恢复到不同 SQLite Home 时，会显示来源与目标并再次确认
- `sync` 和 `switch` 由 Core 先创建 backup，再修改 metadata
- `encrypted_content` 只提示风险，不承诺修复
- 不处理 `auth.json`
- 不登录、不认证
- 不修改对话内容
- 不修改 `updated_at`

## 操作建议

执行 `Sync Metadata Only`、`Switch config.toml and sync`、`Restore Backup` 或 `Clean Old Backups` 前，先关闭：

- Codex CLI
- Codex App
- app-server
- 仍在使用相关 Codex Home 的终端任务

如果看到 `state_5.sqlite is currently in use`，关闭上述进程后重试。

SQLite Home 的解析优先级为：GUI override → `config.toml` 根级 `sqlite_home` → `CODEX_SQLITE_HOME` → `<Codex Home>/sqlite`。GUI override 按 Codex Home 保存在 App settings 中，不会写入 `config.toml`。显式位置缺库时只允许刷新诊断，写操作不会回退到其它数据库；默认布局中的数据库被删除时，可以从有效备份恢复到原默认位置。恢复到不同 SQLite Home 时必须取消勾选配置恢复，并再次确认来源和目标。

如果日志显示跳过 locked rollout files，通常表示当前会话仍占用 rollout 文件。同步大多已完成；等会话结束后再运行一次 sync 可补齐这些文件。

## 发布说明

`scripts/publish-gui-macos.sh` 默认发布 `osx-arm64` self-contained `.app` bundle，并在本机可用时执行 ad-hoc `codesign`。该脚本不修改 Windows GUI 发布脚本 `scripts/publish-gui.ps1`。

macOS 桌面项目当前使用 Avalonia 12.0.4。ad-hoc 签名不等同于 Apple Developer ID 签名或公证。

项目目前提供 macOS 构建脚本，GitHub Releases 中没有预构建的 macOS 产物，请使用上述命令在本地构建。
