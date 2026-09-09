<div align="center">

# codex-provider-sync

### 切换 Provider 后，帮助 Codex 旧会话重新可用

[![CI](https://github.com/Dailin521/codex-provider-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/Dailin521/codex-provider-sync/actions/workflows/ci.yml)
[![CLI / Web](https://img.shields.io/npm/v/%40dailin521%2Fcodex-provider-sync?label=CLI%20%2F%20Web)](https://www.npmjs.com/package/@dailin521/codex-provider-sync)
[![Releases](https://img.shields.io/github/v/release/Dailin521/codex-provider-sync?label=Releases)](https://github.com/Dailin521/codex-provider-sync/releases)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**中文** · [English](docs/README_EN.md) · [日本語](docs/README_JA.md) · [한국어](docs/README_KO.md)

</div>

切换 Provider 后，旧会话可能仍记录着原来的 Provider。本工具将**会话文件与 SQLite 聊天索引中的 Provider 信息**对齐到当前配置，解决元数据不一致的问题。

**不保证跨 Provider / 账号的旧会话一定能继续或压缩**，也不处理登录、认证或加密内容。信息已对齐时，无需重复同步。

## 下载 Windows 桌面版

当前正式版 **v1.0.0 · Windows x64**，无需安装 Node.js。

- [下载安装包（约 100 MiB）](https://github.com/Dailin521/codex-provider-sync/releases/download/v1.0.0/CodexProviderSync-1.0.0-windows-x64-setup.exe)
- [下载便携 ZIP（约 123 MiB）](https://github.com/Dailin521/codex-provider-sync/releases/download/v1.0.0/CodexProviderSync-1.0.0-windows-x64-portable.zip)：完整解压后运行，不要只复制 EXE。
- [版本说明与校验文件](https://github.com/Dailin521/codex-provider-sync/releases/tag/v1.0.0)

此版本未签名，更新需手动安装。旧 .NET 版的更新按钮不能升级到 Electron，请下载完整新版。macOS/Linux Electron 安装包尚未发布；CLI / Web 的 npm 版本独立发布，不等同于桌面版版本号。

## 日常使用

1. 打开“概览”，确认 **Provider、存储路径和同步状态**。
2. 已通过 CCSwitch 等工具切换 Provider：点击“预览同步”查看影响，或“直接同步”立即执行。
3. 查看结果。部分完成时，结束相关占用会话后重试；需要撤销时进入“备份 / 恢复”。

希望由本工具更改配置？使用概览底部的“单独切换 Provider”。它会**修改配置并同步历史 Provider**，不改历史模型；自定义 Provider 必须已在 `config.toml` 中定义。

有实际修改时先备份，默认保留最近 **2 份**，统一在“备份 / 恢复”设置；无需修改时不创建备份，恢复依赖的受保护备份可能超过数量。

还可按项目浏览聊天、右键复制会话 ID / 继续命令、查看操作日志，以及自定义存储位置。高级诊断与专项修复按需使用，**普通同步不会顺带执行修复**。数据不后台轮询；自动同步 Watch 需主动开启。

[桌面完整指南](docs/README_DESKTOP_ZH.md) · [旧版迁移说明](docs/release-notes/v1.0.0-zh.md)

## 本地 Web UI

安装 Node.js `16.20.2+` 后运行，获得 npm 当前已发布的 CLI / Web 版本：

```bash
npm install -g @dailin521/codex-provider-sync
codex-provider web
```

默认只监听本机 `127.0.0.1:8791`，打开浏览器完成配对。跨设备使用见 [Web 指南与 SSH 用法](docs/README_WEB_UI_ZH.md)。

## CLI

安装同一个 npm 包后，先检查，再同步：

```bash
codex-provider status
codex-provider sync
```

CLI 写命令直接执行。切换 Provider、恢复备份、Watch、路径参数及 JSON 退出码见 [CLI 指南](docs/README_CLI_ZH.md)；命令是否可用以安装版本的 `--help` 为准。

## 使用前了解

- **同步范围**：只对齐 Provider，不修改聊天正文、历史模型或 `threads.updated_at`，不读取或修改 `auth.json`。
- **速度**：合格的等字节长 Provider 自动原地替换；其他有效首行流式替换，正文保持逐字节一致。备份、落盘检查和时间戳恢复仍保留，不需要手动开启快速模式。
- **部分完成**：不代表全部失败，也不会自动全量回滚。查看跳过项、失败阶段和备份，再重试或手动恢复。
- **仍无法继续**：若为加密内容或模型兼容问题，请回到原 Provider / 账号，或新建会话；同步不能解决这类问题。
- **存储位置**：以概览实际路径为准。Windows 的 WSL UNC SQLite 路径仅支持诊断；写入需进入对应 WSL 运行 CLI。

## 更多文档

- [完整文档索引](docs/README_ZH.md) · [更新日志](CHANGELOG.md) · [反馈问题](https://github.com/Dailin521/codex-provider-sync/issues)
- [工作原理](docs/WORKING_PRINCIPLE_ZH.md) · [当前 Node Core 架构与读写约束](docs/architecture/NODE_CORE_ARCHITECTURE_ZH.md)
- [贡献与构建指南](CONTRIBUTING.md) · [迁移与发布门禁](docs/migration/VNEXT_MIGRATION_EXECUTION_INDEX_ZH.md) · [AI / Agent 指南](AGENTS.md)

CLI、Web、Electron 共用 Node Core；安装 CLI 不会安装 Electron。旧 .NET 桌面端是独立的 Legacy 实现，保留构建与兼容维护。

## 致谢与许可

感谢 [@tangquanwei](https://github.com/tangquanwei) 贡献本地 Web UI、聊天记录浏览和多语言文档基础，并通过 [PR #80](https://github.com/Dailin521/codex-provider-sync/pull/80) 带入 v0.5.0；感谢所有参与贡献和问题调查的朋友。

[贡献者](CONTRIBUTORS.md) · [GitHub Contributors](https://github.com/Dailin521/codex-provider-sync/graphs/contributors) · [LINUX DO 社区](https://linux.do/) · [MIT License](LICENSE)
