# codex-provider-sync 中文入口

完整中文说明以仓库根目录的 [README.md](../README.md) 为准，包括适用场景、Windows GUI、可选本地 Web UI、CLI、SQLite Home 解析、安全限制和开发命令。

专项文档：

- [Web UI 中文指南](README_WEB_UI_ZH.md)
- [Windows GUI 说明](README_GUI_ZH.md)
- [工作原理与落盘机制](WORKING_PRINCIPLE_ZH.md)
- macOS GUI：[中文](README_MAC_GUI_ZH.md) · [English](README_MAC_GUI_EN.md)

开发与维护：

- [vNext 架构基线（Electron + Node 单核心）](VNEXT_ELECTRON_NODE_ARCHITECTURE_ZH.md)
- [vNext 迁移执行索引](migration/VNEXT_MIGRATION_EXECUTION_INDEX_ZH.md) · [行为 Fixtures](migration/BEHAVIOR_FIXTURES_ZH.md)
- vNext 合同：[Core 外部行为](architecture/contracts/CORE_EXTERNAL_BEHAVIOR_ZH.md) · [CLI](architecture/contracts/CLI_CONTRACT_ZH.md) · [错误码](architecture/contracts/ERROR_CODES_ZH.md)
- vNext ADR：[0001 Electron](adr/0001-electron-over-tauri.md) · [0002 Node Core](adr/0002-node-core-as-single-authority.md) · [0003 CLI](adr/0003-preserve-node-cli-contract.md) · [0004 Renderer](adr/0004-renderer-has-no-node-access.md) · [0005 Utility Process](adr/0005-run-core-in-electron-utility-process.md) · [0006 无应用数据库](adr/0006-no-application-database.md) · [0007 CoreClient](adr/0007-shared-ui-through-core-client.md) · [0008 渐进迁移](adr/0008-incremental-migration-no-big-bang-rewrite.md) · [0009 Plan/Apply](adr/0009-plan-confirm-apply-for-writes.md) · [0010 构建工具](adr/0010-electron-vite-and-electron-builder.md)
- [贡献指南](../CONTRIBUTING.md) · [AI / Agent 操作指南](../AGENTS.md)
- [npm 发布维护指南](NPM_PUBLISHING.md)
- Automation：[快速开始](AUTOMATION_QUICKSTART_ZH.md) · [设计说明](AUTOMATION_DESIGN_NOTES.md)

其他语言：

- [English](README_EN.md)
- [日本語](README_JA.md)
- [한국어](README_KO.md)
