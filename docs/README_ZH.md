# codex-provider-sync 文档索引

初次使用先读 [README](../README.md)，再按入口选择下面的指南。本文档集说明 V1 当前实现；本地构建、Unreleased 和架构目标不代表公开发布。下载以实际 Release 资产为准，不将历史 .NET 下载混作 Electron 安装包。

## 用户指南

- Electron 桌面版：[中文](README_DESKTOP_ZH.md) · [English](README_DESKTOP_EN.md)
- [Web UI：启动、配对、同步与 SSH](README_WEB_UI_ZH.md)
- [CLI：命令、模型策略、路径、备份与 JSON](README_CLI_ZH.md)
- [工作原理与落盘机制](WORKING_PRINCIPLE_ZH.md)
- [更新日志](../CHANGELOG.md)：Unreleased 是当前开发变化；历史版本说明按当时行为保留

## 开发与维护

1. [当前 Node Core 架构与开发约束](architecture/NODE_CORE_ARCHITECTURE_ZH.md)：真实模块、CLI 适配例外、Provider I/O 不变量与验证入口。
2. [vNext 总体架构](VNEXT_ELECTRON_NODE_ARCHITECTURE_ZH.md)：多入口/平台路线及历史迁移上下文，不作为未完成能力的证明。
3. 对应公开合同：[Core 外部行为](architecture/contracts/CORE_EXTERNAL_BEHAVIOR_ZH.md) · [CLI](architecture/contracts/CLI_CONTRACT_ZH.md) · [错误码](architecture/contracts/ERROR_CODES_ZH.md)。注意区分 V1 当前修订与 v0.5 冻结记录。
4. 对应 [ADR 决策目录](adr)及[行为 Fixtures](migration/BEHAVIOR_FIXTURES_ZH.md)：有意改变行为需同时更新决策、合同、测试和用户指南。

常用决策：[轻量普通写](adr/0016-node-core-responsibility-boundaries-and-lightweight-writes.md) · [统一备份](adr/0027-unified-backup-retention.md) · [会话使用数量](adr/0030-writer-owned-session-count.md) · [扫描进度](adr/0032-explicit-scan-and-preview-progress.md) · [Provider 计划复核](adr/0035-provider-plan-semantic-revisions-and-sync-validation.md) · [Windows 文件分项计时](adr/0038-windows-cleanup-and-file-update-timing.md)。

- [迁移执行索引与发布门禁](migration/VNEXT_MIGRATION_EXECUTION_INDEX_ZH.md)：阶段状态与发布证据，不因文档修订标记 Completed
- [贡献指南](../CONTRIBUTING.md) · [AI / Agent 操作指南](../AGENTS.md)
- [npm 发布维护指南](NPM_PUBLISHING.md)
- [Windows Electron 发布操作说明](WINDOWS_ELECTRON_RELEASE_ZH.md)：独立候选流程、最终 SHA 门禁与 RC/正式版边界

## Legacy 与历史资料

.NET Windows/macOS 是独立兼容实现，不是新 Node Core 的分发壳，其旧行为不能作为 V1 规范：

- [Legacy Windows GUI](README_GUI_ZH.md)
- Legacy macOS GUI：[中文](README_MAC_GUI_ZH.md) · [English](README_MAC_GUI_EN.md)
- Legacy 实验性 Automation：[快速开始](AUTOMATION_QUICKSTART_ZH.md) · [设计说明](AUTOMATION_DESIGN_NOTES.md)

## 其他语言

- [English](README_EN.md)
- [日本語](README_JA.md)
- [한국어](README_KO.md)
