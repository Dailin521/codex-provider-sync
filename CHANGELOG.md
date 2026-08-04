# 更新日志

本文件记录面向用户和集成方的重要变化。完整的发布叙事、升级说明和下载入口见对应版本的中文发布说明；实现证据和测试门禁见技术发布说明。

## [0.4.0] - 2026-08-04

### 新增

- 新增实验性 Windows 自动化接口，支持 `describe`、`status`、`plan`、`sync`、`switch`、`restore` 和 `prune`。
- 新增独立 SQLite Home 支持，以及按 Codex Home 保存的 Windows GUI SQLite Home 配置。
- 新增独立 Automation ZIP；单文件 GUI 和包含全部工具的 Windows ZIP 保持可用。

### 变更

- Windows GUI 与自动化接口改为共享 Application 用例，Core 继续统一负责配置、rollout、SQLite、备份、恢复、锁和 WSL 安全策略。
- 新备份使用 metadata v2 记录 SQLite Home 和数据库文件，同时继续支持旧版托管备份。
- GitHub Release 正文改为读取随版本 tag 入库的中文发布说明。

### 修复

- 修复多文件写入部分成功后无法可靠补偿的问题；失败和取消现在会按事务记录回滚。
- SQLite 提交结果无法确认时改为保守恢复，不再把不确定状态报告为成功。
- 强化锁所有权恢复、SQLite 快照恢复和 WSL UNC 路径安全诊断。

### 安全

- 写操作在目标修改前创建绑定备份，并保留崩溃恢复信息。
- 自动化接口的写操作默认只生成计划；实际执行需要 `--apply`、匹配的计划文件和 SHA-256 摘要。
- 自动化路径拒绝 `auth.json`、符号链接、reparse point 和非绝对路径。

### 升级说明

- v0.3.1 / v0.3.2 Windows GUI 可以通过内置更新升级，但内置更新只替换单文件 GUI。
- 升级不要求手动迁移配置；需要自动化接口的用户应单独下载 Automation ZIP 或 Windows 完整包。

[中文发布说明](docs/release-notes/v0.4.0-zh.md) · [技术发布说明](docs/RELEASE_NOTES_V0.4.0.md) · [完整变更对比](https://github.com/Dailin521/codex-provider-sync/compare/v0.3.2...v0.4.0)

更早版本见 [GitHub Releases](https://github.com/Dailin521/codex-provider-sync/releases)。
