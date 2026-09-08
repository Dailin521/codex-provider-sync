<div align="center">

# codex-provider-sync

### 切换 Provider 后，让 Codex 历史会话重新可见

[![CI](https://github.com/Dailin521/codex-provider-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/Dailin521/codex-provider-sync/actions/workflows/ci.yml)
[![CLI / Web](https://img.shields.io/npm/v/%40dailin521%2Fcodex-provider-sync?label=CLI%20%2F%20Web)](https://www.npmjs.com/package/@dailin521/codex-provider-sync)
[![Releases](https://img.shields.io/github/v/release/Dailin521/codex-provider-sync?label=Releases)](https://github.com/Dailin521/codex-provider-sync/releases)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Community](https://img.shields.io/badge/community-LINUX%20DO-2ea043.svg)](https://linux.do/)

**中文** · [English](docs/README_EN.md) · [日本語](docs/README_JA.md) · [한국어](docs/README_KO.md)

</div>

## 它解决什么

切换 `model_provider` 后，旧会话可能从 Codex Desktop 或 `/resume` 中消失。**数据通常仍在磁盘上**，只是会话文件和 SQLite 索引中的 Provider 信息没有同步。

本工具将两者对齐到当前 Provider。有实际修改时先备份，默认保留最近 **2 份**；无需修改时不创建备份。它不负责登录、切换账号、解密或重建消息，也不读取或修改 `auth.json`。

<p align="center">
  <img src="images/README/provider-metadata-sync-flow.png" alt="Provider 元数据同步前后效果" width="760">
</p>

- 已通过 CCSwitch 等工具切换 Provider：使用“同步当前 Provider”。
- 希望由本工具修改 Provider：使用“单独切换 Provider”，修改配置后同步历史中的 Provider 信息。
- Provider ID 没变、历史也已对齐：无需重复同步。
- 历史能显示但无法继续或压缩：可能是跨 Provider 的加密内容不兼容，同步不能解决解密问题。

## 快速开始

> 本页对应 V1 代码。Electron 是主桌面端，.NET 保留为 Legacy。下载以 [Releases 实际资产](https://github.com/Dailin521/codex-provider-sync/releases) 为准，npm 安装得到已发布版本；本地构建和代码版本号不代表已公开发布、签名或启用更新通道。

| 场景 | 入口 |
| --- | --- |
| Windows 桌面，无需安装 Node.js | [Electron 桌面版指南](docs/README_DESKTOP_ZH.md) |
| macOS / Linux 桌面 | [平台与构建说明](docs/README_DESKTOP_ZH.md#程序安装更新与体积)，以对应版本实际资产为准 |
| 浏览器操作 / 跨平台 | [本地 Web UI](#本地-web-ui) |
| 命令行、脚本或 WSL | [CLI](#cli) |

### 桌面版

安装对应平台包，或解压完整程序目录运行。**不要只复制 Electron 的单个 EXE。**

1. 打开“概览”，核对当前 Provider、存储路径和同步状态。
2. 在存储配置右侧使用“预览同步”查看影响，或“直接同步”立即执行，两者同步范围相同。
3. 需要修改 Provider 时，在概览底部使用“单独切换 Provider”，选择目标和模型策略，预览后确认。
4. 查看结果。部分完成时，结束相关占用会话后重新同步；需要撤销时前往“备份 / 恢复”。

“单独切换”不是仅修改配置：它仍会同步历史 Provider，但不修改历史模型。自定义 Provider 必须已在 `config.toml` 中定义；模型策略见[同步与切换说明](docs/README_DESKTOP_ZH.md#同步与切换-provider)。

### 本地 Web UI

安装 Node.js `16.20.2+`，然后运行：

```bash
npm install -g @dailin521/codex-provider-sync
codex-provider web
```

服务只监听本机 `127.0.0.1:8791`，默认打开浏览器完成配对。日常同步流程与桌面版相同，Web 不提供桌面操作日志和应用更新。

```bash
codex-provider web --no-open       # 不自动打开浏览器
codex-provider web --port 8792     # 指定端口
codex-provider web --reset-access  # 撤销旧连接并重新配对
```

[Web 完整指南与 SSH 用法](docs/README_WEB_UI_ZH.md)

### CLI

安装同一个 npm 包后，先检查，再执行：

```bash
codex-provider status
codex-provider sync
```

| 命令 | 用途 |
| --- | --- |
| `status` | 检查当前 Provider、存储位置和同步状态 |
| `sync` | 同步到 config 当前 Provider |
| `switch <provider-id>` | 修改配置中的 Provider，然后同步 |
| `diagnostics` | 手动执行一次完整只读诊断 |
| `repair <targets>` | 显式执行选定的非 Provider 元数据修复 |
| `restore <backup-dir>` | 恢复受管备份 |
| `prune-backups --keep N` | 清理较旧受管备份 |
| `watch` | 主动开启自动同步；按 Ctrl+C 停止 |

CLI 写命令直接执行，不再弹出交互确认。有限命令支持 `--json`；例如 `codex-provider sync --json`。脚本必须检查 `outcome` 和退出码，部分完成的 JSON 退出码为 `3`。

[CLI 参数、模型策略、路径、备份与 JSON 指南](docs/README_CLI_ZH.md)

## 界面里还可以做什么

| 页面 | 用途 |
| --- | --- |
| 概览 | 核对路径和两侧 Provider 分布，预览/直接同步，单独切换 Provider |
| 备份 / 恢复 | 统一设置备份保留数、查看备份、恢复和手动清理 |
| 聊天记录 | 按项目浏览主会话/子任务，查看消息；右键复制会话 ID、继续命令等 |
| 操作日志（桌面） | 左侧选操作、右侧看详情，查看目标、结果、数量和阶段耗时 |
| 存储配置 | 新建具名配置，指定 Codex Home 和可选的 SQLite Home |
| 高级功能 | 手动诊断、专项修复和独立的历史模型调整 |
| 设置 | 语言、主题、主动开启的 Watch；桌面端另有更新检查 |

数据首次加载、手动刷新或明确操作后更新，**不后台轮询**。聊天正文只有明确查看/提交正文搜索才加载；手动 Diagnostics 与 Repair 按各自范围扫描。正文不进入日志、诊断包或持久缓存。

备份保留数量统一在“备份 / 恢复”设置：同一应用内各项操作共用，每个 Codex Home 分别保留。保存设置不会立即删除备份；恢复依赖的受保护备份可能超过数量。桌面、Web 浏览器与 CLI 不共享偏好。

日常同步不需要高级修复。专项修复只处理明确选择的目录/用户消息标记/工作区设置；统一历史模型名称单独放在“高级调整”。当前不提供会话序号改写或历史显示索引重建。

## 同步速度与读写边界

- **合格的等字节长 Provider** 自动原地更新，只覆盖首行中的 Provider 字节，保留文件身份、大小和正文。
- **不等长或不满足原地资格** 的有效首行采用临时文件流式替换，正文逐字节复制，不解析聊天正文。
- 没有 `--fast` 或 `sync --provider` 模式，不要求用户统一 Provider 名称长度。原地写失败不会强制改成整文件替换。
- 时间戳恢复、备份和落盘检查不会为了提速取消。会话文件很多或正文很大时，复制和落盘仍可能耗时。

概览可展开“如何加快同步”。Windows 新同步/切换日志提供复制、落盘、替换、清理和时间戳恢复的分项耗时；旧日志不补造计时。实测速率取决于数据、磁盘和占用，不以合成样本承诺真实同步耗时。

[工作原理](docs/WORKING_PRINCIPLE_ZH.md) · [开发必守的 Provider I/O 约束](docs/architecture/NODE_CORE_ARCHITECTURE_ZH.md#3-provider-io-不变量必须保持)

## 常见结果与注意事项

| 情况 | 怎么处理 |
| --- | --- |
| 会话文件数与索引行数不同 | 不一定是故障，以 Provider 分布和同步状态为准 |
| 部分完成 / 会话被占用 | 查看跳过项和失败阶段，结束相关占用后重新同步；需要撤销时手动恢复 |
| 状态待刷新 / 数据已变化 | 手动刷新或重新预览，不要删除锁文件或数据库 |
| 自定义 Provider 不存在 | 先在配置/常用 Provider 工具中补齐定义；本工具不会擅自切回 OpenAI |
| SQLite busy | 停止相关 Codex 写入后重试；若已部分写入，先查看结果和备份 |
| 旧聊天可见但无法继续 | 回到原 Provider/账号或新建会话；本工具不能解密旧内容 |

普通 Sync/Switch/Repair 在修改前创建覆盖实际目标的备份；写入后失败报告部分完成，不自动全量回滚。Restore 独立保留恢复前快照、journal 和补偿。所有操作都不会通过修改 `threads.updated_at`、消息顺序或时间来强行刷新历史。

SQLite Home 优先级：显式参数/存储配置 → `config.toml` 的 `sqlite_home` → `CODEX_SQLITE_HOME` → `<Codex Home>/sqlite`。只有默认布局允许回退到旧位置 `<Codex Home>/state_5.sqlite`。Windows 的 WSL UNC SQLite Home 仅诊断；写入须在对应 WSL 内使用 Linux 路径运行 CLI。

## 一个共享核心，多种入口

```text
CLI → 兼容适配器 ──────────────────────────────┐
Web UI → HttpCoreClient → Local Web Host → CoreFacade ─┤
Electron UI → 窄 IPC → Utility Process → CoreFacade ──┤
                                                    ↓
                                    同一 Node Core 业务用例
                                                    ↓
                              config / 会话文件 / SQLite / 受管备份
```

CLI 当前保留 `src/public-api.js` 适配层，和 Web/Electron 共用 ProviderSync；UI/Host 不另写同步算法，也不启动 CLI 解析其文本输出。Electron 不进入根 npm 包，安装 CLI 不会额外安装 Electron。

.NET Windows/macOS 是独立的 **Legacy fallback**，保留构建和兼容维护，不是上述 Node Core 的另一个分发壳；它们的旧 journal/自动回滚行为不能套到新核心。

## 文档与开发

- 用户指南：[桌面中文](docs/README_DESKTOP_ZH.md) / [English](docs/README_DESKTOP_EN.md) · [Web](docs/README_WEB_UI_ZH.md) · [CLI](docs/README_CLI_ZH.md)
- [完整文档索引](docs/README_ZH.md) · [更新日志](CHANGELOG.md) · [贡献指南](CONTRIBUTING.md)
- 开发先读：[当前 Node Core 架构](docs/architecture/NODE_CORE_ARCHITECTURE_ZH.md) → [总体架构基线](docs/VNEXT_ELECTRON_NODE_ARCHITECTURE_ZH.md) → 对应合同、ADR 和测试
- [迁移与发布门禁](docs/migration/VNEXT_MIGRATION_EXECUTION_INDEX_ZH.md) · [AI / Agent 指南](AGENTS.md)

工作区构建使用 Node 24；已安装根 CLI 仍支持 Node 16.20.2。

```bash
npm ci
npm run architecture:check
npm test
npm run web:build
npm run desktop:build
```

仅本地改文档不代表完成代码、平台或发布验收。具体验证和发布命令见 [贡献指南](CONTRIBUTING.md)与 [npm 发布维护指南](docs/NPM_PUBLISHING.md)。

## 致谢

感谢 [@tangquanwei](https://github.com/tangquanwei) 提出并实现本地 Web UI，贡献聊天记录浏览和多语言文档基础，并通过 [PR #80](https://github.com/Dailin521/codex-provider-sync/pull/80) 将其带入 v0.5.0；也感谢所有参与代码、文档、测试和问题调查的贡献者。

[贡献者名单](CONTRIBUTORS.md) · [GitHub Contributors](https://github.com/Dailin521/codex-provider-sync/graphs/contributors)

## License

[MIT](LICENSE)
