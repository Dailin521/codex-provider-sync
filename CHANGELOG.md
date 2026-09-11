# 更新日志

本文件记录面向用户和集成方的重要变化。完整的发布叙事、升级说明和下载入口见对应版本的中文发布说明；实现证据和测试门禁见技术发布说明。

## [Unreleased]

## [1.0.1] - 2026-09-11

Windows x64 Electron 补丁版，未签名、手动安装；不发布 npm 或 Legacy 安装包。[下载与升级说明](docs/release-notes/v1.0.1-zh.md)。

- 修复已退出进程留下的锁导致持续显示“操作执行中”、无法进入同步的问题（#95）：只读检查识别已确认失效的锁，正常同步前重新验证并安全回收；未知锁仍阻止写入，并显示不同的检查提示。
- 修复 Windows 检查锁所属进程时进程恰好退出的竞态；状态和诊断不会自行删除锁。
- 锁阻断时不再继续扫描聊天完整性，仍可导出诊断状态与操作日志。

- 修复备份清理期间读取列表/摘要可能失败的竞态：跳过读取过程中消失的整条备份，其余条目正常显示；不放宽恢复校验。
- 补齐同步内部失败的安全阶段、底层错误码与关联 ID，并在 CLI JSON/stderr 和桌面操作日志保留，便于定位无进度的首次失败；不记录异常原文、路径或聊天正文。

- 将 Desktop 更新器与构建链的传递依赖 `js-yaml` 从 `4.3.1` 更新至 `4.3.2`，修复空 YAML 合并源绕过处理上限的问题（[GHSA-2883-xcg3-v3hh](https://github.com/advisories/GHSA-2883-xcg3-v3hh)）。不改变 Provider 读写；已发布安装包不会因此自动更新。

## [1.0.0] - 2026-09-08

本次发行范围为 **Windows x64 Electron**，未签名、手动安装。以下也记录同一源码中共享 Core、CLI/Web 的开发变化，但本轮不发布 npm、macOS/Linux 或 Legacy 安装包，不代表签名或线上自动升级已验收。发布状态与下载以 [GitHub Release](https://github.com/Dailin521/codex-provider-sync/releases/tag/v1.0.0) 为准；[安装与迁移说明](docs/release-notes/v1.0.0-zh.md)。

### 新增

- 独立 Windows Electron 发布准备流程：精确 tag/SHA 与 main CI 门禁、NSIS/ZIP 容器验收及 Draft；默认 RC，显式 stable-manual 只接受 Windows 1.0.0。不触发 Legacy 或 npm 发布，核验后再独立公开并设置 latest。

- Electron 主桌面端与重建后的共享 Web UI：概览集中同步/切换，备份统一管理，聊天按项目和父子任务组织，存储配置、双语、三主题及高级功能独立提供。
- “直接同步”以一次点击授权，内部继续 Prepare/Apply；“单独切换 Provider”先修改配置再同步 Provider，根模型三种策略均不改历史模型。
- Desktop 操作日志采用列表/详情独立滚动，记录计划、结果、数量、备份、阶段及总耗时；切换历史保存 Provider/根模型前后值，最近成功目标仅填草稿。
- Windows 新 Sync/Switch 日志提供复制、落盘、替换、清理和时间戳恢复分项；旧记录不补造数据，数值观测错误不改变操作结果。
- 聊天右键菜单提供 ID/继续命令等操作，正文延迟读取、项目/子任务独立分页、手动搜索；应用不会读取首条消息猜标题或把正文写入日志。
- 具名存储配置支持 Codex/SQLite 目录选择；默认配置由启动环境管理。备份保留设置统一在“备份 / 恢复”，默认 2 份，按 Home 独立保留。
- 每天首次启动或手动检查更新，有更高 Electron 正式版本时提示；不自动下载/安装。Legacy 单 EXE 更新不能直接升级到 Electron。
- 有限 CLI 命令新增 opt-in `--json`，stdout 固定为一个 schema v1 终态对象；帮助、输入失败、成功、partial、recovery、busy 和取消共享同一顶层结构。
- JSON Mode 固化 `0/1/2/3/4/5/130` 退出码矩阵，并使用 Canonical Core Error Code。
- Provider Sync 收窄为固定的首行路径：目标始终来自 `config.toml` 当前 Provider；安全且唯一可定位、JSON 字面量 UTF-8 字节等长的值原地更新，其余有效首行流式生成临时文件并原子替换，聊天正文逐字节保持不变。原地失败不强制改成整文件替换。
- 新增用户主动触发、完整且只读的 `diagnostics`，以及显式目标化的 `repair`；Web 与 Electron 使用同一 Core 语义。
- 普通 Sync/Switch/Repair 使用 Codex Home 锁、SQLite 原生事务和覆盖实际目标的 UndoBackup；mutation 后故障返回带备份、失败阶段和重试建议的 `partial`，由重复执行收敛。

### 修复与优化

- 完成、警告和错误浮动通知支持点击内容/× 关闭，也支持 Enter、空格和 Esc；仅关闭通知，不删除结果、日志或备份。
- README 及现行中英日韩入口改为“切换 Provider 后帮助旧会话重新可用”，不再以恢复列表可见性为主要定位；保留跨 Provider 解密、继续与 compact 的兼容限制。

- Sync/Switch 计划按 Provider 相关状态复核，普通聊天追加和无关 SQLite 更新不单独导致 stale；真实配置、目标集合/身份和 Provider 变化仍拒绝旧计划。未定义自定义 Provider 时在修改前停止。
- Prepare 复用同轮首行事实；Windows 优化临时文件清理并保留 Force 回退，不采用更大 CopyTo 缓冲区，不取消备份、Flush 或文件时间戳恢复。耗时基准只作对应样本参考。
- 状态漂移与实际操作占用分开；会话使用数量来自 writer 观察，包含已对齐会话，未知不报零。
- 显式诊断/修复预览显示阶段进度与耗时；诊断保留单轮事实扫描，数据变化不触发整轮反复扫描；索引字段累计与去重会话分开显示。
- Windows 包精简重复依赖、保留两种 Chromium locale 并归档完整许可证；ASAR/解包/NSIS/ZIP 预算分别验证，不以下载大小代替解包占用。

### 兼容性

- 未传入 `--json` 时继续使用既有 Human 输出和 `0/1` 行为；partial sync 在 Human Mode 仍为成功退出。
- V1 尚未发布的 `sync --provider`、`--fast`、`syncMode` 与 `FAST_MODE_UNSUPPORTED` 已移除；Sync 始终跟随 config Provider，非 Provider 元数据改由 Repair 显式处理。
- `watch` 与 `web` 暂不提供单文档 JSON 模式，并在创建长运行资源前返回结构化 `INVALID_INPUT`；未来流式机器接口需要独立协议。
- 当前有限 CLI 写命令未提供终端信号的受控取消入口；JSON 130 是已取消结果映射，不保证 Ctrl+C 会输出终态对象。
- CLI 根包继续支持 Node 16.20.2，现代构建使用 Node 24。Electron 不进入根 npm 包；.NET 保留为独立 Legacy，不同步改造其旧业务行为。
- npm tarball 或 Windows npm shim 使用短路径、长路径或符号链接形式启动 CLI 时，会对入口两侧做物理路径规范化，避免已安装的 `codex-provider` 被误判为模块导入而静默退出。

### 安全

- JSON 进度只写 stderr 且不报告 backup path；固定错误文案、命令级 result allowlist 和枚举化 details 会阻止非法参数值、未知异常、底层 warning、凭据样式字段、prompt 与消息正文进入 stdout。
- stdout broken pipe 只尝试一次终态写入；stderr observer 失败不能改变已启动业务操作的结果。

## [0.5.0] - 2026-08-15

### 新增

- Web UI 改为零输入自动配对：短时一次性 fragment 换取持久设备凭证，服务端只保存凭证哈希。
- 新增服务端存储配置，浏览器写操作仅提交 `profileId`；支持命名配置、`--reset-access` 和“忘记此浏览器”。
- 新增 `--no-open`、SSH 隧道和无桌面环境说明；已有 Web UI 实例会被复用。

### 变更

- 最低 Node.js 版本明确为 16.20.2，CI 同时覆盖 16.20.2 和当前 LTS。
- npm 包和仓库内发布型 .NET 项目版本统一为 0.5.0。
- CLI/Web 通过 npm 独立发布；本次没有创建 v0.5.0 Windows GUI Release，Windows GUI 继续使用独立的 GitHub Release 版本线。

### 修复

- 服务固定监听 `127.0.0.1`，并按实际回环 Host/端口校验浏览器 Origin。
- History 搜索增加 300ms 防抖、Enter 立即搜索、旧请求取消和最新响应守卫；切换会话时显示加载状态。
- 浏览器打开器不可用或端口已被占用时给出可操作结果，不再以未处理错误退出。

### 安全

- 匿名首页不再下发写凭证；未配对浏览器不能调用 API 写接口。
- 一次性配对凭证不可重用，且不进入查询参数、HTML 或服务活动日志。

[中文发布说明](docs/release-notes/v0.5.0-zh.md) · [技术发布说明](docs/RELEASE_NOTES_V0.5.0.md)

## [0.4.1] - 2026-08-08

### 新增

- Release 新增独立的 Windows x64 Automation ZIP，并继续提供单文件 GUI、Windows 完整包和 SHA-256 校验文件。
- 新增版本化中文发布说明、自动化接口中文快速入门，以及发布元数据和打包契约校验。

### 变更

- Windows Node CLI 在批量改写 rollout 时复用持久 PowerShell worker，避免为每个文件重复启动进程。
- 合并 rollout 内容扫描并复用不可变内容摘要；事务日志在 Windows 上复用已校验的写入租约。
- 缓存托管备份的大小与文件数统计，并在提交、恢复和回滚结束后刷新统计。

### 修复

- 活跃 rollout 在扫描期间变化时按“已跳过锁定文件”处理，不再中止其余历史会话同步。
- 备份统计刷新失败会作为警告返回，已提交或已恢复的操作仍会继续完成清理流程。
- 保持事务记录顺序、崩溃恢复、checked apply 重新校验和旧版备份恢复兼容性。

### 升级说明

- v0.4.0 Windows GUI 可以直接使用内置更新升级；内置更新仍只替换单文件 GUI。
- 升级不要求迁移配置、SQLite 或备份；自动化接口用户请手动下载新的 Automation ZIP 或 Windows 完整包。

[中文发布说明](docs/release-notes/v0.4.1-zh.md) · [完整变更对比](https://github.com/Dailin521/codex-provider-sync/compare/v0.4.0...v0.4.1)

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
