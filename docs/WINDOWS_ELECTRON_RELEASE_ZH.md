# Windows Electron 发布操作说明

本轮仅交付 Windows x64 Electron。CLI/Web 的 npm 发布、macOS/Linux 公开安装包及旧 .NET 发布独立处理；跨平台自动测试仍须全部通过。

## 1. 提交与合并

1. 保留本地已有变更，核对源码、合同、配套生成物与测试清单。不要提交 `output/`、Playwright traces、真实数据或本地安装目录。
2. 确认 V1 包含最新 `origin/main`。以可审计提交推送到 V1，等待 PR 当前 head 对应的完整 `ci-gate` 成功；旧 head 的绿灯不可复用。
3. 处理所有未解决审查，采用 merge commit 合并，禁止 squash、rebase、force-push 或绕过失败门禁。
4. main 合入后的实际 SHA 必须再次通过 `ci-gate`。所有产物与测试结论记录实际 SHA，不以 PR 分支名代替。

## 2. 候选包与独立流程

`.github/workflows/publish-electron-windows.yml` 是独立的手动候选准备流程。现有 `publish.yml` 仍构建 Legacy .NET；`publish-npm.yml` 仍发布 npm。本轮不得运行后两者。

新流程接受已经存在、与候选版本匹配的 tag、完整预期 SHA，以及是否创建 Draft 的显式选项。tag 必须解析到该 SHA，且在 main 历史中；对应 main push 的 `ci-gate` 必须成功。流程不创建 tag、不发布 stable、不设置 latest、不发布 npm。

候选版本使用 `1.0.0-rc.N`（N 为本次唯一编号）。默认只生成 Actions artifact；选择创建 Draft 后仍需核对产物与证据，才能按维护者授权公开为 prerelease。不要复用或覆盖已存在的 tag/Release。源码清单保持 `1.0.0`，候选版本由构建注入。

仅允许以下 Windows 安装资产及其审核文件：

- `CodexProviderSync-<version>-windows-x64-setup.exe`
- `CodexProviderSync-<version>-windows-x64-portable.zip`
- SHA-256、SBOM、ASAR/原生 SQLite 审核和容器验收证据。

准备流程复用现有候选构建与审核，明确 `--publish never`。现有 CI 仍会验证其他平台候选，但不意味着本轮会公开那些下载。

## 3. 验收清单

- 核心回归、架构/Provider I/O 门禁、完整跨平台 CI、根包 Node 16 安装兼容通过。
- 生产依赖无 moderate/high/critical，完整依赖树无 high/critical；其他告警如实列明。
- NSIS 安装、启动、fixture Status、Sync → Restore、正常退出、卸载；portable ZIP 解压后完成同样业务 smoke。
- 原生 SQLite fallback、ASAR、包体门禁、依赖许可证与无敏感内容检查通过。
- Windows 人工功能测试单独记录版本/日期/范围；不能将旧本地包的人工结论自动套到新产物。
- 签名、线上下载/升级、真实 WSL 以及其他平台人工验收，未做就明确列为未验证。

本地测试用 D 盘独立临时目录，窗口隐藏或在副屏；不触碰真实 Codex Home。CI 使用 runner 的临时夹具环境。

## 4. RC 与正式版

尚无签名或线上更新证据时，公开包明确标记为 Windows RC、未签名、手动安装，不声称自动升级或全部平台 stable。RC 不会被现有正式版本查更推荐；测试者从明确的 prerelease 下载。

首次从 Legacy .NET 迁移必须安装或完整解压 Electron；不要把 NSIS 安装器改名为旧 `CodexProviderSync.exe`。测试和公告不得宣称旧单 EXE 更新器能直接完成迁移。

稳定 `1.0.0` 必须重新构建并验收正式版本对应资产；签名/自动更新范围与限制需维护者明确接受。安装版线上更新需要对应实际版本的 metadata/安装器及升级证据，不能把本地生成的 `latest.yml` 当成线上更新已通过的证明。本次 RC 准备流程不启用这条通道。

## 5. 证据与公告

公告列明：版本、源 SHA、仅 Windows x64、主要变化、备份默认 2、Legacy 首次安装方式、未验证事项与下载文件 hash。最新发布状态以 GitHub 为准，不能提前更新迁移阶段或写成已发布。

规则依据：[ADR-0039](adr/0039-windows-first-electron-release.md)、[Core 不变量](architecture/NODE_CORE_ARCHITECTURE_ZH.md)、[迁移执行索引](migration/VNEXT_MIGRATION_EXECUTION_INDEX_ZH.md)。
