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

默认 `release_channel=rc`，候选版本使用 `1.0.0-rc.N`（N 为本次唯一编号）。正式版批准后可显式选择 `stable-manual`，仅接受 Windows x64 `1.0.0` 和 `refs/tags/v1.0.0`。默认只生成 Actions artifact；选择创建 Draft 后仍需核对产物与证据，再按维护者授权独立公开（RC 为 prerelease；正式版为 latest）。不要复用或覆盖已存在的 tag/Release。源码清单保持 `1.0.0`，构建注入与 tag 匹配的版本。

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

未取得正式版批准时，公开包使用 Windows RC。维护者已于 2026-09-08 批准未签名、手动安装的 Windows `1.0.0` 正式版；这一批准不替代最终 SHA 的 CI/产物验收，也不包含自动安装、签名或全部平台 stable。RC 不会被现有正式版本查更推荐；正式 `1.0.0` 可由 Electron 查更入口推荐并打开下载页。

首次从 Legacy .NET 迁移必须安装或完整解压 Electron；不要把 NSIS 安装器改名为旧 `CodexProviderSync.exe`。测试和公告不得宣称旧单 EXE 更新器能直接完成迁移。

稳定 `1.0.0` 必须重新构建并验收正式版本对应资产。安装版线上更新需要对应实际版本的 metadata/安装器及升级证据，不能把本地生成的 `latest.yml` 当成线上更新已通过的证明。本次 `rc` 和 `stable-manual` 都不启用这条通道，不上传更新 metadata。审核文件保留公开前候选和未授权自动安装状态，不能改写哈希覆盖的证据。

## 5. 证据与公告

公告列明：版本、源 SHA、仅 Windows x64、主要变化、备份默认 2、Legacy 首次安装方式、未验证事项与下载文件 hash。最新发布状态以 GitHub 为准，不能提前更新迁移阶段或写成已发布。

公开 Draft 前再次核对 tag 解析到已验收 main SHA，Release 的 tag、版本与源码一致。下载 Draft 的全部 8 个资产至新的隔离目录，确认仅有 NSIS、ZIP 和工作流所列 6 个审核文件；核验 GitHub asset digest、SHA256SUMS 中所有文件以及 release/staging manifest 的 SHA、版本、容器 smoke 结果。重新取得同 SHA 的最新成功 main `ci-gate`，任何缺项/不符都停止。

正式版公告先通过 `node scripts/read-release-metadata.js --tag v1.0.0`，然后按明确授权执行 `gh release edit v1.0.0 --draft=false --prerelease=false --latest=true --notes-file docs/release-notes/v1.0.0-zh.md`。公开后再次检查 `/releases/latest`、tag SHA、8 个下载资产与哈希。RC 只公开 prerelease，不使用正式版命令，也不设 latest。

规则依据：[ADR-0039](adr/0039-windows-first-electron-release.md)、[Core 不变量](architecture/NODE_CORE_ARCHITECTURE_ZH.md)、[迁移执行索引](migration/VNEXT_MIGRATION_EXECUTION_INDEX_ZH.md)。
