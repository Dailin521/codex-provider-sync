# Windows Electron 发布操作说明

本轮仅交付 Windows x64 Electron。CLI/Web 的 npm 发布、macOS/Linux 公开安装包及旧 .NET 发布独立处理；跨平台自动测试仍须全部通过。

## 1. 提交与合并

1. 保留本地已有变更，核对源码、合同、配套生成物与测试清单。不要提交 `output/`、Playwright traces、真实数据或本地安装目录。
2. 首次 V1 迁移使用 V1 分支；后续补丁从最新 `origin/main` 创建独立修复分支。以可审计提交推送并创建指向 main 的 PR，等待当前 head 对应的 `ci-gate` 成功；产品改动必须完整检查，普通文档例外见下文，旧 head 的绿灯不可复用。
3. 处理所有未解决审查，采用 merge commit 合并，禁止 squash、rebase、force-push 或绕过失败门禁。
4. main 合入后的实际 SHA 必须再次通过 `ci-gate`。所有产物与测试结论记录实际 SHA，不以 PR 分支名代替。

部分重跑：GitHub 的“仅重跑失败 job”会保留本次 workflow 中已成功 job 的产物。C10 可复用同一仓库、同一 `runId`、同一 tested commit 且来自当前或更早正整数 `runAttempt` 的历史 Release 验证证据；拒绝跨运行、跨提交、未来或无效次数。全部 required jobs 仍必须成功，Release 资产/二进制/备份哈希校验不变。新 bundle 的 `workflow.runAttempt` 记录当前次数，`historicalFormalRelease.sourceRunAttempt` 如实记录证据来源次数，不改写源证据；v1 schema 将该新增字段设为可选，仅为兼容历史 bundle。重跑成功不抹去之前失败的记录，也不授权发布。

### 普通文档 PR 的轻量检查

仅 `pull_request` 的整个 base…head 差异非空、且所有路径都属于根 `README.md`、`CHANGELOG.md` 或 `docs/release-notes/v*-zh.md` 时，允许轻量检查。不按最后一次提交判断；重命名检查新旧两个路径。合同、ADR、行为夹具、发布流程文档、工作流、配置、依赖和代码均触发完整 CI；未知路径或空差异也走完整 CI，Git 比较失败直接使门禁失败。

轻量检查无需安装依赖，验证改动文档的本地链接目标、全项目版本一致性和改动的中文发布说明。远程链接与 Markdown 锚点不做可达性验证。重型任务和 C10 明确跳过，但 `ci-gate` 始终执行，只有分类与文档检查成功且跳过状态符合预期才通过。轻量 PR 不生成候选包或 C10 发布证据。

main push 始终完整执行全部原有任务和 C10，最终 SHA 的发布门禁不变。同一 PR 新提交可取消过期检查；main 检查和独立发布工作流不被该规则取消。首次提交前一次性核对代码、版本、文档和测试要求，避免补漏引发重复 CI。

## 2. 候选包与独立流程

`.github/workflows/publish-electron-windows.yml` 是独立的手动候选准备流程。现有 `publish.yml` 仍构建 Legacy .NET；`publish-npm.yml` 仍发布 npm。本轮不得运行后两者。

新流程接受已经存在、与候选版本匹配的 tag、完整预期 SHA，以及是否创建 Draft 的显式选项。tag 必须解析到该 SHA，且在 main 历史中；对应 main push 的 `ci-gate` 必须成功。流程不创建 tag、不发布 stable、不设置 latest、不发布 npm。

默认 `release_channel=rc`，候选版本使用根 `package.json` 的实际严格 `1.0.x-rc.N` 基础版本（`x` 为无前导零的补丁号，`N` 为本次唯一编号）。正式版批准后可显式选择 `stable-manual`，仅接受 Windows x64 严格 `1.0.x` 与同名 `refs/tags/v1.0.x`。默认只生成 Actions artifact；选择创建 Draft 后仍需核对产物与证据，再按维护者授权独立公开（RC 为 prerelease；正式版为 latest）。不要复用或覆盖已存在的 tag/Release。源码清单、构建注入版本与 tag 必须完全一致。

仅允许以下 Windows 安装资产及其审核文件：

- `CodexProviderSync-<version>-windows-x64-setup.exe`
- `CodexProviderSync-<version>-windows-x64-portable.zip`
- SHA-256、SBOM、ASAR/原生 SQLite 审核和容器验收证据。

准备流程复用现有候选构建与审核，明确 `--publish never`。现有 CI 仍会验证其他平台候选，但不意味着本轮会公开那些下载。

## 3. 验收清单

从 1.0.2 起，`stable-updater` 的安装版和便携版容器 smoke 都实际调用公开源检查更新（`CPS_VERIFY_PUBLIC_UPDATE_CHECK=true`），必须返回已检查结果而非 `error`。这一步保留真实模块加载、网络栈和发布源；网络失败须诊断后再重跑，不能退回只验证 `idle`。不会下载或安装更新，不能据此宣称跨版本升级已验收。单元回归 `updater-module.test.mjs` 仅替换 Electron host，保留真实 CommonJS 依赖，以检出 ESM 导入兼容问题。

应用内更新的显式渠道为 `stable-updater`，仅 Windows x64 严格 `1.0.x`；仍默认 RC。此渠道增加 `latest.yml` 与安装器 `.exe.blockmap`，两者必须来自同次构建并通过 metadata/大小/SHA512、blockmap、包内 GitHub 配置和 SHA256 清单审核。下载和安装均需用户确认，便携版仍手动。固定依赖对有 publisherName 的包继续校验签名；无发布者的未签名包不能冒充已签名更新。

不得移动已有 tag 或覆盖既有 Release；当前 1.0.2 使用新标签和新 Release，见第 6 节。历史 1.0.1 的单次覆盖例外已经结束。真实安装版跨版本下载/重启、数据保持、失败重试与安装门禁需独立记录，首批更新能力上线仍须在公告明确线上跨版本尚未验收，不以单元测试替代。

- 核心回归、架构/Provider I/O 门禁、完整跨平台 CI、根包 Node 16 安装兼容通过。
- 生产依赖无 moderate/high/critical，完整依赖树无 high/critical；其他告警如实列明。
- NSIS 安装、启动、fixture Status、Sync → Restore、正常退出、卸载；portable ZIP 解压后完成同样业务 smoke。
- 原生 SQLite fallback、ASAR、包体门禁、依赖许可证与无敏感内容检查通过。
- Windows 人工功能测试单独记录版本/日期/范围；不能将旧本地包的人工结论自动套到新产物。
- 签名、线上下载/升级、真实 WSL 以及其他平台人工验收，未做就明确列为未验证。

本地测试用 D 盘独立临时目录，窗口隐藏或在副屏；不触碰真实 Codex Home。CI 使用 runner 的临时夹具环境。

### 推送前先做本地预检

- 先一次性核对受影响的 IPC 合同、单元测试和生产 E2E 断言，再运行对应测试；不能只验证开发态或 unpacked 的一种模式。
- 更新功能同时验证便携版的手动更新和授权安装版的应用内更新。容器验收按已审计的授权渠道与容器类型传入 `CPS_EXPECTED_UPDATE_MODE`：只有授权 NSIS 是 `installer`，其余为 `manual`；不能根据应用返回值反推预期。
- 本地复用同一生产构建检查两种布局时，安装版布局只能使用 D 盘独立副本；测试仅检查状态与界面，不执行下载、安装或卸载。这不算真实 NSIS 安装或线上跨版本升级通过。
- 不在日常用户环境运行真实 NSIS smoke；自定义安装目录仍可能影响同一应用的注册信息。真实安装、退出、卸载在隔离 Windows 环境或 CI 完成，其他平台在对应 runner 验证。
- 仅测试脚本变更无需重复本地打包；记录使用的构建来源。发布前仍须对最终 SHA 的正式产物完成完整验收，不能拿旧本地包代替新产物证据。

## 4. RC 与正式版

未取得对应版本的正式版批准时，公开包使用 Windows RC。维护者已于 2026-09-08 批准未签名、手动安装的 Windows `1.0.0` 正式版；后续严格 `1.0.x` 正式版仍须逐版取得发布批准。这不替代最终 SHA 的 CI/产物验收，也不包含自动安装、签名或全部平台 stable。RC 不会被现有正式版本查更推荐；正式 `1.0.x` 可由 Electron 查更入口推荐并打开下载页。

首次从 Legacy .NET 迁移必须安装或完整解压 Electron；不要把 NSIS 安装器改名为旧 `CodexProviderSync.exe`。测试和公告不得宣称旧单 EXE 更新器能直接完成迁移。

稳定 `1.0.x` 必须重新构建并验收正式版本对应资产。安装版线上更新需要对应实际版本的 metadata/安装器及升级证据，不能把本地生成的 `latest.yml` 当成线上更新已通过的证明。本次 `rc` 和 `stable-manual` 都不启用这条通道，不上传更新 metadata。审核文件保留公开前候选和未授权自动安装状态，不能改写哈希覆盖的证据。

## 5. 证据与公告

公告列明：版本、源 SHA、仅 Windows x64、主要变化、备份默认 2、Legacy 首次安装方式、未验证事项与下载文件 hash。最新发布状态以 GitHub 为准，不能提前更新迁移阶段或写成已发布。

公开 Draft 前再次核对 tag 解析到已验收 main SHA，Release 的 tag、版本与源码一致。下载全部资产至新的隔离目录：手动渠道为 8 个，`stable-updater` 为 10 个（额外 `latest.yml` 与 `.exe.blockmap`），其余为 NSIS、ZIP 和工作流所列 6 个审核文件。核验 GitHub asset digest、SHA256SUMS 中所有文件以及 release/staging manifest 的 SHA、版本、容器 smoke 结果。重新取得同 SHA 的最新成功 main `ci-gate`，任何缺项/不符都停止。

正式版公告先通过 `node scripts/read-release-metadata.js --tag v1.0.x`，然后按该版本的明确授权执行 `gh release edit v1.0.x --draft=false --prerelease=false --latest=true --notes-file docs/release-notes/v1.0.x-zh.md`。公开后再次检查 `/releases/latest`、tag SHA、对应渠道的全部下载资产与哈希。RC 只公开 prerelease，不使用正式版命令，也不设 latest。

## 6. 当前 1.0.2 发布与历史例外

维护者已明确选择发布 **1.0.2**，修复更新器模块加载失败。本次遵循第 1～5 节：最终 main SHA 的 CI 通过后创建新的 `v1.0.2` 标签，以 `stable-updater` 准备并验收 10 项附件，再创建/公开对应的新 Release。不得移动 `v1.0.1` 标签、覆盖其附件或撤下原 Release。

受影响的 1.0.1 安装版无法正常检查更新，公告必须要求手动安装 1.0.2 一次，并如实列明未签名、线上跨版本下载安装尚未验收；不发布 npm、Legacy 或其他平台包。

历史记录：2026-09-11 曾按单次明确授权重新发布 1.0.1，原标签 SHA 为 `30c276a585344fa3f4f8fee4066565c97981d038`，原 Release ID 为 `386924117`；原文件与哈希先备份再替换。该例外已经结束，仅保留审计来源，不再是当前操作指令，也不授权任何后续覆盖。

规则依据：[ADR-0039](adr/0039-windows-first-electron-release.md)、[Core 不变量](architecture/NODE_CORE_ARCHITECTURE_ZH.md)、[迁移执行索引](migration/VNEXT_MIGRATION_EXECUTION_INDEX_ZH.md)。
