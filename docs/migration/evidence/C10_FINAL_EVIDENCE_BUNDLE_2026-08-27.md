# C10 最终证据与 Legacy 交接候选（2026-08-27）

状态：C10 的脱敏 evidence bundle 合同、生成器、required CI job、迁移文档和本地 Windows 门禁已准备；远端四平台候选、aggregate、全部 required jobs、最终 `1.0.0` source version、受保护 `main` 合入后复验、真实 WSL UNC、签名/公证和发布授权均未闭合。因此 C10/Phase 6 仍为 Pending，不构成 Beta、Stable、Electron 默认入口、.NET Legacy 或公开发布声明。

输入为 C9 实现 commit `73256f3187dd337bb681a1cc9810edad8f6309bb` 及 C9 证据 commit `d34654994ad790b09ed4284ce8f5d87aeace8723`。开始 C10 前已重新 fetch，并确认 `origin/main` 为 `c7ff85218a07a8e5f14132c582cad1239c52865e`；按 ADR-0011 执行 `git merge --no-ff --no-edit origin/main`，结果为 Already up to date，没有 rebase 或 force-push。C10 输出 commit 和 CI-tested commit 只由最终 bundle 记录，本文不预填尚未产生的 SHA。

## Evidence bundle 合同

- `C10_EVIDENCE_BUNDLE.v1.schema.json` 固定 `scope: vnext-c10-evidence`、`outcome: ci-verified-not-release`，发布授权、tag、npm、GitHub Release、签名、公证、更新 metadata 和跨版本升级字段只能为 `false`。
- `scripts/write-c10-evidence-bundle.mjs` 只接受 GitHub Actions 的固定 workflow context、13 个 required job 的聚合结论、C0～C9 固定 checkpoint 链，以及 C9 四目标 `candidate-index.v1.json`。它要求 tracked checkout 干净、`GITHUB_SHA` 等于实际 `HEAD`、C0～C9 证据 commit 是 tested commit 祖先，并校验四个 target、固定资产名、version/commit/lockfile/tool/audit policy 一致。
- 输出只包含 commit、公共 workflow/job 状态、版本/buildId、公共工具版本、相对证据路径、资产名/大小/hash 和 Pending 条目。绝对路径、UNC、Codex Home/SQLite/backup/profile 标识、认证文件名、凭据标记、History/消息内容与原始日志都会 hard-fail。
- CI 从 `electron-release-candidate-set` 下载已经过 C9 aggregate 验证的索引，不复制第二套平台 manifest，也不读取 runner 原始日志。输出为 `artifacts/c10/evidence-bundle.v1.json` 与绑定它的 `SHA256SUMS.txt`，只上传 CI artifact，不提交动态实例，避免“提交证据后 HEAD 再变化”的循环；四个平台候选、aggregate index 与 C10 bundle 统一保留 30 天，保证审查期内可重算引用 hash。
- `c10-evidence-bundle` 直接依赖现有 13 个 required jobs并使用 `always()` 收集终态；任一失败、取消、跳过、缺失候选 artifact 或 schema/脱敏校验失败都会让 C10 job 失败。唯一 `ci-gate` 同时把 C10 job 作为 strict dependency。

## README 与交接边界

- 中、英、日、韩入口统一说明：当前公开发布桌面端仍是 Windows .NET GUI；仓库内 Electron 是未发布候选，不是默认、Stable 或可下载产品。
- 当前架构图同时显示 CLI/Web/Electron 候选到同一 Node Core 的数据流，以及仍受支持的 .NET GUI。Electron Renderer 的 Node/任意路径/通用 IPC 禁止边界保持可见。
- 新增中英文 Electron 候选说明，记录页面能力、Prepare/Apply、Restore journal、Watch/Update、安全窗口、hidden 内部测试、四平台容器和另行发布授权边界。
- .NET Windows/macOS 文档明确是当前发布或迁移期保留实现，尚未正式标记 Legacy。只有同一最终 commit 的远端门禁闭合、source version 定为 `1.0.0`、README 最终切换并完成发布验证后，才执行 Phase 6 的 Electron 主入口/.NET Legacy 交接。
- .NET 删除不属于本 PR；稳定版发布并经过两个维护周期后再单独立项。

## 本地 Windows 门禁

环境：Windows 11 x64 build `26200`，Node `24.11.1`，npm `11.10.0`，PowerShell `7.6.4`，Git `2.52.0.windows.1`，.NET SDK `10.0.400`。全部开发测试使用临时 fixture；Electron 设置 `CPS_DESKTOP_WINDOW_DISPLAY=hidden`，没有显示或占用主屏窗口。

| 门禁 | 结果 |
| --- | --- |
| `npm test` | 418 passed，0 failed/skipped |
| `npm run workspaces:check` | 102 workspace tests passed；build/import/package boundary 通过 |
| `npm run web:build` + `npm run web:test:e2e` | production build 通过；2/2 E2E passed |
| `npm run desktop:test:e2e`（hidden） | 15 passed，1 skipped；唯一 Skip 为不可用的真实 WSL UNC 环境 |
| `npm run desktop:pack:dir` + packaged production smoke（hidden） | production boundary、native fallback、真实 fixture Status、Sync→Restore、graceful exit，2/2 passed |
| `dotnet build CodexProviderSync.sln --configuration Release` | 0 warning，0 error；包含 Windows GUI 与 macOS Avalonia 项目 |
| .NET Core/Application/Automation/App/GuiE2E Tests | 411 passed，1 skipped；唯一 Skip 为同一真实 WSL UNC 环境 |
| `npm run package:smoke` | 根 tarball content/help/status/Web shell 与 source-map 拒绝通过（本机 Node 24） |
| `npm run package:smoke:lifecycle` | lifecycle install + SQLite smoke 通过（本机 Node 24） |
| 两层 npm audit | production moderate/high/critical 为 0；完整树 high/critical 为 0 |
| C10 定向合同测试 | required jobs、四目标/资产/commit 绑定、tool/audit 一致性、脱敏拒绝与 release-false-only schema 全部通过 |

`npm run package:verify-root-tree` 只能在 `npm ci --workspaces=false --omit=dev` 的干净根 production tree 上执行；本机完整 Node 24 workspace 含 React/Electron 开发依赖，直接运行时按合同拒绝 `react`，不能把该环境误记为 Node 16 兼容门禁。Windows/Ubuntu Node `16.20.2` 的 clean-install、root-tree、tarball 和 lifecycle 继续由 required `root-package-compat` job 闭合。

## C9 Windows 候选引用

C10 不重写 C9 的精确 Windows 候选证据。当前可引用的本地候选仍绑定 C9 实现 commit `73256f3187dd337bb681a1cc9810edad8f6309bb`：

| 资产 | SHA-256 |
| --- | --- |
| Windows x64 portable ZIP | `96c0ab0c49bce31999e1d45dad01821f4a1433d72350f1366f1464c3fddcd33d` |
| Windows x64 NSIS setup | `e5d7076a571ab2742119878ac6d0efb40baf4465c4d5bc057c51bad15ea7619a` |
| ASAR | `f60ed82f18f52d25bf4ac9071cc664509817486d70c70aa89d8dd737e3534f0f` |
| native binding | `e21e5efd71fba66578e95b62554d9028064a80dafd7221bf8a8ef155de8d240a` |

这些 hash 只证明该 Windows C9 commit，不覆盖当前 C10 工作树，也不能替代最终 commit 的远端 Windows/macOS/Linux 原生候选矩阵。

## 必须由远端闭合

- 最终 PR 同一 tested commit 上，13 个 required jobs、Windows x64、macOS x64、macOS arm64、Linux x64 native candidate、四目标 aggregate 与 C10 bundle 全部只能为 `success`；任何 failed/cancelled/skipped 都阻断。
- 首轮 RC 全绿后才把根包与 Desktop source manifest 统一为 `1.0.0`，随后在新 commit 上重新运行全部门禁。版本变更不是公开发布授权。
- 最终 PR 必须保留 C0～C10 checkpoint commits，不 squash；合入后的实际 `main` commit 若与 PR tested commit 不同，必须在该 `main` SHA 再生成 bundle。
- 真实 WSL UNC、安全签名、公证、update metadata/download/restart upgrade、平台安装/卸载以及 PR 审查/分支保护结果不能从本地 Windows 推断。
- 没有明确授权时，不创建 tag，不发布 npm/GitHub Release，不签名、公证或写更新通道；README 不切换 Electron 默认入口，.NET 不标记 Legacy。
