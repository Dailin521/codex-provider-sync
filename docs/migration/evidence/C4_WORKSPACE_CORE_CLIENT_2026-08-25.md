# C4 Workspace、Core、Contracts 与 CoreClient 证据（2026-08-25）

状态：本地门禁通过，等待远端 CI。输入 checkpoint 为 `166f6ff`；C4 最终 commit SHA 在本 checkpoint 提交后及 C10 evidence bundle 中索引。

## 已实现边界

- 根启用 npm workspaces 和单一 lockfile，建立 `apps/cli`、`apps/web`、`apps/desktop`、`packages/core`、`packages/contracts`、`packages/core-client`、`packages/app-ui`、`packages/design-system`、`packages/test-fixtures`。所有内部包均 private，根包仍是唯一 npm 发布面。
- `packages/core` 是可导入、可测试、checkJs 的 ESM JavaScript bridge，只允许导入 `src/public-api.js`；模块只导出 `createCoreFacade({resolveProfile})`，由可信 Host 将 profile ID/revision 解析为绝对路径，facade 实例精确提供 15 个 Core 方法。无效/mismatched profile 在接触 Core 路径前 fail closed，显式测试证明即使进程 `CODEX_HOME` 指向另一目录也只读取已选 profile。没有搬迁或复制锁、备份、journal、SQLite、rollout 与 service 算法。
- `packages/contracts` 用 TypeScript 固化 schema/protocol v1、Core Error DTO、Status/Plan/Operation/Progress DTO、固定方法集合以及 request/response/progress envelope。所有产品输入拒绝任意路径/未知字段；Apply 只接受 planId；成功 payload 有按方法 runtime guard；ProgressEvent 拒绝消息正文、路径和诊断扩展字段。
- `packages/core-client` 用同一接口实现 `TransportCoreClient`、`HttpCoreClient` 与 `MockCoreClient`；HTTP request 上限为 64 KiB，响应必须匹配 protocolVersion/requestId，非 2xx 不能返回成功 envelope。公共错误使用固定 code 文案、canonical severity/retryability、UUID 和 details 白名单；未知异常文本、路径、Token、消息正文和 suggestedAction 不透传，畸形 payload 收口为安全 `INTERNAL_ERROR`。
- `packages/test-fixtures` 落地 schema v1、严格字段/安全 ID、无真实用户数据标记、临时复制并自动清理的 runner；源根和树拒绝 symlink/reparse、敏感文件名和越界输入，复制后重新验证 staged tree/manifest，callback 失败仍清理。Node/.NET 未裁决差异固定为 `blocked`。
- `app-ui`、`design-system` 和 `desktop` 是可编译/可测试的受限 ownership boundary。它们明确标为 C4 contract/tokens/not-enabled 状态；没有提前声称 React 页面、Electron BrowserWindow/IPC/Utility Process 或写能力已实现。
- 根 Web 依赖迁移到 `apps/web` 的精确 devDependencies；根不再声明 React。尚未搬迁的 `web/` 源仍由该 workspace 构建；根 production-only npm 8 安装树不含 React/Vite/TypeScript/Electron，tarball 不依赖 workspace symlink。
- 真实 tarball smoke 暴露并修复了 Windows 安装目录 8.3 short-path 导致 CLI direct-execution 判断失效的问题；入口现在比较两侧 realpath，并由安装态 help/status 长期回归。

## 依赖与安全证据

依赖选择和兼容理由记录于 [ADR-0014](../../adr/0014-npm-workspace-and-dependency-boundaries.md)：TypeScript `7.0.2`、`@types/node 24.13.3`、Vite `8.2.2`、React plugin `6.1.0`、React/React DOM `19.2.8`，全部为 2026-08-25 对应稳定线的 exact version。现有 `better-sqlite3 8.7.0` 为根 Node 16 optional fallback，未强升到不兼容版本。

- 全部仓库直接依赖版本由静态门禁拒绝 `^`、`~`、`workspace:*` 和 `file:`；传递树由单一 lockfile 锁定。
- `npm audit --omit=dev --audit-level=moderate`：0 vulnerabilities。
- `npm audit --audit-level=high`：0 vulnerabilities。
- 根 packlist 为 90 entries；不存在 `apps/`、`packages/`、workspace dist、node_modules 或 Electron runtime。当前既有 Web source map 仍在 packlist，按计划由 C9 移除。

## 本地验证

环境：Windows x64，Node `v24.11.1`，npm `11.10.0`，Node 16 smoke 使用 Node `v16.20.2` + npm `8.19.4`；输入 SHA `166f6ff`。

| 命令 | 结果 |
| --- | --- |
| `npm run workspaces:build` | TypeScript project references 与 Core checkJs 通过，0 errors |
| `npm run workspaces:test` | 29 passed，0 failed/skipped；覆盖可信 Profile facade、Contracts 输入/输出/error guard、HTTP/Mock Client、Fixture hardening 与 ownership boundary |
| `node scripts/verify-workspace-boundaries.js` | package、direct dependency、import direction、root publish allowlist 全部通过 |
| `npm test` | 343 passed，0 failed/skipped |
| `npm run web:build` | Vite 8.2.2 + React 19.2.8 production build 成功，22 modules transformed |
| `npx --yes --package node@16.20.2 node --test test/workspace-boundaries.test.js test/public-api-contract.test.js test/cli-json.test.js` | 实际 Node `v16.20.2`；3 个目标文件通过 |
| Node 16.20.2 + npm 8.19.4：`npm ci --workspaces=false --omit=dev`、`runtime:verify-node16`、`package:verify-root-tree` | 真正 root-only production install 通过；无 React/Vite/TypeScript/Electron/workspace link |
| Node 16.20.2 + npm 8.19.4：`npm run package:smoke:lifecycle` | tarball 正常 lifecycle 安装；真实 bin shim help、`better-sqlite3` 创建/打开 synthetic State DB、显式临时 Home `status --json` 通过 |
| `npm run package:smoke` | Node 24 tarball 安装态 smoke 通过 |
| `npm audit --omit=dev --audit-level=moderate` | 0 vulnerabilities |
| `npm audit --audit-level=high` | 0 vulnerabilities |
| `npm pack --dry-run --json` | 90 entries；无 workspace/Electron/Fixture；既有 source map 已登记为 C9 gap |
| `dotnet build CodexProviderSync.sln --configuration Release` | 13 projects；0 warnings / 0 errors |
| `dotnet test desktop/CodexProviderSync.Core.Tests/... --no-build` | 220 passed，1 个 Windows WSL safety 测试按平台预期 skipped |
| `dotnet test`（Application / Automation / App / GUI E2E，Release `--no-build`） | 49 / 27 / 67 / 36 passed，0 failed/skipped |

CI 新增 Windows/Ubuntu Node 24 workspace contract job，以及 Windows/Ubuntu Node 16.20.2 + npm 8 root-only install、正常 tarball lifecycle、SQLite driver/bin smoke；所有 job 被 `ci-gate` 视为 required，失败、取消或跳过都会阻断。npm publish 与 tag release workflow 也在任何发布动作前复用 workspace/root package 门禁；本 checkpoint 没有触发发布。

## 已知未闭合项

- C4 只建立共享 UI ownership 和 CoreClient transport；现有 Web 尚未通过 `HttpCoreClient`，现代页面、i18n/theme/accessibility 与 Web 安全等价属于 C5。C4 不使 Phase 2 Completed。
- `HttpCoreClient` 当前通过 fake transport 契约验证；Local Web Host 的统一 `/api/core` envelope handler 与现有 pairing/profile/storage 安全规则接入属于 C5，不能把当前 client package 描述为已上线 Web transport。
- 双向 Node/.NET Backup Round-trip 和 cross-runtime Foreign Pending Restore 的共享 Fixture 证据仍是 Phase 2 退出门槛，必须在 C5 结束前闭合；目前只有 schema/runner/difference format 落地，未宣称运行时等价。
- `apps/desktop` 不含 Electron 依赖和可运行 runtime；安全 BrowserWindow、Preload、IPC、Utility Process 与只读能力属于 C6。
- 根 tarball 的既有 Web source map 仍存在，必须在 C9 的包内容扫描中移除；没有 tag、npm/GitHub Release、签名、公证或更新通道写入。
