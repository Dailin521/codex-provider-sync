# C5 共享 UI、Web 与跨运行时 Fixture 证据（2026-08-26）

状态：本地门禁通过，等待远端 required CI 与最终 PR 合入。输入 checkpoint 为 `d6b0fef`；C5 输出 commit SHA 在本 checkpoint 提交后及 C10 evidence bundle 中索引。

## 已实现边界

- `apps/web` 是唯一 Web source/build owner；旧 `web/src`、旧 Vite 配置与旧页面测试已移除，`web/dist` 仅保留根 npm 包使用的 production 静态产物。
- `packages/app-ui` 从零建立共享 React AppShell，固定八个页面：Overview、Sync、Switch Provider、Backups/Restore、History、Profiles、Diagnostics、Settings；Recovery、Operation、Error Boundary 与 Toast 是全局状态，不伪装成额外页面。
- UI 使用 React、检入的 Radix/shadcn 风格组件、Tailwind、TanStack Query、React Hook Form、Zod、Lucide 和 react-i18next；提供 `zh-CN` / `en`（英文 fallback）与 `system` / `light` / `dark`，并实现键盘操作、可见焦点、reduced motion 和 200% 等效窄视口布局。
- Core 业务流固定为 `Browser → HttpCoreClient → POST /api/core → Web Core adapter → createCoreFacade`。旧直写路由继续返回 `410 PLAN_REQUIRED`，不调用兼容 `run*`；Sync/Switch/Restore 先 Prepare，再以精确 `{schemaVersion:1, planId}` Apply。
- Web Host 保留 loopback、一次性 pairing、设备凭据 hash、Origin、64 KiB、server-managed profile/storage revision 和受管 `backupId`。响应保留 requestId；协议/输入/输出由共享 contract guard 校验，非 2xx 不能携带成功 envelope。
- Public Status 不返回 Codex/SQLite/State DB 路径，History summary 不返回 `cwd`，warning 只映射为固定安全类别/文案；未知异常不回显 message/stack/cause。备份只暴露受管 ID 与有界元数据。
- History 仅在进入页面后加载列表，点击会话后才读取详情；正文不进入 TanStack Query cache，离开详情时清空并 abort pending request。E2E 以合成 marker 证明 Overview/其他页面不会预取消息正文。
- Production HTML 每响应生成随机 CSP nonce，`script-src` / `style-src` 均不使用 `unsafe-inline`；Web Host contract test 与 Chromium E2E 都验证 nonce/header 对应且 CSP 生效。

## Phase 2 跨运行时 Fixture

- 检入 `packages/test-fixtures/static/bidirectional-backup-roundtrip` 与 `foreign-pending-restore`。输入只含 fake `example.invalid` Provider、空正文 SQLite row、`session_meta` rollout 和 seed SQL；无 SQLite 二进制、认证材料、消息正文或真实用户数据。
- `test-support/cross-runtime-fixtures.mjs` 每次在临时目录 materialize SQLite，并从同一静态输入复制四个独立方向：Node Backup→.NET Restore、.NET Backup→Node Restore、Node crash journal→.NET Restore、.NET crash journal→Node Restore。它只由专用 Windows job 调用，不混入 Node-only/Node 16 matrix。
- 比较 config、global-state primary/backup 与 rollout 原始字节 hash，并对 SQLite 全部 `threads` 列（含 `updated_at` / `updated_at_ms` / sentinel）、schema、user_version 做固定排序语义 hash 与 `integrity_check`；不把 SQLite page layout、绝对路径、运行时间或 operationId 当作跨实现合同。四个方向均恢复为初态，source journal 均重读为合法 `rolledBack` terminal，无 pending/invalid tail。
- 首次真实运行发现 .NET 写入长路径而 Node 进程看到同一目录的 Windows 8.3 短路径。Node Restore 改为以存在目录的 `realpath` 证明物理身份，同时对 manifest 原始路径逐段拒绝 symlink/junction/reparse、冻结 canonical identity，并在每个 rollout 写入前重新验证；canonical target 仍须位于 canonical rollout root、无重复且为 regular file，无法证明时 fail closed。目录链接与初检后替换均有回归测试。
- `.NET FixtureHost` 只调用公开 Core sync/restore API 并输出最小 JSON；Node/.NET CrashHost 在同一 rollout mutation 窗口真实终止。Windows `cross-runtime-fixtures` job 已加入 `ci-gate`，失败、取消或跳过均阻断。

这组证据只闭合 Phase 2 的双向 backup round-trip 与 foreign source pending 兼容，不提前声称 C8 Restore v2 自身 snapshot/journal 或完整 crash matrix 已实现。

## 依赖与发布面

- C5 依赖解析与 exact version 记录在 [ADR-0014](../../adr/0014-npm-workspace-and-dependency-boundaries.md)；所有直接依赖不使用 `^`、`~`、`workspace:*` 或 `file:`。
- 根包继续没有普通 production dependency；为已发布的 Local Web Host 批准窄 tarball runtime：`packages/contracts/dist` 与 `packages/core/src`。边界测试/packlist 禁止其他 workspace、Fixture、Electron、node_modules 和 UI source；Node 16.20.2 与 Node 24 安装态实际执行 CLI、SQLite、Web pairing/profile、真实 `/api/core getStatus` / `PROFILE_CHANGED` 脱敏以及 shell/strict CSP。造库优先使用现代 Node 的 `node:sqlite`，Node 16 才使用安装态 fallback。
- `npm audit --omit=dev --audit-level=moderate`：0 vulnerabilities；`npm audit --audit-level=high`：0 vulnerabilities。

## 本地验证

环境：Windows x64，Node `v24.11.1`，npm `11.10.0`；兼容 smoke 使用 Node `v16.20.2` + npm `8.19.4`；输入 SHA `d6b0fef`。

| 命令 | 结果 |
| --- | --- |
| `npm run workspaces:check` | build/checkJs、边界与 9 个 workspace tests：34 passed，0 failed/skipped |
| `npm run fixtures:cross-runtime` | 2 top-level tests / 4 directed Node↔.NET cases passed；最终 hash、integrity、journal terminal 全通过 |
| `npm run web:build` | Vite 8.2.2 production build 成功，2032 modules；JS 530.55 kB、gzip 163.97 kB |
| `npm run web:test:e2e` | 2 Chromium tests passed；八页、History lazy/detail/clear、精确 Apply、partial/recovery/operation/error、html lang/本地化读屏标签、双语/主题、Skip/Escape 焦点、CSP nonce、八页 640px/200% 等效 reflow 与 reduced-motion computed style，0 console errors |
| `npm test` | 325 passed，0 failed/skipped；包含 28 个 Web Host/API tests；跨运行时 fixture 由专用命令/job 隔离执行 |
| Node 16：三个边界/API/CLI contract tests | 3 files passed，0 failed/skipped |
| Node 16 + npm 8、Node 24：`npm run package:smoke:lifecycle` | 两个 runtime 均通过安装态 bin、synthetic SQLite、status JSON、Web pairing/profile、真实 Core status/stale error 脱敏、index/strict CSP |
| `npm audit --omit=dev --audit-level=moderate` / full-tree high | 均为 0 vulnerabilities |
| `dotnet build CodexProviderSync.sln --configuration Release`，另显式 build FixtureHost/CrashHost Release | 均为 0 warnings / 0 errors |
| `.NET Core/Application/Automation/App/GUI E2E tests` | Core 220 passed（1 个 Windows WSL safety 按平台预期 skipped）；其余 49 / 27 / 67 / 36 passed，0 failed |

## 已知未闭合项

- 远端 required CI 尚未运行，最终 PR 合入前 Phase 2 不标记 Completed；C5 checkpoint 也不等同于发布。
- 当前 production Web 单 JS chunk 为 530.55 kB，Vite 产生约 2.30 MB source map；这是 C9 的 code-split/生产产物排除与包内容扫描门槛，不能据 C5 声称 release artifact 已安全闭合。
- Electron BrowserWindow、Preload/IPC、Utility Process、只读 packaged smoke 与 Renderer 原生路径 picker/token 属于 C6；当前 Web Profiles 仍是浏览器 Host 管理界面，不能直接复用于 Electron 任意路径输入。
- C5 保留既有 atomic rollout Restore，不用 `truncate + copy` 临时方案换取表面上的路径绑定，也不声称最终 namespace commit 已 descriptor-bound。C8 必须把 parent-handle-relative atomic install、恢复前 snapshot/journal、逐阶段故障注入及正文/换行逐字节不变量一起闭合。
- Restore v2 独立恢复前 snapshot/journal、Watch/Diagnostics export/Update、三平台打包、SBOM/checksum、签名/公证和 C10 evidence bundle 均未在 C5 实现。
- 本 checkpoint 未创建 tag，未发布 npm/GitHub Release，未签名、公证或写更新通道。
