# C6 Electron Read-only Alpha 证据（2026-08-26）

状态：候选实现的本地 Windows x64 门禁通过；C5 required CI、远端 Windows/macOS/Linux C6 CI 与最终 PR 合入均未闭合，因此 C6/Phase 3 仍为 Pending。输入 checkpoint 为 `8a53ce8`；C6 输出 commit SHA 在本 checkpoint 提交后及 C10 evidence bundle 中索引。

## 已实现边界

- 数据流固定为 `Electron Renderer → DesktopCoreClient → sandboxed Preload → Main IPC/Supervisor → Electron Utility Process → @codex-provider-sync/core`。Main 不执行 Core 业务，Utility 不深度导入根 `src/`。
- BrowserWindow 固定 `nodeIntegration:false`、`nodeIntegrationInWorker:false`、`contextIsolation:true`、`sandbox:true`、`webSecurity:true`，并关闭 insecure content、experimental features 与 webview。Renderer 只从 `cps-app://app` 本地协议加载，CSP 无 `unsafe-inline`/`unsafe-eval`；导航、新窗口、webview 与权限默认拒绝。
- C6 四层 allowlist 精确为 `getStatus/listBackups/listHistory/getHistorySession/getDiagnostics`。DesktopCoreClient、Preload、Main IPC 与 Utility 都拒绝 Sync/Switch/Restore/Prune/Watch；Renderer 不提交路径或任意 IPC channel。
- Profile Host 文件由 Main/Utility 可信解析；Renderer 只见 `id/name/revision/codexHomeConfigured/sqliteHomeConfigured`。Status、Backups、Diagnostics 和 History summary 均不回传 Codex Home、SQLite Home、backup/rollout 路径或任意异常原文。
- production build 通过编译期 flag 移除测试 bridge；设置运行时 `CPS_DESKTOP_E2E=1` 也不能启用 `test.requestRaw/crashRuntime`。只有 `electron-vite --mode test` 的非发布测试构建包含受 sender 校验的 crash hook。
- History 列表标题只能来自显式 session metadata；无标题由 UI 本地化显示“Untitled session/未命名会话”，不得回退到首条用户正文。正文仅在显式打开详情后读取，返回列表时 abort/清空且不进入 Query cache。

## Runtime 与并发证据

- Utility Hello 同时绑定 runtime/core protocol、app/core version、buildId、32-byte 随机 nonce、generation 和精确只读 capabilities；任何漂移在业务请求前 fail closed。
- Runtime crash 立即把全部 pending request 归类为 `CORE_RUNTIME_CRASHED`，不后台重启；下一次用户请求才启动一个新 generation，并对每个并发 profile/revision 先执行 `getStatus` pending-journal preflight。
- preflight 失败保留“仍需预检”状态，下一次请求重新检查，不能因 Runtime 已完成 Hello 而绕过；真实 E2E fixture 的 valid pending journal 使重启后 `recoveryBlocked:true`。
- shutdown 是终结性幂等操作，即使 Runtime 尚未启动，调用后也永久拒绝新请求；timeout 会终止当前 generation，杜绝迟到响应与复用 requestId 错误关联。response 的 requestId/generation/operationId 与 preflight profile 必须关联。
- 独立只读审查首次发现 History 标题正文泄漏及 shutdown/timeout/多 Profile/preflight-failure 竞态；实现与回归测试在本 checkpoint 内修复后重新验证。

## 构建、依赖与 CI

- C6 精确锁定 Electron `44.0.0`、electron-vite `5.0.0`、electron-builder `26.15.7`、Desktop Vite `7.3.6`、React plugin `5.2.0`；版本裁决见 [ADR-0014](../../adr/0014-npm-workspace-and-dependency-boundaries.md)。
- production bundle 不含 source map、workspace import 或 test hook；sandbox preload 唯一 runtime `require` 为 `electron`。Root manifest/production tree/tarball 显式拒绝 Electron 与 `electron-*`，根 CLI 继续保持 Node `>=16.20.2`。
- `electron-readonly` required job 使用 Node 24，在 Windows、Ubuntu、macOS 执行 unit/security contract、production bundle、`electron-builder --dir`、unpacked production SQLite/History smoke，以及 test-build Utility crash/restart；Linux 通过 Xvfb。该 matrix 已加入唯一 `ci-gate`，任一失败、取消或跳过都阻断。
- C6 的 `--dir` 只证明 unpacked Alpha 的 builder 布局与启动，不是 C9 发布产物。Installer/DMG/AppImage/deb、macOS 双架构、native fallback ABI/asar、SBOM/checksum、签名/公证与更新通道仍被 C9 阻断。

## 本地验证

环境：Windows 11 x64，Node `v24.11.1`，npm `11.10.0`；输入 SHA `8a53ce8`。

| 命令 | 结果 |
| --- | --- |
| `npm run desktop:test` | Desktop security/IPC/profile/runtime/protocol unit contracts：26 passed，0 failed/skipped |
| `npm run desktop:build` + `npm run desktop:verify-production-bundle` | production Main/Utility/CJS Preload/Renderer 构建成功；无 source map/workspace import/test hook，Preload 仅 require Electron |
| `npm run desktop:test:e2e:production` | 1/1 passed；production bridge 无 test/Node，真实 SQLite `openai=1`，valid pending journal，写方法 fail closed，History 正文未预取 |
| `npm run desktop:test:e2e` | 1/1 passed；安全 webPreferences/CSP/导航/权限、只读页面、路径脱敏、显式 History detail、Utility crash→generation+1→journal preflight、fixture Hash 不变 |
| `npm run desktop:pack:dir` + `npm run desktop:test:e2e:packaged` | Windows x64 unpacked production app 1/1 passed；asar 内 Main/Utility/Preload/Renderer 实际启动并读取真实 SQLite |
| `npm run workspaces:check` | 9 个 workspace 共 62 passed，0 failed/skipped；TypeScript/checkJs、依赖、导入与 root publish 边界通过 |
| `npm test` | 345 passed，0 failed/skipped |
| `npm run web:build` + `npm run web:test:e2e` | Vite 8.2.2 production build 成功，2034 modules；JS 532.99 kB、gzip 164.78 kB；Chromium 2/2 passed |
| Node 24、Node 16.20.2 + npm 8.19.4：`npm run package:smoke:lifecycle` | 两个 runtime 均通过 root tarball 安装态 bin、synthetic SQLite、JSON status、Web pairing/profile、真实 Core status/stale error 脱敏、strict CSP；安装树无现代 UI/Electron 依赖 |
| `npm audit --omit=dev --audit-level=moderate` / `npm audit --audit-level=high` | 均为 0 vulnerabilities |
| `js-yaml` parse CI / builder YAML | `.github/workflows/ci.yml` 与 `apps/desktop/electron-builder.yml` 均解析成功 |

## 未闭合项与后续 TODO

- C5 的远端 required CI 尚未闭合，C6 只能作为候选实现保留；远端 `electron-readonly` matrix 也尚未运行，因此 macOS/Linux unpacked 启动、runner architecture 与平台库证据仍为 Pending，Phase 3/C6 不标记 In Progress 或 Completed。
- C7 才开放 Electron Sync/Switch 的 Prepare/Apply、model intent、Progress/Cancel、Busy/Partial 和 Backup→Restore 回环；C6 UI 与 IPC 都无写入口。
- C8 才实现 Restore v2 snapshot/journal/crash matrix、Watch、诊断包落盘、Update 与 recovery action；C6 Diagnostics 只是只读脱敏摘要。
- C9 才收敛 production app 体积和依赖闭包，完成 `node:sqlite`/`better-sqlite3` fallback、ABI rebuild/`asarUnpack`、四类发行产物、包内容扫描、SBOM/checksum 与全平台安装/卸载 smoke。
- 本 checkpoint 未创建 tag，未发布 npm/GitHub Release，未签名、公证或写更新通道；.NET 实现保持可构建且未删除。
