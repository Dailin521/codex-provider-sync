# vNext/ADR-0014：npm Workspace、根发布包与依赖边界

- Status: Accepted
- Date: 2026-08-25
- Amended: 2026-09-03 (Node Core ownership transfer and compatibility-adapter boundary)
- Scope: vNext C4 workspace baseline, C5 shared UI/Web, C6 Electron, C8 updater, C9 release engineering and C10 evidence hardening dependency boundaries

## Context

vNext 需要让 CLI、Web、Electron、Core、Contracts、CoreClient 与共享 UI 各自拥有明确包边界，同时继续发布现有根包 `@dailin521/codex-provider-sync`、原 `codex-provider` bin 和 Node `>=16.20.2` 运行合同。若根 CLI 在迁移期依赖 workspace symlink，开发仓库可能正常而发布 tarball 会缺包；若 Node 24 的 Vite/Electron 工具链进入根生产树，Node 16 安装合同也会被间接破坏。

## Decision

仓库使用 npm workspaces 和一个根 `package-lock.json`，不引入第二套包管理器。工作区固定为 `apps/*` 与 `packages/*`，初始所有内部包均为 `private: true`；根包继续是唯一 npm 发布面。

C4 建立以下依赖方向：

```text
contracts <- core
contracts <- core-client <- app-ui <- apps/web / apps/desktop
design-system -----------^
test-fixtures（仅测试，不进入产品依赖图）
```

`packages/core` 的唯一公开导出仍是 `createCoreFacade({resolveProfile})`；可信 Host 解析 profile ID/revision 为路径后，facade 实例才提供 vNext 固定方法，不导出 `runSync/runSwitch/runRestore/runWatch` 兼容适配器或存储辅助函数。2026-09-03 起，业务编排、Watch 和 Diagnostics 已归属 `packages/core/src/application`，根 `src/service.js`、`src/watch.js`、`src/diagnostics.js` 只作 CLI/Web 兼容转发；Core 不再经过根 `src/public-api.js`。四个 CodexStorage 端口由 `packages/core/src/infrastructure` 组合，具体 Node 存储适配器仍静态引用根存储模块，以保持 Node 16 和 Electron bundler 可分析的 ESM 依赖图。Core workspace 的新边界文件逐文件启用 `@ts-check`；尚待继续拆细的成熟 service/watch runtime 显式保留为受集成测试约束的迁移实现，不能把它们误报为已完成逐函数类型化。Contracts、CoreClient、App UI 与 Desktop 边界使用 TypeScript。

根包不得声明 React、Vite、TypeScript 或 Electron 生产依赖。C5 为 Local Web Host 批准一个窄运行时例外：根 `src/web-core-adapter.js` 可以导入随 tarball 检入的 `packages/contracts/dist` 与 `packages/core/src`，根 `files` allowlist 也只允许这两个 `packages/` 子树。例外不包含 workspace manifest、TypeScript source、CoreClient、App UI、Design System、Fixture、Electron 或 node_modules，且必须由 Node 16 安装态 Web smoke 证明不依赖 workspace symlink。其余 `apps/`、`packages/` 和 workspace build output 继续禁止进入根 tarball。

C5 已把旧 `web/src`、`web/index.html` 和 Vite 配置迁入 `apps/web`；`web/dist` 仅是根发布包使用的静态部署物。React 与所有现代构建依赖只归 private workspace，根 manifest 不重复声明。这样 npm 8 读取单一 workspace lockfile 并执行 `npm ci --workspaces=false --omit=dev` 时，根生产树不会因 hoisted production 标记误装 React。

C6 的 Electron、electron-vite、electron-builder、Desktop Vite/React plugin 和 Playwright 只声明在 private `apps/desktop` workspace。Main、Utility、Preload 与 Renderer 均由 electron-vite 打成自包含边界；根 npm manifest、生产树和 tarball 明确拒绝 `electron` / `electron-*`。正常 production build 以编译期常量移除测试 bridge；只有显式 `--mode test` 的本地/CI 测试构建才包含 crash/raw-request hook，运行时环境变量不能把 production bridge 升格为测试 bridge。

## Dependency Resolution

2026-08-25 通过 npm registry 的 `latest`、`engines` 和 `peerDependencies` 元数据解析 C4 实际引入或迁移的依赖：

| Dependency | Exact version | Resolution |
| --- | --- | --- |
| TypeScript | `7.0.2` | 最新 stable，非 beta/rc/next；workspace 构建固定 Node 24 |
| `@types/node` | `24.13.3` | Node 24 类型线的最新 stable；仅用于 Core bridge 的 Node 内置模块 checkJs |
| Vite | `8.2.2` | 最新 stable；要求 Node `^20.19.0 || >=22.12.0`，由 Node 24 job 执行 |
| `@vitejs/plugin-react` | `6.1.0` | 最新 stable；peer `vite ^8.0.0`，与 Vite 8.2.2 相容 |
| React / React DOM | `19.2.8` | 最新 stable；React DOM peer `react ^19.2.8` 闭合 |
| React types | `@types/react 19.2.18`、`@types/react-dom 19.2.5` | 仅供 Node 24 Web/共享 UI TypeScript 构建 |
| TanStack Query | `@tanstack/react-query 5.102.3` | C5 页面状态与显式失效；History 正文不进入 Query cache |
| React Hook Form / resolver / Zod | `react-hook-form 7.86.0`、`@hookform/resolvers 5.9.1`、`zod 4.4.3` | C5 产品输入 schema 与表单验证 |
| Radix primitives | `@radix-ui/react-dialog 1.1.23`、`@radix-ui/react-select 2.3.7`、`@radix-ui/react-slot 1.3.3`、`@radix-ui/react-toast 1.2.23` | C5 检入组件的无障碍 primitives；不引入远程运行时 |
| i18n | `i18next 26.4.0`、`react-i18next 17.0.12` | `zh-CN` / `en`，英文 fallback |
| UI utilities | `lucide-react 1.34.0`、`class-variance-authority 0.7.1`、`clsx 2.1.1`、`tailwind-merge 3.6.0` | 图标与检入组件样式组合 |
| Tailwind | `tailwindcss 4.3.3`、`@tailwindcss/vite 4.3.3` | 仅在 Node 24 Web workspace build 使用 |
| Playwright | `@playwright/test 1.62.1` | C5 production bundle 的真实 Chromium 验收；仅 dev dependency |
| Shared UI unit test | `vitest 4.1.11`、`@testing-library/react 16.3.2`、`@testing-library/dom 10.4.1`、`@testing-library/user-event 14.6.6`、`@testing-library/jest-dom 7.0.1` | C10 审计补强；全部只在 private `app-ui` workspace。Vitest 支持 Node 24 且与现有 Vite peer range 闭合 |
| Shared UI DOM runtime | `jsdom 29.1.1` | 支持 Node `>=24.0.0` 的最新稳定线；`30.0.1` 要求 `^24.15.0`，不满足既有 Node 24.11 本地/证据基线，故不采用 |
| Electron | `44.0.0` | C6 解析时最新 stable，并处于 Electron 官方支持线；只在 Desktop workspace |
| `electron-vite` | `5.0.0` | C6 最新 stable；peer 支持 Vite 5～7，与 Desktop Vite 7.3.6 闭合 |
| `electron-builder` | `26.15.7` | C6 最新 stable；先提供三平台 unpacked Alpha 构建，发布目标与 native fallback 留到 C9 |
| `electron-updater` | `6.8.9` | C8 解析时最新 stable，非 `next`/preview；仅 Desktop production Main 使用，与 `electron-builder` 的 GitHub provider 元数据闭合 |
| Desktop Vite / React plugin | `vite 7.3.6`、`@vitejs/plugin-react 5.2.0` | `electron-vite 5.0.0` 的兼容组合；与 Web workspace 的 Vite 8/plugin 6 分开锁定 |
| 根 `better-sqlite3` | `8.7.0` | 保留现有根 optional fallback；更新版本不满足根 Node 16 合同，不进入根生产树升级 |
| Desktop `better-sqlite3` | `13.0.3` | C9 最新 stable；仅 Electron production fallback，按 Electron 44 ABI rebuild，包内只保留 target-native binding |
| `@electron/asar` / `@electron/fuses` | `4.3.0` / `2.1.3` | C9 build-only 审计工具；读取 ASAR header/entry integrity 与最终 executable fuse wire |
| `resedit` / `plist` | `3.1.0` / `5.0.0` | C9 build-only 审计工具；分别验证 Windows PE 与 macOS Info.plist 的 embedded ASAR integrity binding |

所有直接 dependency/devDependency/optionalDependency/peerDependency 使用精确版本，不使用 `^`、`~`、`workspace:*`、`file:` 或未锁定 URL。传递依赖由唯一 lockfile 锁定。候选经 `npm audit --omit=dev --audit-level=moderate` 与全树 `npm audit --audit-level=high` 检查；任一不合格候选不得进入 checkpoint。

Electron、electron-vite 与 electron-builder 已在 C6 按上述规则解析。C9 解析并锁定 Desktop `better-sqlite3`、ASAR/Fuse/PE/plist 审计工具；这些依赖只存在于 private Desktop workspace，不得写入根 manifest、根 production tree 或根 npm tarball。Desktop runtime SBOM 从唯一 lockfile 投影 production closure，必须包含 Electron framework 与 native fallback，但排除 Playwright、builder、Vite、审计工具和 test fixtures。

C8 增加的 `electron-updater` 只能由 `apps/desktop/src/main/updater.ts` 动态加载；Renderer、Preload、Utility、共享 UI 和 Core 不得导入 updater、指定 URL/channel 或接触原始 `UpdateInfo`。安装前由同一 `CoreRuntimeSupervisor` 同步关闭 restart gate，排空已 admission 的写请求，再执行 active Watch 与全部 Profile recovery 复核；失败路径必须重新开放 gate。C8 实现受控状态机与安装门禁，但 `apps/desktop` 版本仍为 `0.0.0` 时发布通道保持 disabled；实际版本注入、签名、更新 metadata 与真实跨版本升级 smoke 属于 C9/C10 发布门禁，不因依赖已接入而视为已发布。

## Invariants

- 根 package name、bin、Node engine 和公开 CLI 文件闭包保持兼容；
- Node 16 job 只安装根生产树并运行现有 Node 测试；现代 workspace、Web 和未来 Electron 只在 Node 24 构建；
- `contracts` 不依赖 Node、DOM、React 或 Electron；`core-client` 只依赖 contracts；App UI 不依赖 Node/Electron；Renderer 将来不得导入 Core；
- HTTP、Desktop 和 Mock transport 使用同一版本化 request/response/progress envelope；协议不兼容先于业务失败；
- Apply transport 输入严格为 `{schemaVersion: 1, planId}`；Legacy error adapter 按结构化 code/DTO 分类，不解析 message；
- 根 tarball 必须在真实临时目录安装并执行 help/status/Web health 与 production shell，而不是只依赖 workspace 开发环境或 pack 预览。

## Consequences

现代应用通过共享 Core 运行同一业务实现，根 CLI/Web 仍能独立安装。C4 的 `web/` source ownership 过渡已在 C5 结束：唯一现代 Web source 位于 `apps/web` 与 `packages/app-ui`，根只承载静态 `web/dist`、兼容适配器和经过审计的 Host/Core runtime 闭包。窄 tarball 例外增加了 packlist 与 Node 16 回归责任，任何扩大都必须另行修改本 ADR、边界测试和安装态 smoke。

## Validation

- Node 24：完整 `npm ci`、TypeScript build、Core 文件级 checkJs、workspace contract tests、Web production build；
- Node 16.20.2 + npm 8：根 production-only `npm ci` 无 workspace/UI 链接；根 tarball 分别完成无 lifecycle 内容检查与正常 lifecycle 安装，实际 bin help、synthetic SQLite 创建/打开和显式临时 Codex Home `status --json`；
- packlist：不存在 `apps/`、未批准的 `packages/`、workspace manifest、Electron、Fixture 或 node_modules；只允许 `packages/contracts/dist` 与 `packages/core/src`；
- import contract：CoreFacade 不得导入根实现；`packages/core/src/infrastructure/node-core-ports.js` 是 Core 到既有 Node 存储模块的唯一适配面，根侧只允许已登记的 Web host 与 service/watch/diagnostics 兼容转发导入 Core 实现；
- security：生产树 moderate/high/critical 为零，全树 high/critical 为零。
- C6：Node 24 production/test 两种 Electron bundle、production bundle test-hook 排除、Windows unpacked production SQLite/Utility smoke、真实 crash/restart/journal preflight E2E；同一 job 在 Windows/macOS/Linux 运行并受唯一 `ci-gate` 约束。
- C9：Node 24 host-native 四目标 candidate build；Electron ABI fallback probe、最终容器 ASAR/Fuse/敏感内容审计、Status 与 Sync→Restore smoke、SBOM/checksum/manifest，以及四目标 commit/lockfile/tool/policy aggregate；CI 命令固定 `--publish never`。

## Related

- [保留 Node CLI 合同](0003-preserve-node-cli-contract.md)
- [共享 UI 通过 CoreClient](0007-shared-ui-through-core-client.md)
- [渐进迁移](0008-incremental-migration-no-big-bang-rewrite.md)
- [Electron 构建选择](0010-electron-vite-and-electron-builder.md)
