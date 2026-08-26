# vNext/ADR-0014：npm Workspace、根发布包与依赖边界

- Status: Accepted
- Date: 2026-08-25
- Amended: 2026-08-26 (C5 shared UI/Web runtime)
- Scope: vNext C4 workspace baseline and C5 shared UI/Web dependency boundary

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

`packages/core` 在 C4 仅通过一个已审计例外导入根 `src/public-api.js`，不移动或翻译锁、备份、journal、SQLite、rollout 和 service 算法。包本身只导出 `createCoreFacade({resolveProfile})`；可信 Host 解析 profile ID/revision 为路径后，facade 实例才提供 vNext 固定方法，不导出 `runSync/runSwitch/runRestore/runWatch` 兼容适配器或存储辅助函数。Core workspace 保持 ESM JavaScript；C4 的 JSDoc、`checkJs`、`tsc --noEmit` 只覆盖该可信边界 bridge，仍在根 `src/` 的高风险算法继续由既有 JS 与集成测试约束，后续迁入时逐模块加入 checkJs。Contracts、CoreClient、App UI 边界和 Desktop 边界使用 TypeScript。

根包不得声明 React、Vite、TypeScript 或 Electron 生产依赖。C5 为 Local Web Host 批准一个窄运行时例外：根 `src/web-core-adapter.js` 可以导入随 tarball 检入的 `packages/contracts/dist` 与 `packages/core/src`，根 `files` allowlist 也只允许这两个 `packages/` 子树。例外不包含 workspace manifest、TypeScript source、CoreClient、App UI、Design System、Fixture、Electron 或 node_modules，且必须由 Node 16 安装态 Web smoke 证明不依赖 workspace symlink。其余 `apps/`、`packages/` 和 workspace build output 继续禁止进入根 tarball。

C5 已把旧 `web/src`、`web/index.html` 和 Vite 配置迁入 `apps/web`；`web/dist` 仅是根发布包使用的静态部署物。React 与所有现代构建依赖只归 private workspace，根 manifest 不重复声明。这样 npm 8 读取单一 workspace lockfile 并执行 `npm ci --workspaces=false --omit=dev` 时，根生产树不会因 hoisted production 标记误装 React。

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
| `better-sqlite3` | `8.7.0` | 保留现有根 optional fallback；更新版本不满足根 Node 16 合同，不在 C4 强升 |

所有直接 dependency/devDependency/optionalDependency/peerDependency 使用精确版本，不使用 `^`、`~`、`workspace:*`、`file:` 或未锁定 URL。传递依赖由唯一 lockfile 锁定。候选经 `npm audit --omit=dev --audit-level=moderate` 与全树 `npm audit --audit-level=high` 检查；任一不合格候选不得进入 checkpoint。

Electron、electron-vite、electron-builder 和 native fallback 的具体版本不在 C4/C5 提前解析：分别在 C6/C9 按同一规则选择并记录。尤其不得为了“先搭骨架”把 Electron 写入根 manifest。

## Invariants

- 根 package name、bin、Node engine 和公开 CLI 文件闭包保持兼容；
- Node 16 job 只安装根生产树并运行现有 Node 测试；现代 workspace、Web 和未来 Electron 只在 Node 24 构建；
- `contracts` 不依赖 Node、DOM、React 或 Electron；`core-client` 只依赖 contracts；App UI 不依赖 Node/Electron；Renderer 将来不得导入 Core；
- HTTP、Desktop 和 Mock transport 使用同一版本化 request/response/progress envelope；协议不兼容先于业务失败；
- Apply transport 输入严格为 `{schemaVersion: 1, planId}`；Legacy error adapter 按结构化 code/DTO 分类，不解析 message；
- 根 tarball 必须在真实临时目录安装并执行 help/status/Web health 与 production shell，而不是只依赖 workspace 开发环境或 pack 预览。

## Consequences

现代应用可逐步迁移到共享包而不同时搬动高风险 Core；根 CLI/Web 仍能独立安装。C4 的 `web/` source ownership 过渡已在 C5 结束：唯一现代 Web source 位于 `apps/web` 与 `packages/app-ui`，根只承载静态 `web/dist` 和经过审计的 Host/Core runtime 闭包。窄 tarball 例外增加了 packlist 与 Node 16 回归责任，任何扩大都必须另行修改本 ADR、边界测试和安装态 smoke。

## Validation

- Node 24：完整 `npm ci`、TypeScript build、Core checkJs、workspace contract tests、Web production build；
- Node 16.20.2 + npm 8：根 production-only `npm ci` 无 workspace/UI 链接；根 tarball 分别完成无 lifecycle 内容检查与正常 lifecycle 安装，实际 bin help、synthetic SQLite 创建/打开和显式临时 Codex Home `status --json`；
- packlist：不存在 `apps/`、未批准的 `packages/`、workspace manifest、Electron、Fixture 或 node_modules；只允许 `packages/contracts/dist` 与 `packages/core/src`；
- import contract：除 `packages/core -> src/public-api.js` 的单一过渡例外外，禁止深度导入；
- security：生产树 moderate/high/critical 为零，全树 high/critical 为零。

## Related

- [保留 Node CLI 合同](0003-preserve-node-cli-contract.md)
- [共享 UI 通过 CoreClient](0007-shared-ui-through-core-client.md)
- [渐进迁移](0008-incremental-migration-no-big-bang-rewrite.md)
- [Electron 构建选择](0010-electron-vite-and-electron-builder.md)
