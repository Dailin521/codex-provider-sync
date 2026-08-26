# vNext/ADR-0010：采用 electron-vite 与 electron-builder

- Status: Accepted
- Date: 2026-08-24
- Amended: 2026-08-26 (C6 exact toolchain and unpacked Alpha gate)
- Scope: vNext

## Context

Electron 需要统一构建 Main、Preload、Renderer 和 Core Runtime，并为三平台生成可安装产物。工具链还必须正确处理 ESM、Source Map、原生 SQLite 模块、asar 和发布元数据。

## Decision

采用 `electron-vite` 组织 Electron 构建，采用 `electron-builder` 生成安装包与发布产物。阶段 0 只冻结工具组合；C6 已按 ADR-0014 锁定 Electron `44.0.0`、electron-vite `5.0.0`、electron-builder `26.15.7`，并使用 Desktop 专属 Vite `7.3.6` / React plugin `5.2.0` 兼容组合。

## Decision Drivers

- Main/Preload/Renderer 多入口配置清晰；
- 与 React + TypeScript + Vite 生态一致；
- 支持 Windows、macOS 和 Linux 打包；
- 能显式配置 asar、native module 和签名产物。

## Invariants

- 构建产物不得把 Node 能力注入 Renderer；
- Preload 和 Runtime 只打包允许的依赖；
- 若需要 `better-sqlite3`，必须针对 Electron ABI 重编译并放入 `asarUnpack`；
- 构建不得把测试 Fixtures、真实用户数据、凭据或开发密钥打入产物；
- Electron Major 升级必须运行 SQLite 驱动与 packaged smoke matrix；
- 版本、签名、公证、更新通道和回滚策略由 Release 门槛验证。

## Consequences

仓库会增加桌面专用构建和平台 CI；同时获得可重复、多入口且可审计的打包路径。

## Rejected Alternatives

- **手写 Vite/Webpack + 平台脚本**：配置面和维护成本更高。
- **Electron Forge**：可行，但当前路线选择 builder 的产物配置模型；改变需要新 ADR。
- **把 CLI 打入完整 Electron 包作为核心**：形成文本适配并增加 CLI 依赖。

## Migration and Validation

阶段 3/C6 在 Windows、macOS、Linux 的 host-native runner 上验证 `electron-builder --dir` unpacked app 启动、真实 SQLite、Utility Process 握手和 Renderer 隔离，不生成或发布安装器。macOS x64/arm64 两个发布目标、Windows/Linux 发行格式、native fallback/asar、安装卸载、SBOM/checksum 属于 C9 的发布矩阵；两层门槛都必须通过，不能用开发态 Electron E2E 替代 unpacked smoke。版本选择必须处于 Electron 官方支持线。

## Related

- [Electron 选型 ADR](0001-electron-over-tauri.md)
- [Utility Process ADR](0005-run-core-in-electron-utility-process.md)
