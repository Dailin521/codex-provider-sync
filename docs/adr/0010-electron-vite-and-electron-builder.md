# vNext/ADR-0010：采用 electron-vite 与 electron-builder

- Status: Accepted
- Date: 2026-08-24
- Amended: 2026-08-27 (C9 release candidate packaging, fuses and final-container audit)
- Scope: vNext

## Context

Electron 需要统一构建 Main、Preload、Renderer 和 Core Runtime，并为三平台生成可安装产物。工具链还必须正确处理 ESM、Source Map、原生 SQLite 模块、asar 和发布元数据。

## Decision

采用 `electron-vite` 组织 Electron 构建，采用 `electron-builder` 生成安装包与发布产物。阶段 0 只冻结工具组合；C6 已按 ADR-0014 锁定 Electron `44.0.0`、electron-vite `5.0.0`、electron-builder `26.15.7`，并使用 Desktop 专属 Vite `7.3.6` / React plugin `5.2.0` 兼容组合。

C9 固定生成 Windows x64 NSIS/ZIP、macOS x64/arm64 DMG/ZIP、Linux x64 AppImage/deb。候选版本通过构建参数注入 `1.0.0-alpha|beta|rc.<run>`，不改写根 npm 或 Desktop source manifest；构建命令始终带 `--publish never`。`better-sqlite3 13.0.3` 作为 Electron production fallback 针对当前 ABI 重建，包内只保留当前平台 binding，并将该 binding 单独放入 `app.asar.unpacked`。

生产 Fuse 固定关闭 RunAsNode、NODE_OPTIONS、CLI Inspect、Browser Process Custom V8 Snapshot 与 File Protocol Extra Privileges，启用 Cookie Encryption、Embedded ASAR Integrity 与 OnlyLoadAppFromAsar。审计必须读取最终 executable 的 fuse wire；Windows/macOS 还必须把 executable/plist 内嵌的 ASAR header hash 与实际 header 对齐，Linux 明确记录该 runtime binding 为 unsupported-platform，而不能伪报已验证。

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
- ZIP/Installer/DMG/AppImage/deb 必须逐个解包或安装，再重复 ASAR、Fuse、native binding、敏感路径与 source-map 审计；builder 的 unpacked 目录不能替代最终容器证据；
- checksum 清单必须精确覆盖资产、审计、SBOM、容器报告与 release manifest，候选目录不得夹带未清单文件；
- C9 CI 只上传 unsigned、not-authorized 的短期候选，不自动创建 tag、npm 包或 GitHub Release。
- Main 的更新能力使用编译期 `releaseAuthorized` fail-closed gate。缺省与所有 C9 candidate 固定为 false，不能创建 updater port、排定网络检查或安装；只有另行获授权且具备签名/metadata/升级证据的正式发布构建才可显式置 true，运行时环境变量不能事后开启。

## Consequences

仓库会增加桌面专用构建和平台 CI；同时获得可重复、多入口且可审计的打包路径。

## Rejected Alternatives

- **手写 Vite/Webpack + 平台脚本**：配置面和维护成本更高。
- **Electron Forge**：可行，但当前路线选择 builder 的产物配置模型；改变需要新 ADR。
- **把 CLI 打入完整 Electron 包作为核心**：形成文本适配并增加 CLI 依赖。

## Migration and Validation

阶段 3/C6 在 Windows、macOS、Linux 的 host-native runner 上验证 `electron-builder --dir` unpacked app 启动、真实 SQLite、Utility Process 握手和 Renderer 隔离，不生成或发布安装器。C9 把四个原生目标、native fallback/ASAR、最终容器审计、安装或解包 smoke、SBOM/checksum 与 aggregate index 纳入唯一 `ci-gate`；两层门槛都必须通过，不能用开发态 Electron E2E 替代 unpacked 或最终容器 smoke。

V1 的 Windows x64 候选已在本地完成 ZIP/NSIS 最终容器验证；macOS x64/arm64、Linux x64 与四目标 aggregate 必须由对应 host-native CI 闭合。签名、公证、真实更新 metadata 和跨版本升级仍未获授权，不属于该本地证据。

## Related

- [Electron 选型 ADR](0001-electron-over-tauri.md)
- [Utility Process ADR](0005-run-core-in-electron-utility-process.md)
