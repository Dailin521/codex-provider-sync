# ADR-0039：Windows Electron 首发与独立发布流程

- Status: Accepted
- Date: 2026-09-08
- Scope: Windows x64 Electron 首发；不改变 Core、CLI、Web 或 Legacy 行为。

## 决策

维护者确认先交付已进行人工功能测试的 Windows Electron。整理并推送 V1，在最终 PR 的完整 CI 通过后，使用 merge commit 保留既有 checkpoint 历史。最新 main 必须已包含于候选；main 合入后的实际 commit 需要重新取得成功的 `ci-gate` 与绑定该 SHA 的产物证据。

本轮不发布 npm、macOS/Linux 安装包或 Legacy .NET，也不以缩小公开下载范围为理由跳过既有跨平台 CI。Windows 人工测试、自动 fixture 验收、其他平台 CI 与尚未执行的真人验收分别记录。

新增独立 Windows Electron 发布准备流程，不复用 `.github/workflows/publish.yml` 的 Legacy 单 EXE 打包方式。准备流程固定 Windows x64 NSIS 和 portable ZIP，并复用候选构建、SQLite/ASAR 审核、体积门禁、SBOM、hash 与安装/解包 smoke。构建必须传递 `--publish never`；默认仅生成 Actions artifact，显式选择才创建 Draft prerelease，不自动公开或设为 latest。

首次暂不宣称签名、线上跨版本自动更新或全部平台 stable 已通过。若安装、签名或更新验收未闭合，先发布明确标注限制的 RC；`1.0.0` 源码版本号不等于公开稳定版。稳定版需另有对应最终包、安装/升级证据及发布批准，不能把 RC 文件直接改名成稳定版。

## 更新与兼容

- 保持既有查更语义：每日首次启动检查一次，之后仅用户手动检查；公共手动查更只推荐更高的正式 Electron 版本，不向普通用户推荐 RC。
- RC 为人工下载安装；不启用生产自动下载/安装授权，不发布生产更新 metadata。
- Legacy .NET 的同名 EXE 自替换不能接收 Electron NSIS；首次迁移须安装或完整解压新版，不复用 `CodexProviderSync.exe` 旧资产名。
- 不删除用户数据、旧 Release 或 Legacy 实现；安装/卸载自动验收仅用隔离夹具。

## 发布证据

公开产物必须绑定完整 commit、版本、构建标识与 SHA-256；日志和本地截图默认不上传。仅发布确定名单中的安装包、便携包与无敏感信息的审核/依赖/校验文件。已有 tag/Release 不得被自动覆盖。

本 ADR 缩小本轮发布范围，不将总体迁移 Phase 6/7 标为完成，不降低完整合并 CI、Provider I/O、备份、Restore、Node 16 或 Legacy 兼容门禁。

详见 [Windows Electron 发布操作说明](../WINDOWS_ELECTRON_RELEASE_ZH.md)、[执行索引](../migration/VNEXT_MIGRATION_EXECUTION_INDEX_ZH.md)、[更新约束](0020-desktop-manual-release-check.md)。
