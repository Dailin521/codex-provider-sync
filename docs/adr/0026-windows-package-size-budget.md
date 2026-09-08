# ADR-0026：Windows 包体优化与无损许可证归档

- 状态：Accepted
- 日期：2026-09-05
- 范围：Electron 构建和 Windows 产物，不改变 Core/CLI/IPC 业务合同
- 依据：用户要求继续优化大小；保持 Electron 路线和所有现有业务能力

## 决策

1. Main、Utility、Preload、Renderer 构建显式设置 `minify: "esbuild"`；保留生产依赖选择、所有 source map 禁用及原生 SQLite 外置边界。不能依赖上层 Vite 默认值推断 electron-vite 输出已压缩。完整应用输出预算 1.5 MiB，防止无意恢复未压缩输出。
2. Windows `LICENSES.chromium.html` 原文无损归档到**程序目录内**的 `LICENSES.chromium.zip`。附中英文离线阅读说明 `THIRD-PARTY-NOTICES.txt` 和原文长度/SHA256 清单。构建使用既有、精确锁定 electron-builder 的 7-Zip 工具，验证解压 bytes 与原文相等后才移除重复的未压缩 HTML；失败即中止且保留原文。不删减许可证条款，不把许可证变成需要联网或单独下载的外部资产。Electron 本身及依赖包内的 LICENSE 文件保持原样。
3. 打包目录审计和最终容器审计都必须验证许可证归档；压缩归档损坏、原文 hash 不符或阅读说明缺失均失败。归档在安装包/便携 ZIP 内随程序一起分发，不改变固定 Release 资产集合和 updater 选择。
4. 不删除更多 Chromium/GPU/ICU/媒体/SQLite 文件，不降级 Electron，不使用 EXE 加壳或自定义引擎。保留已有 Windows D3D 路径及既有 afterPack 规则；本批不增加硬件支持声明。macOS/Linux 许可证布局不变。
5. Windows 预算收紧为 ASAR ≤ 3 MiB、解包目录 ≤ 280 MiB；安装包/便携 ZIP 仍为 ≤ 105/130 MiB。`verify-size-budget` 支持显式 `--output`、`--version` 和目录模式，避免错误读取旧产物；拒绝重复/未知/缺值参数。Windows candidate 在 Builder 完成后必须对本次输出目录及注入版本执行完整门禁，不能只验证目录而跳过最终 NSIS/ZIP。

## 测量与边界

- 基准是 `manual-20260905-ux-polish`，不是工作区所有旧构建目录之和；分别报告逻辑解包 bytes 和下载压缩 bytes。
- 许可证归档主要减少解包空间，外层安装包本来就压缩 HTML，不能宣称等量降低下载大小。
- 新包使用独立本地目录，不覆盖旧手测包。必须验证生产构建边界、原生 SQLite 和隐藏窗口 packaged Sync → Restore；许可证解压进行 byte/hash 检查。
- Provider 等字节长度原地更新、变长尾部复制、备份保留 2、首次/手动刷新均不改；Legacy .NET 不改。
- 没有发布、签名、公证、Tag、提交、推送或更新 PR 授权。只生成本地产物；安装器生成不等于已验证真实升级/卸载。

## 证据入口

- `apps/desktop/tests/package-size.test.mjs`：无损归档/损坏/失败保留原文/边界；独立目录及版本门禁。
- `apps/desktop/scripts/verify-production-bundle.mjs`、`release-audit.mjs`：生产输出和最终容器审计。
- `output/windows-size-checks-20260905.md`：本次实际大小、哈希与运行结果；未执行项单独列出。
