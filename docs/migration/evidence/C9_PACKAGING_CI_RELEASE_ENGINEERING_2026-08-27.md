# C9 打包、CI 与发布工程证据（2026-08-27）

状态：候选实现和本地 Windows x64 门禁通过；C8 及其前置 Phase 的 required CI、macOS x64/arm64、Linux x64、四目标 aggregate、真实签名/公证、更新 metadata、跨版本升级与最终 PR 合入均未闭合。因此 C9/Phase 6 仍为 Pending，不构成 Beta、Stable 或发布声明。

输入 checkpoint 为 `1673147f6d993d3a5923615d41dec2cf9f37c293`（C8）；C9 release-engineering 实现 commit 为 `73256f3187dd337bb681a1cc9810edad8f6309bb`。本文档与执行索引形成后续 C9 evidence commit；C10 最终 bundle 必须同时索引二者。

## 实现边界

- `.github/workflows/ci.yml` 新增四个 host-native candidate target：Windows x64、macOS x64、macOS arm64、Linux x64。每个目标完成 build、stage audit、最终容器 smoke 后才上传；`electron-candidate-set` 下载四份候选，验证 version/commit/lockfile/tool/fuse/audit policy 一致，两个 job 都是唯一 `ci-gate` 的 strict dependency。
- 候选版本从 CI run 注入 `1.0.0-alpha|beta|rc.<run>`，buildId 绑定 commit 与 target；根包仍为 `0.5.0`，Desktop source manifest 仍为 `0.0.0`。远端 RC 门禁全部闭合前不把 source version 改为 `1.0.0`。
- electron-builder 固定 Windows NSIS/ZIP、macOS DMG/ZIP、Linux AppImage/deb；所有候选构建使用 `--publish never`。旧 tag-push 发布入口已改为显式 `workflow_dispatch + release_tag`，且只允许 tag commit 位于 `main`；本 checkpoint 没有触发该入口。
- Electron runtime 首选 `node:sqlite`，并打包 Desktop 专用 `better-sqlite3 13.0.3` fallback。ABI rebuild 后，ASAR 只引用当前 target 的一个 native binding，`app.asar.unpacked` 也只允许该文件；其它平台 prebuild、native source/build/deps 全部排除。
- fallback 可执行证据使用独立 `electron-vite --mode test` 的编译期常量剔除 `node:sqlite` import，并验证 bundle 只保留 `better-sqlite3`；运行时环境变量不能切换生产 driver。production verifier 与最终 artifact audit 同时拒绝该 selector symbol，避免测试 gate 进入发布包。
- production bundle 预检与最终 ASAR 审计共用 `artifact-audit-policy.v1.json` 的完整文本扩展集合和 forbidden-text 规则，覆盖 JS/CJS/MJS/HTML/CSS/JSON/SVG/manifest/文档配置，不再出现早期门只扫描少数扩展的差异。
- production Fuse、ASAR entry/block integrity、Windows PE/macOS plist embedded ASAR binding、敏感路径/文件、高置信 credential marker、fixture/test/source map 与 native binding 都由数据化 policy 审计。最终 ZIP/Installer/DMG/AppImage/deb 必须逐个解包或安装后再次审计，不能用 builder unpacked 目录替代。
- CycloneDX SBOM 从唯一 `package-lock.json` 投影 Desktop production closure；Playwright、builder、Vite、审计工具和 fixture 不进入 runtime closure。每个候选的 checksum 精确覆盖资产、audit、SBOM、staging、container report 与 release manifest，候选目录不得夹带未清单文件。
- 根 Web production build 已关闭 source map；根 npm packlist 从 111 个条目降为 110 个条目，`.map` 为 0。安装态 tarball smoke 固化了这一拒绝规则。

## Windows x64 精确候选

本地候选版本为 `1.0.0-rc.0`，buildId 为 `1.0.0-rc.0-73256f3187dd-windows-x64`，manifest commit 精确绑定 `73256f3187dd337bb681a1cc9810edad8f6309bb`。窗口策略为 `CPS_DESKTOP_WINDOW_DISPLAY=hidden`；未占用主屏。

| 资产 | Size | SHA-256 |
| --- | ---: | --- |
| `CodexProviderSync-1.0.0-rc.0-windows-x64-portable.zip` | 157,668,528 bytes | `96c0ab0c49bce31999e1d45dad01821f4a1433d72350f1366f1464c3fddcd33d` |
| `CodexProviderSync-1.0.0-rc.0-windows-x64-setup.exe` | 123,164,018 bytes | `e5d7076a571ab2742119878ac6d0efb40baf4465c4d5bc057c51bad15ea7619a` |

审计摘要：

- ASAR SHA-256：`f60ed82f18f52d25bf4ac9071cc664509817486d70c70aa89d8dd737e3534f0f`
- ASAR header SHA-256：`93a5304bad0ac35bb3638e0549c23de5bd277407d4c58f425dc4dd42c31d0d79`
- ASAR entries：4,538；带 entry/block integrity 的文件：4,177；Windows embedded binding：`verified`
- native binding SHA-256：`e21e5efd71fba66578e95b62554d9028064a80dafd7221bf8a8ef155de8d240a`
- container report SHA-256：`b8470d1ffbb6bf1f2b7fe1ea02f5f7749723acfaea5318b6a95194602e1a166d`
- lockfile SHA-256：`59a6bd220bce2ce5ba0ddd909c6ecbfe70f76aaa0f6376f93b55068551884e57`
- `SHA256SUMS.txt` 的 7 个条目已从磁盘逐项重算通过。

ZIP 与 NSIS 都完成：最终容器内容复审、候选 ASAR loader + unpacked binding 的真实 `better-sqlite3` 内存库 probe、production bridge 排除、synthetic SQLite Status、真实 Sync→Restore byte/hash 回环和正常退出。NSIS 另完成静默安装、uninstaller 存在性与卸载目录清理。

## 本地门禁

| 门禁 | 结果 |
| --- | --- |
| `npm test` | 414 passed，0 failed，0 skipped |
| `npm run workspaces:check` | workspace build/import/package boundary 通过；102 tests passed |
| `npm run desktop:test:e2e`（hidden） | 15 passed，1 skipped；Skip 仅为本机不可用的真实 WSL UNC fixture |
| `npm run web:test:e2e` | 2 passed |
| Windows ZIP/NSIS `desktop:smoke:candidate:artifacts`（hidden） | 两个最终容器各 2/2 production tests passed；container gate passed |
| `npm run package:smoke` | 根 tarball content/help/status/Web shell 通过；source map 为 0 |
| `npm run package:smoke:lifecycle` | lifecycle install + SQLite smoke 通过 |
| `npm audit --omit=dev --audit-level=moderate` | 0 vulnerabilities |
| `npm audit --audit-level=high` | 0 vulnerabilities |
| C9 独立最终代码复核 | 0 P0 / 0 P1 / 0 P2 |

本机工具：Node `24.11.1`、npm `11.10.0`、PowerShell `7.6.4`、Git `2.52.0.windows.1`、.NET SDK `10.0.400`、Windows build `26200`。本机不是 Node 16.20.2 环境；Node 16 根 tarball 兼容性继续由 required CI 与 C4 已有安装态合同证明，不能把本地 Node 24 smoke 记作 Node 16 实机结果。

## 2026-08-28 后续 source-head 发布门 hardening

这些变更不改变上面的历史资产 Hash，也不能沿用该 checkpoint 的计数；PR #90 后续 source head 必须重新生成候选和 C10 artifact：

- 根 tarball 安装态 smoke 从 help/status 扩展为真实 npm bin `sync --json → managed backup → synthetic drift → restore --json`，断言 config/rollout 字节、SQLite Provider 与 pending recovery 全部恢复；Windows/Ubuntu Node 16.20.2 required matrix 继续执行同一脚本。
- Windows cross-runtime job 以 `fetch-depth:0` 获取冻结 tag，校验 `v0.2.9@1a2b290...` 与 `v0.4.1@75f45756...` 后构建历史 .NET Core，真实产生 synthetic metadata v1/v2 backup，再由当前 Node Restore；CI 只上传不含真实数据的 commit/hash evidence。该等级是 repository-tag-source，不是 hosted formal Release binary。
- 后续 source head 新增独立的 checksum-bound hosted Release fixture：固定 `v0.4.1` Release/tag/commit、Automation ZIP asset ID/size/SHA-256、发布页 checksum 资产及 archive entry Hash，校验通过后才在严格环境白名单中执行正式托管的旧 Automation Plan/Apply，并由当前 Node Restore synthetic backup。fork PR 不执行 hosted binary；同仓库 PR artifact 仍是审查预览，受保护 `main` 必须重新运行。它只上传同一 CI run/tested commit 绑定的脱敏 hash evidence；是否通过只以对应 source head 最新成功的 C10 artifact 为准，不能回填到上方历史 checkpoint。
- Web 与 npm 手工发布工作流在任何发布动作前都安装 Chromium 并运行 production Web E2E；普通 Web/Desktop Playwright 配置启用 `forbidOnly`。
- 候选构建显式注入 `CPS_DESKTOP_RELEASE_AUTHORIZED=false`，所以 unsigned/not-authorized 候选不会建立真实 update port。仓库当前没有把该值置 true 的正式发布路径；这是未获发布授权时的预期 fail-closed 状态。

聚焦验证已通过安装态 tarball lifecycle、历史 tag-source Restore 和 Desktop 71/71；完整四目标 candidate set、全量门禁与远端 artifact 仍以新 checkpoint 的 CI 为准。

## 未闭合项与停止边界

- 本地 Windows 不能替代 macOS x64、macOS arm64、Linux x64 的 native build、DMG/ZIP/AppImage/deb 解包/安装、embedded integrity、native SQLite 与 graceful-exit 证据；四目标 aggregate 也尚未产生。required CI 未全绿前 C9 保持 Pending。
- 本机 Ubuntu 注册项缺少 `ext4.vhdx`，真实 WSL UNC 测试按合同 Skip。C10 必须保留该限制，不得把 synthetic path 测试冒充 WSL 实机。
- 候选明确是 unsigned、not notarized、release not authorized。没有 tag、npm publish、GitHub Release、签名、公证或更新通道写入；真实 update metadata/download/restart upgrade 仍阻断 Stable。
- tag-source historical fixture 仍只证明冻结源码；后续 source head 已具备执行固定 v0.4.1 hosted Automation Release binary 的 synthetic backup→当前 Node Restore fixture，但在其 source-head CI/C10 artifact 成功前不得声称闭合。即使该 artifact 成功，它也只闭合“历史正式托管 backup 格式兼容”，不闭合真实 Beta、Windows/macOS 签名、公证、真实用户数据或生产跨版本升级。
- 当前 builder 使用 Electron 默认应用图标；仓库尚无经确认的跨平台 product icon 资产。它不改变本 checkpoint 的数据安全结论，但在公开 Stable 前应由产品资产验收决定是否补齐。
- macOS/Linux 任一容器审计、native probe、Status/Sync→Restore、正常退出或 aggregate 失败时，必须停在 `73256f3` 的 Windows-only evidence，不得降级门禁、跳过 job 或写入 `1.0.0` source version。
