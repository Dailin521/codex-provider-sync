# Windows Electron 发布准备检查（2026-09-08）

状态：本地发布前检查，不是公开发布证明。维护者已确认 Windows 首发；签名和线上更新未闭合时先 RC。范围与操作约束见 [ADR-0039](../../adr/0039-windows-first-electron-release.md)。

## 输入与变更

- 本轮开始 source：`c6e33e1c300c3c2aba421142106e47bf379c2006`（V1）。
- 本轮核对的 main：`c7ff85218a07a8e5f14132c582cad1239c52865e`，已在 V1 历史内；执行正常 merge 确认无新提交，无 rebase/force-push。
- 本轮包含此前本地验收的 Windows/UI/History/Profile/日志/备份与同步优化、契约/文档及配套 Web bundle；新增独立 Windows RC 准备流程。
- 原有 C0–C10 与轻量化 C1–C3 提交保留。此检查不改写历史阶段状态。
- 临时日志、截图、trace、测试残留与安装目录不进入提交。根包必需的 `packages/contracts/dist`、`web/dist` 配套版本化；不因“生成物”而遗漏。

## 本轮自动检查

环境：Windows x64；Node `v24.11.1`、npm `11.6.2`；兼容安装使用 Node `v16.20.2` / npm `8.19.4`。测试只用 D 盘临时夹具，不操作真实用户 Home。

| 检查 | 结果 |
| --- | --- |
| 根 `npm test` | 537 项：483 passed、54 平台 skip、0 failed |
| `npm run architecture:check` | 成功；包含 workspace 类型/合同、UI 和 Provider I/O 回归 |
| 共享 UI | 31 个文件、156 tests passed |
| Provider I/O | 61 项：47 passed、14 平台 skip、0 failed；原地身份/正文 hash/mtime、partial 和备份约束保留 |
| 独立发布脚本/合同 | 10 tests passed；拒绝旧/其他分支/PR 绿灯、较新失败或 pending、版本/SHA/tag 不匹配与已有 Release |
| Web production build | 成功 |
| Node 16 根 tarball lifecycle + SQLite smoke | 成功；首次依赖下载 ECONNRESET，保留失败记录，重试后通过，未修改或跳过门禁 |
| 生产与完整依赖审计 | 均 0 vulnerability |
| 文档与示例 | 20 篇、375 本地链接、57 锚点；CLI JSON help、diff whitespace 通过 |
| Workflow YAML | 解析通过；实际 hosted 执行仍待最终提交 CI/发版准备验证 |

上述数字只对应本轮实际执行；最终提交的完整 hosted CI、C10 bundle 与 final-container evidence 以 PR/Main 的对应 SHA 为准，不能套用 9 月 3 日旧 head 的绿灯。

## 人工与未验证

维护者报告已手测 Windows Electron 功能；未把旧本地包的报告自动视为新安装资产的验收。最终 NSIS/portable 将由对应 runner 容器测试重新验证安装/解包、真实 fixture Sync→Restore、退出和 NSIS 卸载。

本检查没有执行签名、公证、线上跨版本安装更新、真实 WSL、macOS/Linux 真人验收。仅 Windows 公开下载范围不取消其他平台 CI。Root npm、macOS/Linux、Legacy .NET 不属于本轮发布对象。

## 发布防误用

- 新流程不触发旧 .NET `publish.yml` 或 npm 发布；默认 Actions artifact，显式操作才建立 Draft RC。
- 仅已有 RC tag、完整 source SHA、main ancestry 与对应最新成功 main push `ci-gate` 可进入；PR merge-ref 绿灯不能代替 main SHA。
- Draft 前再次复核 SHA/CI，按完整校验清单核对资产，create-only 上传，不覆盖既有 Release；RC 不设置 latest 或启用生产更新通道。
- 旧 .NET 单 EXE 自更新不能直接接收 Electron。首次手动安装/完整解压，不伪造旧资产名。

本文件不包含本地真实路径、认证信息、聊天正文、诊断包或性能样本正文。完整本地日志保留在忽略目录，公开证据由 CI 的明确清单生成。
