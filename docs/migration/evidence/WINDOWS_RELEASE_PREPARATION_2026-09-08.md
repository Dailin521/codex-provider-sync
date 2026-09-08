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

## 首轮 hosted CI 发现的打包接线问题

PR source `2c90e112ab8eefb73b979e1cd188532b776f1432` 的 run `34203674254` 中，macOS/Linux 通用 `pack:dir` 完成构建后错误调用了 Windows 目录预算，尝试查找不存在的 `win-unpacked`。该 run 保留为失败证据，不能用于合并。

修复将通用 `pack:dir` 的 Windows 预算调用显式标为 native-host：Windows 继续强制目录/ASAR/locale 预算并拒绝缺失资产，其他 host 明确输出此 Windows 预算不适用；非 Windows 的后续原生审核/E2E 不跳过。显式 Windows artifact/fixture 检查不传该选项，仍在任意 host 校验合成 Windows 目录，所有阈值保持不变。新增回归覆盖这两条路径；最终 hosted CI 以修复后 head 为准。

同一 run 的 Windows Node 24 用例发现：可共享读取但禁止独占写的中文 rollout 文件名，经 PowerShell 默认代码页回传路径后无法匹配原路径，导致 Preview 写入受阻数量漏报。修复仅改内部探测响应为完整 JSON 计数/整数索引，拒绝损坏响应；新增真实 Windows 持锁、强制 ASCII stdout 的合成回归。Apply 原生句柄仍复核占用，Provider I/O、正文、时间戳、备份和公开 DTO 不变。

Windows Electron E2E 的未配置 Provider 场景仍期待旧的通用错误文案，实际已按 ADR-0035 展示明确原因。修正断言以验证“Provider 未配置”和“零数据改动”；原文件 hash、无 Plan 对话框和无备份断言全部保留。以上失败均须由新 head 完整 CI 重新裁决，不采用旧 head 局部通过结果绕过门禁。

上述修复的本地复核：根测试 540 项（486 passed、54 平台 skip、0 failed）；`architecture:check` 成功；包体/发布接线 18 passed；未配置 Provider 的隐藏 Electron E2E 1 passed；新探测三项测试在 Node 24 与 Node 16.20.2 均通过（Node 16 runner 汇总为一个文件）。测试夹具清理使用 Node 16 支持的 `afterEach`。

## Node 16 完整测试夹具兼容

首轮 Node 16 根测试长时间不退出，定位为 `status-coordination` 使用 Node 16 不存在的 `TestContext.mock`：HTTP 服务启动后注入抛错，finally 再次调用缺失 API，跳过了关闭 socket。另有 Plan/Watch/进度/首行事实测试使用 `TestContext.after`，History DTO 测试使用全局 `structuredClone`，这些测试 API 同样不支持 Node 16。

修复仅限测试：改为已有 `afterEach` 清理队列、显式保存/恢复 fs 方法、JSON DTO 副本；原业务断言、故障注入与测试范围不删减，不修改生产 Web 服务或放宽 Node 16 兼容声明。已知失败/挂起的旧 run（`34203674254`、修复前的 `34205277322`）主动终止以避免持续占用 runner；它们不可用于合并。最终必须重新完成全部 CI。独立 Node 16 验收使用 D 盘隔离 checkout 和其生产依赖树，不借用 Node 24/Electron native ABI。

独立 Node 16.20.2 / npm 8.19.4 生产依赖安装 43 项、审计 0 vulnerability；最终完整 `npm test` 的 Node 16 文件级汇总为 46 passed / 0 failed，53.89 秒正常退出（不把文件汇总中的 0 skip 冒充没有平台子测试 skip）。Node 24 复核仍为 540 tests / 486 passed / 54 平台 skip；架构门禁通过。

## Windows 测试调度与隐藏窗口截图

修复后 source `4c287475afcd190825a8de1f085fbea5e3340779` 的 run `34206036370` 中，Windows Node 16 同时启动原生夹具时，一项 PowerShell 锁探测超过现有 10 秒期限。仅在 Windows Node 16 的根测试入口使用其公开 `node:test.run`，把文件并发限制为 2；其他平台和 Node 24 的入口不变，生产探测期限与 fail-closed 行为不放宽。新增合成测试同时证明成功退出为 0、失败退出为 1，且前一个文件失败仍运行后续文件。

独立 Node 16 生产依赖树在并发 2 下再次完整通过 46 个测试文件，94.74 秒正常退出；新增 runner 合成用例另外在 Node 16/24 均通过。Node 24 完整根测试现为 542 项：488 passed、54 平台 skip、0 failed。

本地隐藏生产包测试曾分别遇到元素截图的 10 秒与整项 180 秒超时；这些失败记录保留，不当作通过。改用当前固定视口截图，避免元素截图驱动隐藏窗口表面调整；只允许同一路径的一次截图超时重试，断言失败、其他异常和第二次截图失败仍然失败。200% 等效视口、横向边界、确认按钮可见性及所有业务断言保留。四项生产包 E2E 再次全部通过（34.3 秒），检查了真实生成的 200% 预览截图；桌面单元/合同测试为 156 项：155 passed、1 本机环境 skip、0 failed。最终 hosted CI 必须在包含这些修改的新 head 上全部完成。

同一 hosted run 的 Windows Electron integration 有两项总时限超时（文件锁 partial 为 45 秒，rollout 写后崩溃重试为 90 秒），原日志不足以定位等待阶段，不能直接判定为业务缺陷或通过。相同未修改的 integration 在本地隐藏模式复核为 16 passed、1 真实 WSL 环境 skip，4.7 分钟；为避免测试锁在业务断言之前自动过期，将合成文件锁从固定 30 秒改为显式释放，并限定锁启动/退出时间。为这两类场景增加不含数据的阶段名称，原业务断言和总时限不放宽。修改后文件锁及四个 crash matrix 场景共 5 passed，29.7 秒；生产 Core/Runtime 代码未为未经证实的原因改动。hosted run `34206036370` 最终失败（连带严格 evidence/ci-gate 失败），不能用于合并。
