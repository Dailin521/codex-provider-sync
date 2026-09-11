# Node Core 当前架构与开发约束

> 状态：Accepted，适用于 V1 当前代码；最近校对：2026-09-08（已纳入 ADR-0038）。
> 本文是 Node Core 日常开发入口，不是发布证明。Electron 总体路线仍见 [vNext 架构基线](../VNEXT_ELECTRON_NODE_ARCHITECTURE_ZH.md)，阶段及发布状态只记在[执行索引](../migration/VNEXT_MIGRATION_EXECUTION_INDEX_ZH.md)。

## 1. 文档职责与变更规则

失效 Home 锁恢复遵循 [ADR-0041](../adr/0041-stale-home-lock-status-recovery.md)：Status 只读验证全部锁 owner/claims，确认失效时允许正常预览/同步，由 Apply 既有 acquireLock 重验并回收；未知或真实活动锁继续阻断且分别提示。Status/Diagnostics 不清锁，不新增解锁 API，不改变 Provider I/O。

备份只读与失败诊断遵循 [ADR-0040](../adr/0040-backup-read-races-and-failure-diagnostics.md)：只读枚举遇到清理导致的 ENOENT 整条省略，写入/恢复验证不放宽；普通操作按真实调用边界附加安全阶段和底层码，经 CLI/Host 日志透传，不改变 PIO、取消、partial 或进度合同。

修复预览与显式诊断遵循 [ADR-0034](../adr/0034-repair-preview-counts-and-single-diagnostic-scan.md)：索引字段累计、去重会话、工作区设置类别分开统计；全局修复保留只读明细。Diagnostics 通过内部 `getDiagnosticSnapshot` 最多扫描一次完整事实，rollout revision 仅 stat，不因漂移重复全文扫描、不污染 Status 缓存；漂移时保留本轮观察但明确未完整，实际 Home 锁仍优先。独立有界记录完整性检查、普通 Status 重试及 Plan/Apply 内容校验不变。

高级功能请求进度遵循 [ADR-0032](../adr/0032-explicit-scan-and-preview-progress.md)：`prepareRepair/getDiagnostics` 经可信 Host control 推送既有 ProgressEvent，传输使用无 operationId 的 request-progress。不得伪造写 Operation、后台轮询或为显示进度增加正文扫描；Prepare control 不保存在 Apply 计划。Core 与 Storage observer 失败不影响业务，UI 只显示真实阶段进度和本地等待耗时。

状态读取遵循 [ADR-0033](../adr/0033-status-drift-is-not-operation-busy.md)：rollout/SQLite/config revision 变化不代表工具写操作。只有实际 OperationRuntime/Home 锁证据产生 `operationInProgress`；读取漂移/失败独立标记 `statusReadBlocked`、扫描未完整，保留最后完整快照，不允许据此放行写入。仅一次受限重试及用户手动刷新，不能靠后台轮询掩盖误判。

| 文档 | 唯一职责 |
| --- | --- |
| 本文 | 当前模块归属、依赖方向、不可静默改变的核心约束与验证入口 |
| [Core 合同](contracts/CORE_EXTERNAL_BEHAVIOR_ZH.md)、[CLI 合同](contracts/CLI_CONTRACT_ZH.md)、[错误码合同](contracts/ERROR_CODES_ZH.md) | 公开输入、结果、默认值、失败语义与兼容范围 |
| [ADR-0016](../adr/0016-node-core-responsibility-boundaries-and-lightweight-writes.md)及其明确列出的后续修订 | 职责拆分和普通写轻量化的设计裁决 |
| [Fixture 清单](../migration/BEHAVIOR_FIXTURES_ZH.md)与对应测试 | 可执行证据，不用文案代替验证 |
| README / [工作原理](../WORKING_PRINCIPLE_ZH.md) | 用户使用说明与简明解释，不另立一套规则 |

代码和测试说明当前事实，但新代码不能凭“已经写了”自动推翻 Accepted 决策。出现冲突时先登记范围、判断 bug 还是有意变更；有意改变合同必须同时修改 ADR、合同、测试及用户说明。历史目标和已被取代的 ADR 不是恢复旧行为的依据。

## 2. 当前依赖与实际文件

```text
新版 Web /api/core / Electron Utility → CoreFacade → 业务用例 → Runtime / Storage 端口
CLI → src/public-api.js 兼容适配器 ────→ 同一业务用例
```

旧 Web `/api/*/prepare`、`/apply` 写路由仍保留 service/public-api 兼容适配；不能把所有 HTTP 路径都称为 Facade 调用，也不能从该例外扩张新产品入口。

| 职责 | 当前文件（packages/core/src/ 下） | 边界 |
| --- | --- | --- |
| CoreFacade | [index.js](../../packages/core/src/index.js)、[index.d.ts](../../packages/core/src/index.d.ts) | 可信 Profile 解析、schema/DTO、公开用例组合；不实现文件算法 |
| 用例组合 | [application/core-application.js](../../packages/core/src/application/core-application.js) | 组合用例，不能成为第二套流程 |
| Status | `application/status.js` | 默认轻量 metadata 快照；不触发 Repair 或完整诊断 |
| 同步目标写入占用 | `application/session-usage.js` | Provider Prepare/Apply 共用写目标占用探测，用于跳过/复核，不是首页会话活动数量 |
| 会话活动观察 | `infrastructure/node-core-ports.js` 接入 `src/session-activity.js` | SessionStore 的 `readSessionActivity` 观察 writer owners；与写目标占用独立，仅投影 `sessionActivity` |
| ProviderSync | [application/provider-sync.js](../../packages/core/src/application/provider-sync.js) | 从 config 取 Provider，仅对齐 rollout 与 SQLite Provider |
| ProviderSwitch | `application/provider-switch.js` | 计算 config 根级变更，调用内部 ProviderSync，共用一次操作/锁/备份 |
| Diagnostics / Repair | `application/diagnostics.js`、`application/repair.js`、`application/repair-targets.js` | 手动只读扫描与显式 target 修复分离 |
| Backups / Restore | `application/backups.js`、`application/restore.js` | 受管备份查询/清理与恢复流程分离 |
| History | `application/history.js` | 只读查询，标题/项目/正文展示规则独立于同步 |
| Watch | `application/watch.js`、`application/watch-runtime.js` | 事件合并，调用内部 ProviderSync，给人工操作让路 |
| OperationRuntime | `application/operation-runtime.js`、`plan-apply-guard.js`、`concurrency-guard.js` | Plan 生命周期、运行状态、取消和进程内协调 |
| 普通写执行器 | `application/ordinary-write-runtime.js` | 固定写入顺序与 partial 收敛；不吞并各用例的扫描/业务规则 |
| 存储组合 | [infrastructure/codex-storage.js](../../packages/core/src/infrastructure/codex-storage.js) | `config/sessions/stateDb/globalState` 四端口，仅存储能力 |
| SQLite / Undo / Recovery | `infrastructure/sqlite-transaction.js`、`undo-backup.js`、`restore-recovery.js` | 数据库事务、用户撤销备份、Restore 恢复机制各司其职 |

成熟的具体存储算法仍位于根 `src/config-file.js`、`session-files.js`、`sqlite-state.js`、`workspace-roots.js`、`backup.js`、`restore-v2.js` 等，通过 [node-core-ports.js](../../packages/core/src/infrastructure/node-core-ports.js) 静态接入；不要为目录整齐复制实现。实际文件名以代码为准，概念端口不等于必须建立同名大类。

Windows 写目标占用探测的私有协议位于 `src/windows-lock-probe.js`：只返回已检查数量与被占用目标的整数索引，由调用方映射原路径；不得通过 stdout 的路径文本匹配，以免系统代码页导致中文路径漏报。缺失/损坏/不完整响应必须拒绝，不能冒充全部可写；不改变活动会话展示、公开 DTO 或实际写入句柄检查。

`src/service.js`、`watch.js`、`diagnostics.js` 保留兼容转发；`service-runtime.js` 中的 `runSync/runSwitch/runRepair/runRestore` 在同进程连续 Prepare/Apply。CLI 和旧 Web 写路由尚有适配层，不应写成所有入口物理上都只调用 Facade。新 Web `/api/core`/IPC 必须使用 `createCoreFacade`，不得扩张兼容辅助函数为新产品 API。

禁止：用例或 Storage 反调 Facade；Switch/Watch 再创建嵌套同步计划；Renderer/Main/CLI/Web 复制 Provider 算法；Core 引入 React/Electron；RestoreRecovery 重新覆盖所有普通写。高风险存储仍是 ESM JavaScript，不能夹带整体 TypeScript 翻译。

## 3. Provider I/O 不变量（必须保持）

[ADR-0038](../adr/0038-windows-cleanup-and-file-update-timing.md) 仅优化 Windows 临时文件清理，并增加批次级数值计时。保持 Force 回退、原地/流式写法、默认复制缓冲区、Flush 与时间戳恢复；日志不扩张进度协议、不增加文件扫描。计时嵌套不能相加，旧/缺失数据不能补零。

### PIO-1：只改 Provider

Sync 目标始终取 `config.toml` 根级 `model_provider`，缺失时为 `openai`。公共输入不接受 `provider/model/fast/syncMode`。正常扫描经 `collectProviderChanges`，只解析第一行 `session_meta`（上限 1 MiB）；不得为模型、cwd、用户事件、加密字段、会话序号或历史显示索引扫描正文。

这里“只读首行”指**业务扫描边界**：底层按 64 KiB 分块，可能在包含换行的块中预读少量尾部。按 [ADR-0037](../adr/0037-switch-history-and-provider-preparation-facts.md)，Sync/Switch Prepare 的分布、revision 和 change descriptor 复用同一次首行事实，短期记录不进入计划 ledger/公开输入/持久缓存。Apply 锁内复核、实际目标重扫和落盘前仍重新读取。不能声称整个 Sync 只读一次或只读取 Provider 的几个字节。备份/哈希及变长复制有各自 I/O，但不能借此恢复正文业务扫描。

### PIO-2：合格的等长更新必须原地写

当前 [getInPlaceProviderMutation](../../src/session-files.js) 的资格同时要求：

- 原、新 Provider ID 均匹配 `[A-Za-z0-9._-]+`，且值确实不同；
- `JSON.stringify` 后 UTF-8 字节长度相同；不能用 JavaScript 字符数代替；
- 首行 `payload` 和 `model_provider` 唯一、可明确定位；原始字面量与预期一致；
- 替换后的 JSON 语义与只改 Provider 的预期一致；无模型改写、无多硬链接目标；
- 写前文件身份、快照、路径边界及占用检查通过。

符合条件时，`applySessionChanges` 必须使用 `provider_bytes_in_place` 描述符定位覆盖，只写 Provider JSON 字面量的字节，不生成整份 rollout 临时副本。成功结果 `inPlaceSessionFiles` 增加；文件身份、大小及首行以外的正文 hash 保持。

Windows 通过 `session-files.js` 内的 PowerShell worker / [windows-provider-bytes.cs](../../src/windows-provider-bytes.cs) 执行既有句柄协议；POSIX 使用同一已校验句柄定位写入。不得因重构适配层而无条件改回 rename/整文件重写。原地写失败、锁定或快照变化时，不得偷偷切换到整文件替换来绕过失败。

### PIO-3：不符合原地资格时，正文仅流式复制

Provider 字节不等长、原始字面量转义、非 ASCII ID、字段有歧义等不满足原地资格时，对仍可合法处理的首行使用同目录临时文件：更新首行、保留 LF/CRLF 分隔符、流式复制尾部、校验原文件、原子替换。正文不解析/重序列化，必须逐字节一致；此路径允许文件身份变化，记入 `rewrittenSessionFiles`。

首行无效/超限、目标不可验证或活动文件变化是明确失败/跳过，不得通过扫描后续行“找个 session_meta”或强制替换来兜底。用户不需要把所有 Provider 改成固定长度；原地优化是自动策略，不再提供 `--fast` 开关。

### PIO-4：不能顺便修改其他数据

Sync 不改根 config、历史 model、cwd、user-event、workspace roots、title、ordinal、消息/工具/加密正文或 SQLite `threads.updated_at`。ProviderSwitch 仅另外修改用户选择的 config 根 Provider/model，历史模型仍不动。文件 mtime 由既有实现尽力保留，**不得为了性能取消时间戳恢复**；Apply 使用锁内重新读取的时间，不能回拨真正的追加时间。变长恢复现有精度为毫秒，不承诺纳秒精确；不能把 mtime 当成并发证明或用线程时间改写制造可见性。

### PIO-5：普通写固定顺序，不恢复重型保护

```text
消费一次性 Plan → 启动 Operation → Home 锁内复核
→ 首行扫描 / 占用检测 / 实际目标集合
→ 无目标直接返回，不备份
→ SQLite 可写预检 → UndoBackup（默认保留 2）
→ 最后取消点 → config（仅 Switch）→ rollout → global state（仅显式 Repair）
→ SQLite 原生事务提交 → 结果 / 受管备份清理 → 释放 Home 锁
```

只使用 `<Codex Home>/tmp/provider-sync.lock`；跨 Home 的 SQLite 竞争交给原生事务。预检不预留未来数据库写锁：之后仍可能 busy。首次 mutation 前失败零业务写入；mutation 后失败为带 backupId/failedStage/failureCode/retryRecommended 的 `partial`，重新 Prepare/执行收敛，或用户手动 Restore。

不创建普通跨文件 journal，不自动全量回滚。SQLite 自身的 ROLLBACK 与单文件字节写入失败的局部复原不是全操作回滚。锁定/变化 rollout 分别报告，不能吞掉或误报 completed。全被跳过也可 partial 且无备份，不能因“没写”就报健康。

### PIO-6：性能以可证明行为验收

[ADR-0035](../adr/0035-provider-plan-semantic-revisions-and-sync-validation.md)：Sync/Switch 的计划只绑定相关状态——配置与路径、会话文件集合/物理身份/首行、threads schema/ID/Provider；允许正文追加和非 Provider SQLite 更新，不将 WAL/SHM 生命周期作为过期依据。替换/截断/集合变化/实际 Provider 漂移仍在备份前拒绝；Apply 落盘前的快照与占用检查不变。Repair/Restore 仍使用内容 revision。自定义目标 Provider 未定义时零写入拒绝，不能自动改回 openai。

计时只作参考。门禁验证是否开启正文扫描、写入策略/字节范围、32 MiB 正文 hash、文件身份/大小、非 Provider 字段与重试收敛；不能用“运行更快”代替这些检查。

## 4. 其他能力不得挤入 Sync

- **性能说明与日志布局**：[ADR-0036](../adr/0036-sync-performance-guidance-and-log-split-view.md) 仅为 UI 投影：概览折叠说明、实际替换计数驱动的结果入口、Desktop 日志双栏/窄屏详情切换。不得为提示扩张扫描、改名 Provider 或添加 Fast 模式；Core 原地写/流式替换资格仍唯一遵循 PIO-1～PIO-6。

- **PlanApplyGuard**：随机不透明 ID，10 分钟、进程内单次消费；Apply 仅 `{schemaVersion:1, planId}`。重启/过期/漂移重新 Prepare，不能重放任意执行参数。预览确认与“直接同步”共用此机制；直接同步点击本身是授权，不是绕过 Plan。
- **Diagnostics**：仅用户主动触发完整只读扫描；高级界面进入、同步失败、状态刷新都不能自动运行它。
- **Repair**：显式 `models/cwd/userEvent/workspaceRoots`；models 取当前根模型，缺失即拒绝；workspaceRoots 含 cwd 且保持全 Profile。按会话范围/修后核验见 [ADR-0021](../adr/0021-scoped-advanced-repair-and-readonly-integrity.md)。不提供 ordinal 改写、历史显示索引重建或解密。
  - UI 按 [ADR-0031](../adr/0031-repair-choices-and-optional-model-adjustments.md) 将 models 独立为“高级调整 → 统一历史模型名称”，其余三项为“专项修复”；不改变 Core targets。当前完整手动诊断可提供仅展开/聚焦的入口，绝不自动勾选、扫描或写入。两组独立默认空选择，按 Profile/revision 重置；旧/不完整结果不推荐。
- **Restore**：独立恢复前快照、耐久 journal、hash 验证与补偿；见 [ADR-0013](../adr/0013-restore-v2-recovery-state-machine.md)。旧普通 journal 兼容读取，不阻断新 Sync，但 Restore/Prune 仍保护关联证据；未解决 Restore journal 继续阻断普通写。
- **History**：显式只读、项目/父子树/独立分页见 [ADR-0022](../adr/0022-history-project-roots-and-child-pagination.md)。正文不进入日志、诊断包、持久缓存或列表 DTO；项目别名只属 Host 偏好。
- **Host**：Profile 目录选择、剪贴板、操作日志、更新属于 Desktop Host，不加入 Core 业务方法；Main 不处理 Provider SQL/rollout 算法。
- **统一备份策略**：[ADR-0027](../adr/0027-unified-backup-retention.md) 规定“备份与恢复”为唯一管理入口；Host 保存数量，UI 在 Prepare/Watch 启动时注入。Core 继续共用 Home 备份池、UndoBackup 和清理算法；Restore 的恢复前快照和受保护证据不受强制数量裁剪。
- **状态与结果作用域**：[ADR-0024](../adr/0024-profile-scoped-controls-and-actionable-feedback.md) 约束 Watch 的 Profile/revision 查询与物理 Home 别名去重、受管备份选项和 partial 日志/跳转。全局 Watch 查询仍供 Host 守卫，不能被 UI 当前配置过滤替代；日志重试入口只导航，不重放已消费 Plan。
- **刷新**：首次加载和手动刷新，无数据轮询。用户明确开启的 Watch 与每天首次启动的更新检查是独立能力，不授权后台 Diagnostics/History 扫描。
- **正在使用的会话**：[ADR-0030](../adr/0030-writer-owned-session-count.md) 用 SessionStore 的 `readSessionActivity` 只读观察当前 Home 的 Codex writer owners，含已对齐/等待输入/子会话；Status 与 Provider Plan 的 `sessionActivity` 只含 state/count，未知不返回零。这不是正在生成数。独立的 `impact.lockedRolloutFiles` 仍是本次同步写入受阻集合；Apply 原样重检。不要以 Provider 差异、mtime、遗留文件、进程数或新 app-server 的空状态冒充当前使用情况，也不要将此展示观察变为写入锁或新业务流程。
- **展示与偏好**：[ADR-0025](../adr/0025-user-feedback-and-window-preferences.md) 约束缺失状态、清理确认上限估计、当前 Profile 标记、Apply 后单次 Status 复核及 History 草稿筛选；窗口位置只由 Main 保存在 userData。不得为这些 UI 能力扩张 Core API、增加扫描或改 Provider I/O。
  - 浮动通知可点击内容/× 或键盘关闭，只移除该 Toast，保留自动收起；操作结果、日志、备份及全局阻断独立，不触发业务或状态刷新。
- **Legacy .NET**：保留构建及兼容维护，不复制本次 Node 普通写模型到旧 GUI，也不能引用旧 GUI 自动回滚证明新 Node 行为。
- **包体优化**：[ADR-0026](../adr/0026-windows-package-size-budget.md) 只调整构建压缩和 Windows 许可证归档；不能通过替换 SQLite、改写 Provider 算法或裁掉功能达到体积门禁。压缩后的 Utility 必须通过 packaged Sync/Restore。
- **实测前优化**：[ADR-0028](../adr/0028-pretest-feedback-and-validation.md) 固定日志 DTO/内存轮转一致性、Watch 主动终态反馈和 History 有界定位复用。History 定位表只含 ID/路径/身份，不含正文或标题；仍核对全部候选文件身份，不能宣传为恒定时间查询。Watch 私有 Host 协议 v3 不改变公开 Core v1，也不增加后台轮询。

## 5. 防漂移验收

在 Node 24 开发环境执行：

```bash
npm ci
npm run architecture:check
npm test
```

`architecture:check` 复用唯一的 `workspaces:check`（类型、公开 DTO、端口/跨层导入、依赖和根包边界），再执行 `core:test:provider-io`；不维护第二套规则扫描器。CI 的 workspace-contract job 执行同一命令，根测试继续由 Windows/Ubuntu 的 Node 16.20.2/24 矩阵运行。单项通过不能替代完整发布门禁。

类型检查边界：Core JS 每个文件必须显式 `@ts-check`，既有 13 个编排迁移豁免在 `packages/core/checks/core-js-check-boundary.contract.mjs` 固定列出，新增文件不得扩张豁免。端口泛型与 `checks/storage-types.ts` 证明实际方法/参数/只读属性不会退化成任意 `Function`。全局 `checkJs:false` 仍为根存储传递导入保留，不能据此宣称 Core 已全部类型检查；这是渐进改造边界，不授权整体翻译高风险算法。

| 不变量 | 可执行证据 |
| --- | --- |
| PIO-1/2/3/4/5/6、Facade 与适配器相同行为 | [provider-sync-lite.test.js](../../test/provider-sync-lite.test.js) |
| 字节描述符、歧义输入、原地失败与恢复兼容 | [in-place-transaction.test.js](../../test/in-place-transaction.test.js) |
| Windows 原生原地写、共享 worker 和占用/变化 | [windows-rewrite-worker.test.js](../../test/windows-rewrite-worker.test.js) |
| Prepare 单轮首行事实、Apply 重新复核及时间戳 | [provider-preparation-facts.test.js](../../test/provider-preparation-facts.test.js) |
| 可选数值计时投影、观测错误隔离和 Desktop 日志 | [file-update-timing-json.test.js](../../test/file-update-timing-json.test.js)、[contracts 检查](../../packages/contracts/checks/file-update-timing.contract.mjs)、[Desktop 日志测试](../../apps/desktop/tests/file-update-timing-log.test.mjs) |
| Plan/revision/单次消费/取消、Home 协调 | [plan-apply.test.js](../../test/plan-apply.test.js)、[status-coordination.test.js](../../test/status-coordination.test.js) |
| Restore 独立 crash matrix | [restore-v2-state-machine.test.js](../../test/restore-v2-state-machine.test.js) |
| 模块边界及禁止深导入 | [verify-workspace-boundaries.js](../../scripts/verify-workspace-boundaries.js)、`packages/core/checks/` |

修改读写核心必须保留以上测试，不得把断言改成兼容退化后的行为来“刷绿”。新行为先裁决再改合同；正常重构不改变既有 Provider I/O 算法。不支持的平台用例必须注明 skip，不能冒充实机通过。完整 CI、根 npm Node 16 安装和 packaged SQLite/E2E 仍独立要求。

## 6. 文档维护边界

- README 负责选择入口和快速上手；[桌面](../README_DESKTOP_ZH.md)、[Web](../README_WEB_UI_ZH.md)、[CLI](../README_CLI_ZH.md) 各自说明操作，不复制整份架构/合同或持续追加调试记录。
- 文档更正须核对源码和已接受 ADR；仅修正过时描述不改变行为，也不把历史合同、设计目标或合成基准改称发布事实。
- 改用户行为时同步中文入口、相关英文/日文/韩文摘要、指南、当前架构、合同、ADR、fixture 和 Unreleased；旧 Release 说明按当时版本保留。
- 提交前检查受影响链接、CLI 示例与帮助、界面用词和默认值。文档校验不能替代 Core/产物测试；证据必须说明实际运行范围。
