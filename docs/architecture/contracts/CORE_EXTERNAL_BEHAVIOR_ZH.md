# Node Core 外部行为兼容合同

> 状态：Phase 0 兼容基线
>
> 基线版本：`@dailin521/codex-provider-sync` v0.5.0
>
> 冻结日期：2026-08-24
>
> 适用范围：当前 Node service、CLI/Web adapter 依赖及 vNext Core 提取

## 1. 文档目的

V1/ADR-0016 当前增量（尚未公开发布）：

- ProviderSync 固定从 config 当前根级 `model_provider` 取目标，只扫描每个 rollout 的首条 `session_meta`。公开 Provider override、`syncMode`、`--fast` 和 `FAST_MODE_UNSUPPORTED` 已移除。
- Provider 字面量等长时原地更新并保留文件身份、大小与正文 Hash；长度不同时流式生成临时文件并原子替换，聊天正文逐字节保持不变。
- Diagnostics 是用户主动触发的一次完整只读流式扫描；Repair 通过 `prepareRepair/applyRepair` 显式修复 `models`、`cwd`、`userEvent`、`workspaceRoots`。加密内容只诊断、不修改。
- 普通 Sync/Switch/Repair 只持有 Codex Home lock，SQLite 并发由原生事务处理；实际 mutation 前才创建 UndoBackup，不创建跨文件普通 journal，也不自动全量回滚。mutation 后故障返回带 backup、阶段和重试建议的 `partial`。
- Restore 继续使用独立的恢复前 snapshot、耐久 journal、Hash 验证与补偿状态机，但同样只使用 Codex Home lock。
- ADR-0015 只保留等长原地更新与正文 byte-preservation 决策；当前职责与写入模型详见 [ADR-0016](../../adr/0016-node-core-responsibility-boundaries-and-lightweight-writes.md)。

本文冻结 vNext 迁移开始时 Node 实现已经提供的外部行为。这里的“外部”不仅指 npm 最终用户，也包括当前 CLI 与 Local Web UI 对 Node service 的真实依赖。

本文解决三个问题：

1. 当前哪些行为已经是事实，提取 Core 时不能改变；
2. 当前哪些能力散落在 `service.js` 之外，不能在重组时漏掉；
3. 哪些设计是 vNext 目标，尚不能反向描述为 v0.5 已实现。

本文不把所有内部函数、测试注入点和物理深导入路径升级为永久 Public API。

## 2. 合同层级

### 2.1 v0.5 当前合同

当前 `src/service.js` 导出：

```text
SyncTransactionError
getStatus
renderStatus
runSync
runSwitch
runRestore
runPruneBackups
```

这些导出构成当前 CLI/Web 迁移所依赖的 service 边界，但 npm 包尚未通过 `exports` 声明正式可供第三方导入的 Core 包入口。

### 2.2 当前散落但必须纳入 vNext Core 的能力

Local Web UI 和 CLI 还直接依赖：

| 当前模块 | 能力 |
| --- | --- |
| `backup.js` | `listBackups` |
| `history.js` | `listHistory`、`getHistorySession` |
| `watch.js` | `runWatch` |
| `config-file.js` | Web adapter 读取 config 与根级 model |
| `storage-layout.js`、`sqlite-state.js` | Web adapter 在确认操作前解析并固定 storage |

因此，不能把 `service.js` 的当前导出列表误写成“完整 Core 已经存在”。Phase 1 的 Public API 必须覆盖 CLI、Web 和未来 Desktop 所需的完整能力。

### 2.3 internal / test-only

以下选项当前存在于实现或测试中，但不自动成为长期外部合同：

- `faultInjector`；
- `sqliteBusyTimeoutMs`；
- 预构造的底层 storage 对象本身的全部内部字段；
- 测试替身 `services`；
- 自定义时间、环境、文件系统或 URL opener；
- transaction journal 内部事件格式之外未明确发布的实现细节。

其中 `signal` 代表未来取消能力的基础，但 v0.5 CLI 尚未形成稳定的取消结果合同。

## 3. 统一存储解析合同

### 3.1 Codex Home

优先级：

1. 调用输入中的显式 `codexHome`；
2. `CODEX_HOME`；
3. `~/.codex`。

所有路径最终解析为绝对路径。

### 3.2 SQLite Home

优先级：

1. 调用输入或已确认 Storage Profile 中的显式 `sqliteHome`；
2. `config.toml` 根级 `sqlite_home`；
3. `CODEX_SQLITE_HOME`；
4. `<Codex Home>/sqlite`。

结果必须携带来源：

```text
cli | config | env | default
```

Web Profile 虽然不是 CLI，其显式 SQLite Home 仍沿用现有来源值 `cli`；vNext 可以在新 DTO 中使用更准确的来源枚举，但不得改变存储优先级或兼容判断。

### 3.3 state DB 选择

- 只处理一个被解析为当前活动状态数据库的 `state_5.sqlite`；
- 只有 `default` SQLite Home 可以回退到 `<Codex Home>/state_5.sqlite`；
- 显式、配置或环境 SQLite Home 缺少数据库时，写操作失败；
- status 可以报告数据库缺失而不失败；
- Windows WSL UNC SQLite Home 不允许安全读写，写操作必须在备份和其他 mutation 之前失败；
- 不得因为某个候选 DB 存在，就同时改写多个数据库。

### 3.4 vNext 单 Home 锁与 SQLite 原生事务（V1/C3）

Sync、Switch、Repair、Restore 与 Prune 统一只使用 `<Codex Home>/tmp/provider-sync.lock`。SQLite 的跨 Home 并发交给原生事务裁决；不能取得写事务时返回结构化 SQLite busy，不另建 State DB resource lock。ADR-0012 的双层资源锁只保留为 v0.5/V1 早期历史决策，当前行为由 [ADR-0016](../../adr/0016-node-core-responsibility-boundaries-and-lightweight-writes.md) 取代。

该合同不改变本节的 storage 优先级，也不授权未经阶段门槛的入口扩展写入。Node/.NET 兼容、owner metadata、Busy/不可验证错误和 Hash 无副作用已有本地 Fixture；只有同一最终提交的 required CI 与最终合入闭合后，才成为发布合同。

## 4. `getStatus`

### 4.1 当前输入

```text
codexHome?
sqliteHome?
storage?      # Web adapter 已确认的 storage
configText?   # 与 storage 同一确认快照的 config
platform?
```

CLI 通常提供路径；Web adapter 提供 server-resolved storage 与同一时刻读取的 configText。

### 4.2 当前返回字段

以下字段构成 v0.5 状态快照合同：

```text
codexHome
sqliteHome
sqliteHomeSource
sqliteAccess
checkedStateDbPaths
currentProvider
currentProviderImplicit
configuredProviders
rolloutCounts
lockedRolloutFiles
encryptedContentCounts
encryptedContentWarning
sqliteCounts
stateDbLocation
sqliteRepairStats
projectThreadVisibility
backupRoot
backupSummary
pendingTransactions
```

关键嵌套结构：

```text
rolloutCounts.sessions
rolloutCounts.archived_sessions

sqliteCounts.sessions
sqliteCounts.archived_sessions

backupSummary.count
backupSummary.totalBytes

pendingTransactions[].operationId
pendingTransactions[].state
pendingTransactions[].backupDir
pendingTransactions[].journalPath
```

`sqliteCounts` 在数据库不存在时可以为 `null`；数据库损坏或 busy 时可以返回带 `unreadable` 和 `error` 的诊断对象。

### 4.3 状态语义

- 根级 `model_provider` 缺失时，`currentProvider="openai"` 且 `currentProviderImplicit=true`；
- `configuredProviders` 至少包含内置 `openai`；
- rollout 分布分别统计 `sessions` 和 `archived_sessions`；
- status 扫描读取不到锁定 rollout 时，记录 `lockedRolloutFiles`，不把它当成写失败；
- status 不通过修改 `updated_at` 或其他数据来推断可见性；
- status 报告 pending transaction，后续写操作必须要求恢复；
- encrypted content warning 只解释继续会话风险，不修改或导出加密内容。

### 4.4 `renderStatus`

`renderStatus(status)` 是当前 CLI presenter，不是未来 Core Domain API。Phase 1 可以把它移动到 CLI adapter，但迁移时必须保持 CLI 人类输出合同。

## 5. `runSync`

### 5.1 当前 adapter 依赖输入

```text
codexHome?
sqliteHome?
storage?
expectedConfigText?
keepCount = 5
onProgress?
platform?
signal?
```

公开 `prepareSync` 只接受可信 profile 与可选 `keepCount`；Provider、model、模式和路径不属于 Renderer/HTTP/IPC 产品输入。`runSync` 仅作为 CLI/旧调用方的弃用适配器。

### 5.2 目标 Provider 与扫描边界

目标 Provider 只取自当前 config 根级 `model_provider`，缺失时为 `openai`。Sync 不接受 Provider override，不读取根模型，不修改历史模型、cwd、user-event、workspace roots 或加密内容。扫描每个 rollout 时只打开首行；模型等完整问题只由 Diagnostics/Repair 路径处理。

### 5.3 前置条件

- `keepCount` 必须是整数且 `>=1`；
- Codex Home 与 `config.toml` 必须可读；
- 获取同一 Codex Home 的 provider-sync 锁；
- 不存在未解决 Restore transaction；旧普通 Sync/Switch journal 只读展示，不阻断新普通写；
- `expectedConfigText` 提供时，磁盘 config 必须完全一致；
- Windows WSL UNC storage 不支持 sync；
- 非 default SQLite Home 必须存在目标 `state_5.sqlite`；
- SQLite 必须在 rollout mutation 和备份之前通过原生事务可写性检查。

### 5.4 写入流程与安全顺序

以下顺序属于兼容安全合同：

1. 消费 Plan，启动 OperationRuntime；
2. 获取 Codex Home lock，在锁内重新读取 config 并验证 Plan；
3. 扫描 rollout 首行、检查 session 占用并确定实际写入集合；
4. 无实际写入时返回 completed/noop，不创建备份；
5. 预检 SQLite 可写性；
6. 创建只覆盖实际目标的 UndoBackup；
7. 进入首次 mutation 后不再接受取消；
8. 更新可写 rollout；Switch 先更新 config；
9. 最后在一个 SQLite 原生事务中提交 Provider/Repair 修改；
10. 刷新 inventory、清理旧托管备份并释放 Home lock。

普通写不创建跨文件 transaction journal，也不自动全量回滚。mutation 前失败直接返回错误且业务状态不变；mutation 后失败返回 `partial`、backupId、`failedStage`、`failureCode` 与 `retryRecommended=true`。用户重复执行相同操作负责收敛，或显式 Restore UndoBackup。

### 5.5 部分成功

- 启动/活动进程锁定的 rollout 可以被跳过；
- 可写 rollout 与 SQLite Provider 仍可提交；
- 所有跳过路径必须出现在 `skippedLockedRolloutFiles`；
- 新普通写以 `OperationResult.outcome="partial"` 表示锁定 session 或 mutation 后故障；
- v0.5 CLI 仍以 exit `0` 返回 partial。

### 5.6 当前结果字段

成功结果冻结为以下字段集合：

```text
codexHome
sqliteHome
sqliteHomeSource
targetProvider
previousProvider
backupDir
backupDurationMs
changedSessionFiles
skippedLockedRolloutFiles
sqliteRowsUpdated
sqliteProviderRowsUpdated
sqlitePresent
rolloutCountsBefore
inPlaceSessionFiles
rewrittenSessionFiles
partialReason
failedStage
failureCode
retryRecommended
autoPruneResult
autoPruneWarning
```

其中：

```text
autoPruneResult.backupRoot
autoPruneResult.deletedCount
autoPruneResult.remainingCount
autoPruneResult.freedBytes
```

`autoPruneResult` 可以为 `null`；主事务提交后的 inventory/prune 失败通过 `autoPruneWarning` 报告，不得把已提交成功伪装成整体失败。

### 5.7 进度事件

成功 sync 的 start stage 顺序固定为：

```text
scan_rollout_files
check_locked_rollout_files
create_backup
rewrite_rollout_files
update_sqlite
clean_backups
```

每个事件至少具有：

```text
stage
status = start | complete
```

现有 complete 事件可包含：

- 扫描变化数与锁定读取数；
- 可写和锁定数量；
- backupDir 与 durationMs；
- rollout applied/skipped 数量；
- SQLite updatedRows；
- prune deletedCount 与 warning。

`onProgress` 是非权威观察通道。同步或异步 observer 抛错不得改变事务状态、触发补偿或把已提交操作报告为失败。

## 6. `runSwitch`

### 6.1 当前输入

```text
codexHome?
sqliteHome?
storage?
expectedConfigText?
provider             # 必填
model?
keepRootModel=false
keepCount=5
onProgress?
platform?
signal?
```

### 6.2 Provider 和 model 规则

- `provider` 缺失时失败；
- 内置 `openai` 始终有效；
- 自定义 Provider 必须在 config 声明；
- `model` 与 `keepRootModel` 互斥；
- 显式 model 优先；
- 否则 `keepRootModel=true` 时保留根级 model；
- 否则从目标 Provider section 读取 model；
- 自定义 Provider section 没有 model 时保留根级 model，并返回 warning；
- 最终 model 只写入根级 config；历史线程 model 不随 Switch 修改，必须显式 Repair。

### 6.3 事务行为

- 在修改 config 前完成备份；
- 备份保存切换前的原始 config；
- config 与内部 ProviderSync 共用一个 Operation、Home lock 和 UndoBackup，不产生嵌套 Plan、重复备份或普通 journal；
- 后续 mutation 失败返回可重试 partial；不会自动恢复已经写入的 config；
- 额外进度 stage 为 `update_config`，发生在 `create_backup complete` 之后；
- config 在等待锁期间被其他进程改变时必须失败，而不是覆盖新内容。

### 6.4 结果扩展

`runSwitch` 返回完整 SyncResult，并增加：

```text
configUpdated = true | false
modelSync.applied
modelSync.source = explicit | provider-section | none
modelSync.model
modelSync.warning
```

### 6.5 `getDiagnostics` 与 Repair

`getDiagnostics` 只在调用方显式请求时执行一次完整流式扫描，返回有界聚合计数；不后台刷新、不定时运行、不写入目标，也不把消息正文、路径、凭据或加密内容写入结果。

`prepareRepair/applyRepair` 支持 `models`、`cwd`、`userEvent`、`workspaceRoots`。目标必须显式选择且不可重复；`workspaceRoots` 自动包含 `cwd`。模型修复取 config 当前根模型，缺失时 Prepare 失败。Repair 只扫描所选目标需要的数据；SQLite 修改在单个原生事务中提交，并与 Sync/Switch 使用相同的 Home lock、UndoBackup 和 partial/retry 语义。

## 7. `runRestore`

### 7.1 当前输入与默认值

```text
codexHome?
sqliteHome?
storage?
expectedConfigText?
backupDir                         # 必填
restoreConfig=true
restoreDatabase=true
restoreSessions=true
allowSqliteHomeRelocation=false
platform?
```

### 7.2 安全合同

- 获取同一 Codex Home 的 restore 锁；
- `expectedConfigText` 提供时必须与磁盘完全一致；
- WSL UNC storage 在读取或修改备份目标前失败；
- 备份 namespace、版本、Codex Home 与 manifest 必须通过校验；
- session restore 路径必须位于允许的 rollout roots，拒绝路径逃逸、符号链接和重复目标；
- restoreDatabase 时，配置选中的 SQLite Home 缺 DB 必须失败；
- SQLite Home 与备份记录不一致时，默认拒绝 relocation；
- relocation 必须由显式 SQLite Home 和 `allowSqliteHomeRelocation=true` 共同授权；
- relocation 时禁止同时恢复旧 config；
- 未完成 transaction 所需的 rollout、SQLite、config 或 global state 不能被部分关闭；
- 恢复目标完成后尝试把绑定 journal 标记为 rolledBack；
- 当前 journal terminal 标记失败会使 `runRestore` 整体 reject，即使部分恢复结果已经持久化；只有 inventory 刷新失败会降级为 warning。

### 7.3 已发布 v0.5 Restore 历史安全债（V1/C8 候选已关闭）

已发布 v0.5 的 `runRestore` 会获取正式 operation lock，也会校验被选中备份及其 journal，但**恢复操作自身不会先创建新的恢复前备份，也没有独立的 restore transaction journal**。因此，该发布版若在 config、global state、SQLite 或 rollout 已部分落盘后发生进程崩溃，没有与 sync/switch 同等级的自动补偿证据。V1 当前分支已由 7.4 的 Restore v2 候选取代这条运行路径；在远端门禁与最终合入前，7.3 仍作为已发布 v0.5 的兼容和迁移依据保留。

这属于 v0.5 已知安全债，不是允许 vNext 继续保留的目标合同：

- Phase 0 必须如实记录，不能宣称 restore 已具备完整事务性；
- 在补齐 restore backup/journal 前，不得扩大 restore 自动化或无人值守使用范围；
- vNext 必须为 restore 建立恢复前 snapshot、目标清单、持久 journal 和崩溃恢复测试；
- 当前恢复目标已落盘但 rolledBack terminal 标记失败时，调用方只会收到失败，不能据此证明目标未恢复或 Journal 已收敛；vNext 必须增加 commit/terminal acknowledgement reconciliation；
- 补齐安全机制时必须继续兼容 v1/v2 旧备份格式和现有 restore 选项。

### 7.4 vNext Restore v2（V1/C8 候选已实现，尚未发布）

[ADR-0013](../../adr/0013-restore-v2-recovery-state-machine.md) 冻结 Restore v2：在任何 restore mutation 前创建独立的恢复前 snapshot，并用独立 restore operation journal 记录 source backup、目标清单、每目标状态和 durable terminal。Node 与仍受支持的 .NET Core 都实现该协议。`prepared/applying/committing/rollback-pending` crash 必须由显式 recovery 依据该 snapshot 补偿，或由同 source、同物理 Home、完整目标覆盖的新 Restore 创建自己的 snapshot/journal 后以 completed resolver 收敛；`committed-pending-ack` 必须按目标 Hash 前进到 `completed` 或 `recovery-required`，不得对已提交目标启动反向补偿。

snapshot manifest 与 durable `prepared` event 必须全量绑定 schema/protocol、operation、source backup、storage（含持久化 `codexHomePhysical`）、required target kinds、resolver operation IDs、按顺序排列的完整 targets，以及 snapshot 的稳定物理目录；任何不一致都在 compensation 或 commit acknowledgement 前进入 `recovery-required`，不能仅凭重算后的 manifest hash 放行。

completed resolver 只在 source backup 稳定物理目录/revision、pending 与 resolver 持久化的 `codexHomePhysical`、当前已加锁 Codex Home 的稳定物理 identity、唯一 operationId 与 required target kinds 全部匹配时解除 write blocker；`backupId` 是由最终物理目录 basename 生成的展示/索引字段，不作为旧 journal 的独立安全键。Windows 下 source backup 与 Home 的长路径、8.3 短路径、junction 与大小写别名必须先解析到同一稳定物理目录，不得用 lexical path 或旧式 backupId 差异误判 foreign pending。Prepare、Apply、journal、备份读取和 inventory 刷新全程只使用该物理 source path，并在 mutation 前再次核对 source revision；无法两次可靠 realpath 时仍 fail closed。不得重新解析可变 lexical `codexHome` 来替代历史 physical binding。旧 raw journal 不改写，Prune 按物理目录继续保护其 journal/source/snapshot。foreign pending、重复 operationId、覆盖不足、invalid tail 或未知 journal/schema 必须在新 snapshot/journal/mutation 前 fail closed，不能依赖 message 推断结果。source backup v1/v2、现有 restore options 与 relocation 双重授权保持兼容。

已发布 v0.5 仍是 7.3 行为；本节实现只有在 C8、本 PR 远端门禁和最终合入完成后才成为发布合同。

### 7.5 当前结果

成功时返回经过验证的备份 `metadata.json` payload。v2 托管备份通常包含：

```text
version
namespace
codexHome
sqliteHome
targetProvider
createdAt
dbFiles
sqliteDbFiles
globalStateFiles
changedSessionFiles
sizeBytes
fileCount
```

兼容 v1 备份时字段会有所不同。inventory 刷新失败时增加：

```text
backupInventoryWarning
```

调用者不得假定 restore 结果与 SyncResult 同构。

## 8. `runPruneBackups`

### 8.1 当前输入

```text
codexHome?
keepCount=5
```

`keepCount` 必须是非负整数，允许 `0`。

### 8.2 当前行为

- 确认 Codex Home 存在；
- 获取同一 Codex Home 的 prune 锁；
- 只删除具有 `provider-sync` namespace 的托管备份；
- 由新到旧保留前 N 份；
- 未完成 transaction 引用的备份受到保护，即使 N 为 0 也不删除；
- 非托管目录不参与计数和删除。

### 8.3 当前结果

```text
backupRoot
deletedCount
remainingCount
freedBytes
```

## 9. 备份、历史与 Watch 能力

这些能力当前不全部位于 `service.js`，但已被 Web/CLI 使用，迁移时必须保留。

### 9.1 `listBackups`

输入：Codex Home。

结果：

```text
backupRoot
backups[].id
backups[].path
backups[].sizeBytes
backups[].metadata
```

只列出托管 backup namespace，顺序为目录名由新到旧。

### 9.2 `listHistory`

当前输入：

```text
codexHome
page=1
pageSize=50
query=""
project=""
provider=""
archived=all | active | archived
```

约束：

- page 是正整数；
- pageSize 为 `10..100`；
- 搜索从 rollout 只读读取；
- 相同 thread id 保留 mtime 更新的文件；
- 无 thread id 的会话使用基于 rollout 绝对路径 Hash 的稳定有界 ID；
- 结果按 updatedAt、mtime 由新到旧；
- 无 query 的普通列表只读取最大 64 KiB 的首行 `session_meta` 和文件元数据，不读取消息正文；超限、非首行或格式无效的 metadata 不进入列表；
- 搜索可以匹配会话标题、cwd、Provider 和安全抽取的用户/助手消息文本。

结果：

```text
page
pageSize
total
hasNextPage
sessions[]
```

每个公开 session 包含：

```text
id
rolloutPath
title
cwd
provider
model
archived
createdAt
updatedAt
messageCount
messageCountKnown?
```

vNext 公共 Core/HTTP/IPC 列表投影不返回 `rolloutPath`、`cwd` 或 `firstUserMessage`。`title` 只能来自显式 session metadata；metadata 没有标题时返回空字符串，由 UI 本地化显示“未命名会话”，不得用消息正文回退。列表搜索可以在本次只读扫描内匹配安全抽取文本，但消息正文不能进入列表 DTO、日志或缓存；正文只能由用户明确调用 `getHistorySession` 后返回。

无 query 的普通列表以受限首行 metadata 构造摘要：thread id、title、cwd、Provider/model、timestamp 分别限制为 512、1024、32768、512、128 字符，越界字段为空、使用固定缺省值或基于路径的 fallback ID；`updatedAt` 使用经复核的文件 mtime，`messageCount=0` 且 `messageCountKnown=false`；UI 必须隐藏该占位计数，不得显示为“0 条消息”。旧实现或全文扫描结果缺省 `messageCountKnown`，以及显式 `true`，均表示 `messageCount` 精确。用户显式输入 query 时才允许全文流式扫描，以匹配安全正文并返回精确计数和最后可见时间；扫描仍只保留计数、时间与 query 命中等常量聚合状态，不按消息数常驻 descriptor 或正文。

详情定位先用同一受限 metadata 路径去重并选定 rollout，只对用户选择的目标文件做一次全文读取；其他 rollout 不得因详情定位而扫描正文。目标必须绑定定位阶段记录的 regular-file identity、稳定物理路径与 sessions 根边界，从同一文件句柄读取并在读前/读后复核；删除、替换、symlink/junction 逃逸或保留 mtime 的换档均返回 `STALE_STATE`，不得返回另一文件的正文。

### 9.3 `getHistorySession`

- sessionId 必填；
- 默认最多返回最后 200 条安全抽取的用户/助手消息；
- 不返回认证信息、token、推理内容或任意原始 JSONL；
- 结果包含 `session`、`messages`、`truncated`、`returnedMessageCount`。

### 9.4 `runWatch`

当前行为需保留：

- 默认 debounce 750 ms；
- 默认监听 config 和当前唯一 state DB/WAL/SHM；
- 配置变化后重新解析 storage；
- SQLite busy 软跳过；
- 连续 5 次非 busy 失败后停止；
- once 在首次成功 sync 后停止；
- 返回 handle：Codex Home、config path、动态 state DB path、动态 SQLite Home、`stop()`、`done` 和可选 `signalPromise`。
- `startWatch` 在创建 watcher 前以 `realpath(Codex Home)` 建立物理 scope（Windows 不区分大小写）；同一物理 Home 的并发或重复启动返回同一个活动 snapshot，首个启动的 options 保持权威，不建立第二组 OS watcher；
- 物理 Home 无法可靠解析时 fail closed：权限错误为 `PERMISSION_DENIED`，其它缺失或不可解析状态为 `CODEX_HOME_NOT_FOUND`。手工或自动停止完成后释放活动 scope；终态 registry 只保留最近 64 条 stopped 记录。

## 10. 跨进程锁与恢复状态

### 10.1 锁位置

当前同一 Codex Home 的写操作使用：

```text
<Codex Home>/tmp/provider-sync.lock
```

这是正式 Core operation lock。当前协议为 protocol v2，并使用 canonical lock directory、`owner.json`、独立 claims 目录、不可变 instance identity 和有界 stale reclaim。锁 owner 记录 Node/.NET 跨运行时识别所需的 PID、进程启动身份、instance id、label 和工作目录。

.NET GUI 的 SingleInstanceGuard 只负责阻止同一 GUI 重复启动，**不是 Core operation lock**，不能用于证明 CLI、Web、旧 GUI 或 Electron 之间互斥。未来 Electron 的应用单实例锁同样只能负责 UX，不能替代上述 storage operation lock。

### 10.2 必须冻结的语义

- sync、switch、repair、restore、prune 在完整操作周期持有 Codex Home lock；
- 活跃、无法验证或未来协议的锁 fail closed；
- 只有能够证明 owner 已不存在时才回收 stale lock；
- 不得要求用户常规手工删除锁；
- 释放时必须确认仍是同一 owner/generation；
- 同一 Codex Home 的 CLI、旧 GUI、Web 与未来 Electron 必须使用兼容锁合同；
- 不能以 UI 层“避免同时点击”代替跨进程互斥。

### 10.3 锁内重解析

执行写操作时，Plan 或 Web status 阶段解析出的 storage 只能作为待确认快照。获得正式 operation lock 后，Core 必须重新读取 config，并重新解析：

- Codex Home；
- SQLite Home 与来源；
- legacy fallback 资格；
- 当前活动 `state_5.sqlite` 路径；
- pending transaction；
- SQLite 可写性。

重解析结果与确认快照不一致时必须返回 stale/changed，不得继续使用锁外缓存的 storage 对象。

当前 direct CLI sync/restore 的主要路径会在锁内读取 config；但 Web 会传入预解析 storage，switch 也存在锁外准备再交给 sync 的路径。`expectedConfigText` 能防止部分 config 漂移，却不能证明所有 DB 选择和文件系统状态均未改变。这是 Phase 1/Plan-Apply 下沉时必须关闭的 TOCTOU 缺口。

### 10.4 跨 Codex Home 共享 SQLite Home

不同 Codex Home 可以解析到同一个 SQLite 数据库，因此它们持有不同 Home lock 时仍可能同时到达数据库。V1/C3 不再增加 State DB 文件资源锁；竞争由 SQLite 原生事务串行化或以 busy 失败。调用方必须把 SQLite busy 视为 mutation 前可重试失败，不能把不同 Profile 直接等同于不同数据库资源。

### 10.5 pending transaction

- 未持久化 terminal 的 journal 表示需要恢复；
- status 必须报告；
- 新 Sync/Switch/Repair 只被未解决 Restore journal 阻止；旧普通 journal 继续展示并受 Prune 保护，但不再阻止新的普通写；
- prune 必须保护绑定备份；
- restore 必须覆盖 journal 已开始或无法安全排除的所有目标；
- 完成恢复后持久化 rolledBack terminal。

V1/C8 已统一 Node 与 .NET 对“选择的恢复备份之外仍存在其他 pending transaction”的处理：只要存在与所选物理 source/revision 不匹配的 foreign pending，Restore 在新 snapshot、journal 或目标 mutation 前以 `RECOVERY_REQUIRED` fail closed，且不得改写原 pending 证据。

vNext 统一规则必须是：

- restore 只解决与所选备份明确绑定且恢复覆盖完整的 transaction；
- 其他 foreign pending 保持 recovery blocker，不得被顺带清除或忽略；
- Node/.NET 迁移期交叉 Contract Test `restore-v2-cross-runtime-foreign-pending` 必须在两个方向验证：另一运行时创建的 pending 保持字节不变、选择 foreign backup 被拒绝、数据和受管备份树无 mutation；
- 本地 Fixture 通过不等于发布完成；同一最终提交的 required CI 与合入证据仍是正式合同门槛。

## 11. 当前错误合同

### 11.1 已结构化的 Core 错误

#### `SyncTransactionError`

可能的 `code`：

```text
SYNC_FAILED_ROLLED_BACK
RECOVERY_REQUIRED
```

公开诊断字段：

```text
name
code
originalError
rollbackErrors
backupDir
completedTargets
uncompletedTargets
rollbackStatus
recoveryRequired
recoveryInstructions
```

`SYNC_FAILED_ROLLED_BACK` 与普通写 `RECOVERY_REQUIRED` 仅用于 v0.5/旧 journal 兼容解析；新的 Sync/Switch/Repair 不产生它们。新普通写 mutation 后故障返回 `partial` 和 UndoBackup；Restore 仍可返回 `RECOVERY_REQUIRED`。

#### pending recovery

`RecoveryRequiredError` 使用：

```text
code = RECOVERY_REQUIRED
pendingTransactions
```

#### abort

安全取消点直接生成：

```text
name = AbortError
code = ABORT_ERR
```

取消只在首次 mutation 前生效并映射为稳定的 cancelled；进入 mutation 后取消请求不再中断普通写。

### 11.2 当前 message-only 错误类别

下列错误目前大多只是普通 `Error` 和稳定核心 message，没有统一 code：

- 参数缺失或非法；
- Provider 未配置；
- config 在确认后改变；
- Codex Home/config 缺失；
- configured SQLite Home 缺少 DB；
- Windows WSL UNC 不支持；
- SQLite busy；
- SQLite malformed/unreadable；
- lock busy 或无法安全验证；
- backup/manifest 不兼容；
- SQLite Home relocation 未确认；
- history 条目不存在；
- 文件权限、空间和其他 I/O 错误。

PR 3 引入结构化 Error Code 时必须保留现有人类提示的关键语义，并为 CLI、Web、Runtime/IPC 分别建立映射测试。

### 11.3 vNext 不得假设的行为

- 不得把所有普通 Error 通过字符串匹配长期分类；
- 不得让 Desktop 解析 CLI stderr；
- 不得让 Web 丢失 `RECOVERY_REQUIRED` 后仍显示成普通可重试失败；
- 不得因 observer 或 inventory warning 把已提交操作改判为失败。

## 12. Local Web UI Revision 合同

当前 Web adapter 已实现的并发与确认保护必须在迁移时保留，并最终下沉到 Core 的 Plan/Revision/Apply 能力。

### 12.1 Server-managed Profile

- API 操作通过 `profileId` 选择路径；
- 请求体不得直接提供 `codexHome` 或 `sqliteHome`；
- default Profile 由 Web 启动参数控制，不可通过 Profile API 修改或删除；
- Profile revision 是 Profile 内容 Hash；
- 更新已有 Profile 和删除 Profile 需要当前 revision；
- 创建新 Profile 不提供 revision。

### 12.2 写操作 Revision 要求

| Web 操作 | profileRevision | storageRevision |
| --- | --- | --- |
| sync | 必须 | 必须 |
| switch | 必须 | 必须 |
| restore | 必须 | 必须 |
| prune | 必须 | 当前不要求 |

`storageRevision` 当前覆盖：

- profile id/revision；
- config 内容 Hash；
- Codex Home；
- SQLite Home 与来源；
- SQLite access 支持状态；
- legacy fallback 资格；
- 当前选中的 state DB 路径与来源。

缺失或变化时返回 HTTP 409 和以下 code：

```text
PROFILE_REVISION_REQUIRED
PROFILE_CHANGED
STORAGE_REVISION_REQUIRED
STORAGE_CHANGED
```

确认快照失效时，必须刷新并重新确认，不得静默重新解析后继续写入。

### 12.3 Web 到 service 的调用映射

| Endpoint | 当前调用 |
| --- | --- |
| `POST /api/status` | `getStatus({ storage, configText, ...profile })` |
| `POST /api/backups` | `listBackups(codexHome)` |
| `POST /api/history` | `listHistory(codexHome, body)` |
| `POST /api/history/session` | `getHistorySession(codexHome, sessionId)` |
| `POST /api/sync` | `runSync({ storage, expectedConfigText, provider, keepCount, model, onProgress })` |
| `POST /api/switch` | `runSwitch({ storage, expectedConfigText, provider, keepCount, model, keepRootModel, onProgress })` |
| `POST /api/restore` | `runRestore({ storage, expectedConfigText, backupDir, restore*, allowSqliteHomeRelocation })` |
| `POST /api/prune` | `runPruneBackups({ codexHome, keepCount })` |

### 12.4 单写操作

- 一个 Web server 进程同时只允许一个 sync/switch/restore/prune；
- 已有操作运行时返回 HTTP 409；
- 该进程内活动日志记录 start、progress、success/partial/error；
- activity 是内存态，不是持久审计日志。

### 12.5 Web 结果映射

成功写操作返回：

```json
{
  "result": {
    "outcome": "success"
  }
}
```

如果结果中的 `skippedLockedRolloutFiles` 非空：

```json
{
  "result": {
    "outcome": "partial"
  }
}
```

Web 的 `alignment.aligned` 当前要求：

- target Provider 存在；
- SQLite 可读；
- status 未跳过锁定 rollout；
- rollout 与 SQLite 两个 scope 中所有非零 Provider 都等于 target Provider。

rollout 与 SQLite 总数不要求完全相等。

### 12.6 当前兼容缺口

Web `withOperation` 当前把 operation 异常映射为 HTTP 400 `{error}`，没有透传 Core `error.code`。这是已知缺口，不应被描述为理想合同。

后续结构化错误改造需要：

- 保留 409 revision code；
- 让 recovery、busy、cancel 和普通失败有稳定映射；
- 保持现有人类 message；
- 不改变成功/partial payload；
- 增加 HTTP Contract Test。

## 13. Local Web UI 安全合同

以下行为属于当前外部安全边界：

- 只监听 `127.0.0.1`；
- 健康检查不要求配对；
- 其他业务 API 需要已配对 device credential；
- pairing token 随机、一次性、默认 5 分钟过期；
- 服务端只持久化 device credential Hash；
- 非 GET 浏览器请求要求同一回环 Host/Origin；
- JSON 请求体最大 64 KiB；
- storage path 只能通过 server-managed Profile；
- restore 只接受当前 Codex Home 的 managed `backupId`；
- 同一运行时描述文件复用需要内部 challenge、HMAC proof、instance id 和 storage identity 一致；
- runtime descriptor 权限尽可能设置为 `0600`；
- History 明确只读，不得被 sync/switch 以外的页面路径修改。

Electron 不需要复用浏览器 pairing，但不能削弱 Core 的 path、revision、backup 和 transaction 安全边界。

## 14. 不得改变的业务边界

迁移期间必须保持：

- ProviderSync 只同步会话可见性所需的 Provider；其他模型、SQLite flags/cwd 和 workspace root 元数据只能由显式 Repair 修改；
- 不读取、复制、记录或修改 `auth.json`、token 或凭据；
- 不修改消息正文；
- History 仅在用户显式打开只读页面时抽取安全的用户/助手文本；
- 不修改 thread `updated_at`，不通过重排历史制造可见性；
- 不承诺跨 Provider 解密 `encrypted_content`；
- 有实际写入目标时在首次 mutation 前创建 UndoBackup；noop 不创建备份；
- SQLite busy 时在 rollout mutation 前停止；
- 普通写不创建 transaction journal 或自动补偿；Restore 独立保留 snapshot、journal 与补偿；
- 保留 default-only legacy DB fallback；
- partial 必须列出 skipped rollout；
- active session 导致 rollout/SQLite 数量短暂不同不等于 Provider 未对齐。

## 15. legacy tolerated 行为

以下事实存在，但不应直接升级为长期 Core API：

- 第三方可通过 npm 安装目录物理深导入 `src/service.js`；
- service 接受测试用 `faultInjector`、busy timeout 和预构造 storage；
- Web 直接注入 service 替身；
- 某些错误只能通过英文 message 区分；
- Web API 的 validation 错误有时由外层 catch 映射为 HTTP 500，而 operation 内错误为 400；
- Web prune 前端会多发送当前 server 不使用的 `storageRevision`；
- sync Core 本身不限制 Provider 字符集，而 Web adapter 会限制为字母、数字、点、下划线和连字符。

处理原则：

- Public API 提取时只承诺明确列出的 DTO 和方法；
- adapter 正在真实使用的参数必须先迁移再删除；
- test-only hook 留在内部入口；
- 收紧输入或统一 HTTP status 需要独立兼容说明和 Contract Test；
- 不得借“清理内部 API”破坏 CLI/Web 当前正常路径。

## 16. vNext 目标 Public API

vNext 目标能力为：

```text
getStatus
prepareSync
applySync
prepareSwitch
applySwitch
prepareRepair
applyRepair
listBackups
prepareRestore
applyRestore
pruneBackups
listHistory
getHistorySession
startWatch
stopWatch
getWatchStatus
getDiagnostics
```

目标原则：

- CLI、Web、Desktop 只从一个 `public-api` 导入；
- Core 不依赖 Electron、React、DOM 或 CLI presenter；
- 所有写操作采用 Plan/Revision/Confirm/Apply；
- Web 已有 revision 保护下沉到 Core；
- Core 返回结构化 Result/Error/Progress；
- CLI human presenter、CLI JSON presenter、HTTP mapper 与 Electron IPC mapper 位于 adapter；
- `--json` 是外部自动化合同，不是 Electron IPC；
- Public API 可以新增 schemaVersion，但迁移适配器必须保持本文中的 v0.5 用户行为。

V1/C3 已实现上述边界；`runSync/runSwitch/runRepair/runRestore/runWatch` 仍作为 CLI 和旧调用方的弃用兼容适配器保留，不得供 Renderer 或新的 HTTP/IPC transport 使用。实现完成不等于已发布：已发布版本的兼容责任持续到对应迁移门槛和最终 PR 合入完成。

### 16.1 C3 Plan / Apply 合同

- `prepareSync/prepareSwitch/prepareRepair/prepareRestore` 返回不可变 `PlanSummary` schema v1。`planId` 为 32-byte 随机不透明标识；TTL 固定 10 分钟；ledger 仅驻留当前进程并单次消费，重启、过期、重放和跨 operation 使用均返回 `PLAN_EXPIRED`。
- Plan ledger 必须按最早 expiry 使用不阻止进程退出的自治 timer 清理弃置计划；不得依赖后续 consume 或新的 Prepare 才回收。人工 Plan intent 同样按每个 Home 的最早 expiry 自治清理并重新 arm，多个 Watch waiter 不得各自创建 10 分钟 timer。
- `applySync/applySwitch/applyRepair/applyRestore` 只接受精确的 `{schemaVersion: 1, planId}`。任何附加路径、Provider、model、backupId 或 mutation 参数都返回 `INVALID_INPUT`，且不消费合法 Plan。
- Apply 在 Codex Home lock 内重新读取可信 Profile、config、rollout inventory、State DB main/WAL/SHM 与 Restore source backup revision；任一漂移统一返回 `STALE_STATE`，且在 Backup/mutation 前停止。
- Web 只公开 `*/prepare` 与 `*/apply`。旧 `/api/sync`、`/api/switch`、`/api/restore` 固定返回 `410 PLAN_REQUIRED`，不得调用兼容 `run*` 写入口。
- Switch Plan 固定表达 `provider-default`、`keep-root-model`、`explicit` 三种 model intent；Apply 不再接收 model 参数。

### 16.2 协调、Status 与 Watch

- 本进程协调器为同一 Codex Home 生成 operationId，并缓存最近一次完整 Status。写操作期间 Status 返回该完整 snapshot 加 `operationInProgress`；无缓存时返回 `rolloutScanComplete:false` 的保守快照。
- Status 不获取写锁，而是只读检查 Home lock，并在扫描前后核对 config/rollout/State DB revision。外部写者活跃或锁不可验证时不得扫描业务中间态；`LOCK_UNVERIFIABLE` 状态不得显示 aligned/healthy。Pending Restore Journal 和旧普通 journal 仍可作为诊断证据读取。
- Status 只读取每个 rollout 的首条 `session_meta` 与文件元数据，仍返回 Provider 分布、SQLite 分布、锁、pending、backup 和 revision 状态；它不扫描 `encrypted_content`、user event 或 `turn_context` 正文。只有显式 Diagnostics 和所选 Repair target 执行所需的更深扫描。
- `ProgressEvent` observer 的异常不能改变 operation result。普通写 OperationResult 为 `completed`、`partial`、mutation 前 `cancelled` 或 `stale`；旧 rolled-back/recovery 结果只作兼容解析。
- Watch 保持单飞、合并重复事件，每次重新 Prepare/Apply 并获取 Home lock。遇到本进程人工操作时保留当前事件批次、等待 operation completion 后只运行一次合并 follow-up；外部 Busy/不可验证锁不轮询、不计入连续业务失败，并等待新的受保护文件事件。
- Diagnostics 只返回有界安全元数据；不得读取、复制或序列化 `auth.json`、token、凭据或消息正文。

### 16.3 C4 Trusted Profile Facade 与 CoreClient

- `packages/core` 的模块导出仅为 `createCoreFacade({resolveProfile})`；factory 返回对象的业务方法集合精确等于本节 15 个目标方法。根 `src/public-api.js` 继续承载 CLI 与迁移适配器，不被描述为 Renderer 稳定 API。
- `resolveProfile({profileId, profileRevision?})` 只能由 Local Web Host、Electron Main/Utility Host 或测试 Host 注入，返回可信的 `{id, revision, codexHome, sqliteHome?}`。Facade 必须验证 ID、revision 和绝对路径；selector revision 漂移时 fail closed，不能回退到 `CODEX_HOME` 或默认用户目录。
- UI/HTTP/IPC 产品输入只包含 profile ID/revision、Provider/model mode、受管 backupId 等产品字段；不得携带 `codexHome`、`sqliteHome`、backup path 或底层 apply 参数。Apply 仍精确只收 schemaVersion/planId。
- Status 在 facade 处移除 `codexHome`、`sqliteHome` 和 State DB 路径，只保留来源枚举、revision、分布与安全状态；warning 只能由固定类别/固定文案投影，不得透传底层任意字符串。
- 备份列表在 facade 处移除 backup root、绝对 path 与 metadata 中的存储路径，只返回 `backupId`、size 和有界展示元数据；History 列表移除 rollout path、`cwd` 与首条消息预览，正文只能由用户明确调用详情方法后读取。
- `TransportCoreClient` 对成功 payload 执行按方法的最小 runtime guard；协议版本不兼容映射为固定 `PROTOCOL_VERSION_MISMATCH`，其他畸形 envelope/result 收口为固定 `INTERNAL_ERROR`。HTTP 非 2xx 不得携带成功 envelope。

### 16.4 C5 Local Web Host 与共享 UI

- `/api/core` 只接受带 `protocolVersion`、`requestId`、可选 `operationId`、`method`、`payload` 的版本化 POST envelope；请求体上限 64 KiB，content type、结构、方法输入和成功输出均由共享 contracts guard 验证。
- 响应必须保留同一 `requestId`。非 2xx 不得伪装成功 envelope；不可信异常只返回固定、安全的 `INTERNAL_ERROR` DTO，不输出 stack、cause、路径、token、消息正文或原始异常文本。
- Web Host 在进入 envelope handler 前验证一次性 pairing、设备凭据 hash 与 loopback Origin；Facade 只解析 server-managed profile ID/revision，Prepare 绑定 storage revisions，Apply 在 Home lock 内重新核对。受管 backupId 在可信 Host/Core 边界解析；Renderer 不能通过 Core 输入提交任意路径。
- React UI 的业务调用固定为 `HttpCoreClient → /api/core → createCoreFacade`；profile 管理、配对和忘记浏览器属于 Host API，不得把业务实现复制进 UI。
- 共享 UI 的 Status、Watch 与 Update 状态只在首次进入时加载，并仅由用户明确点击刷新；不得使用定时器、窗口聚焦或网络重连触发后台刷新。用户明确执行写操作后的受控安全刷新仍属于该操作的完成确认，不视为后台轮询。
- History 列表仅在用户进入 History 页面后读取；详情仅在用户明确选择会话后延迟读取，正文不进入 TanStack Query cache，离开页面时清空并取消 pending detail request。
- Production HTML 使用每响应随机 nonce 的严格 CSP；无 `unsafe-inline`，外部导航、远程脚本和跨源 Core 请求不在允许面内。

### 16.5 C6 Electron Read-only Alpha

- 数据流固定为 `Renderer → DesktopCoreClient → sandboxed Preload → Main IPC/Supervisor → Utility Process → createCoreFacade`。Renderer、Preload 和 Main 都不能导入 Core 实现；Utility 的唯一 Core 业务实现入口是 `@codex-provider-sync/core`，可依赖共享 contracts/client allowlist，但不得深度导入根 `src/`。
- C6 IPC 仅允许 `getStatus`、`listBackups`、`listHistory`、`getHistorySession`、`getDiagnostics`。Sync/Switch/Restore/Prune/Watch 在 DesktopCoreClient、Preload、Main 和 Utility 四层均 fail closed 为 `PERMISSION_DENIED`；协议漂移在业务调用前返回 `PROTOCOL_VERSION_MISMATCH`。
- Preload 公开面固定为 version、`core.requestReadOnly` 与 `profiles.list`，不暴露原始 IPC、Node、路径或通用 channel。production build 不含 test bridge；测试 hook 只能存在于编译期 test build。
- Main 只接受主窗口顶层 `cps-app://app` sender，Core envelope 上限 64 KiB；Profile 列表只返回 `id/name/revision/codexHomeConfigured/sqliteHomeConfigured`，不得返回 Codex/SQLite 路径。
- Runtime Hello 必须同时匹配 runtime/core protocol、app/core version、buildId、随机 nonce、generation 和精确只读 capability。崩溃立即拒绝全部 pending 为 `CORE_RUNTIME_CRASHED`；不后台重启；下一次用户请求每个 profile/revision 都必须先完成 `getStatus` pending-journal preflight，失败不得被后续请求绕过。
- request timeout 必须终止当前 Runtime generation，避免迟到响应与复用 requestId 错误关联；下一次用户请求按 crash restart/preflight 规则处理。shutdown 是终结性、幂等操作，调用前后都拒绝新请求，不能产生孤儿 Utility。response 的 requestId/generation/operationId 及 preflight profile 必须与请求关联。
- History 列表标题只能来自显式 metadata；无标题返回空字符串并由 UI 本地化。消息正文只在用户显式打开详情后返回，离开详情立即清空/abort，不进入 Query cache、日志或 Diagnostics。

### 16.6 C7 Electron Sync / Switch 候选边界

- DesktopCoreClient、Preload、Main IPC、Supervisor 与 Utility 只增加 `prepareSync/applySync/prepareSwitch/applySwitch`，Apply 仍只接收 `{schemaVersion:1, planId}`；Main 持有并一次性消费 renderer sender 绑定的 Plan ownership。
- Renderer 只能提交 profile、Provider 和 `provider-default/keep-root-model/explicit` 三种 model intent；不得提交 Codex/SQLite/backup 路径或底层 apply 参数。自定义 Provider 必须由 Core 从可信 config 验证。
- pending recovery 阻断 Sync/Switch。apply lifecycle 必须以 requestId/operationId 关联进度与取消；Runtime crash/timeout 立即拒绝 pending，下一请求重新 Status preflight。

### 16.7 C8 Electron Restore / Watch / Diagnostics / Update 候选边界

- DesktopCoreClient、Preload、Main IPC、Supervisor 与 Utility 只按精确方法组增加 `prepareRestore/applyRestore`、`pruneBackups/startWatch/stopWatch/getWatchStatus`。Main 持有 Restore Plan 与 Watch ID；Renderer 只提交 profile、受管 backupId、Restore options、keepCount 或有限 Watch 输入。
- Recovery Required 时，Sync/Switch/startWatch 继续阻断；Restore 与 Prune 可作为 recovery-safe 操作进入 Core，stop/get Watch status 仍可用。Restore Apply 属于 cancellable write lifecycle，完成后使 Supervisor 的 Status preflight 失效并重新读取。
- Restore snapshot/journal 持久化 `codexHomePhysical`；pending、resolver 与当前已加锁 Home 必须匹配该稳定物理 identity，不得用可变 lexical 路径的当前 realpath 擦除历史 binding。snapshot manifest 与 durable `prepared` event 必须全量绑定 schema/protocol、operation、source、storage、required kinds、resolver IDs、ordered targets 和 snapshot 物理目录。config、global state 与 rollout 的固定名称、物理 parent、reparse/symlink 边界必须在 snapshot、每目标 apply、补偿与 commit acknowledgement 前反复验证。任一绑定、边界或物理 identity 无法可靠证明时返回 `LOCK_UNVERIFIABLE(codex-home)` 或 `RECOVERY_REQUIRED`，不得读写被换接到 Home 外的目标。无目标 mutation 的取消只能写入验证型 compensation evidence，不得为“回滚”而重写原目标。
- Watch 每次 apply 都重新 Prepare/Apply 并获取 Codex Home 锁；SQLite 竞争由该次原生事务裁决。已 Prepare 的人工 Plan 具有优先级；Watch 合并重复事件并等待人工 intent 释放或过期，只运行一次 follow-up。首次遇到 `RECOVERY_REQUIRED/PENDING_TRANSACTION` 即停止，不继续自动写。同一物理 Codex Home 只能有一个 active/pending Watch；停止后释放 scope，终态历史有界。
- Diagnostics Renderer 请求严格只有 `{schemaVersion:1, profile}`。输出目标由 Main 原生文件选择器产生并转换为 5 分钟、单次消费的随机 capability；最多保留 32 个未消费 capability，同一规范化目标只能被一个 capability 保留，写入前还必须按父目录 realpath 拒绝指向同一物理 ZIP 的并发路径别名。过期、显式 revoke、消费成功或失败都会释放目标 reservation；同 token 并发导出最多一方成功。token 和目标路径不跨 Renderer。ZIP 条目固定且再次执行共享 Diagnostics DTO exact validation，排除 `auth.json`、凭据、token、路径、rollout/DB、消息正文与 `encrypted_content`。
- Update 只由 Main 的 `electron-updater` controller 管理，固定使用打包 metadata 中的 GitHub provider；不得调用 `setFeedURL`，Renderer 不得提交 URL、channel、路径、版本、silent/force 参数或接收 release notes、下载 URL、缓存路径和原始异常。Preload 仅暴露无参数的 `getStatus/check/download/install`，响应为脱敏 schema v2 状态。
- `autoDownload` 与 `autoInstallOnAppQuit` 均关闭。检查、下载或更新错误不得改变 Core 结果。安装意图必须在 Supervisor 内同步关闭 restart gate，将已经入场但尚处于 preflight/dispatch 前的写请求计入 admission，并等待这些请求排空；此后新的 Sync/Switch/Restore/Prune/startWatch 立即返回 busy，只有 `getWatchStatus` 仍为只读。排空后，Main 必须通过既有 Utility `getWatchStatus` 重新核对其持有的 Watch ownership，清除已自动停止的缓存；查询失败或仍有 active Watch 时 fail closed。随后还必须确认 update 已下载、无写操作，并对全部已知 Profile 强制刷新 Status、证明无 pending recovery，最后才可调用 `quitAndInstall`；任一 Profile 无法验证、installer 抛错或安装未启动时均 fail closed 并重新开放 gate。
- C8 只接入受控状态机和门禁。只有 Main 编译期 `releaseAuthorized=true`、packaged、受支持目标且版本/通道已配置时才允许创建 updater port、排定检查或执行任何网络/安装动作；缺省及所有未授权候选固定为 `disabled/not-authorized`。C9 候选显式注入 `releaseAuthorized=false`；签名、Update metadata、跨版本 packaged smoke，以及覆盖外部 CLI/Web/Watch 的跨运行时 maintenance lease 仍属于发布前门禁，未获得发布授权前不得把通道描述为已上线。

### 16.8 C9 Electron 候选产物与 CI 边界

- 候选版本固定为 `1.0.0-alpha|beta|rc.<run>`，buildId 必须绑定完整 commit、target 与 run；source manifest 不因候选构建被改写。所有 builder 调用带 `--publish never`，候选 manifest 固定 `releaseAuthorized:false`、`signingStatus:unsigned-candidate`、`notarizationStatus:not-authorized`。
- 目标集合恰为 Windows x64 NSIS/portable ZIP、macOS x64/arm64 DMG/ZIP、Linux x64 AppImage/deb。每个目标必须由同架构 host-native runner 构建；不得 cross-build native SQLite 后冒充实机证据。
- Electron 优先使用 `node:sqlite`；`better-sqlite3` 作为 production fallback 必须针对当前 Electron ABI 验证加载。ASAR 只能引用当前 target 的一个 native binding，且该 binding 是 `app.asar.unpacked` 中唯一文件；其它平台 prebuild、source、build/deps 不得进入包。
- 每个最终容器都必须实际解包或安装，并与 staging audit 逐字段一致；审计覆盖 ASAR 全文件/block hash、embedded header binding、fuse wire、敏感路径/文件/高置信 token、fixture/test/source map、native binding 与 production buildId。Windows NSIS 还必须完成静默卸载清理。
- packaged smoke 只使用临时 synthetic fixture，以隐藏窗口启动正式 executable，验证 production test bridge 不存在、真实 SQLite Status、Sync→Restore byte/hash 回环与正常退出；不得访问真实 Codex Home、`auth.json`、凭据或消息正文。
- 每个目标输出 CycloneDX SBOM、最终容器报告、release manifest 和 `SHA256SUMS.txt`。checksum 必须精确覆盖所有资产与 metadata；aggregate 必须证明四目标 version/commit/lockfile/tool versions/fuse policy/audit policy 一致，且任一 matrix job 失败、取消或跳过都使唯一 `ci-gate` 失败。
- 推送 tag 不得自动发布。旧发布工作流改为显式 `workflow_dispatch` 并要求既有 `v` 前缀 tag 位于 `main`；这只是发布授权后的入口，不表示当前已获 tag、npm/GitHub Release、签名、公证或更新通道授权。

## 17. Phase 1 提取要求

Phase 1 只提取边界，不改变算法或结果：

1. 新增唯一 `public-api`；
2. 包装当前 `service.js` 能力；
3. 把 `listBackups`、History 和 Watch 纳入公开边界；
4. CLI 与 Web 改为只从 public-api 导入；
5. `renderStatus` 和 CLI 文本格式留在 CLI adapter；
6. Web DTO、alignment 与 HTTP status 映射留在 Web adapter；
7. 保持所有现有测试通过；
8. 增加 Public API 导出快照和结果字段 Contract Test；
9. 通过 Storage/Runtime 端口迁移备份、锁、rollout 与 SQLite 能力，避免业务用例反向依赖 Facade；
10. 不删除 .NET 实现；.NET 继续作为 Legacy，普通 Node 写入模型不要求同步重构 .NET 业务能力。

## 18. 最低 Contract Test 清单

### 18.1 Status

- implicit `openai`；
- configured Provider 列表；
- default 与 legacy state DB 选择；
- configured SQLite Home 缺 DB；
- WSL UNC 诊断；
- malformed/busy SQLite 降级；
- locked rollout 降级；
- pending transaction 与项目可见性字段。

### 18.2 Sync/Switch

- backup-first；
- noop 不创建备份；
- SQLite busy 时 rollout 不变；
- locked rollout partial；
- config/storage revision 变化阻断；
- result 字段和 progress stage；
- observer 失败不影响事务；
- Provider Sync 不打开正文；等长更新保留文件身份/大小/正文 Hash，不等长更新保持正文 bytes；
- 模型、cwd、user-event、workspace roots 与加密内容不被 Sync/Switch 修改；
- mutation 前故障零业务写入，mutation 后故障为带 UndoBackup 的 partial，重复执行可收敛；
- default/custom keep 自动清理与 warning 降级；
- switch Provider/model 三策略和未知 Provider；
- Diagnostics 完整只读扫描一次；Repair 逐目标隔离、组合目标、workspaceRoots 隐含 cwd、缺失根模型和单 SQLite 事务。

### 18.3 Restore/Prune

- v1/v2 backup；
- Codex Home 与 SQLite Home 绑定；
- relocation 显式确认；
- relocation 禁止 config restore；
- manifest path boundary；
- pending transaction 完整恢复；
- pending backup 不被 prune；
- 非托管目录不被删除。

### 18.4 Web

- 未配对 `/api/core`、一次性 pairing、Origin 和 device credential hash；
- 非 JSON content type、超过 64 KiB、畸形/版本不兼容 envelope 与未知 method；
- requestId correlation、非 2xx success 拒绝、Core/Host error 固定脱敏；
- raw storage path 拒绝；
- profile/storage revision required/changed；
- 单写操作；
- managed backupId；
- success/partial mapping；
- alignment 不要求总数相等；
- runtime storage identity 与安全复用；
- History 列表/详情必须显式读取，详情正文不缓存且离页清空；
- production CSP nonce、八个共享页面、双语/主题、键盘焦点、reduced motion 与 200% 等效窄视口。

## 19. 变更控制

修改以下任一内容，必须同时更新本文、相关 ADR、Contract Test 和发布说明：

- Public API 方法或 DTO；
- Core 结果字段；
- progress stage 或顺序；
- Storage Profile/Revision 语义；
- SQLite Home 优先级或 DB 选择；
- 锁路径、协议、持有周期或 stale reclaim；
- backup metadata、journal 或补偿语义；
- partial、busy、recovery、cancel 分类；
- Web revision、pairing 或回环边界；
- History 可读取的数据范围；
- auth/message body 安全边界。
