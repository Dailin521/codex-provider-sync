# Node Core 外部行为兼容合同

> 状态：Phase 0 兼容基线
>
> 基线版本：`@dailin521/codex-provider-sync` v0.5.0
>
> 冻结日期：2026-08-24
>
> 适用范围：当前 Node service、CLI/Web adapter 依赖及 vNext Core 提取

## 1. 文档目的

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

### 3.4 vNext 目标：双层资源锁（未实现）

v0.5 当前以 Codex Home operation lock 为现状事实；它尚未实现共享 State DB 的独立资源锁。vNext 的目标合同由 [ADR-0012](../../adr/0012-dual-resource-lock-contract.md) 冻结：会修改 SQLite 的操作在同一 Codex Home lock 之外还必须持有按物理 DB identity 计算的 State DB resource lock，并以固定顺序在锁内重新校验 storage、Revision、pending journal 和可写性。

该目标不改变本节的 storage 优先级，也不授权当前入口扩展写入。双层 lock 的 Node/.NET 兼容、owner metadata、Busy/不可验证错误和 Hash 无副作用证据属于后续实现与 Fixture 门槛。

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
provider?
expectedConfigText?
keepCount = 5
model = null
onProgress?
platform?
signal?
```

`configBackupText` 是 `runSwitch` 编排时使用的内部输入，不应由普通外部调用者构造。

### 5.2 目标 Provider 与 model

Provider 选择顺序：

1. 显式 `provider`；
2. 当前根级 `model_provider`；
3. `openai`。

`runSync` 自身默认 `model=null`，表示不要求重写每线程 model。当前 CLI 与 Web 会读取根级 model 后显式传入；提取 Public API 时必须保留“入口使用根级 model 同步线程”的现有用户行为。

### 5.3 前置条件

- `keepCount` 必须是整数且 `>=1`；
- Codex Home 与 `config.toml` 必须可读；
- 获取同一 Codex Home 的 provider-sync 锁；
- 不存在未完成 provider-sync transaction；
- `expectedConfigText` 提供时，磁盘 config 必须完全一致；
- Windows WSL UNC storage 不支持 sync；
- 非 default SQLite Home 必须存在目标 `state_5.sqlite`；
- SQLite 必须在 rollout mutation 和备份之前通过可写性检查。

### 5.4 写入流程与安全顺序

以下顺序属于兼容安全合同：

1. 获取锁；
2. 检查 pending transaction；
3. 读取并校验 config；
4. 解析唯一 storage/state DB；
5. 扫描 rollout 与修复信息；
6. 识别不可写或读取锁定的 rollout；
7. 验证 SQLite 可写；
8. 创建不可变原始备份；
9. 创建 transaction journal；
10. 在受控事务中应用 rollout、global state/workspace roots 与 SQLite 更新；
11. 持久化 committed terminal；
12. 刷新 backup inventory；
13. 自动清理旧托管备份；
14. 释放锁。

不得把 rollout mutation 移到 SQLite 可写检查或备份之前。

### 5.5 部分成功

- 启动/活动进程锁定的 rollout 可以被跳过；
- 可写 rollout、SQLite provider、user-event、cwd 和 workspace roots 仍可提交；
- 所有跳过路径必须出现在 `skippedLockedRolloutFiles`；
- v0.5 service 以正常结果表示 partial，不抛出异常；
- Web adapter 根据该数组非空添加 `outcome="partial"`；
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
sqliteUserEventRowsUpdated
sqliteCwdRowsUpdated
updatedWorkspaceRoots
savedWorkspaceRootCount
sqlitePresent
rolloutCountsBefore
encryptedContentCounts
encryptedContentWarning
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
update_sqlite
rewrite_rollout_files
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
- 最终根级 model 用于本次线程 model 同步。

### 6.3 事务行为

- 在修改 config 前完成备份；
- 备份保存切换前的原始 config；
- config mutation 纳入 transaction journal 与补偿；
- 后续写入失败时恢复原始 config；
- 额外进度 stage 为 `update_config`，发生在 `create_backup complete` 之后；
- config 在等待锁期间被其他进程改变时必须失败，而不是覆盖新内容。

### 6.4 结果扩展

`runSwitch` 返回完整 SyncResult，并增加：

```text
configUpdated = true
modelSync.applied
modelSync.source = explicit | provider-section | none
modelSync.model
modelSync.warning
```

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

### 7.3 当前 Restore 安全债

`runRestore` 当前会获取正式 operation lock，也会校验被选中备份及其 journal，但**恢复操作自身不会先创建新的恢复前备份，也没有独立的 restore transaction journal**。因此，若 restore 在 config、global state、SQLite 或 rollout 已部分落盘后发生进程崩溃，当前实现没有与 sync/switch 同等级的自动补偿证据。

这属于 v0.5 已知安全债，不是允许 vNext 继续保留的目标合同：

- Phase 0 必须如实记录，不能宣称 restore 已具备完整事务性；
- 在补齐 restore backup/journal 前，不得扩大 restore 自动化或无人值守使用范围；
- vNext 必须为 restore 建立恢复前 snapshot、目标清单、持久 journal 和崩溃恢复测试；
- 当前恢复目标已落盘但 rolledBack terminal 标记失败时，调用方只会收到失败，不能据此证明目标未恢复或 Journal 已收敛；vNext 必须增加 commit/terminal acknowledgement reconciliation；
- 补齐安全机制时必须继续兼容 v1/v2 旧备份格式和现有 restore 选项。

### 7.4 vNext Restore v2 目标（未实现）

[ADR-0013](../../adr/0013-restore-v2-recovery-state-machine.md) 冻结 Restore v2 的目标：在任何 restore mutation 前创建独立的恢复前 snapshot，并用独立 restore operation journal 记录 source backup、目标清单、每目标状态和 durable terminal。`prepared/applying/committing/rollback-pending` crash 必须由显式 recovery 依据该 snapshot 补偿；`committed-pending-ack` 必须按目标 Hash 前进到 `completed` 或 `recovery-required`，不得对已提交目标启动反向补偿。

当前 `runRestore` 仍是 7.3 所述 v0.5 行为。source backup v1/v2、现有 restore options、relocation 双重授权和 foreign pending 处理必须保持兼容；未知 journal/schema 必须 fail closed，不能依赖 message 推断结果。

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
firstUserMessage
```

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

## 10. 跨进程锁与恢复状态

### 10.1 锁位置

当前同一 Codex Home 的写操作使用：

```text
<Codex Home>/tmp/provider-sync.lock
```

这是正式 Core operation lock。当前协议为 protocol v2，并使用 canonical lock directory、`owner.json`、独立 claims 目录、不可变 instance identity 和有界 stale reclaim。锁 owner 记录 Node/.NET 跨运行时识别所需的 PID、进程启动身份、instance id、label 和工作目录。

.NET GUI 的 SingleInstanceGuard 只负责阻止同一 GUI 重复启动，**不是 Core operation lock**，不能用于证明 CLI、Web、旧 GUI 或 Electron 之间互斥。未来 Electron 的应用单实例锁同样只能负责 UX，不能替代上述 storage operation lock。

### 10.2 必须冻结的语义

- sync、switch、restore、prune 在完整操作周期持锁；
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

### 10.4 跨 Codex Home 共享 SQLite Home 盲区

正式 operation lock 当前按 Codex Home 定位。因此，两个不同 Codex Home 如果显式解析到同一个 SQLite Home/同一个 `state_5.sqlite`，会获得不同的 operation lock，当前实现不能阻止它们并发修改同一数据库。

该行为是已知并发盲区，不是允许并发的合同。ADR-0012 已冻结 vNext 选择：对规范化后的 SQLite 资源增加跨 Codex Home 的 resource lock；在该合同完成真实跨运行时验证前，任何入口仍不得宣称多 Profile/shared SQLite 写入安全。

在此问题解决前，测试和文档不得把“不同 Profile/Codex Home”直接等同于“不同存储资源”。

### 10.5 pending transaction

- 未持久化 terminal 的 journal 表示需要恢复；
- status 必须报告；
- sync/switch 必须阻止新写；
- prune 必须保护绑定备份；
- restore 必须覆盖 journal 已开始或无法安全排除的所有目标；
- 完成恢复后持久化 rolledBack terminal。

当前 Node 与 .NET 对“选择的恢复备份之外仍存在其他 pending transaction”的处理尚未形成经过跨运行时测试证明的一致合同。Node restore 主要检查并标记所选 backup 内绑定的 journal；这不能自动证明另一个 foreign pending 已被解决。

vNext 统一规则必须是：

- restore 只解决与所选备份明确绑定且恢复覆盖完整的 transaction；
- 其他 foreign pending 保持 recovery blocker，不得被顺带清除或忽略；
- Node/.NET 迁移期需要增加同一 Codex Home 下多 pending/foreign backup 的交叉 Contract Test；
- 在测试证明一致之前，不得用任一运行时的当前行为替代正式合同。

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

`SYNC_FAILED_ROLLED_BACK` 表示观察到的变更已经自动恢复，不需要手工恢复。`RECOVERY_REQUIRED` 表示补偿不完整，必须保留备份和 journal 证据。

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

如果取消发生在已经开始写入的事务中，最终可能经过补偿后表现为 `SyncTransactionError`。v0.5 尚未保证单一的最终取消 code。

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

- 只同步会话可见性所需的 Provider/model、SQLite flags/cwd 和 workspace root 元数据；
- 不读取、复制、记录或修改 `auth.json`、token 或凭据；
- 不修改消息正文；
- History 仅在用户显式打开只读页面时抽取安全的用户/助手文本；
- 不修改 thread `updated_at`，不通过重排历史制造可见性；
- 不承诺跨 Provider 解密 `encrypted_content`；
- 同步前备份；
- SQLite busy 时在 rollout mutation 前停止；
- 保留 transaction journal 与自动补偿；
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
listBackups
prepareRestore
applyRestore
pruneBackups
listHistory
getHistorySession
startWatch
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

这些目标在对应实现和测试完成前，不得用来否认当前一次性 `runSync/runSwitch/runRestore` 接口的兼容责任。

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
9. 不移动备份、锁、journal、rollout 或 SQLite 算法；
10. 不删除 .NET 实现或改变跨运行时锁。

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
- SQLite busy 时 rollout 不变；
- locked rollout partial；
- config/storage revision 变化阻断；
- result 字段和 progress stage；
- observer 失败不影响事务；
- config、rollout、SQLite、global state 的完整补偿；
- rolled-back 与 recovery-required 两类 SyncTransactionError；
- default/custom keep 自动清理与 warning 降级；
- switch Provider/model 三策略和未知 Provider。

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

- pairing、Origin 和 device credential；
- raw storage path 拒绝；
- profile/storage revision required/changed；
- 单写操作；
- managed backupId；
- success/partial mapping；
- alignment 不要求总数相等；
- runtime storage identity 与安全复用；
- History 只读与分页边界。

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
