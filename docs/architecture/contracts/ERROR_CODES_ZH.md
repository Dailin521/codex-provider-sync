# vNext Error Code 合同

> **状态：Accepted（阶段 0 合同；ADR-0016 C2/C3 轻量写增量已实施）**
>
> **日期：2026-08-24；当前实现增量：2026-09-03**
>
> **适用范围：Node Core、CLI、Local Web UI、Electron 与迁移期 .NET 适配层**
>
> **架构基线：[vNext Electron + Node 单核心架构](../../VNEXT_ELECTRON_NODE_ARCHITECTURE_ZH.md)**

## 1. 目的

V1/ADR-0016 增量（尚未公开发布）：`FAST_MODE_UNSUPPORTED` 与 `fastModeReason` 已随公开 Fast 模式删除。新的 Provider Sync 自动在等长原地更新与不等长流式替换之间选择；普通 Sync/Switch/Repair 不再产生 `SYNC_FAILED_ROLLED_BACK` 或普通写 `RECOVERY_REQUIRED`，mutation 后失败以结构化 partial Result 表达。旧码继续可读，仅用于 v0.5 backup/journal 兼容；Restore 仍可产生 `RECOVERY_REQUIRED`。

本文冻结 vNext 的错误分类、兼容映射和演进规则，使调用方依据稳定的 `code` 决策，而不是解析自然语言 `message`、异常类型名或堆栈。

V1 的 Node Public API 已实现 Canonical `CoreError`/DTO，CLI JSON Adapter 与 C5 Web Core envelope 已按 Canonical Code 输出；Web 的非 Core Host transport code 与迁移期 .NET 仍存在不同大小写、命名和结构，这些现状继续列为 Legacy Surface，并由后续 Adapter 渐进收口。

## 2. 稳定边界

vNext Core 对外错误 DTO 采用以下语义：

```ts
interface CoreErrorDto {
  code: CoreErrorCode;
  message: string;
  severity: "info" | "warning" | "error" | "fatal";
  retryable: boolean;
  recoveryRequired: boolean;
  operationId?: string;
  details?: Record<string, unknown>;
}
```

稳定性规则：

- `code` 是程序判断依据；Canonical Code 使用大写蛇形命名。
- C4 CoreClient/HTTP/IPC 公共边界的 `message` 按 code 使用固定安全文案；UI 本地化只依据 code，不能回显内部异常原文。Core 内部异常仍可携带操作建议，但 `suggestedAction` 不进入公共 DTO。
- 公共 `details` 只允许 `busyScope`、`lockScope`、`causeCode`、`reason`、`missing`、`sqliteHomeSource`、SQLite 整数错误码和 `operationKind` 的固定枚举/范围；未知 key、路径、认证信息、Token、消息正文或任意建议文本全部丢弃。
- 公共 `operationId` 只接受 UUID；不可信值不得透传。
- 普通用户默认不接收 Error Stack；诊断日志也必须遵守相同的隐私边界。
- 一个失败只选择一个最能指导恢复动作的顶层 Canonical Code；底层 OS/SQLite Code 可放入安全的 `details.causeCode`。
- Partial Result 可以携带 warning 级错误码，但不能把未完成写入伪装成完整成功。

普通写的 partial 使用固定安全字段：`partialReason=locked-session|rollout-changed|mutation-failed`。`skippedLockedRolloutFiles` 与 `skippedChangedRolloutFiles` 分别列出活动会话锁定和 Apply 期间漂移的 rollout；两者同时存在时主原因固定为 `locked-session`，mutation 后失败优先为 `mutation-failed`。所有 partial 都携带 `retryRecommended=true`；mutation 后失败还携带枚举化 `failedStage`、`failureCode` 与可用于手动 Restore 的受管 `backupId`。这些字段不得包含路径或底层异常文本。

## 3. Canonical vNext CoreErrorCode

### 3.1 正式集合

| Code | 触发语义 | Severity | Retryable | Recovery Required |
| --- | --- | --- | --- | --- |
| `INVALID_INPUT` | 输入缺失、互斥参数、格式或范围无效 | error | 修改输入后是 | 否 |
| `PROFILE_CHANGED` | 已确认的 Profile Revision 已变化 | warning | 重新读取并确认后是 | 否 |
| `STORAGE_CHANGED` | 已确认的存储解析结果或 Revision 已变化 | warning | 重新准备 Plan 后是 | 否 |
| `PLAN_STALE` | 现有/过渡入口用于表示 Plan 绑定的文件、目标或指纹已变化；vNext Prepare/Apply 统一映射为 `STALE_STATE` | warning | 重新准备 Plan 后是 | 否 |
| `CODEX_HOME_NOT_FOUND` | Codex Home 不存在或不可解析 | error | 修正路径后是 | 否 |
| `STATE_DB_NOT_FOUND` | 权威 SQLite Home 中缺少要求存在的 `state_5.sqlite` | error | 修复目标后是 | 否 |
| `SQLITE_UNSUPPORTED_PATH` | 当前平台不能安全访问该 SQLite 路径，例如 Windows WSL UNC | error | 更换执行环境或路径后是 | 否 |
| `SQLITE_BUSY` | SQLite 被其他进程占用，无法安全获得写能力 | warning | 关闭占用方后是 | 否 |
| `SQLITE_UNREADABLE` | SQLite 损坏、格式不受支持或无法可靠读取 | error | 修复或恢复数据库后是 | 否 |
| `ROLLOUT_LOCKED` | rollout 被活动进程锁定；可形成 Partial Result | warning | 活动会话结束后是 | 否 |
| `ROLLOUT_CHANGED` | rollout 在扫描与应用之间变化，当前写入被跳过或 Plan 失效 | warning | 重新扫描后是 | 否 |
| `PENDING_TRANSACTION` | 发现未终结 Restore Journal，新普通写必须停止；旧普通 journal 只诊断 | error | 完成显式恢复后是 | 是 |
| `BACKUP_FAILED` | 备份未完成；mutation boundary 尚未跨越 | error | 修复空间或权限后是 | 否 |
| `SYNC_FAILED_ROLLED_BACK` | v0.5/旧普通写兼容：同步失败且自动回滚完成 | error | 修复原始原因后是 | 否 |
| `RECOVERY_REQUIRED` | Restore 补偿不完整或状态无法证明一致；旧普通 journal 兼容读取 | error | 完成显式恢复后是 | 是 |
| `RESTORE_VALIDATION_FAILED` | 备份、清单、目标边界或 relocation 校验失败 | error | 修复选择或备份后是 | 否 |
| `PERMISSION_DENIED` | 文件、目录、锁或数据库权限不足 | error | 修复权限后是 | 否 |
| `OPERATION_BUSY` | 已证明存在活跃的冲突操作或同一目标写者 | warning | 活跃操作结束后是 | 否 |
| `OPERATION_CANCELLED` | 用户在首次 mutation 前取消普通操作，或 Restore 在安全点取消 | info | 是 | 仅 Restore 另有 Journal 证据时是 |
| `CORE_RUNTIME_CRASHED` | Electron Core Runtime 非正常退出 | fatal | Runtime 可重启时是 | 由 Journal 检查决定 |
| `PROTOCOL_VERSION_MISMATCH` | 调用方与 Core/IPC Schema 不兼容 | error | 升级兼容组件后是 | 否 |
| `INTERNAL_ERROR` | 未被更具体类别覆盖的内部错误 | fatal | 默认否 | 由 Journal 检查决定 |

### 3.2 阶段 0 补充项

以下代码补充了架构基线已经描述、但初始错误码列表没有单独命名的语义。它们属于 vNext Canonical Code；V1/C1～C3 的 Node 边界及迁移期 .NET 锁入口已经统一发出这些代码，但尚未作为已发布版本合同对外宣称。

| Code | 补充原因 | Severity | Retryable | Recovery Required |
| --- | --- | --- | --- | --- |
| `PLAN_EXPIRED` | Plan 已有明确的过期时间和重新准备动作；它与状态漂移导致的 `STALE_STATE` 不同 | warning | 重新准备 Plan 后是 | 否 |
| `STALE_STATE` | Apply 加锁后发现 profile/config/rollout/state DB 或 storage revision 与 Plan 不一致 | warning | 重新准备 Plan 后是 | 否 |
| `LOCK_UNVERIFIABLE` | 无法可靠验证锁所有者、进程启动身份、协议版本或锁目录身份；不能误判为普通 Busy，也不能冒险删除 | error | 消除不确定状态后是 | 否 |

`LOCK_UNVERIFIABLE` 必须 fail closed。只有确认存在活跃冲突所有者时才使用 `OPERATION_BUSY`；未来协议、损坏 owner、身份读取失败或 ABA/目录身份不确定均使用 `LOCK_UNVERIFIABLE`。用户提示不得建议盲目删除锁目录。

V1/C3 的普通写只使用 Codex Home lock，因此新结果的 `OPERATION_BUSY.details.busyScope` 与 `LOCK_UNVERIFIABLE.details.lockScope` 为 `codex-home`。共享数据库竞争由 SQLite 原生事务返回 `SQLITE_BUSY`。旧 `state-db` scope 与 protocol v2 `scope/resourceKey` 只作既有锁/证据兼容读取。

vNext Prepare/Apply 对任何加锁后 revision 漂移只发出 `STALE_STATE`，并可用安全的 `details.reason=profile|config|storage|rollout|state-db` 说明维度。`PLAN_STALE`、`PROFILE_CHANGED` 与 `STORAGE_CHANGED` 在旧 Web/适配器完成迁移前仍可读取，但不得成为新 CoreClient/IPC 的稳定写操作结果。

ADR-0013 的 Restore v2 实现后，非 terminal restore operation journal 使用 `PENDING_TRANSACTION` 或 `RECOVERY_REQUIRED`；`details` 可安全包含 `operationKind=restore`、operationId、source backup identity、pre-restore snapshot identity 和 completed/uncompleted target 摘要，但不得包含消息正文、认证信息或任意原始路径。

## 4. Legacy Surface 现状

### 4.1 Node Core / CLI

| 当前标识 | 当前来源 | 说明 |
| --- | --- | --- |
| `RECOVERY_REQUIRED` | `RecoveryRequiredError`、部分 Restore 校验 | 已与 Canonical 同名 |
| `SYNC_FAILED_ROLLED_BACK` | `SyncTransactionError` | 已与 Canonical 同名 |
| `ABORT_ERR` | Node 取消路径 | Node 习惯码，不作为 vNext Canonical Code |
| Node/OS `ENOENT`、`EACCES`、`EPERM` 等 | 普通 `Error` 或系统调用 | 只能作为底层 cause；当前大量错误还没有稳定业务码 |
| `OPERATION_BUSY` / `LOCK_UNVERIFIABLE` | Node Home lock；旧 State DB resource lock 兼容读取 | 新普通写只发出 `codex-home` scope；禁止解析 message |

当前 CLI 只把错误 `message` 写入 stderr 并以 `1` 退出。本文不把当前人类提示提升为机器协议。

### 4.2 Local Web UI

| 当前标识 | 分类 | vNext 处理 |
| --- | --- | --- |
| `PROFILE_CHANGED` | 业务并发/Revision | Canonical 同名 |
| `STORAGE_CHANGED` | 业务并发/Revision | Canonical 同名 |
| `PROFILE_REVISION_REQUIRED` | HTTP Profile 前置条件 | 保留为 Web Transport/Precondition Code，不加入 CoreErrorCode |
| `STORAGE_REVISION_REQUIRED` | HTTP Storage 前置条件 | 保留为 Web Transport/Precondition Code，不加入 CoreErrorCode |
| `PAIRING_REQUIRED` | Web 认证与配对 | Web Transport Code，不映射为 Core 错误 |
| `INVALID_ORIGIN` | Web Origin 防护 | Web Transport Code，不映射为 Core 错误 |
| `INTERNAL_CHALLENGE_REQUIRED`、`INVALID_INTERNAL_CHALLENGE`、`INVALID_INTERNAL_PROOF` | Web 内部配对握手 | 内部 Transport Code，不得泄漏握手材料 |

### 4.3 .NET Application / Automation 0.4

.NET Application 和实验性 Automation 0.4 主要使用小写蛇形码，例如：

- `operation_busy`、`target_busy`；
- `plan_stale`、`plan_expired`；
- `cancelled`、`recovery_required`、`sync_failed_rolled_back`；
- `operation_failed`；
- `request_required`、`provider_required`、`retention_invalid` 等输入校验码。

.NET Core Lock 还使用过 `TARGET_BUSY`。Automation protocol `0.4` 是实验性协议；其 casing、退出码和 Schema 不是 vNext Core/IPC/CLI JSON 的稳定合同。

## 5. Adapter 映射

| Legacy Code / 状态 | Canonical Code | 映射要求 |
| --- | --- | --- |
| `RECOVERY_REQUIRED`、`recovery_required` | `RECOVERY_REQUIRED` | 保留备份路径、operationId 与恢复要求 |
| `SYNC_FAILED_ROLLED_BACK`、`sync_failed_rolled_back` | `SYNC_FAILED_ROLLED_BACK` | `recoveryRequired=false`，保留 rollback status |
| `ABORT_ERR`、`cancelled` | `OPERATION_CANCELLED` | 映射前检查 Journal；不能仅凭取消认定无需恢复 |
| `TARGET_BUSY`、`target_busy`、`operation_busy` | `OPERATION_BUSY` | 仅适用于已证明存在活跃竞争者；新普通写使用 `details.busyScope=codex-home` |
| 锁 owner/协议/进程身份无法验证，未来协议或目录身份不确定 | `LOCK_UNVERIFIABLE` | 必须 fail closed；新普通写使用 `details.lockScope=codex-home`；不得降级为 Busy 或自动清锁 |
| `plan_stale`、`PLAN_STALE` | `STALE_STATE` | Prepare/Apply 调用方必须丢弃旧 Plan；旧直连 Web 入口迁移前可保留原码 |
| `plan_expired` | `PLAN_EXPIRED` | 调用方必须重新生成 Plan |
| `PROFILE_CHANGED` | `STALE_STATE` | Prepare/Apply 使用 `details.reason=profile`；旧直连 Web 入口迁移前可保留原码 |
| `STORAGE_CHANGED` | `STALE_STATE` | Prepare/Apply 使用 `details.reason=storage`；旧直连 Web 入口迁移前可保留原码 |
| `operation_failed` | 优先映射具体码，否则 `INTERNAL_ERROR` | 禁止把所有失败永久折叠为 `INTERNAL_ERROR` |
| Node/OS `ENOENT` | 由操作上下文映射 | Codex Home、State DB、Backup 的缺失必须分别分类 |
| Node/OS `EACCES`、`EPERM` | `PERMISSION_DENIED` | 原始系统码可放入安全 details |
| SQLite busy/locked | `SQLITE_BUSY` | 不依赖英文 message；使用驱动错误码或 typed cause |

Adapter 必须按 typed exception、明确属性和调用上下文映射。禁止使用 `message.includes(...)` 作为稳定分类方案。

## 6. 各入口展示合同

### Core / Core Runtime

- 返回 Canonical Code 和稳定 DTO。
- CoreClient transport 会重新验证固定文案、severity/retryable/recoveryRequired、UUID 与 details 白名单；畸形错误或成功 payload 统一收口为安全 `INTERNAL_ERROR`，protocolVersion 不兼容单独为 `PROTOCOL_VERSION_MISMATCH`。
- Runtime Crash 与业务失败分开；重启后首先检查 Pending Journal。
- Progress Event 失败不能替换最终业务错误。

### CLI

- 当前 Human Mode 保留现有人类提示兼容性。
- C2 `--json` 只输出 Canonical Code；stdout 为单一 schema v1 JSON 文档，日志和进度进入 stderr。
- CLI Exit Code 与 Error Code 是两层合同，不一一等同。
- CLI JSON presenter 不透传 Core/系统原始 message：每个 Canonical Code 使用固定安全文案，details 使用枚举/格式 allowlist，suggestedAction 不直接透传，operationId 只接受 UUID。

JSON Mode 的当前映射为：

| Exit Code | Canonical Code / Result |
| --- | --- |
| `0` | completed 或 noop |
| `1` | 其他普通失败，包括 `SYNC_FAILED_ROLLED_BACK` |
| `2` | `INVALID_INPUT`、`PLAN_EXPIRED`、`PLAN_STALE`、`STALE_STATE` 及迁移期 revision 漂移码 |
| `3` | partial Result |
| `4` | `RECOVERY_REQUIRED`、`PENDING_TRANSACTION`，或 DTO 标记 `recoveryRequired:true` |
| `5` | `OPERATION_BUSY`、`SQLITE_BUSY`、`LOCK_UNVERIFIABLE` |
| `130` | `OPERATION_CANCELLED` |

未知或不可信异常必须收口为稳定的 `INTERNAL_ERROR` 文案；CLI JSON 不回显 stack、cause 或任意未知异常 message。Human Mode 的既有 `0/1` 行为不随该表改变。

### Local Web UI

- Core 业务失败映射 Canonical Code。
- Pairing、Origin、Profile Revision Required 等仍属于 Web Transport Code。
- 已发布的 Web Code 不因 Core 收口而被静默删除。

### 迁移期 .NET

- Adapter 将小写 Legacy Code 映射为 Canonical Code，原协议 0.4 可继续输出旧码直到其兼容窗口结束。
- 对同一 Fixture 的 Node/.NET 差异必须登记并裁决，不能按字符串相似度自动合并。

## 7. 变更规则

- 新增 Canonical Code：更新本文、Contract Test 与至少一个行为 Fixture。
- 删除或合并 Canonical Code：属于外部合同变更，必须有独立 ADR 和迁移说明。
- 修改 `message`：通常不是破坏性变更，但不得改变 code 的恢复语义。
- 修改 `retryable` 或 `recoveryRequired`：视为合同变更，必须补充故障注入测试。
- Legacy Adapter 可新增映射；不得让已知 Legacy Code 回退为字符串解析。

## 8. 阶段实现边界

阶段 0 冻结分类与映射；V1 C1 已实现 Node 异常类和 DTO，C2 已实现 opt-in CLI JSON/退出码与真实子进程 Contract Test，C4 已实现 Contracts/CoreClient 的公共错误净化与 runtime guard。V1 C5 的 React Web Core 业务请求已统一经 `HttpCoreClient → POST /api/core` 的 versioned envelope；Host 在调用 Core 前继续执行 pairing、loopback Origin 和可信 profile revision 校验，`PROFILE_REVISION_REQUIRED` / `STORAGE_REVISION_REQUIRED` 等仍是 Web transport code。未知 Host/Core 异常收敛为固定 `INTERNAL_ERROR`，不回显原始 exception。迁移期 .NET protocol 0.4 仍保持旧码兼容，因此尚不能据此声称所有入口的错误合同已完全统一。
