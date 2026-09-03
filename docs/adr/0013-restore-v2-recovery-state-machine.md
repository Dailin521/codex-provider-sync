# vNext/ADR-0013：Restore v2 的恢复前快照与独立 Journal 状态机

- Status: Accepted
- Date: 2026-08-25
- Scope: vNext Restore contract; implemented by the V1/C8 candidate, not released until the final PR gates and merge complete

## Context

v0.5 Restore 会校验被选中的受管 backup 与已有 journal，但不会在恢复前创建新的 snapshot，也没有独立的 restore journal。若 config、global state、SQLite 或 rollout 已部分替换后进程崩溃，不能以与 Sync 相同的证据自动补偿。该已知安全债已在 Core 外部行为合同中记录。

## Decision

Restore v2 在任何目标 mutation 前，必须在目标 Codex Home 的 managed backup root 创建一个**恢复前 snapshot**，并为这一次 Restore 创建独立、持久、追加式的 restore operation journal。该 snapshot 记录恢复前的允许目标、SQLite Home/DB identity、source backup identity、manifest hash、目标清单和 hashes；它不是对用户所选 source backup 的就地修改，也不改变 source backup v1/v2 的可恢复格式。

Restore v2 先校验 source backup、relocation 授权和目标边界，再取得 ADR-0012 所定义的资源锁；在锁内重新验证 preflight snapshot、storage identity、pending journal 和可写性，随后才创建恢复前 snapshot 和 journal。任何 snapshot 失败必须在 mutation boundary 前以 `BACKUP_FAILED` 或更具体错误失败。

## Restore Journal State Machine

每个 Restore v2 journal 具有独立 `operationId`、`operationKind=restore`、source backup identity、pre-restore snapshot identity 和 schema/protocol version。其持久状态只允许：

```text
prepared
  -> applying
  -> committing
  -> committed-pending-ack
  -> completed

prepared | applying | committing
  -> rollback-pending
  -> rolled-back
  or recovery-required
```

`completed`、`rolled-back` 与 `recovery-required` 是耐久终态；其中 `recovery-required` 仍是 write blocker。`committed-pending-ack` 表示目标内容已经提交，但调用方尚未完成终态确认：恢复流程必须重新读取 journal、核对 operationId、目标 manifest 与 hashes，然后只允许前进到 `completed`；不得在该窗口对已提交目标启动补偿。若无法证明目标与 manifest 一致，则进入 `recovery-required` 并保留 snapshot、journal、source backup 和已完成/未完成 targets。

pre-restore snapshot manifest 与 durable `prepared` event 必须绑定同一个 schema/protocol、operation、source backup、storage（含持久化的 `codexHomePhysical`）、required target kinds、resolver operation IDs、按顺序排列的完整 targets，以及 journal 所记录的 snapshot 物理目录。任一字段不一致，即使攻击者或故障同时重算 manifest hash，也必须在 compensation 或 commit acknowledgement 前进入 `recovery-required`，不得读取另一个 snapshot、补偿目标或确认完成。

历史 `transaction-journal.jsonl` 的 `protocolVersion: 1` 是 Sync/Switch transaction journal，并不是独立 Restore journal；Restore v2 继续通过既有兼容路径校验其 source backup 绑定，并在新 Restore 完成 commit acknowledgement 后标记该 source transaction 已恢复。独立 Restore journal 的首个格式就是 `restore-journal.v2.jsonl`（`schemaVersion: 2`、`protocolVersion: 2`），不存在 standalone Restore journal v1，也不得把旧 transaction journal 的 `committed`、`rolledBack` 或 `recoveryRequired` 状态猜测为 Restore v2 状态。未知 Restore journal schema/version 必须 fail closed 且不得改写；旧 reader 遇到未知 v2 文件同样必须 fail closed。

## Compensation, Crash and Foreign Pending Rules

- mutation 或 crash 发生在 `prepared`、`applying`、`committing` 或 `rollback-pending` 时，后续普通写入必须被阻断。显式 recovery 可以重新打开原 journal 并依据 pre-restore snapshot 补偿到 `rolled-back`；也可以执行下述 evidence-preserving resolver Restore。
- 补偿失败、目标 identity 改变、snapshot 覆盖不足或 journal 尾部不可信时，必须保留全部证据并返回 `RECOVERY_REQUIRED`；不得无 journal 地报告部分成功。
- 只有与所选 source backup identity 完全一致、Codex Home 物理 realpath 一致、operationId 唯一且本次 Restore 覆盖其全部必要目标的 pending journal，才可由新的显式 Restore 收敛。新 Restore 必须创建自己的 pre-restore snapshot 和 journal；仅当新 journal 耐久到 `completed` 后，其 `resolvesOperationIds` 才把严格绑定的旧 journal 投影为“已解决”。旧 journal 原始 bytes 和原非终态保持不变，作为 crash evidence 保留；不得伪造其已执行过 `rolled-back` 补偿。
- resolver projection 只影响 write-blocker 判断，不授予删除证据的权限。Prune 仍必须保护旧 journal 自身、其 source backup 与 pre-restore snapshot。不同 source、不同物理 Home、目标覆盖不足、重复 operationId、invalid tail 或未知 schema 的 pending 都继续阻断，且必须在新 snapshot/journal/mutation 前失败。
- 物理 Home binding 以每个 journal `prepared.storage.codexHomePhysical` 的持久值为准；completed resolver 只有在 pending、resolver 和当前已加锁 Codex Home 的稳定物理 identity 全部一致时才可解除 blocker。不得重新解析可变的 lexical `codexHome`，把 junction/reparse 换接后的新位置当作历史证据的 identity。
- Node 与 .NET 都必须识别对方生成的 Restore v2 journal、source backup v1/v2 和 terminal 语义；未知 schema/version 必须 fail closed，而非解析 message 或猜测状态。
- 用户取消只允许在 Core 定义的安全点；取消后的 journal 是否需要恢复由 durable journal 状态决定，而不是由取消信号本身决定。

## Invariants

- Restore v2 继续保留现有 restore options、relocation 双重授权以及 relocation 时不恢复旧 config 的规则。
- Restore v2 不读取/复制认证数据、Token 或消息正文；所有测试只使用临时 Fixture。
- Prune 不得删除 source backup、恢复前 snapshot、任一非 terminal Restore journal 引用的 backup，或被 completed resolver 投影为已解决但原始 journal 尚非 terminal 的证据。
- 本 ADR 不把已发布 v0.5 Restore 描述为已经事务化；V1/C8 候选实现和 release 仍受执行索引阶段 5、C8 证据与最终合入门槛约束。

## Migration and Validation

必须以真实 crash/fault-injection 验证恢复前 snapshot 失败、每个 journal 状态窗口、commit/rollback acknowledgement、restore-mid-failure、foreign pending 与 Node/.NET 双向恢复。仅当这些证据通过后，才可开放 Electron Restore 或宣称跨入口 Recovery 等价。

## Related

- [Plan / Confirm / Apply ADR](0009-plan-confirm-apply-for-writes.md)
- [双层资源锁 ADR](0012-dual-resource-lock-contract.md)
- [Core 外部行为合同](../architecture/contracts/CORE_EXTERNAL_BEHAVIOR_ZH.md)
- [行为 Fixtures](../migration/BEHAVIOR_FIXTURES_ZH.md)
