# vNext/ADR-0012：Codex Home 与 State DB 的双层资源锁合同

- Status: Accepted
- Date: 2026-08-25
- Scope: vNext target lock contract; not implemented by v0.5

## Context

现有入口以 Codex Home 为主要操作边界；但两个不同 Codex Home 可以解析到同一 SQLite Home。仅持有各自 Codex Home 锁时，两个写者仍可能并行修改同一 `state_5.sqlite`。UI 内的“操作中”状态不能提供跨进程或跨运行时互斥。

## Decision

vNext 的每个写操作按解析后的双层资源身份协调：

1. **Codex Home lock**：绝对规范化的 Codex Home identity，锁路径固定为 `<CodexHome>/tmp/provider-sync.lock`；它保护 config、rollout、global state、backup root 和同一 Codex Home 的操作顺序。
2. **State DB resource lock**：保护最终解析出的权威 `state_5.sqlite`，且独立于 Codex Home identity。资源身份由数据库物理父目录的 `realpath` 与规范化文件名组成；Windows 对完整 identity 进行大小写折叠。数据库存在时使用最终物理路径；Restore 将创建缺失数据库时，先可靠解析父目录，再使用父目录与 basename。无法证明父目录、重解析点或最终目标身份时返回 `LOCK_UNVERIFIABLE`。

State DB resource lock 路径固定为：

```text
<real-db-parent>/.codex-provider-sync/locks/<sha256(resourceIdentity)>.lock
```

Hash 输入使用 UTF-8 编码的规范化 resource identity；Node 与 .NET 必须产生相同小写十六进制 SHA-256。锁继续使用 protocol v2 的 canonical owner/claims/instance identity；owner/claim 可增加可选 `scope` 与 `resourceKey` 字段，旧 v2 reader 必须仍能读取基础字段并 fail closed。

需要 SQLite 的写操作必须按以下顺序执行：获取 Codex Home lock → 在 Home lock 内重新读取 config 并解析权威 State DB → 建立物理 resource identity → 获取 State DB resource lock → 在两锁内重新校验 Revision、pending journal 和 SQLite writable → 创建 backup → mutation/验证/终结 journal → 逆序释放锁。两个锁必须一直持有到 journal 落入耐久终态。`prune-backups` 仅获取 Codex Home lock；Status/History 不获取写锁；Watch 不长期持锁，而是每次 apply 按同一顺序获取。Windows WSL UNC 等不受支持路径在创建任一 backup、journal 或业务 mutation 前失败。

Node、迁移期 .NET、CLI、Web、Electron Runtime 必须使用相同锁路径、identity/hash 规则、owner metadata schema、持有周期和冲突语义。现有 protocol v2 Codex Home lock 兼容测试继续是迁移门槛；引入 State DB resource lock 时，双方必须通过真实争锁测试，之后才能宣称双层锁合同成立。

## Error Semantics

- 已证明存在活跃、兼容的 owner 时返回 `OPERATION_BUSY`，`details.busyScope` 为 `codex-home` 或 `state-db`。
- owner、协议、进程启动身份、锁目录/resource identity 或 ABA 状态不可验证时返回 `LOCK_UNVERIFIABLE`，并保留 `details.lockScope`；不得自动删除锁目录或降级为 Busy。
- 已获取资源锁但 SQLite 引擎仍拒绝写入时返回 `SQLITE_BUSY`；它不等同于资源锁冲突。
- 获锁后的 pending journal 返回 `PENDING_TRANSACTION` 或 `RECOVERY_REQUIRED`；该错误优先于普通写入。
- lock path 创建/访问被拒绝时返回 `PERMISSION_DENIED`，而不是假装无竞争者。

## Invariants

- 锁不是 backup 或 journal 的替代物；获得两锁不允许跳过 Plan、Revision、Backup-first、transaction journal 或验证。
- 同一 State DB 的败方不得创建 backup、journal、config/rollout/SQLite/global-state mutation，且所有原始 Hash 保持不变。
- 任一正式入口不得自行发明锁路径、只靠 UI 禁用按钮，或在持有 State DB resource lock 后再以相反顺序等待 Codex Home lock。
- 当前 v0.5 单层行为仍是现状事实；本文不声称它已实现双层锁。

## Migration and Validation

实现前必须完成 `node-dotnet-lock-contention`、`shared-sqlite-home-contention`、双层 lock-order/deadlock、`lock-unverifiable` 和 Windows UNC 无写入 Fixture。真实 Node/.NET 进程必须在同一临时资源上争锁；Mock 不足以证明兼容。任何旧协议的兼容读取或拒绝策略必须有独立 Fixture 和差异登记。

## Related

- [Core 单一权威 ADR](0002-node-core-as-single-authority.md)
- [Plan / Confirm / Apply ADR](0009-plan-confirm-apply-for-writes.md)
- [Error Code 合同](../architecture/contracts/ERROR_CODES_ZH.md)
- [行为 Fixtures](../migration/BEHAVIOR_FIXTURES_ZH.md)
