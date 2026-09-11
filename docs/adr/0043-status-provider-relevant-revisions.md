# ADR-0043：状态读取只校验相关数据变化

- Status: Accepted
- Date: 2026-09-11
- Scope: 普通轻量 Status；独立于 ADR-0042 更新功能。Core 写入算法不变。

## 问题与决定

ADR-0033 以 rollout stat 和 SQLite/WAL/SHM 内容核对 Status，持续聊天会让一次重试后仍失效，从而阻断首页操作。正常正文追加和非 Provider 索引更新不是同步目标变化，也不是工具占用。

普通 Status 改用内部 `status` revision 模式，复用已有 Provider 首行/文件身份及 SQLite 语义校验：

- rollout 仍核对文件集合、路径、物理身份、链接数和有界首行；后续核对保留最小大小，拒绝截断/替换。不把正文增长或 mtime 当成 Provider 变化，不开启正文流/全文哈希。
- SQLite 在只读事务中读取 threads schema、ID、Provider；Status 另外绑定 archived 分类。标题、模型、工作目录、预览和 WAL/SHM 变化不使 Provider 状态失效；真实 Provider/分类/集合/schema/物理身份变化仍重新核对。
- 状态前后不再整读/哈希 SQLite 文件及其 WAL/SHM。SQLite 无法读取仍显示未知/不可读，绝不补造健康数据；WSL UNC 仍 diagnostic-only，不执行 SQLite 查询。
- 大/非法首行继续沿用 Status 不完整处理，不把新模式的容错用于 Sync Prepare。默认最大发现首行 1 MiB；首行之外最多底层有界块预读。
- 真实相关变化仍最多重试一次，再不一致继续返回 `statusReadBlocked`；真实活动/无法验证锁及 pending Restore 仍优先阻断。不是直接忽略错误或返回健康，也不增加后台刷新。

仅改变普通 Status：完整 Diagnostics、显式 full Status、Repair/Restore 的内容 revision 不变；Sync/Switch 既有 Provider Plan、一次性消费、锁内重验/占用跳过、备份、时间戳、原地字节更新不变。Status 可用不代表自动跳过 Apply 验证。

## 验证

`status-coordination.test.js` 在隔离夹具持续追加正文和更新非 Provider SQLite 列/执行 WAL checkpoint，证明 Core/HTTP Status 可用且不整读 rollout/数据库；持续修改 Provider/archived/config、三个 revision 故障点、真实锁、缓存降级仍阻断。旧“追加必然失效”的 fixture 明确由本 ADR 改为正常聊天可用；原拒绝真实漂移的断言使用持续 Provider 改动保留。

Core 改动必须通过 `architecture:check`、`npm test`，缺失平台/真实用户数据验证单独报告。本次不操作用户 Codex Home，不发布或覆盖现有 Release。
