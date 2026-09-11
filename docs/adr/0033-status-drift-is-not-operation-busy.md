# ADR-0033：状态读取变化不等于写操作占用

- Status: Accepted
- Date: 2026-09-07
- Scope: V1 共享 Status、CLI 与 Web/Electron 展示；仅本地修复，不授权发布

## 问题

正常聊天追加 rollout 或 SQLite 更新可能使 Status 前后 revision 不一致。旧实现将这种情况构造为不存在的 external Operation，启动界面因此误报“操作执行中”。它不是同步锁存在的证据。

## 决策

2026-09-11：[ADR-0041](0041-stale-home-lock-status-recovery.md) 进一步区分已证明失效的锁与未知锁；前者允许只读状态/正常同步重新验证，后者仍阻断。下文活动锁、revision 和最后完整快照规则保持有效。

1. 只有进程内 OperationRuntime 或实际 Home lock 检查才能产生 `operationInProgress`。实际活动/无法验证的 Home lock 仍按现有规则阻断；不删除锁、不根据文件变化推断工具操作。
2. revision 漂移或读取失败分别返回现有 `statusReadBlocked.reason=state-changed-during-status/revision-unverifiable`，无真实占用证据时 `operationInProgress=null`、`rolloutScanComplete=false`。首轮、扫描后和一次重试后的 revision 失败采用相同语义。
3. 保留同 Profile/revision 的最后完整快照，但标注未核验，不缓存本次中间扫描结果。首次启动没有旧快照时返回降级 DTO，不推断健康、Provider 或零计数。pending Restore 仍检查，检查失败保持恢复阻断。
4. rollout/SQLite 变化仍最多重试一次；最终锁检查优先于变化提示。用户可手动重新读取，不增加后台轮询、无限重试或正文扫描。
5. 共享 UI 区分中性“状态待刷新”和真实“操作执行中”；未核验时 Provider/计数占位、对齐未知。`statusReady` 排除 `statusReadBlocked`，沿用全部写入/确认/Watch 启动/安装门禁。恢复与清理也不得依赖此未核验快照放行。
6. CLI Human 对降级快照输出明确状态及重试说明，不解引用缺失计数或把旧值当本次成功核验。JSON 保留白名单 `statusReadBlocked.reason`，不输出异常文本。成功读取降级状态仍退出 0，`ok` 不是 Provider 对齐证明；Apply outcome/退出码不变。

## 不变与验收

PIO-1 至 PIO-6、Plan/Apply revision 校验、锁协议、备份优先、默认保留 2、Restore journal 均不变。Diagnostics 复用此 Status 时 `rolloutScanComplete=false`，不能误报完整；不扩张 Diagnostics API。

`test/status-coordination.test.js` 使用临时合成数据，在首行读取后注入追加，验证首次/缓存快照、一次重试、三个 revision 失败阶段、真实锁优先及 HTTP DTO。CLI、共享 UI 和 Web E2E 验证无假 busy、写入禁用、手动刷新恢复。Windows 产物隐藏窗口 smoke 与包体结果另列 evidence，不代替完整跨平台发布门禁。
