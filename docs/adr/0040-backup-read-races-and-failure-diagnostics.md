# ADR-0040：备份只读竞态与普通操作失败诊断

- Status: Accepted
- Date: 2026-09-09
- Scope: Node Core、CLI JSON、Desktop 操作日志；本地修复，不代表发布

## 问题

备份列表/摘要不持有写锁。发现受管目录后，清理可能先删除其中的文件，再删除 metadata 或目录；原先后续 read/stat 的 ENOENT 会让整次查询失败。Watch retention 测试在自动同步尚未结束时轮询摘要，也会撞上该窗口。

另一次 Windows 安装态 CLI 首次 Sync 仅返回 INTERNAL_ERROR，没有进度或足够细节，同 SHA 重跑通过。目前不能证明其根因；需要补齐实际失败边界，而不是据此放宽 Plan 校验或更改写入算法。

## 决策

1. `getBackupSummary/listBackups` 在发现受管备份后的 read/stat/readdir 遇到 ENOENT 时，整体省略该条备份，不累计部分大小或数量。其它备份继续读取。读取完成后复核 metadata；有效摘要仍使用现有 inventory 快路径，不额外扫描全部备份正文。列表是一次尽力读取，不是锁定快照或可恢复性证明。
2. 只读递归遍历使用独立 strictMissing 选项，子目录消失不能当作零字节成功。发现受管备份后的权限错误、JSON 损坏、namespace 变化仍抛错。初始受管目录发现规则不变；Restore、Prune、备份写入 inventory 的原校验不放宽。Watch 测试等待真实 finished activity 后再断言保留数量，不以中间摘要充当完成信号。
3. 内部 `withFailureStage` 仅在实际执行边界附加固定 `details.failureStage`，内层具体阶段优先；可附带白名单 OS/SQLite `causeCode`。不解析错误文本，不新增业务 API、扫描或 ProgressEvent，不虚构阶段耗时。原 Error code、partial/retry、取消、Home 锁、备份优先与 PIO-1～PIO-6 不变。不能注解的冻结异常原样传递，不让诊断替换原错误；取消转换不承诺附带阶段。
4. 阶段枚举唯一来源为 Contracts 的 `OPERATION_FAILURE_STAGES`：prepare_config、prepare_storage、prepare_rollouts、prepare_status、prepare_revisions、prepare_usage、acquire_lock、read_config、resolve_storage、check_pending_restore、validate_plan、scan_rollout_files、check_locked_rollout_files、preflight_sqlite、create_backup、update_config、rewrite_rollout_files、update_sqlite、release_lock。覆盖 Provider Prepare 和普通写执行器真实边界，不保证每种业务校验都有阶段。
5. 公共 INTERNAL_ERROR 仍固定 fatal / retryable=false / recoveryRequired=false；只额外允许上述阶段、`SAFE_CAUSE_CODES` 内底层码和合法 UUID operationId。cause 白名单新增 ENOSPC、EMFILE、ENFILE、ETIMEDOUT；未知码、原始异常、堆栈、路径、配置、凭据和聊天正文不透传。无信息时留空，不从未知字符串猜测。
6. CLI JSON stdout 仍恰好一个 schema v1 对象，退出码不变；失败时可向 stderr 写 `Failure stage: <enum>`、`Cause code: <enum>`。stderr 失效不替换业务失败。Human 输出保持兼容。
7. Desktop Main 将失败 DTO 投影到既有日志 `failedStage/failureCode`，保留 requestId/planId/operationId 的关联和重启读取。UI 仅本地化固定阶段；旧记录不补造详情。日志导出沿用现有结构化日志通道，不增加后台轮询。

## 验证

- `test/backup-read-race.test.js`：确定性屏障复现 metadata/stat/子目录消失、metadata 尚在而 payload 已消失、幸存条目的数量/大小、权限/损坏错误和写入 inventory 严格性。
- `test/watch.test.js`：自动同步终态完成后验证显式 1 / 默认 2 的保留数量。
- `test/operation-failure-stage.test.js`：真实 Prepare 读失败、备份前零 mutation、mutation 后 partial 与重试收敛、typed/frozen/getter/取消语义、stderr observer 失败。
- `test/cli-json-contract.test.js`、`test/cli-json.test.js`、Contracts 检查：无进度首次失败也有阶段/cause；未知值和原始异常不进入 JSON/stderr。
- Desktop 日志测试：失败阶段/错误码落盘、重启读取、未知值拒绝；既有 UI 日志/阶段测试继续运行。
- 架构、Provider I/O 与完整根测试仍为门禁；本 ADR 不充当跨平台或发布证据。
