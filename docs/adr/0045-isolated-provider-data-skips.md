# ADR-0045：隔离问题会话，继续正常 Provider 同步

- 状态：Accepted
- 日期：2026-09-15
- 范围：共享 Node Core 的 Sync、Switch、Watch 及 Electron、Web、CLI；不回移 Legacy .NET。

## 决策

单个会话的无效首行、128 MiB 输入/输出超限、不可读/占用、消失或变化，作为逐文件跳过事实。写入失败只有在底层明确证明源文件未受损时才可继续。配置/Profile/存储位置、目录完整枚举、路径边界、数据库身份/schema/损坏/忙碌、备份失败、磁盘写满和未知写入结果仍停止；已发生写入则返回部分完成并保留备份。

Provider 计划保存内部文件身份、首行校验、SQLite 行原 Provider 和排除集合。预览排除项在本次执行始终排除，修好后需新预览；正常候选逐个复核，新增文件和行留待下一次。Repair/Restore 的严格计划校验不变。Status 保留不完整标记，允许预览健康部分，不能将未知数据计为对齐。读取中发生单条变化时仍只允许一次受限重读；新一轮完整且稳定的事实可以成为有效快照，持续漂移仍不完整。

POSIX 原地写在打开描述符时发现硬链接数量已变化，按 `changed` 跳过且不回退替换；后续身份复核、写后及恢复校验仍严格。短写、零进度或 fsync 失败仅在立即恢复并验证原字节成功后返回 `write-not-applied`；恢复失败仍停止并保留备份。

SQLite 通过有效 metadata 的 ID 或经规范化和边界验证的 `rollout_path` 建立关联。不猜文件名，不扫描正文找 ID。跳过文件的关联行不更新；冲突及未知归属保留。有无法关联的坏文件时，仅更新正向确认健康的索引；无歧义时继续支持 SQLite-only。预览与 SQL 使用同一选择集合，空集合零更新。事务内逐行比较原 Provider，变化/消失的行跳过，数据库级故障停止。

执行前复核所有预览时已观察到的 SQLite 行，包括当时已经对齐、无需更新的行。Provider 或关联路径变化、行消失均记为跳过，不能把后来失配的行计为完成，也不能临时扩大已确认的写入集合；没有其他写目标时返回部分完成且不建备份。

已有 SQLite 写事务还会在事务内复核完整预览行快照，记录文件处理期间发生的行变化；实际 UPDATE 仍只执行原确认候选，不为纯观察创建额外写事务或备份。

备份先于业务写入，仅包含可写候选。写后复用 sessions manifest 的恢复范围，排除明确未写入的文件，保留写入成功和结果不确定的文件。范围落盘失败需提示，不能声称已排除。旧备份和崩溃时未记录结果的备份保守恢复；SQLite Restore 仍是整库快照。普通同步不引入 journal。

有跳过即部分完成。全部历史跳过且无其他写目标时 Sync 不创建备份；Switch 仍备份并切换配置，明确报告历史成功 0 条。成功文件和索引计数分开；跳过摘要含未确认数量。

## 接口和日志

### 首行编码、结构和处理容量

Provider Status/Prepare 严格解码首行 UTF-8；非法字节作为 `metadata-invalid-utf8` 跳过，不能以替换字符静默修改非 Provider 数据。有效 U+FFFD、跨分块多字节字符继续支持；BOM 保留并按现有格式规则拒绝。`payload` 必须为非 null、非数组对象，数组归为 `metadata-invalid`；缺失 Provider 和合法扩展字段仍兼容。

不设置固定 JSON 嵌套层数上限。仅在准备变更的 JSON 序列化和原地资格语义比较边界，将 RangeError 处理容量失败归为 `metadata-too-complex`，其他异常继续抛出。无需改写的有效深层首行仍可进入 Status 的已观察 Provider 分布，Status 候选数量不保证实际可写；已识别跳过项不能报告对齐。成功解析出的有效 ID 可用于保护关联索引，非法解码内容不能用于猜 ID。

上述固定数据问题 `retryable=false`，提示处理后重新预览。严格校验通过私有 Provider 标记接入，Repair/Restore/History 共享读取默认行为不变。Node 和 Windows worker 写前发现合法首行变成非法编码时返回 `SKIP_CHANGED`，确认源文件未写，沿用关联行排除和恢复范围收窄；不得绕过原地写失败改用替换。

沿用公开 schemaVersion/protocolVersion 与 Apply planId 输入。预览 impact、结果和本机操作日志新增 `skipSummary`：`total/rolloutFiles/sqliteRows/unconfirmed/omitted/retryRecommended/items`。每项仅含文件或索引类别、固定 reason、stage、retryable，以及本机文件完整路径或安全索引标识。相同对象只计一次，最多 200 条明细，展示总数和省略数。

原因白名单：`metadata-invalid/metadata-invalid-utf8/metadata-too-complex/metadata-too-large/locked/unreadable/missing/changed/write-not-applied/association-unknown/association-conflict/row-changed/row-missing/deferred`。阶段：`scan/plan/revalidate/write/sqlite`。保留锁定和变化计数字段；其他跳过使用 `partialReason=skipped-data`。永久格式/超限问题提示处理后新预览；占用/变化才提示稍后同步。

诊断导出独立移除路径和索引标识，不直接复制本机日志。不记录正文、原始异常或凭据。CLI JSON 部分完成退出 3；Human 继续既有退出约定。

## 替代范围与验证

替代 ADR-0037/0043 的 Provider 全量 revision 失效规则及 ADR-0044 的单条 metadata 错误全局拒绝规则；保留 128 MiB、PIO-1～PIO-6、时间戳、备份优先及完整枚举安全校验。旧 ADR 中相冲突描述为历史行为。

证据：`test/provider-skip-data.test.js`、计划漂移/首行/原地及 Windows worker 回归、Contracts 和本机日志重启/导出/UI 测试；生产 Electron 混合数据 Sync→Restore。执行 `npm run architecture:check` 和 `npm test`。本地验证不代表跨平台 CI、真实安装或发布完成。
