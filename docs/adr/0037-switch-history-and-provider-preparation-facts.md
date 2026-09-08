# ADR-0037：切换历史、单轮预览首行事实与时间戳保留

- 状态：Accepted
- 日期：2026-09-08
- 范围：V1 Provider Prepare、Desktop 日志/最近使用；不改普通写算法、Restore 或 Legacy

## 决策

1. `prepareSwitch` 的 schema v1 `target` 保留 `provider/model/modelMode`，增加可选 `previousProvider/previousRootModel`。原/目标值来自同一份 config；缺失根模型用 null，config 竞争重新预览。它们是计划前后值，不是已完成证据。三种根模型策略、历史模型不随 Switch 修改的边界保持。
2. Desktop 可选 `switchPlan` 投影为 `previousProvider/targetProvider/previousRootModel/targetRootModel/modelMode`。Prepare/Apply 仍合并一条，结果由原 outcome/counts 表达；partial 不声称目标全部写入。旧日志缺失内容明确未记录，不能从当前 config/Status/备份反推。模型名仅允许受限字段，配置原文、路径、认证/令牌和正文不进入日志。
3. “最近成功使用”复用现有 Desktop Host 日志 list，不新增 Core 方法或持久偏好仓库。查询当前 Profile ID/revision 最近最多 100 条 completed Switch，去重取最多 5 个仍在当前配置声明的目标（含内置 openai）。排除 partial、旧格式、旧 revision 和已移除 Provider；点击只填写 Provider 草稿，不修改模型选项、不预览、不执行。成功写入后的单次刷新和概览手动刷新更新它，无轮询。普通日志浏览仍保留旧 revision 历史，重试/恢复动作保持 revision 门控。
4. Sync/Switch Prepare 复用 `captureRolloutRevision(provider)` 的既有目录枚举、物理身份前后校验、首行 hash、locked causeCode 和 observedSizes。私有 callback 将单次有界首行事实借给现有 Provider-only collector，生成 counts 与 change descriptor；Status 投影与 revision 共用事实。临时 Map 只在本次 Prepare 中存活，不进入 Facade 参数、公开 DTO、ledger、持久缓存或日志，无第二套 manifest/替换算法。
5. Prepare 每个可读 rollout 从 3 次首行读取降至 1 次；64 KiB 分块预读不变，不能声称整个 Sync 只读一次。Apply 不复用 Prepare descriptor，仍在 Home 锁内重捕 revision、重扫实际目标与 mtime，写前再检查。正文追加可保留，替换、缩短、首行和文件集合漂移仍拒绝；扫描期不稳定候选跳过，非法/超过 1 MiB 首行不能靠正文兜底。
6. **时间戳恢复必须保留，不是可删的性能开关。** 保留等长原地句柄写/恢复时间，及变长复制/Flush/原子替换后的 mtime 恢复；不回拨真正追加产生的新时间，不修改消息时间或 SQLite `threads.updated_at`。变长路径现有实现是 best-effort、毫秒精度，不冒称所有文件系统纳秒精确；本次不扩大失败/补偿行为。
7. `scripts/benchmark-windows-provider-rewrite.mjs` 只创建临时合成数据，用现有 `spawnImpl` seam 对 worker 字符串内存插桩。唯一语句标记不匹配即失败，不改生产协议/DTO、脚本文件或写算法。测 startup、round trip、CopyTo、Flush、Replace 和 mtime 恢复；阶段嵌套不可相加。三组默认样本为 1×32 MiB 等长、1×32 MiB 变长、32×1 MiB 变长，warm-up 后各 5 次；小文件压力档需显式 `--stress`，不接受真实 Home/路径。

## 验证

第 7 条临时插桩方案已由 [ADR-0038](0038-windows-cleanup-and-file-update-timing.md) 的正式批次计时取代；旧实验结果仍保留为历史证据，不等于当前生产实现。

- `provider-preparation-facts.test.js`：3→1 首行次数、无正文流、相同 revision/descriptor、锁/非法/超限/链接/跨目录、扫描期追加跳过，Apply 采用新 mtime 并保留追加字节。
- `plan-apply.test.js`：三种根模型及原值、过期/单次消费、配置/Provider/DB/文件漂移、正文追加与非 Provider WAL 活动边界。
- Desktop operation-log service/validation/IPC 与共享 UI Switch/日志测试：新旧字段、partial、Profile/revision、手动刷新与只填草稿。
- 原 Provider I/O、Restore crash matrix、CLI/root suite、架构门禁继续有效，不弱化断言。
- benchmark 断言正文 SHA-256、正确大小变化、固定毫秒样本 mtime；等长另验文件身份。计时只是该机合成参考，不证明真实 Home 或 packaged 全链路提速。

本地实现/测试不授权提交、推送、PR 修改、签名、公开发布或阶段 Completed。
