# ADR-0038：Windows 清理与文件更新分项耗时

- 状态：Accepted
- 日期：2026-09-08
- 范围：Node Windows 首行写入、可选结果计时、Desktop 日志；不改业务、Legacy 或发布状态

## 依据

历史 2,347 个变长文件更新为 21.465 秒，当时没有分项数据，不能事后精确分摊。D 盘 2,387 个、约 8.117 GiB 合成数据的旧路径三次中位数为 15.452 秒，清理 1.931 秒；清理单项实验中位数 14.240 秒。增大到 1 MiB 复制缓冲区反而变慢，不采用。合成参考不等于真实 Home 提速保证；实验裸 File.Delete 不等于 Force 清理，正式路径另行回归。

## 决策

1. 仅优化 worker 的确切临时文件/替换备份路径清理：.NET File.Delete 快路径，异常仍用 `Remove-Item -LiteralPath -Force -ErrorAction SilentlyContinue`。保留只读/隐藏与最佳努力语义，不递归、不新增路径输入，不取消 File.Replace 备份参数。
2. 保留 CopyTo 默认缓冲区、独占句柄、单请求顺序、Flush(true)、原子替换和 mtime 恢复。等长原地资格与失败语义、PIO-1～PIO-6 不变。
3. applySessionChanges 的内部 onTiming 在 Windows 首行批次结束时汇总一次；观测异常隔离。每文件复用现有 worker 响应，不额外发进度或写日志。Node/C# 使用单调时钟；Main 既有阶段/墙钟不变。
4. Sync/Switch OperationResult.result、CLI JSON result 可选增加 fileUpdateTiming；Watch finished 可信 activity 复用严格投影，Main 存为日志同名字段。不增加 Core 方法、输入开关、ProgressEvent 字段或 IPC channel。
5. 固定 schemaVersion=1、scope=windows-first-line；非负整数计数 attemptedFiles/measuredFiles/inPlaceFiles/rewrittenFiles/skippedFiles；有限非负毫秒 totalMs/workerStartupMs/workerCloseMs/requestRoundTripMs/workerMs/sourceOpenMs/readHeaderMs/tempCreateMs/copyTailMs/flushMs/replaceMs/cleanupMs/restoreMtimeMs。允许小数，禁止额外字段、路径、单文件 ID、正文、原始错误；无效观测丢弃，不改变业务结果。
6. attempted 是已发起请求，不是预计全部目标；measured 是收到完整计时的结果（含跳过）。失败请求可保留已测时间，但不算完整计时。原地/替换数是成功文件响应数，不证明全操作成功。中途故障经 ordinary-write partial 保留聚合；Runtime 整体崩溃可能无聚合，日志仍 interrupted，不伪造。
7. 时间嵌套：批次包含启动/请求/Node mtime/回调/关闭；请求包含 worker；worker 包含文件阶段。原地 mtime 在 worker 内、变长在 Node 内，不能相加或把 request-worker 差额全称为 IPC。非 Windows、noop、仅 SQLite、未测或旧日志缺失，不补零。
8. 选中日志详情显示复制/落盘/替换/清理/mtime；技术时间按需展开，无后台刷新。诊断导出复用既有脱敏日志。
9. 取代 ADR-0037 第 7 条临时插桩：基准使用正式 onTiming，不再修改 worker 文本/提供研究变体。保留 D 盘至少 2,000 文件档、正文 hash/大小/mtime/原地身份断言；旧结果仍为历史对照，不能覆盖为新结果。

## 验证

Windows worker 覆盖 Force 回退、失败阶段计时、未知计时、observer/worker 故障；Core 覆盖 Facade/CLI、partial、noop 与手动 Restore。Desktop 覆盖日志重启/导出、Watch 白名单、中英细项/缺失、packaged Utility Switch/日志与 Sync→Restore。D 盘 2,347 等长/变长和 2,387 代表尺寸样本复测。根测试与 architecture:check 仍必需，平台 skips 单列。

具体结果另记 evidence；本 ADR 不是验收/发布证明，不授权提交、推送、合并、签名、Tag 或发布。
