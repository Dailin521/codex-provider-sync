# ADR-0024：按存储配置隔离操作与可继续处理的结果反馈

- 状态：Accepted
- 日期：2026-09-05
- 范围：V1 共享 UI、Desktop Host 日志和 Watch 状态查询
- 依据：用户确认修复七项体验审查发现；不改变 Provider I/O 不变量

## 决策

1. `getWatchStatus` 保持无参数/`{}` 的全局守卫语义、`{watchId}` 的单项查询，增加互斥的 `{profile}` 查询。Profile 必须可信解析并核对 revision。设置页按 Profile ID/revision 隔离缓存、请求、错误和启停结果；旧请求可取消，不添加轮询。不同 Home 的 Watch 不得互相显示或停止。同物理 Home 仍只创建一个 watcher，显式启用的 Profile 别名登记在该 watcher 上，启用返回值和后续查询一致；关闭共享 watcher 对所有已登记别名生效。保留首次启用者的 options/SQLite override 权威，后启用别名不得覆盖 Main 所有者；自动执行和资源停止日志归首次启用的 Profile，不扇出重复日志。UI 明示共享语义及通过“全部配置”查看日志；不扩张 stopWatch 输入。
2. 备份读取区分加载中、读取失败、真实空列表。失败显示固定安全错误及手动重试，禁止使用失败快照执行恢复/清理。日志跳转指向已被保留策略清理的备份时显示不可用，不能自动选择另一份。
3. Restore 选项由 `metadata.capturedTargetKinds` 决定：配置对应 `config || globalState`，索引对应 `sqlite`，会话对应 `rollout`。未捕获项不选且禁用；缺少该字段的旧备份保持完整备份兼容。SQLite relocation 自动取消并禁用配置恢复，只在选中索引恢复时可用；要求明确目标，说明不恢复配置/工作区设置。Core Restore journal、补偿和 Apply 输入不变。
4. 删除非当前 Profile 不切换当前选择；删除当前具名 Profile 才回到默认配置。默认 Profile 不可删除。
5. 原概览顺序为状态摘要、同步/切换主操作、完整路径、分布。**该排序已被 ADR-0025 的 2026-09-07 用户布局修订取代**；不隐藏完整路径、不变更直接同步/预览同步授权语义的边界继续有效。
6. Desktop 操作日志 schema v1 增加可选 `profileRevision/failedStage/failureCode/partialReason/retryRecommended`。Main 从嵌套 OperationResult 逐项白名单提取，不保存整个 result、任意字段名或正文。部分失败保留 backupId、计数与失败阶段；Restore 的失败类 outcome 不记为 completed。Watch activity 可带同样的结构化失败字段和 backupId，不带 backupDir；activity 仍不扩张 ProgressEvent。
7. 结果页显示失败阶段/错误码，提供返回相应操作页的入口；仅返回页面，用户重新确认，不重放已消费 Plan。日志只在 Profile ID/revision 与原记录匹配时提供操作和指定备份预览入口；缺少 revision 的旧日志可读，但不自动推断目标。Restore 最终仍需读取现有受管备份、Prepare/Confirm/Apply。
8. History 详情失败提供宽/窄屏可用的本地 Retry，只重新读取选中详情；不重读项目列表、不重置展开/分页。选中摘要在加载/失败时保留标题，详情正文不进入 Query 缓存或持久缓存。
9. 修正 CLI 进度漂移：Sync/Switch 保持原六阶段，Repair 专属 `verify_repair` 只加入 Repair 的七阶段展示表；不改 Core 事件或既有 Sync 断言。

## 验证与边界

- `settings-watch-scope`、Core surface、Desktop IPC：配置隔离、别名去重、旧请求取消和全局守卫。
- `ux-review-fixes`、`overview-paths`、`partial-feedback`：七项体验回归与显式动作。
- `operation-log-service`、`runtime-protocol`、Watch：持久化嵌套 partial、白名单、阶段失败和结构化 activity。
- `cli-json-contract`、`architecture:check`、根测试：六阶段兼容及 PIO-1～PIO-6。
- Windows 自动验证使用隐藏窗口和合成数据，不使用真实 Codex Home。

保持默认保留 2 份备份、首次/手动数据刷新、独立高级功能和 Legacy .NET 构建边界。此决策不授权提交、推送、PR 更新、合并、Tag、签名或发布；本地通过不等于跨平台发布门禁完成。
