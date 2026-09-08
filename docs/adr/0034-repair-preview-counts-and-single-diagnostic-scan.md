# ADR-0034：修复预览计数与单次诊断事实扫描

- Status: Accepted
- Date: 2026-09-07
- Scope: V1 本地 Core / 共享 UI；不执行真实修复，不授权提交或发布

## 问题

Repair 的 `sqliteRowsToChange` 是 model/cwd/has_user_event 字段差异累计值，同一行可多次计入；`workspaceRootsToChange` 是工作区设置类别数。旧界面将二者描述为索引记录数和目录数，并在全局工作区修复时隐藏全部会话预览。长诊断还复用了 Status 的漂移重试，导致事实扫描重复，甚至丢失本轮检查结果。

## 决策

1. 保留现有累计字段及 Apply 行为。Repair impact 可选增加 `sqliteModelRowsToChange`、`sqliteCwdRowsToChange`、`sqliteUserEventRowsToChange`，仅返回选中目标的非负整数。UI 明示字段合计与分类；独立显示已有 `repairPreviewTotal`，它是 rollout/SQLite 受影响原生会话 ID 的去重并集，不含全局设置项。
2. `workspaceRootsToChange` 继续计设置类别，增加固定枚举数组 `workspaceSettingsChangeKinds`：`savedRoots/projectOrder/activeRoots/labels/openTargets/settingsBackup`。分类直接来自已有修复计算，不重新扫描、不导出路径或动态设置值。缺少设置备份确实占一项，UI 说明为“补建缺失的设置备份”。
3. workspaceRoots 仍隐含 cwd 且只支持全 Profile。只隐藏局部选择控件，不隐藏只读会话 ID、字段差异、去重总数和截断提示。最多显示 100 条并独立滚动，不读取正文/名称以补全预览。
4. Diagnostics 使用内部 `getDiagnosticSnapshot`：前后 rollout revision 复用 stat 元数据模式，不再为了该只读观察额外全文哈希；完整事实扫描每次请求最多一次，不执行 Status 的漂移重试，也不写 Status 的最后完整快照缓存。不新增公共请求选项。
5. 本次扫描后 revision 漂移或无法核验时保留本轮事实、`safety.rolloutScanComplete=false`，UI 明示仅供参考且不自动重新检查。前 revision 失败时尚无本轮事实，仍保守降级。真实 Operation/Home lock 始终优先，不能因消除重扫而绕过最终锁检查。未知/阻断结果不得作为修复建议的完整证据。
6. 独立有界 `historyIntegrity` 扫描保持原样（`integrity_*` 阶段），不与 `scan_*` 混为重复。它有单独格式、超限与未核验语义。Repair Prepare 独立重新检查所选目标，不能复用诊断作写入计划。

## 不变与证据

公开方法和 schema v1 不变；只增加兼容 Plan impact 字段。普通 Status 仍遵循 ADR-0033 的一次受限重试；Plan/Apply 内容 revision、Home 锁、UndoBackup、默认 2 份、PIO-1～PIO-6、SQLite 事务与 Restore journal 均不变。

回归使用合成临时数据：`repair-scoped.test.js` 的同一会话三字段累计/去重；`advanced-repair-facade.test.js` 的全局分类/安全 DTO/零写入；`status-coordination.test.js` 的一次扫描/后 revision 失败/真实锁优先；`request-progress.test.js` 的每个扫描阶段只完成一次；共享 UI 的双语分类/全局只读明细/截断和手动诊断提示。Windows 隐藏窗口 packaged E2E 和包体证据另列，不代表跨平台稳定发布。
