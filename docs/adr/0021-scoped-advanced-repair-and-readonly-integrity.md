# ADR-0021：专项修复说明、会话范围与修后核验

- Status: Accepted
- Date: 2026-09-04
- Scope: V1 本地 Node Core / 共享 UI；不授权提交、发布或修改真实用户数据

## 决策

补充 ADR-0016/0019。日常 Provider Sync 的首行读写与 SQLite Provider 同步保持不变，不自动执行高级扫描或修复。

1. 专项修复明确说明适用场景和事实来源。models 以 config 当前根模型为目标；cwd 从 rollout 元数据恢复 SQLite 工作目录；userEvent 仅依据实际用户消息事件补齐 SQLite 标记；workspaceRoots 处理整个 Profile 的工作区设置并包含 cwd。历史模型差异和加密字段的存在不是损坏证据。
2. `prepareRepair` 可选 `sessionIds` 为原生会话 ID，1～100 个、唯一、只含字母/数字/下划线/连字符，最多128字符；不接受路径。省略仍为全 Profile。选择 workspaceRoots 时不得指定会话子集，避免以局部会话目录覆盖全局工作区。不存在的会话在 Prepare / Apply 写入前拒绝。
3. Plan target 包含 `scope`（all/selected），选择范围绑定到进程内 Plan；Apply 仍只接受 `{schemaVersion, planId}`。Plan impact 包含最多100条 `repairPreview`、总数与截断标记。每条只有会话 ID 和目标字段的 before/after 摘要；模型可显示有界模型名，cwd只显示固定语义标记，不返回路径、标题或正文。UI 修改选择后必须重新 Prepare，不能将新选择套用到旧 Plan。
4. Repair 在 Home 锁内重新扫描所选目标，返回 `verification`（verified/remaining/unavailable）、剩余 rollout 文件、SQLite 字段差异、工作区设置项与跳过项。它们不是可相加的唯一会话数。核验不完整或仍有差异形成 partial，不自动回滚；用户可再次修复收敛或通过既有 Restore 预览恢复对应备份。no-op 不创建备份。
5. 所有写操作使旧 Diagnostics 结果过期。过期提示不触发后台诊断；只有用户重新检查才读取新结果。

## 会话历史只读检查

`getDiagnostics` 增加可选 `historyIntegrity`。仅在明确启动完整诊断时读取受限 JSONL 流，检查 JSON/UTF-8、元数据格式、根级数字 ordinal 重复/倒退及扫描期间文件变化。结果只有计数、有界会话 ID/行号/固定问题代码与扫描上限；不保存正文，不改文件。没有末尾换行、超限、未知格式、占用/变化或扫描跳过均不能报告完整核验。

目前没有经确认的 Codex 显示索引格式契约，因此显示索引状态明确为 `unsupported/no-known-display-index-schema`，不能以未发现 JSON/ordinal 问题推断显示索引健康。ordinal 重复/倒退只是观察，不假定序号从0起、连续或无间隔；不开放改序号、重建索引、删除记录或加密内容修复。那些写能力仍需独立授权和设计。

Desktop 诊断包只纳入历史完整性聚合计数，不包含问题明细里的会话 ID；普通操作日志不记录聊天正文或修复预览内容。

## 验证

- `test/repair-scoped.test.js`：临时 SQLite + 多会话，范围隔离、SQLite-only、未知 ID、预览和核验。
- `test/advanced-repair-facade.test.js`：公开 Facade/transport、严格输入、脱敏与 Apply 不扩参。
- `test/history-integrity.test.js`：只读、JSON/UTF-8、序号观察、活动文件、尾记录、格式/读取上限和路径边界。
- 共享 UI 回归：目标说明、选择重新预览、过期结果、核验/恢复入口、只读完整性提示。
- Provider-only、Plan/Apply、Restore 与安装态兼容门禁继续适用；仅本地自动测试不代表多平台正式发布证据。
