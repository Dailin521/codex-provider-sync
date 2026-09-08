# ADR-0019：日常同步与高级修复分层

- Status: Accepted
- Date: 2026-09-04
- Scope: V1 本地产品分层，不授权提交、推送或发布

## 决策

日常操作简单直接，少见故障独立处理。本决策补充 ADR-0016，不扩张普通 Sync 的扫描、写入或保护职责。

- Overview 保留同步当前 Provider、切换并同步。Sync 只更新 config 当前 Provider 对应的 rollout 首行和 SQLite `model_provider`，不修复模型、cwd、用户事件、工作区、会话序号或历史显示索引。
- Switch 保留三种 config 根模型策略，然后在同一操作中调用相同 Provider 同步；不修改历史模型。
- 原 Diagnostics 页面以“高级功能 / Advanced features”呈现，内部 route `diagnostics`、CLI diagnostics/repair、Core DTO 和方法保持兼容。
- 完整诊断只读，进入页面、展开修复、普通同步失败均不触发诊断，也不把诊断结果自动应用为修复。
- 诊断计数显示为元数据差异/兼容性提示，不概括为损坏数量或强制修复。模型差异可能来自正常的历史模型选择；cwd和用户消息标记计数按SQLite会话行；工作区计数是设置待调整项（包括缺失设置备份），不是目录数；encrypted_content文件数只表示检测到该字段，不验证解密、不表示损坏。Core原始字段名和计数算法保持兼容。
- 已实现的 models/cwd/userEvent/workspaceRoots 专项修复集中在默认折叠的独立区域；目标默认全不选，用户选择后经过既有 Prepare/Confirm/Apply。
- 保留备份默认2份、Home lock、SQLite事务、revision复核、占用跳过和partial；“简单同步”不等于取消必要的写入保护。不增加普通同步中的全量序号扫描。

## 本地交互补充：直接同步与诊断结果

- Overview 同时提供“预览同步”和“直接同步”。后者的点击即为明确执行意图，不弹第二次确认；内部仍先 `prepareSync`，再将同一 `planId` 交给 `applySync`。不新增 Core 方法或绕过备份、锁、revision 检查和占用处理；Switch/Repair/Restore 保留预览确认。
- 直接同步在 Prepare 期间也阻止重复提交和 Profile 切换，展示检查/执行进度；取消或 Prepare 失败不得继续 Apply。已进入 mutation 的取消仍遵循 Core 边界，成功/partial 使用原结果界面。
- 完整诊断使用独立 15 分钟 Runtime 请求预算；普通只读请求默认 30 秒和写请求默认 15 分钟不变。超时仍清理当前 Runtime generation 并隔离迟到响应。
- 诊断明确展示扫描中、失败和重试；重新扫描失败时保留上次成功结果及其时间，并标注非本次结果。按 Profile/revision 隔离，仅手动触发，不因导航、普通同步或刷新状态自动扫描。

## 尚未实现的高级能力

2026-09-04 补充：ADR-0021 已加入只读 JSON/UTF-8/格式检查及根级数字 ordinal 观察；以下限制继续适用于序号改写和显示索引重建，不能将只读观察解释为已有修复能力。

会话 `ordinal` 异常和 Codex 历史显示索引重建属于独立的“会话历史显示修复”，不包含在当前四种 Repair targets 中。页面不得宣传已具备此能力，也不提供直接运行临时修复脚本的按钮。

后续能力需独立完成：按会话诊断、已知格式识别、索引与原记录一致性检查、备份、确认该会话停止写入、隔离副本验证、显式确认、保留正文/消息ID/时间、修后完整性核验及失败恢复。未知格式、无法证明完整或仍有写者时停止。原始序号正常时不得擅自改序号；根因未确证时不归咎于某次同步。

## 验证

- UI：中英入口、修复折叠与空选择验证、只请求 Prepare、默认保留2份。
- 集成：普通Sync失败不自动调用Diagnostics/Repair；高级页面首次进入/重入不扫描；诊断失败不自动修复。
- 既有 Provider-only fixture：不解析正文，等长/不等长改写保持正文一致，备份与占用语义不变。
- 自动桌面测试仍使用临时fixture和隐藏窗口，不操作真实Codex Home。
