# ADR-0025：状态可信展示、清理确认与桌面窗口偏好

- 状态：Accepted
- 日期：2026-09-05
- 范围：V1 共享 UI 和 Electron Main
- 依据：用户确认下一批六项体验改进；承接 ADR-0024

## 决策

1. 概览未取得有效状态快照时，Provider、备份和占用数量显示 `—`，对齐状态显示读取中/尚未验证。保留真实 `0` 与缺失数据的区别。手动刷新期间可展示上次完整快照，但必须标明；读取失败不得用缓存旧值宣称健康。
2. 手动清理备份先展示基于当前受管列表和保留数的数量上限，再确认执行；保留 0 明示不可通过应用撤销清理。恢复保护由既有 Core Prune 决定，UI 不承诺精确删除数量，也不新增受保护标记 DTO 或 Prune Plan API。确认期间 Profile revision/列表改变、读取失败或写入受限时不得使用旧确认执行。
3. 存储配置标记“正在使用”，只依据当前选中 Profile ID，与编辑表单选中项分开。默认配置由环境管理、不可删除等规则不变。
4. 操作结果与最终 Provider 检查分开：复用 Apply 后既有 Status 请求，不添加后台扫描。只有请求成功、Profile ID/revision 与原操作和当前配置一致时才展示快照。对齐结论采用 Core `alignment`，且要求 SQLite 可读、rollout 扫描完整、无占用、无进行中操作或阻断恢复。不从 Apply outcome 或总数相等推断健康。失败/不完整时明确“尚未验证”，保留独立的操作结果。所有 Profile 的旧 Status 缓存标记失效，不额外自动读取其他配置。
5. History 搜索词、Provider 和范围继续草稿/提交分离，未提交变化显示提示。“清除筛选”重置到无关键词、无 Provider、元数据范围和全部会话；仅清除草稿不额外请求，清除已生效筛选走既有列表请求。归档/会话类型仍即时筛选。不得自动执行正文搜索，正文缓存边界不变。
6. Electron Main 在 `userData/window-state.json` 保存正常窗口边界、最大化状态和显示器 ID。使用 DIP 工作区，启动时及显示器移除/工作区变化时将窗口放回可见范围。有界 JSON、原子写、短延迟合并和退出 flush；损坏/写入失败不阻止启动或退出。`hidden`/`secondary` 自动验证不读取或覆盖用户窗口偏好；不向 Renderer 或 Core 增加窗口位置 API。

## 验证与不变边界

### 2026-09-07 切换表单状态补充

概览异步读到的当前 Provider 必须同步到尚未编辑的切换目标，不能保留加载时兜底的 openai。用户已编辑目标不得被刷新覆盖；该目标成为当前 Provider 后可作为新的默认基线。读取失败不重置草稿，Profile ID/revision 变化重置 Provider 和模型选项。按用户要求卡片改名“单独切换 Provider”，但仍沿用修改 config 后同步历史 Provider 的 Prepare/Apply，不增加仅改配置的新能力或轮询。`packages/app-ui/tests/switch-provider-state.vitest.tsx` 固定上述行为。

### 回归范围

2026-09-07 用户明确调整概览排序，随后补充首屏布局：状态摘要 → 会话记录文件/本地聊天索引分布 → 当前存储配置及完整路径（左）与同步当前 Provider（右）同排 → 最底部“单独切换 Provider”。取代 ADR-0024 第 5 项的操作优先排序；Sync 和 Switch 本身仍上下独立。宽屏（≥1024 CSS px）四个摘要卡片一排、存储/同步并排，收紧留白，使常用 1280×720 / 1366×768 视口能直接看到预览与直接同步按钮；窄屏保持存储在前、Sync 在后，不压缩字体或裁剪路径。`overview-paths.vitest.tsx` 固定 DOM/键盘顺序；Web/Electron E2E 验证实际坐标和按钮完全在首屏，而不仅是存在于 DOM。该变更只调整共享 UI，不隐藏路径、合并业务或改变授权语义。

2026-09-07 占用口径补充：用户否决仅展示“读取受阻文件”，明确使用“正在使用的会话”并与预览同步一致。[ADR-0029](0029-overview-preview-session-usage.md) 增加可选 `syncSessionUsage`，复用预览的目标范围和独占写探测；不再用旧 `lockedRolloutFiles` 冒充该值。`overview-session-usage.vitest.tsx` 验证双语、已验证零值/非零、未知、旧 Host、阻断/缓存快照。

- `packages/app-ui/tests/ux-polish.vitest.tsx` 和既有 retention/操作测试覆盖状态、清理、配置标记、结果复核、筛选。
- `apps/desktop/tests/window-state.test.mjs` 覆盖负坐标、显示器缺失/工作区缩小、最大化、损坏文件、写入失败和退出持久化。
- 继续验证架构门禁、Provider I/O、根测试和隐藏窗口 packaged E2E；模拟显示器测试不冒充真实热拔插验证。

不改变 CoreFacade、Apply DTO、Provider I/O PIO-1～PIO-6、默认备份数 2、首次/手动刷新或 Legacy .NET。此决策不授权提交、推送、PR 更新、合并或发布。
