# ADR-0022：History 项目主会话与按需子任务

- Status: Accepted
- Date: 2026-09-04
- Scope: 用户确认的本地共享 UI / Electron 改进；不授权提交、推送、合并或发布
- Supersedes: ADR-0018 中“先全局分页、再按 cwd 分组”的 UI 行为，以及禁止本地项目显示别名的限制；不改变平铺 API 和真实数据写入边界

## Decision

- `listHistory` 增加显式 `view: "projects"`。省略或 `flat` 继续保留既有全局分页、过滤和 DTO；CLI 不增加命令。
- 项目视图先收集并去重全体首行 metadata，再建立明确 `parentSessionId` 关系；不根据标题、聊天内容或目录相近推断父子。子任务继承有效主会话的项目，即使工作目录不同。无父、父缺失、自引用、循环及其后代统一放入可显式展开的“未关联子任务”，不丢弃、不伪装成主会话。
- 工作区来源仅是当前 Home `.codex-global-state.json` 的 `project-order`、`electron-saved-workspace-roots`、`active-workspace-roots` 与 `electron-workspace-root-labels`。只读有界（1 MiB）并校验文件身份/物理边界；无效、过大、链接或不可读则回退。按最长包含的已保存根归属，Windows 盘符/UNC/extended alias 规范化且不区分大小写，POSIX 区分大小写。不遍历真实工作目录、Git 仓库或消息正文来猜项目。
- 无已保存工作区匹配时使用已记录目录，明确标记“工作目录”，不能宣称识别了项目；无有效绝对目录则归“其他会话”。同名不同目录使用不同分组指纹，UI 加短标识避免混淆。
- 项目视图返回 `projects: {id,name,kind,total}[]` 和本次 `projectId`；kind 为 workspace/directory/unassigned/orphans。普通 total 按筛选后的主会话计数，不是当前页数量；孤儿单独计数。首次返回首个非孤儿组的第一页，每页默认 10 条。孤儿为空时不显示；只有孤儿时仍返回清单，须显式展开。
- `projectId` 只接受 SHA-256 分组标识或 unassigned/orphans，不接受目录。`parentId` 只接受现有选择 ID，返回直接子任务分页并自动定位父项目；不读取消息。结果 summary 增加可选 `childCount`。父子图先于过滤建立，搜索命中子任务时保留父链作为上下文；main 筛选不显示子任务。
- UI 首次展开首个普通项目，其他项目/子任务显式展开才请求摘要。每项目、每个父会话独立“加载更多”，不再提供全局上下页。默认子任务折叠；列内紧凑单行标题，右键菜单/Shift+F10、独立滚动和窄屏返回保持。刷新重取项目清单并重置已展开分页，不自动轮询或重新拉取所有项目。
- 本地项目显示名通过 Host 提供的 `PreferenceStore` 保存，仅包含 Profile id/revision、项目指纹和最多 160 字显示名。共享 UI 不直接操作持久存储；Desktop/Web 各自命名空间隔离。右键项目可设置/恢复显示名，不改目录、不改会话标题、不写 Core/SQLite/rollout/global-state，也不进入日志/诊断。存储失败必须提示，不能声称保存成功。
- 原始会话标题来源、用户显式正文搜索、最多最近 200 条消息、退出详情清空正文，以及旧平铺接口保持。项目分页没有跨请求冻结快照；活跃会话变化时用户可刷新重取，UI 去重已加载选择 ID。

## Validation

合成临时数据覆盖跨全局页父子关系、每项目超过 50 条、同名目录、工作区子目录与标签、跨目录子代理、循环/缺父后代、搜索保留父链、空孤儿组、路径规范化/过大 metadata。UI 覆盖独立加载、折叠子任务、菜单/焦点、别名及失败、刷新/切换 Profile 清理、正文延迟读取。实际 Chromium 验证滚动/200% 等效视口，隐藏 Electron 与最终本地 Windows 产物验证不修改 fixture 原数据。不代表正式发布或迁移阶段 Completed。
