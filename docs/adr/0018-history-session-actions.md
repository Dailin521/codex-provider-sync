# ADR-0018：History 会话操作与轻量搜索

- Status: Accepted
- Date: 2026-09-04
- Scope: 用户确认的下一批本地 Windows/共享 UI 改进；不授权提交、推送或发布

## Decision

2026-09-04 后续用户确认的项目/主子会话分页与本地项目显示名见 [ADR-0022](0022-history-project-roots-and-child-pagination.md)。下文“当前页分组”和禁止本地别名的原决定由该增量替代，平铺 API 兼容与会话数据只读边界保留。

- 保留全部会话，默认类型为 all。显式 main/subagent 筛选先于分页；只用明确 source.subagent/parent_thread_id 区分子任务，不根据名称或聊天正文猜测。
- History summary 增加可选 nativeSessionId、sessionKind、parentSessionId、fileModifiedAt；nativeSessionId 缺失时为 null，不能使用 rollout fallback ID 冒充真实会话 ID。父关系只读取明确 metadata，不推断 fork 关系。
- UI 提供复制真实 ID、复制 `codex resume <UUID>`、会话信息、复制文件路径、资源管理器定位、父会话跳转。复制命令不创建进程，不改变 Provider；提示用户使用对应 Codex Home 环境及原项目目录，复制命令不承诺会话可继续。无有效原始 ID 时禁用继续命令。
- 列表、详情使用同一稳定选择 ID；文件时间明确标注为“文件更新”，不冒充最后聊天时间。Profile 切换/选择切换时丢弃旧详情；首次读取和显式刷新以外不轮询。
- listHistory 新增 searchScope=metadata/content。共享 UI 默认 metadata，查询标题、ID、项目和 Provider 等 metadata，不扫描正文；切换正文范围后仍须 Enter/按钮提交。省略 scope 的旧 Core/Web 调用保持原来的 content 语义，避免破坏旧接口。
- getHistorySession 新增 metadataOnly=true 只定位首行 metadata 并读取名称索引；返回零条消息、不深读正文。默认详情读取保持最多最近200条。
- 可信 Desktop factory 的 includeLocalDisplayPaths 同时允许详情 storage={cwd,rolloutPath}；默认 Core/Web 不返回路径，列表仍不包含路径。Renderer 只展示/复制这些路径，不能将它们作为文件操作输入。
- Desktop Host 新增 history.reveal，严格只接受 schemaVersion、Profile selector、sessionId。Main 验证来源和大小，经 Utility/Core metadataOnly 重新解析，并再次检查 Profile revision、当前文件及 sessions/archived_sessions 范围；最后调用原生定位文件，不打开/执行文件。无通用路径 IPC，不记录正文/路径到操作日志。
- 不增加删除、导出、收藏、本地别名、全量持久化索引或后台扫描；Legacy .NET 不调整。

### Desktop 复制通道补充

共享 UI 通过 `HostClient.copyText` 复制，Web 未提供此能力时继续使用浏览器 Clipboard API。Electron 使用 `clipboard.writeText({schemaVersion:1,text})` 窄 Preload IPC；Main 验证当前顶层窗口来源、精确字段和完整 JSON UTF-8 大小不超过64KiB，再调用原生 `clipboard.writeText`，只返回 `{copied:boolean}`。不得开放剪贴板读取、任意 channel、执行命令或将复制内容送进 Core、日志、诊断包。拒绝或原生写入失败要显示失败，不能先显示“已复制”。浏览器默认拒绝权限策略保持不变。

会话列表/详情的ID、继续命令、文件路径、已显式打开的代码块及日志关联编号统一走这个通道。自动 Electron 验证在原生写入边界捕获合成文本，不读取/覆盖用户剪贴板；真实系统剪贴板粘贴由用户手测确认。

## Validation

### 项目列表与右键菜单（2026-09-04）

- 会话摘要新增可选 `project={id,name}`（无可用目录时为 null）：仅从已读取的首行 cwd 做词法归一化，name 为有界末级目录名，id 为平台类别加归一化目录的 SHA-256。Windows 盘符/UNC 大小写折叠，POSIX 保留大小写；不访问目录、不解析 realpath，不依据正文猜项目。同名但不同目录保持独立分组。此字段是展示信息，不是权限令牌或不可逆的隐私承诺；完整 cwd/rollout 路径仍不进入摘要。
- 共享 History 左栏按当前分页分组，项目可折叠，每组默认展示五条并在本页内展开；明确说明本页范围，不将当前页计数冒充项目总数，不后台拉取其他页。分组及组内沿用服务返回的顺序，不写入原记录的时间或排序。
- 紧凑行仅展示标题及子任务/归档标记，Provider/model/文件更新时间保留在悬停提示和会话信息。搜索保留显式提交，其他筛选收进“搜索选项与筛选”。
- 移除每行“更多操作”折叠区，改为右键菜单：查看会话、复制真实 ID、复制继续命令、会话信息，以及 Desktop 文件定位。菜单支持 Shift+F10/菜单键、方向键/Home/End、Escape、外部点击和焦点返回；贴近窗口边缘自动收边，滚动或窗口失焦时关闭。右键本身及复制不加载正文，不改变当前选中的会话；只有查看/会话信息才显式打开详情。未授权删除、导出、改名或执行继续命令。
- 详情默认直接展示消息；右键“会话信息”只用 `metadataOnly:true` 读取元数据，须再点“查看会话”才加载正文。菜单绑定打开时的 Profile/revision/筛选分页范围，变化即失效；折叠已选会话的组后返回时焦点落到项目按钮。既有分栏独立滚动、窄屏返回、手动刷新和离页清空保持不变。
- 回归：`history-project-summary.test.js` 验证跨平台目录分组和 DTO 无路径/正文；`history-project-menu.vitest.tsx` 验证分组、同名隔离、展开、不额外读取、键盘菜单和失败反馈；既有 clipboard、History、packaged Electron 用例同步迁移到右键入口。

### History滚动布局补充

History路由单独使用视口高度壳；列表和详情正文各自`min-height:0 / overflow-y:auto / overscroll-behavior:contain`，分页和详情标题位于各自滚动区外，展开会话信息不撑高外层文档。其他页面仍保持原来的页面滚动。低高度窗口的导航/搜索控件区域可独立滚动；窄屏选中会话后收起列表搜索区，详情头提供返回和手动刷新。可聚焦滚动区支持键盘滚动；详情加载聚焦标题使用preventScroll，切换会话只复位右侧滚动，不复位左侧。读取、分页、手动刷新和隐私边界不变。

滚动fixture：`test-support/launch-history-scroll-fixture.mjs`用50个合成会话、82条消息启动隐藏Electron，可由Playwright CLI通过输出的CDP地址连接；`history-scroll.vitest.tsx`验证布局边界、聚焦、复位和返回其他页面的行为。几何验收须使用真实浏览器滚轮检查两栏独立、滚到底不传递到外层、窄屏与200%等效视口无文档溢出，不能用jsdom样式断言冒充几何验证。

合成临时 fixture 验证真实/内部 ID 分离、过滤分页、metadata 查询及 metadataOnly 不读正文、路径输出仅限可信 Desktop；UI 验证复制/失败反馈、父跳转、中英文、搜索提交、手动刷新；Host 验证 sender/schema/revision/路径边界；packaged Electron 隐藏运行，显示信息和复制按钮但不操作用户剪贴板或打开资源管理器。
