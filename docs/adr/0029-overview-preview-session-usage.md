# ADR-0029：首页与预览同步统一会话占用口径

- 状态：Superseded in part by [ADR-0030](0030-writer-owned-session-count.md)（首页/预览展示口径；实际目标写入检查保留）
- 日期：2026-09-07
- 依据：用户明确要求“正在使用的会话（和预览同步保持统一）”。
- 范围：Node Status、ProviderSync 内部占用检查、共享 UI；不修改 Legacy .NET。

## 决策

1. 首页保留“正在使用的会话”，但不再用 `lockedRolloutFiles.length` 冒充该读数。原字段仍只表示首行读取受阻，兼容含义不变。
2. `StatusSnapshot` 可选增加 `syncSessionUsage`：`{state:"checked",count:非负整数}`，或 `{state:"unavailable"|"unsupported",count:null}`。旧 Host 缺少该字段视为未知，禁止回退到零或旧字段。
3. 与 Preview Sync 使用相同目标范围：当前 config Provider 对应的待修改 rollout，加上首行读取时无法判断 Provider 的 busy 文件。可读取且已经对齐的会话不计入；不是所有 Codex 正在执行/等待输入的任务数量，子会话也不另行推断。
4. `application/session-usage.js` 统一调用 SessionStore 的既有独占写探测，合并去重读取受阻和写探测受阻路径；Status、Prepare Sync/Switch 和 Apply 复用。Windows 沿用 ReadWrite/FileShare.None 探测和既有 busy/权限分类，因此该数字是同步处理受阻口径，不是进程活动证明。
5. Status 在已有首行遍历中收集仅含路径的待修改候选，不打开正文流、不生成 Plan、不备份、不持有 Home 写锁。Plan 上下文不重复探测，业务 Prepare 按自己的实际目标探测；Apply 仍在 Home 锁内重新检查，不能复用 Status 的读数授权写入。
6. Windows 检查批次限时 10 秒，探测器隐藏运行。探测失败/超时、首行不完整为 unavailable；当前非 Windows 无同等探测为 unsupported。预览/执行仍按既有路径传播无法探测的错误，不能将失败当作可写。
7. UI 对未知、阻断状态或正在写入的缓存快照显示“未知”；手动刷新期间旧快照仍须标注。保留首次/手动刷新及操作完成后的既有单次核验，无轮询、无 app-server 连接或额外全量扫描。两次采样之间会话使用情况可能改变，数字不是永久写入许可。

## 2026-09-07 手测反馈：展示精简与待解决问题

- 用户明确拒绝首页常驻的同步范围/已同步不计入/写前复核等实现注释。卡片只保留标题和数量；不能获取时直接显示“未知”。中英文同时删除说明，不转移到 tooltip 或其他首页角落。
- 用户仍在 Codex 聊天时读数为 0，说明本 ADR 的同步受阻口径不符合用户对“正在使用”的含义。当前合成锁用例只能证明与 Preview 一致，**不能作为真实会话活动统计已修复的证据**。数据源语义问题仍未解决，删除注释不算修复计数。
- 不将文件修改时间、Provider 差异、文件锁数或 Codex 进程数直接充当实时会话数；运行态接入须另行验证。不得为读取状态启动/恢复会话或更改用户 Codex 启动方式。

## 验证

- `test/sync-service.test.js`：Windows 共享读取但拒绝独占写的合成文件，Status/Preview 同值、排除已对齐会话、释放后归零；探测器启动失败、损坏首行返回未知；内容/文件身份/mtime 不变且不创建备份。
- `test/provider-sync-lite.test.js`：公开 Facade Status 与 Sync 不读取整份 rollout/开启正文流；保留原地写和 32 MiB PIO 门禁。
- Contracts 校验 checked、unavailable、unsupported 及拒绝无效数值/额外字段；UI 覆盖双语、零值/非零、旧 Host、未知、阻断与写入缓存。
- 首页顺序沿用 ADR-0025 的 2026-09-07 补充，实际宽/窄布局及隐藏 Windows packaged E2E 独立验证。

不改变 CoreFacade 方法集合、Apply 输入、Provider I/O、Restore、备份保留数 2 或发布权限。此决定取代本轮曾提出但被用户否决的“读取受阻文件”首页卡片。
