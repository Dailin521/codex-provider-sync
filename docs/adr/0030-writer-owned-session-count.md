# ADR-0030：正在使用的会话与同步写入受阻分离

- 状态：Accepted
- 日期：2026-09-07
- 取代：ADR-0029 将同步目标占用数用于首页“正在使用的会话”的决定；不取代其 Prepare/Apply 写入占用检查。
- 依据：用户手测中正在聊天但首页持续为 0，要求修复读数且不要增加面向开发者的界面注释。

## 决策

1. Status 可选增加 `sessionActivity:{state,count}`，与 `PlanSummary.impact.sessionActivity` 共用同一采集器。`checked` 的 count 为非负整数，`unavailable/unsupported` 的 count 为 null。旧 Host 缺字段显示“未知”。已对齐 Provider 的会话不再被排除。
2. Windows 只读观察所选物理 Codex Home 的 `thread-writer-locks`：要求普通目录、空 `.coordination.lock`、空 UUID `.lock` 文件；逐资源使用 Windows Restart Manager 获取 owner，复核 PID 的创建 FILETIME 及当前进程 image name 为 `codex.exe`。按 UUID 会话计数，不按进程数、不按文件存在数或修改时间计数，不读取锁文件内容。
3. 此处“正在使用”严格指本次资源查询中被上述 Codex writer 持有的会话，包含等待用户输入及后台/子会话。**不是正在生成、前台窗口、所有加载中任务或全局运行态数量。** RM 是 OS 资源使用观察，不是 Codex 公共状态 API；当前 Codex 桌面端的 app-server 使用私有 stdio，不能另起实例并拿空结果冒充现有应用状态。不接管现有 stdio、不改启动参数、不恢复会话。
4. 目录/文件未知格式、非空记录、重解析点、枚举漂移、权限错误、进程身份无法查询、RM 错误或超时均为 unknown；没有该协议的旧 Home 也为 unknown。非 Windows、UNC/WSL Home 为 unsupported。遗留空锁文件无 OS owner 时不计入；多个 owner 指向同一 UUID 最多计 1。PID 重用不冒充旧 owner。
5. 每个快照最多 512 个会话记录；一次隐藏 PowerShell 进程覆盖整批 RM 调用，8 秒 deadline，失败/超时终止子进程，不泄露路径或原生错误文本。仅元数据和资源所有者查询，不打开/获取/释放用户 writer 文件锁、不调用 RM 关进程/重启能力、不读 config/auth/日志/消息来推测活动。采样前后复核目录/文件身份；各资源是时点采样，并非全进程原子生命周期快照。
6. SessionStore 只提供 `readSessionActivity` 只读端口。Status/Provider Prepare 消费它；Main/Renderer 不实现 OS 观察或业务算法。Facade 与契约只允许 `state/count`，禁止传播 PID、线程 ID、文件路径或进程 image path。
7. 首页与同步/切换预览使用相同来源、同一“正在使用的会话”标签，分别在该次请求时采样。预览另有“本次将跳过的会话”，仍是既有 `impact.lockedRolloutFiles`，允许与正在使用数量不同。`sessionActivity` **不参与 Plan revisions、写许可、Apply admission 或写入范围决定**；Apply 在 Home 锁内原样复查实际目标。
8. 停止从 Status 生成已弃用的 `syncSessionUsage`，避免为只读首页再次启动同步写探测；旧可选 DTO 仍可被解析，但 UI 不回退到它或 `lockedRolloutFiles`。`application/session-usage.js` 继续仅供业务 Prepare/Apply 检查实际修改集合。
9. 保持首次/手动刷新、操作完成后的既有单次核验；无后台定时器。正在写入/阻断状态只展示“未知”。卡片只有标题与数字/未知，不增加常驻说明或 tooltip。Provider 字节 I/O、备份 2、CLI 参数、CoreFacade 方法集及 Legacy .NET 不变。

## 验证

- `test/session-activity.test.js`：真实隐藏 Windows 合成 `codex.exe` 进程持有两个 UUID writer leases（含等待输入、中文路径），Status/公开 Plan 均为 2，Provider 已对齐时写入受阻仍为 0；释放后遗留空文件和非 Codex owner 不计入。跨 Home、缺协议、未知格式、变化/权限类失败、deadline、无数据写入均有回归。
- `test/sync-service.test.js` 保留实际目标共享读取但禁止独占写的门禁，明确其不是 activity；`test/provider-sync-lite.test.js` 保留原地写/正文不扫描证据。
- Contracts 与共享 UI 检查旧 Host、未知、双语、Plan 对象白名单、activity 与 blocker 不同值及无额外说明。
- 生产 packaged Desktop E2E 使用合成临时 Home 的真实 OS owner，验证首页/预览为 2、手动刷新后为 0、原始数据不变和无测试 bridge；另跑 Sync → Restore。

本地对真实 Home 只读抽查的结果只证明当前安装版 writer 机制可观察，不代表其他 Codex 版本、Linux/macOS 或所有运行态等价；支持差异显式返回未知，不宣传完整跨平台实时状态 API。
