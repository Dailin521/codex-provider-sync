# vNext 行为兼容 Fixture 清单

## 2026-09-08：Windows 手动安装正式版（ADR-0039 补充）

- `apps/desktop/tests/release-candidate.test.mjs`：默认 RC 严格版本、显式 stable-manual 仅 Windows 1.0.0、tag/SHA 一致、拒绝已有 Release/异常 API、构建/暂存/容器验收共同校验，workflow 保持 Draft-only 且不启用生产自动安装。

## 2026-09-08：Windows 清理与分项计时（ADR-0038）

- windows-rewrite-worker.test.js / windows-provider-bytes.ps1：Force 清理回退、两种更新、正文/身份/mtime、单批聚合、未知计时和 observer/worker 失败。
- provider-sync-lite.test.js、file-update-timing-json.test.js：Facade/CLI、partial 计时、noop 缺失与备份手动 Restore；Contracts file-update-timing.contract.mjs 固定数值白名单。
- Desktop file-update-timing-log.test.mjs 与 UI file-update-timing.vitest.tsx：成功/partial、重启/诊断导出、Watch 终态、旧日志缺失、中英文与毫秒细项；production boundary 增补 packaged Switch 的真实计时和详情。
- benchmark-windows-provider-rewrite.mjs --large-only/--representative --samples=3：正式 worker、D 盘至少 2,000 合成文件，计时参考与 hash/大小/mtime/原地身份门禁分别记录。

## 2026-09-08：切换历史与单轮 Provider Prepare（ADR-0037）

- `provider-preparation-facts.test.js`：3→1 有界首行读取、相同 revision/descriptor、锁/非法/超限/链接/跨目录、读取期追加，以及 Apply 新时间戳和追加字节保留。`plan-apply.test.js` 保留缩短/替换/文件集合变化在备份前拒绝，新增 Switch 根模型与 Provider 原值。
- Desktop `operation-log-service/operation-log-validation/ipc-router`、UI `switch-provider-state/operation-logs-page`：前后值、缺失旧记录、partial、Profile/revision、最近成功的填入/刷新/过滤，不自动执行。
- `scripts/benchmark-windows-provider-rewrite.mjs`：仅手动合成基准，不加入耗时数值门禁；CopyTo/Flush/Replace/mtime 分项与正文 hash/大小/原地身份/时间戳断言。不得用于真实 Home。


## 2026-09-08：同步提速说明与日志双栏（ADR-0036）

- `packages/app-ui/tests/sync-performance-tip.vitest.tsx`：中英说明默认收起、展开零业务调用；Sync/Switch 只凭实际替换 ≥100 提示，不采用预计计数、非法值或 Repair/Restore。
- `packages/app-ui/tests/operation-logs-page.vitest.tsx`：列表/详情独立滚动、选择/返回焦点、筛选/分页/刷新清理、缺失与迟到详情，以及既有字段/手动刷新/复制保持。
- `apps/desktop/e2e/desktop-production-boundary.spec.mjs`：仅临时 userData 合成日志；1366/1024 双栏、683×384 等效 200% 和 390×700 窄屏，验证滚动/布局/焦点，保留临时 Core Sync→Restore fixture。结果另记本轮 evidence。

ADR-0035：`plan-apply.test.js` 覆盖普通聊天追加、非 Provider WAL 更新/checkpoint 不再误过期，实际 Provider/config/schema/清单/替换/截断变化仍拒绝，以及未配置 Provider 的零写入失败；`config-file.test.js` 覆盖单引号/行尾注释/带引号 section。`in-place-transaction.test.js` 仅补齐合成 Provider 声明，保留原有文件身份/正文/partial 断言。Desktop `operation-log-service.test.mjs` 与共享 UI 日志测试覆盖目标/预计计数/实际计数分离、失败原因、复核耗时、未知字段过滤和重启读取。Core I/O 与 Restore crash matrix 必须继续通过，失败与重跑分别记录。

## 2026-09-07：高级功能请求进度

- `test/request-progress.test.js`：真实临时 rollout 的扫描阶段/count、无路径/正文、无写入、无 operation-started、observer 异常、取消，以及 Prepare control 不进入 Apply。
- `packages/app-ui/tests/request-progress.vitest.tsx`：本地耗时、真实阶段进度、未知总数、终态与 Profile 切换清理、晚到帧忽略、明确点击才扫描、修复预览可见进度。
- contracts/core-client、Desktop runtime-protocol/runtime-supervisor/ipc-router/operation-log-service 与 `test/web-server.test.js`：request-progress 白名单、HTTP NDJSON、桌面请求订阅/取消和日志阶段耗时。
- 实现与契约：[ADR-0032](../adr/0032-explicit-scan-and-preview-progress.md)。现有 Provider I/O 与恢复 fixture 保持有效，不能以进度功能放宽写入门禁。

> **状态：Accepted（阶段 0 语义清单；ADR-0016 C2/C3 轻量写 Fixture 已加入）**
>
> **日期：2026-08-24**
>
> **适用范围：Node、迁移期 .NET、CLI、Web 与 Electron Core Runtime 的行为对照**
>
> **架构基线：[vNext Electron + Node 单核心架构](../VNEXT_ELECTRON_NODE_ARCHITECTURE_ZH.md)**

V1 Provider 门禁以 [PIO-1～PIO-6](../architecture/NODE_CORE_ARCHITECTURE_ZH.md#3-provider-io-不变量必须保持) 为准。`test/provider-sync-lite.test.js` 验证无全文正文扫描、32 MiB 合格等字节长度原地更新的正文 Hash/大小/身份、不等长流式复制的正文 bytes，以及 Diagnostics/Repair/C3 partial 收敛。新增 Facade Prepare/Apply 回归限制全文读取入口，检查 ordinal/cwd 与 SQLite model/cwd/updated_at 不变；变化文件跳过后重试可收敛。底层有界首行块预读不等于零尾部字节 I/O。

`npm run core:test:provider-io` 组合上述测试、`test/in-place-transaction.test.js` 与 `test/windows-rewrite-worker.test.js`；`npm run architecture:check` 再组合既有 workspace 边界检查并由 CI 运行。Windows/POSIX 特定用例分别执行或明确 skip。耗时只作参考，不作为数值门禁；本地通过不替代完整平台或发布证据。

## 1. 目的与边界

本文冻结需要被共享 Fixture 表达的场景、输入语义和验收结果，用于迁移期间比较 Node 与 .NET，并为 Node 单核心提供长期回归证据。

C4 已建立私有 workspace、严格 schema 和只向临时目录复制的安全 Runner。C5 检入首批完全合成的 `bidirectional-backup-roundtrip` 与 `foreign-pending-restore` 静态输入：只有 fake provider、空正文 thread row、`session_meta` 和 SQLite seed SQL，不提交 SQLite 二进制、锁文件、平台专属二进制或真实用户数据。Driver 每次在临时目录 materialize SQLite，并为四个 Node↔.NET 方向复制同一输入；现有动态 Node/.NET fixtures 继续补充更广的行为矩阵。

Fixture 不是用户数据样本，严禁从真实 `~/.codex`、认证文件或私人会话复制内容。

## 2. 通用 Fixture 合同

History 项目布局补充：`test/history-project-summary.test.js` 保留平铺词法分组兼容；`test/history-project-tree.test.js` 验证全量 metadata 建树、保存项目根、父子关系、独立分页与孤儿访问。`history-project-menu.vitest.tsx` 验证树形展示、按需展开、无后台读取、右键/键盘菜单和焦点；本地别名由 `project-alias-preferences.vitest.ts` 验证，不写 Codex 数据。`history-actions`、`clipboard-host`、`history-scroll` 和 packaged Electron 回归继续覆盖复制/独立滚动边界，不再以本页五条分组描述当前项目树。

更新入口补充（ADR-0020）：`apps/desktop/tests/public-release-checker.test.mjs` 覆盖固定公开源、正式版本比较、Electron目标资产、超时/HTTP/大小错误；`updater.test.mjs`覆盖手动模式与自动安装隔离、进度推送和失败可重试；`packages/app-ui/tests/settings-updates.vitest.tsx`覆盖设置当前版本、手动查更/下载页、IPC错误反馈和推送不轮询。生产packaged测试只验证入口，不下载真实更新或替换程序。

V1 本地交互补充 fixture：`packages/app-ui/tests/direct-sync.vitest.tsx` 验证默认保留2份、一次 Prepare/同 planId Apply、无二次确认、重复点击、取消 Prepare、失败不写、completed/partial结果与焦点返回；`advanced-features.vitest.tsx` 验证诊断扫描中/失败/重试、旧结果保留与导航不自动扫描；Desktop `runtime-supervisor.test.mjs` 验证诊断独立超时预算、普通读预算保持、超时后迟到响应隔离。均使用合成数据。

未来每个 Fixture 必须声明：

- `fixtureVersion`、稳定 ID、用途和适用平台；
- 输入文件、SQLite Schema/Rows、路径布局和初始 Revision；
- 预期 Status、Plan、Progress、Result 或 Error Code；
- 允许写入的目标；
- 必须保持不变的字段与文件 SHA-256；
- 预期 Backup 内容、Metadata Version 和 Transaction Journal 状态；
- Restore 后的语义状态与逐字节不变量；
- Node/.NET 是否都应运行，以及已裁决的差异。

运行规则：

1. Corpus 永远只读；每次运行复制到新的临时目录。
2. 所有可变绝对路径、时间、PID、operationId 和随机 ID 在比较前规范化。
3. 比较语义结果、目标字段、备份覆盖、Journal 状态和 Canonical Error Code，不比较本地化 message。
4. `auth.json`、Token、凭据和真实消息正文不得进入 Fixture。
5. 故障注入只能作用于临时副本；测试结束后验证未触及真实 Codex Home。
6. 平台无法执行的场景应明确 Skip 原因，不能伪造 Passed。

## 3. Provider、rollout 与模型

| Fixture ID | 输入语义 | 关键预期 |
| --- | --- | --- |
| `default-openai` | 根级显式 `model_provider="openai"`，rollout/SQLite 已对齐 | Status 为 aligned；重复 Sync 幂等；无需改动的字段与 mtime 保持不变 |
| `implicit-openai` | 根级没有 `model_provider` | 当前 Provider 回退为 `openai` 并标记 implicit；Sync 不凭空写入无关配置 |
| `custom-provider` | 配置声明自定义 Provider，历史来自其他 Provider | Switch/Sync 只更新允许的 Provider 元数据；Switch 选择未声明的 Provider 时为 `INVALID_INPUT` |
| `explicit-sync-provider` | V1 调用 `sync --provider ID` 或给 Prepare 注入 Provider | ADR-0016 当前返回 `INVALID_INPUT` 且零写入；v0.5 宽松行为只作 Legacy 兼容记录 |
| `mixed-provider` | sessions、archived_sessions 与 SQLite 中存在多个 Provider | Status 分布完整；Sync 统一目标字段但不要求两类 inventory 数量相等 |
| `archived-sessions` | 只含或混合归档 rollout/SQLite rows | active/archived 作用域保持正确，Restore 能逐字节还原 |
| `root-model` | 根级 model、Provider section model 与 turn_context model 不同 | Follow/Keep/Explicit 三种 Switch 只更新根 config；历史 model 仅由显式 `repair models` 修改 |
| `encrypted-content` | rollout 含来自原 Provider 的 `encrypted_content` | Sync 不读取正文且保持字节；Diagnostics 只报告计数，不修改或导出内容 |
| `large-rollout` | 超大 rollout、超过 64 KiB 的行、Unicode 与特殊 model 字符 | 流式处理且目标字段正确；未修改字节、CRLF 与原 mtime 按合同保持 |
| `status-metadata-boundary` | 大 rollout 首行含 `session_meta`，正文设置禁止读取 sentinel | Status 与 Provider Sync 仅以首行和 stat 完成；只有显式 Diagnostics/Repair 扫描所需正文 |
| `provider-sync-header-only` | rollout 正文设置禁止读取 sentinel | Sync/Switch 成功且正文流未打开；只修改首行 Provider |
| `provider-sync-equal-length-identity` | 32 MiB rollout，Provider JSON 字面量等长 | 只写 Provider bytes；文件 ID、大小和正文 Hash 不变 |
| `provider-sync-unequal-length-body-bytes` | 新旧 Provider 长度不同 | 流式临时文件 + 原子替换；首行之外正文逐字节一致 |
| `diagnostics-read-only-full-scan` | model/cwd/user-event/encrypted 问题并存 | 用户主动运行一次完整扫描；所有目标 Hash 不变，不产生后台刷新 |
| `repair-target-isolation` | 四类 Repair 问题并存 | 每次只修改显式 target；组合 target 共用一次 SQLite 事务 |
| `workspace-roots-implies-cwd` | 仅选择 workspaceRoots | Prepare target 自动包含 cwd，二者在同一 UndoBackup/Operation 中应用 |
| `malformed-rollout` | 截断、无效 JSONL、文件扫描期间消失等 | 不读取越界、不覆盖无法证明安全的内容；按操作返回 skip/error 并保留原字节 |

## 4. SQLite 与存储布局

| Fixture ID | 输入语义 | 关键预期 |
| --- | --- | --- |
| `custom-sqlite-home` | 显式或配置指定外部 SQLite Home，Codex Home 内有陈旧 DB | 只使用权威 SQLite Home；不得回退或修改陈旧 DB |
| `legacy-state-db` | 默认 `<CodexHome>/sqlite/state_5.sqlite` 缺失，根目录 legacy DB 存在 | 仅默认布局允许 legacy fallback；Backup/Restore 原地对应同一位置 |
| `dual-state-db-candidates` | 默认与 legacy 两个候选均存在且活跃度不同 | 两个实现必须选择同一权威候选，未选中的 DB Hash 不变 |
| `sqlite-malformed` | `state_5.sqlite` 不是有效数据库或缺少关键结构 | Status 降级报告 `SQLITE_UNREADABLE`；写操作在 Backup/rollout mutation 前停止 |
| `sqlite-live-wal` | 数据仍在 WAL，数据库由另一连接保持打开 | 官方 online backup 生成单一可独立打开的 main DB；不把 WAL/SHM 当备份清单文件 |
| `wsl-unc-unchanged-hash` | Windows 下 SQLite Home 为 `\\wsl.localhost\...` 或 `\\wsl$\...` | Status 只诊断；写操作返回 `SQLITE_UNSUPPORTED_PATH`，配置、rollout、DB、global state 与 backup root 的 Hash/存在性全部不变 |
| `real-wsl-unc-strict` | 健康 Windows+WSL2 runner 从真实 ext4 Home 调用 Windows Core/Electron | `CPS_REQUIRE_REAL_WSL=1` 时缺少可运行发行版或 `CODEX_PROVIDER_SYNC_WSL_SQLITE_HOME` 必须失败而非 Skip；main DB、WAL、SHM、journal 的存在性与 Hash 全部不变 |

## 5. 锁、并发与恢复

| Fixture ID | 运行方式 | 关键预期 / 安全门槛 |
| --- | --- | --- |
| `locked-rollout` | 平台真实文件锁 | 锁定文件不被写；其他安全目标可形成 Partial Result；返回 `ROLLOUT_LOCKED` 语义并可重试 |
| `active-rollout-changing` | 扫描后、应用前改变目标 | 变化文件被跳过或 Plan 失效；不覆盖 Codex 新写入；Prepare/Apply 返回 `STALE_STATE`（`details.reason=rollout`），旧直连入口可保留 `ROLLOUT_CHANGED` |
| `sqlite-busy` | 真实 SQLite 写锁 | 在 rollout mutation 和 Backup 前阻断，返回 `SQLITE_BUSY`，全部原始 Hash 不变 |
| `node-dotnet-lock-contention` | 启动真实 Node 与 .NET 进程争用同一 `<CodexHome>/tmp/provider-sync.lock` | 恰有一个写者获得 protocol v2 锁；另一方为 `OPERATION_BUSY`；败方不创建 Backup、不改任何目标 |
| `shared-sqlite-home-contention` | 两个不同 Codex Home 指向同一 SQLite Home，并发写 | SQLite 原生事务串行化或返回 `SQLITE_BUSY`；重复执行最终收敛，不产生数据库并行破坏 |
| `dual-resource-lock-order` | 历史 V1 双锁 Fixture | Legacy evidence only；不再作为当前 Node 普通写门禁 |
| `sqlite-resource-lock-unverifiable` | 历史 State DB resource lock Fixture | Legacy evidence only；新普通写不创建或获取 State DB resource lock |
| `restore-missing-state-db-parent` | Metadata v1/v2 Restore 指向缺失 DB，且其物理父目录也不存在 | Restore 通过 Home lock、目标边界和 RestoreRecovery 校验 fail closed；不得在无法证明目标时创建 snapshot 或 mutation |
| `lock-unverifiable` | future protocol、损坏 owner、进程启动身份不可读、ABA/目录身份变化 | fail closed，返回 `LOCK_UNVERIFIABLE`；不得误报普通 Busy，不得自动删除不可证明归属的锁 |
| `external-write-status-snapshot` | 真实第二进程持有 Home lock 并在锁内改变 config/SQLite | Core 与 Local Web 不扫描中间态；有缓存时保留最后完整 snapshot 并附 operation，无缓存时 `rolloutScanComplete:false` |
| `plan-ledger-replay-expiry` | Plan 过期、重放、跨 operation、重启失效、篡改 apply payload | 只允许当前进程内 10 分钟单次消费；失效返回 `PLAN_EXPIRED`，附加字段返回 `INVALID_INPUT`，均无 Backup/Journal/mutation |
| `plan-ledger-abandoned-expiry` | Prepare 后调用方离开且没有 consume/waiter；同一 Home 有多个不同 expiry 的人工 intent | 单一最早到期 timer 自治清理并 rearm，不阻止进程退出；Watch 在最后 intent 到期后只恢复一次，不产生每 waiter timer |
| `watch-manual-priority` | 单一文件事件触发 Watch，但人工 Apply 已持有本进程协调器；等待期间继续产生重复事件 | Watch 不并发、不计失败；保留并合并 reasons，人工 operation completion 后恰运行一次 follow-up；stop 后 callback 不再 Apply |
| `watch-physical-scope-dedupe-and-bounded-history` | 同一物理 Codex Home 通过重复、并发或路径别名启动 Watch，并在自动/手工停止后重启 | 只创建一个活动 watcher、返回同一 watchId 且首个 options 生效；停止释放 scope，旧 watch 仍可查询/幂等 stop；最多保留 64 个 stopped 记录 |
| `pending-journal` | Managed Backup 中存在未终结 Journal | Restore journal 阻断新普通写；旧 Sync/Switch journal 只由 Diagnostics 报告、不阻断，但 Prune 仍保护关联备份 |
| `normal-write-no-journal` | Sync/Switch/Repair 有实际写入 | 只创建 UndoBackup，不创建普通 transaction journal 或 State DB lock |
| `backup-failure-zero-write` | UndoBackup 在 mutation 前失败 | 返回 `BACKUP_FAILED`；config/rollout/SQLite/global state 全部不变 |
| `mutation-failure-partial-retry` | 在 config、rollout 或 SQLite mutation 后注入失败 | 返回 partial、backupId、failedStage/failureCode/retryRecommended；重复相同操作最终收敛 |
| `foreign-pending-restore` | Node 创建 Pending Journal/Backup 后由 .NET Restore，及反方向 | 两个方向都只按受管清单恢复，清除 Pending 前必须落入合法 terminal；差异需显式裁决 |
| `restore-mid-failure` | Restore 在某一目标已替换后注入失败 | 不能报告成功；必须完整补偿，或保留可操作证据并返回 `RECOVERY_REQUIRED`，不得留下无 Journal 的半恢复状态 |
| `restore-v2-pre-snapshot-failure` | Restore v2 的恢复前 snapshot 在任何目标 mutation 前失败 | `BACKUP_FAILED` 或更具体失败；不创建 restore mutation，source backup 与原始目标 Hash 不变 |
| `restore-v2-journal-crash-matrix` | Restore v2 在 prepared/applying/committing/committed-pending-ack/rollback-pending 和 ack 窗口终止 | 非 terminal 阻断普通写并可由 pre-restore snapshot 或目标 hash 显式收敛；completed/rolled-back/recovery-required 经重新读取确认，不能反向改写 terminal |
| `restore-v2-foreign-pending` | Node 或 .NET 留下 Restore v2 pending，另一运行时选择不同 source backup 尝试 Restore | 在新 snapshot/journal/mutation 前返回 `RECOVERY_REQUIRED`；foreign raw journal、全部受管 backup inventory 与业务 Hash 不变；未知版本同样 fail closed |
| `restore-v2-resolver-projection` | 一运行时留下同 source pending，另一运行时以相同 source、持久化物理 Home 和完整 target coverage 显式 Restore | pending、resolver 与当前 locked Home 的稳定物理 identity 全部匹配时，新 Restore 创建独立 snapshot/journal 并耐久到 `completed`；旧 raw journal 不改写，由 exact `resolvesOperationIds` 投影为已解决；Prune 继续保护旧证据 |
| `restore-v2-manifest-prepared-binding` | 重写 snapshot manifest 的 source/storage/resolver/target 或 snapshot 目录并同步重算 `prepared.manifestSha256` | Node 与 .NET 都在 compensation/ack 前返回 `RECOVERY_REQUIRED`；不读取替换 snapshot、不补偿、不确认完成，业务 Hash 与 source backup 不变 |
| `restore-v2-persisted-physical-home-binding` | pending 持久化 Home A；相同 lexical Home 经 junction/reparse 换接到 B，或 resolver 持久化 B | B 上 Restore 在新 snapshot/mutation 前返回 `RECOVERY_REQUIRED`；A 的 raw journal 不改写，completed resolver 不能隐藏它；Node↔.NET 双向一致 |
| `restore-v2-windows-path-alias` | 同一 Windows 物理 source 分别以 8.3 短路径、长路径与 junction 创建/恢复 journal | Node↔.NET 两个方向均把 Prepare/Apply/journal 绑定到稳定物理 source，另一运行时可用另一别名恢复；无法证明时 fail closed，不以 lexical 字符串、旧式 backupId 或换接后的当前目标放行 |
| `restore-v2-reparse-swap` | snapshot/apply 后把 rollout 或其父目录换成指向 Home 外的 junction/symlink，再进入 compensation/ack | 每个 mutation/ack 边界重新验证物理 Home 与 reparse segment；外部目标字节不变，journal 进入 `recovery-required`，不得按相同内容 hash 误确认 |
| `restore-v2-ack-reconciliation` | `committed-pending-ack` 已持久化但 API acknowledgement/observer 失败 | 不把已提交 Restore 报为可回滚失败；重新读取 journal 与目标 Hash 后收敛到 `completed` 或 `recovery-required` |
| `desktop-update-install-gate` | Update 已下载，同时注入已入场但未 dispatch 的 write、后续 write、自动停止/active Watch、pending recovery、无法验证 Profile、installer 异常或安装竞争 | 未授权候选的 check/download/install/timer 均不创建 updater port；获授权路径先关闭 gate、排空写、由 Main 重新查询 Utility Watch 状态并刷新全部 Profile。任一条件不安全或查询/installer 失败都不退出并重新开放 gate。IPC 为 null-only，DTO 不含 URL/path/release notes/raw error |
| `desktop-diagnostics-capability-bound` | Renderer 重复请求诊断导出、复用 token、并发消费、选择相同目标或以路径别名指向同一物理 ZIP | 目标仅由 Main 选择；5 分钟 TTL、最多 32 个 pending、规范化目标独占 reservation，写入时按父目录 realpath 拒绝物理目标并发、单次消费；过期/revoke/完成后释放且 ZIP 无路径/凭据/正文 |
| `bidirectional-backup-roundtrip` | Node Backup→.NET Restore；.NET Backup→Node Restore | 两个方向恢复到等价语义状态；正文和不应变化字段逐字节一致；Metadata v1/v2 兼容边界明确 |
| `historical-tag-produced-backup-restore` | 从冻结 commit 的 `v0.2.9`/`v0.4.1` tag 源构建历史 .NET Core，真实产生 synthetic metadata v1/v2 backup，再由当前 Node Restore | config/rollout/SQLite 恢复；tag commit、metadata/tree Hash 与 synthetic-only 声明进入 CI artifact。证据等级仅为 repository-tag-source，不等于 hosted formal Release binary 或真实用户数据 |
| `historical-formal-release-backup-restore` | 下载固定 release/tag/asset ID、size 和 SHA-256 的 hosted `v0.4.1` Automation ZIP；同时核对 GitHub Release API、独立 `.sha256`、`checksums.txt`、archive entry set 与 executable Hash 后，才在隔离 synthetic Home 执行 Plan/Apply | 历史正式托管二进制真实生成 metadata v2 managed backup；当前 Node Restore 逐字节恢复 config/rollout，并恢复 SQLite Provider；随机 `auth.json` canary 不进入 backup 或脱敏 artifact。证据必须绑定同一 CI run/tested commit，并明确该历史二进制与 tag 未签名，因此不能替代真实 Beta、代码签名或生产升级验证 |
| `journal-crash-matrix` | 历史普通写 crash matrix | Legacy evidence only；新普通写由 partial/retry 取代，Restore v2 crash matrix 继续是现行门禁 |
| `rollback-recovery-required` | 历史普通写自动回滚失败 | Legacy compatibility only；新普通写保留 UndoBackup 并返回 partial |

真实跨运行时测试不能用 Mock 代替进程争锁。Node 与 .NET 必须在同一临时目标上运行，并以文件/SQLite 最终效果作为独立证据。

ADR-0016 C2/C3 executable mapping：Provider-only/32 MiB/Diagnostics/Repair/no-journal/partial-retry 见 `test/provider-sync-lite.test.js`；Plan/revision/cancel/shared SQLite 见 `test/plan-apply.test.js`；Home lock 与外部 Status 见 `test/status-coordination.test.js`；Watch 见 `test/watch.test.js`；Web/CLI/Contracts/Desktop 分别见相应 contract 与 E2E。旧双层锁、普通 journal 与 Node↔.NET State DB resource lock 测试仅作为历史证据，不证明当前 Node 写路径。

C7 的 executable mapping 为 `test-support/desktop-sync-switch-fixture.mjs`、`apps/desktop/e2e/desktop-sync-switch.spec.mjs`、`apps/desktop/tests/ipc-router.test.mjs`、`runtime-supervisor.test.mjs` 与 `test/plan-apply.test.js`。它使用临时 Home、真实 SQLite、Windows `FileShare.None`、受控 test-build Utility 终止和完整目标 Hash/语义快照；生产 Core host control 不包含故障注入能力。`scripts/test-wsl-unc-safety.sh` 以 `CPS_REQUIRE_REAL_WSL=1` 提供严格真实 WSL 门；没有与 source commit 绑定的健康 Windows+WSL 结果时仍是 Pending，不能用 synthetic UNC 或代码开关冒充实证。详见 `evidence/C7_ELECTRON_SYNC_SWITCH_2026-08-26.md`。

C8 的 executable mapping 为 `test/restore-v2-state-machine.test.js`、`RestoreJournalServiceTests`、`RestoreV2IntegrationTests`、`test-support/cross-runtime-fixtures.mjs`、Node/.NET CrashHost、`test/watch.test.js`、`test/operation-coordinator.test.js`、`apps/desktop/tests/updater.test.mjs`、Desktop contracts/unit tests、`apps/desktop/e2e/desktop-restore-relocation.spec.mjs` 与隐藏模式 Electron E2E。跨运行时 harness 覆盖 applying/prepared/committing/rollback-pending、commit ack、foreign pending、unknown schema、Windows 物理路径 alias、manifest/prepared 全量绑定和 persisted physical Home mismatch；Windows alias 用例通过系统返回的实际 8.3 短路径和真实 junction，让 Node 与 .NET 分别以别名创建 pending、由另一运行时以物理长路径恢复，也保留长路径创建/别名恢复矩阵，并校验原 journal bytes 不改写。原 journal 保留与 resolver projection 是显式裁决，不再把 raw 非终态伪报为 `rolled-back`。Updater fixture 只使用注入 port，不访问真实 Release；C9/C10 必须另做获授权签名产物的检查/下载/重启升级 smoke。

历史备份兼容 executable mapping 分为两个证据等级：`test-support/historical-tag-backup-fixtures.mjs` 证明冻结 repository tag-source；`test-support/formal-release-backup-fixtures.mjs` 和 `test-support/formal-release-assets.v1.json` 证明 checksum-bound hosted v0.4.1 Automation Release asset。后者仅在 Windows/Node 24 临时目录、严格环境白名单中执行，先固定并核对 release/tag/asset/archive/executable、解压前拒绝越界条目、执行前二次核对 executable，再执行历史 Plan/Apply 与当前 Node Restore；CI 只上传同一 run/commit 绑定的脱敏 hash evidence，不上传二进制、SQLite、rollout、完整 backup、路径、凭据或正文。fork PR 不执行 hosted binary；同仓库 PR 的 bundle 仍只是未受保护 source 的审查预览，只有受保护 `main` 上由同一 required workflow 重新生成的 artifact 才能成为最终证据。该项不是 vNext/Electron/GUI Release，也不是历史版本自身 Restore；历史 tag 与二进制 `NotSigned`，GitHub API、固定清单和 Hash 只提供完整性与托管来源绑定，真实 Beta、签名和生产升级仍保持独立门禁。

## 6. Restore、Backup 与 Prune

| Fixture ID | 输入语义 | 关键预期 |
| --- | --- | --- |
| `restore-relocation` | Backup 的 SQLite Home 与当前目标不同；Electron Renderer 只看到 `sqliteHomeConfigured` 摘要 | 默认拒绝；只有可信命名 profile 的显式目标和 relocation 确认才允许，且跨 SQLite Home Restore 不恢复 config。隐藏 Electron UI 回归必须证明 source 业务状态不变、目标 DB 恢复 |
| `prune-managed-only` | backup root 同时包含受管备份、普通目录和 Pending Journal 引用 | 只删除超过保留数的受管备份；普通目录和 Pending Journal 所在目录永不删除 |
| `default-backup-retention` | Sync、Switch、Repair 或 Prune 未显式提供 keepCount | Node Core、CLI、Web 与 Electron 默认保留最近 2 份托管备份；显式 keepCount 继续覆盖默认值 |
| `backup-first-no-mutation` | Backup 期间空间、权限或 snapshot 失败 | 返回 `BACKUP_FAILED`；不存在 Journal/目标 mutation；原始 Hash 不变 |

`bidirectional-backup-roundtrip` 与 `foreign-pending-restore` 是迁移期淘汰 .NET 前的强制门槛，不因单向 Restore 成功而视为通过。

## 7. Workspace 与 History

History滚动回归：`history-scroll.vitest.tsx`保护视口壳、独立可聚焦滚动区、切换会话右侧复位及其他页面滚动；`launch-history-scroll-fixture.mjs`为Playwright CLI提供50会话/82消息隐藏Electron，实测滚轮后另一栏、详情标题和外层坐标不变，展开信息无外层溢出，覆盖1280×800、760×560、380×280视口。

Desktop复制回归：`packages/app-ui/tests/clipboard-host.vitest.tsx`覆盖共享UI优先Host、浏览器拒绝时仍可复制、失败提示和不增加History读取；`apps/desktop/tests/clipboard.test.mjs`、`ipc-router.test.mjs`覆盖严格schema、UTF-8字节限制、来源拒绝、无Core/日志调用；`desktop-readonly.spec.mjs`使用合成fixture和隐藏窗口，在原生写入边界捕获文本，不操作用户剪贴板。`advanced-features.vitest.tsx`验证中英文诊断差异/加密计数说明且不自动修复。

| Fixture ID | 输入语义 | 关键预期 |
| --- | --- | --- |
| `workspace-roots` | global state、rollout cwd 与 SQLite cwd 不一致，含跨平台路径形式 | 只修复合同允许的 workspace/cwd 元数据；路径规范化一致；Backup/Restore 覆盖 global state |
| `history-safe-content` | user/event/response-item 重复消息、无 thread id、同 id 多 rollout，并包含大正文 rollout 与多个大 decoy | 无 query 列表只读受限首行 metadata、返回 `messageCountKnown=false` 且 UI 不显示伪 0；显式搜索仍可全文匹配并返回精确计数；详情定位只深读用户选择的 rollout；列表选择稳定会话；正文不进入日志、诊断包、Query cache 或应用数据库 |
| `desktop-readonly-c6` | 临时 Codex Home 含无标题 rollout、带保存标题的真实 SQLite row、valid pending journal 与正文 marker | production bridge 无测试/Node 能力；概览显示当前完整路径；列表使用 SQLite 保存名称，列表/Profiles/Diagnostics 无路径和正文；显式详情后才显示 marker；错误 channel 写 IPC 拒绝；Utility crash 后按 profile preflight 并恢复；测试前后 Codex Home 全树 Hash 不变 |
| `desktop-status-display-paths` | 默认、config、env、Profile override、legacy DB 与缺失 DB | 可信 Desktop 同一 snapshot 显示 Codex/SQLite Home 和实际 DB 路径；默认 Core/Web 无 displayPaths；请求无法开启可信选项；刷新和 Profile 切换后不残留旧路径 |
| `history-saved-titles` | Home 名称索引、重复改名、损坏/超长记录、超长 SQLite title、config 与 default DB 不同、Profile override、缺 title 列 | 名称索引最新有效名称→所选 DB title→metadata，列表和详情一致；超长保存标题截短，坏记录不阻断回退；绝不把 first_user_message 或聊天正文当标题，不写 DB |
| `history-unnamed-identities` | 无标题子代理、嵌套/根级 agent metadata、普通无标题会话、已有标题 | 可选 subagentName 仅包含任务末段/昵称，最长160；已保存标题优先；空标题 UI 显示本地化子任务名或日期+ID末8位；列表详情一致、不隐藏记录、不扫描正文、不写回名称 |
| `history-session-actions` | 真实/内部ID、明确父关系、metadata/content 查询、主子过滤、metadataOnly详情 | UI默认全部+metadata；复制操作不深读正文；无真实ID不生成继续命令；筛选先于分页；按会话ID通过Main定位，不允许任意路径；详情路径只供可信Desktop展示；文件更新时间不冒充聊天时间 |
| `history-project-root-pagination` | 保存工作区/标签、嵌套目录、同名根、超过50条主会话、页外父子、缺父/循环及后代 | 项目视图先建全图再按项目主会话分页；子任务独立按需读取并继承明确父项目；孤儿单独可访问；main计数不含子任务；搜索保留父链；无正文/路径泄漏，平铺API不变。映射 `test/history-project-tree.test.js` |
| `history-project-ui-preferences` | 项目独立加载、子任务折叠、项目本地显示名、失败与Profile切换 | 无后台请求；右键/键盘可操作；显示名只经Host偏好存储，按Profile/revision及项目指纹隔离，失败不伪报成功；不修改会话标题/目录/原始数据；左右栏滚动与窄屏返回保持。映射 `history-project-menu.vitest.tsx`、`project-alias-preferences.vitest.ts` |
| `everyday-sync-advanced-repair` | 普通Sync失败、进入/重入高级功能、完整诊断失败、展开与提交专项修复 | 不自动升级为诊断/修复；完整诊断只手动触发，Repair默认折叠且全不选，先Prepare再确认；默认2份备份；中英文不声称能修ordinal或重建显示索引。映射 `packages/app-ui/tests/advanced-features.vitest.tsx` 和 `test/provider-sync-lite.test.js` |
| `desktop-release-candidate-c9` | 四个 host-native target 各自生成两个最终发行容器；容器只含 synthetic build content 与 target-native SQLite binding | 每个容器解包/安装后重新审计 ASAR/Fuse/embedded integrity/native binding，隐藏执行 Status 与 Sync→Restore，正常退出；NSIS 卸载清理；SBOM/manifest/checksum 完整闭包；四目标 aggregate 的 version/commit/lock/tool/policy 一致；任何 source map、fixture、凭据名、真实数据、非目标 binding 或未清单文件均阻断 |
| `desktop-history-two-pane` | 多会话、Markdown/代码、超过 200 条消息及无标题 metadata | 宽屏双栏、窄屏单页；整行选择后才深读；Assistant Markdown/代码复制；截断提示；输入搜索不触发扫描，Enter/按钮才运行；无标题不读首条消息 |
| `desktop-operation-log` | Prepare 等待后 Apply，success/partial/cancel/dismiss/crash/restart 与多阶段 progress | Prepare/Apply 单条记录；active/wall/阶段耗时、计数和关联 ID 完整；重启遗留为 interrupted；仅初始/手动刷新；5×5 MiB 轮转；诊断包包含脱敏日志 |
| `desktop-profile-directory-token` | 原生目录选择取消、token 重放/类型错用、revision 竞争、默认 Profile 修改与活动写/Watch | Renderer 不见原始路径；token 短时单次消费；Main 生成 ID；默认不可改删；保存后选择新 Profile 并刷新；活动写/Watch 时拒绝修改 |
| `desktop-windows-size-budget` | Windows x64 packaged output | `app.asar≤3 MiB`、解包≤280 MiB、NSIS≤105 MiB、ZIP≤130 MiB，candidate 按本次目录/版本验证全部四项；locale 精确为 en-US/zh-CN；许可证原文归档 byte/hash 一致；隐藏启动加载 SQLite 并完成 Sync→Restore |

C6 executable mapping：`test-support/desktop-readonly-fixture.mjs`、`apps/desktop/tests/*.test.mjs`、`apps/desktop/e2e/desktop-production-boundary.spec.mjs` 与 `desktop-readonly.spec.mjs`。production unpacked smoke 通过 `apps/desktop/scripts/run-packaged-e2e.mjs` 解析当前平台 builder 输出；Windows/macOS/Linux Node 24 job 同时验证正常 production bundle、真实 SQLite/History 边界和 test build 的 Utility crash/restart。正式安装器、双架构 macOS 发行产物和 native fallback 留在 C9。

C9 executable mapping：`apps/desktop/tests/release-candidate.test.mjs`、`apps/desktop/scripts/build-candidate.mjs`、`stage-candidate.mjs`、`release-audit.mjs`、`smoke-candidate-artifacts.mjs`、`verify-candidate-set.mjs`、`apps/desktop/e2e/desktop-production-boundary.spec.mjs` 与 `.github/workflows/ci.yml` 的 `electron-release-candidate`/`electron-candidate-set`。本地 Windows 只证明 Windows x64 ZIP/NSIS；macOS x64/arm64、Linux x64 和四目标 aggregate 必须由 required CI 证明，不能手工补造。

## 8. CLI JSON 动态 Fixture

C2 使用真实 Node 子进程和完全位于临时目录的最小 Core fixture 固化 JSON Mode；这些 harness 不含真实 Codex Home、凭据或消息正文。

| Fixture ID | 运行方式 | 关键预期 |
| --- | --- | --- |
| `cli-json-envelope-v1` | 对所有有限命令启动真实 CLI/组合入口 | stdout 恰好一个 JSON 文档，顶层键固定为 schemaVersion/command/ok/outcome/result/warnings/error |
| `cli-json-exit-matrix` | 子进程注入 success/noop/partial/rolled-back/stale/recovery/busy/lock/cancel | 退出码固定为 `0/1/2/3/4/5/130`，且与 Error Code 分层 |
| `cli-json-progress-isolation` | 真实 Sync 与受控 progress observer | 进度仅进入 stderr；stdout 不含阶段文本或 backup path |
| `cli-json-daemon-rejection` | `watch --json`、`web --json` | 在创建长运行状态、runtime descriptor 或浏览器进程前返回 `INVALID_INPUT`/exit 2 |
| `cli-json-redaction` | 非法参数值、unknown/typed error、恶意 details、越权 result 字段、循环结果、stdout EPIPE | 固定错误文案与命令级字段 allowlist 不泄漏 stack/cause/secret/token/prompt/message body；terminal writer 最多尝试一次 stdout |
| `cli-human-compat` | 不传 `--json` 运行既有 help/input/sync 路径 | Human 输出和既有 `0/1`、partial 行为不变 |
| `installed-root-entrypoint` | 从真实根 npm tarball 安装后，经 npm bin shim/Windows 规范化路径运行 `help`、临时 Home `status --json`，再执行 `sync --json → drift → restore --json` | CLI 必须实际执行并创建 managed backup；config/rollout 字节与 SQLite Provider 恢复且无 pending recovery；不得因短/长路径、大小写或链接形式不同而静默退出，也不得引入 Electron/workspace runtime 依赖 |

这些用例当前由 `test/cli-json-contract.test.js`、`test/cli-json.test.js`、`test-support/cli-json-driver.js` 和真实 Core Sync 回归承载；未来迁入 `packages/test-fixtures` 时必须保持同一外部合同。

## 9. Corpus 结构与后续扩展

目录骨架、Schema、安全 Runner 与首批 `static/` Corpus 已存在；后续 fixture 按同一边界扩展：

```text
packages/test-fixtures/
├─ schema/
│  └─ fixture.schema.json
├─ static/<fixture-id>/
│  ├─ fixture.json
│  ├─ input/
│  └─ expected/
└─ builders/
   ├─ sqlite/
   ├─ locks/
   └─ crash-hosts/
```

SQLite live WAL、真实文件锁、跨进程 crash 和 WSL UNC 不能作为静态字节目录伪造，必须由受控 Builder/Harness 在临时目录创建。静态部分只保存最小、无敏感内容、可审计的源输入。

## 10. Node / .NET 对照与差异登记

每个双运行时 Fixture 使用两份相同输入副本：

1. Node 执行并输出规范化结果与最终 Hash；
2. .NET 执行并输出相同维度证据；
3. 比较 Status、Plan、目标字段、Backup、Restore、Journal 与 Error Code；
4. 差异必须记录“Node 行为 / .NET 行为 / 权威选择 / 安全理由 / 对应测试”；
5. Node 是 vNext 目标核心，但不能以“新实现”为理由静默覆盖更安全的既有行为。

## 10.1 V1 用户体验审查增量（ADR-0024）

| Fixture | 自动证据 | 固定行为 |
| --- | --- | --- |
| profile-scoped-watch-feedback | `packages/core/checks/core-surface.contract.mjs`、`packages/app-ui/tests/settings-watch-scope.vitest.tsx`、Desktop IPC 测试 | 不同 Home 隔离，同 Home 显式别名去重；旧请求取消、错误不跨配置 |
| backup-read-error-and-captured-targets | `packages/app-ui/tests/ux-review-fixes.vitest.tsx` | 错误不是空列表，可重试；选项匹配实际捕获，旧备份兼容，relocation 取消配置恢复 |
| selected-profile-preservation | 同上 | 只在删除当前配置时回默认，不因删除其他配置切换 |
| history-detail-local-retry | 同上 | Retry 不重读列表，不重建展开/分页，正文不缓存 |
| overview-information-before-actions | `packages/app-ui/tests/overview-paths.vitest.tsx`、Web/packaged Electron E2E | 摘要→分布→存储/路径左与Sync右同排→独立最底部Switch；宽屏摘要四项一排，1280×720/1366×768首屏同步按钮完整可见；窄屏存储→Sync，不裁剪路径或改变操作 |
| partial-log-feedback | `packages/app-ui/tests/partial-feedback.vitest.tsx`、`apps/desktop/tests/operation-log-service.test.mjs`、`test/watch.test.js` | 嵌套失败字段/计数及备份 ID 保留，日志重启一致，返回/恢复预览严格匹配配置版本 |
| cli-progress-operation-isolation | `test/cli-json-contract.test.js`、`test/sync-service.test.js` | Sync/Switch 六阶段不变，Repair 单独七阶段 |

## 10.2 V1 反馈与窗口偏好增量（ADR-0025）

| Fixture | 自动证据 | 固定行为 |
| --- | --- | --- |
| unknown-status-is-not-zero | `packages/app-ui/tests/ux-polish.vitest.tsx` | 无快照/加载/失败不误报不对齐或零计数，旧快照刷新提示 |
| writer-owned-session-activity | `test/session-activity.test.js`、`packages/app-ui/tests/overview-session-usage.vitest.tsx`、`plan-review.vitest.tsx`、Contracts、Desktop production E2E | ADR-0030：真实合成 OS owner 持有两个 writer 会话即计 2（同一进程、已对齐、等待输入、中文路径），释放后遗留文件为 0；非 Codex owner 不计；缺协议/未知/失败/超时不伪报零；Status/公开 Plan 同源且与实际写入 blocker 分离，原始数据不变 |
| preview-write-blockers | `test/sync-service.test.js`、`test/provider-sync-lite.test.js`、`test/windows-lock-probe.test.js` | 实际目标共享可读但占用不可写时预览将跳过；已对齐不进入写入集合。Windows 私有探测返回完整计数和整数索引，不回传路径，中文文件名在 ASCII stdout 下仍可正确识别；协议缺失/损坏/越界均拒绝。Apply 原检查和 PIO 门禁不变，不能以 sessionActivity 代替写许可 |
| overview-session-usage-product-copy | `packages/app-ui/tests/overview-session-usage.vitest.tsx` | 中英文卡片只显示标题和数字/“未知”，不常驻同步范围等开发注释；真实数据源另由 writer-owned-session-activity 用例验证 |
| switch-target-current-provider | `packages/app-ui/tests/switch-provider-state.vitest.tsx` | 异步首读和手动刷新跟随当前 Provider，不以候选首项代替；保留手动目标、成功后重建默认基线；失败不重置，切换 Profile 重置草稿；卡片改名不改变 config 后同步的语义 |
| prune-estimate-and-confirm | 同上、`provider-sync-mode.vitest.tsx` | 默认 2，上限估计、0 提示、确认/取消、列表或配置版本改变阻断 |
| current-profile-badge | `packages/app-ui/tests/ux-polish.vitest.tsx` | 当前使用标记与编辑选择独立 |
| post-write-status-verification | 同上 | 单次现有 Status 复核、失败不使用旧值、完整/不完整对齐明确区分 |
| dismissible-operation-notification | `packages/app-ui/tests/toast-dismiss.vitest.tsx` | 点击标题/描述/关闭图标、Enter/空格/Esc、双语标签、仅关闭选中通知、可再次通知及原有自动收起；不接入 Core、日志、备份或刷新 |
| history-draft-reset | 同上 | 草稿不扫描，清除回元数据默认筛选，不缓存正文 |
| desktop-window-state | `apps/desktop/tests/window-state.test.mjs` | 正常边界/最大化分离、负坐标/显示器失效/小工作区、原子偏好、退出 flush 和测试隔离 |

## 11. 阶段验收边界

ADR-0031 fixture：`packages/app-ui/tests/repair-clarity.vitest.tsx`、`advanced-features.vitest.tsx`、`plan-review.vitest.tsx` 和 Desktop production E2E 验证专项修复三项/模型高级调整独立默认空选，双语用途和实际 Plan 说明；当前完整诊断仅提供展开/聚焦，不自动选中或请求；旧/失败/不完整/不可写结果不推荐；切换 Profile/revision 重置、禁用表单提交不绕过。保持原模型调整 → Restore hash 回环和 Provider I/O fixture。

ADR-0028 fixture：`operation-log-validation.test.mjs`、`operation-log-service.test.mjs` 与 Desktop Sync/Switch E2E 验证真实 Preload 可读 revision/partial 日志、阶段耗时及轮转后内存/重启一致；`test/history-lookup-cache.test.js` 用 300 个合成文件证明稳定详情只打开所选文件两次，并验证重复 ID、移动和 Home 隔离。`test/watch.test.js`、Desktop runtime protocol/supervisor/IPC 与共享 Settings Watch 测试验证终态一次、早于响应、旧 generation、别名、离页更新和不复活。`core-js-check-boundary.contract.mjs`/`storage-types.ts` 固定类型豁免与端口参数；`test/publish-npm.test.js` 只用模拟 runner 验证所有失败阻断发布、dry-run 无认证/发布、Node 16 门禁不可省略。四平台 updater 单元测试固定未授权安装通道时的手动查更回退，不冒充 macOS/Linux 实机测试。

ADR-0027：`packages/app-ui/tests/backup-retention.vitest.tsx` 验证唯一数量入口、持久化/重开、跨 Profile 同值、Sync/直接 Sync/Switch/Repair/Watch 参数一致；草稿/保存不清理、活动 Watch/查询/存储失败不保存、非法值拒绝。`test/watch.test.js` 以真实临时备份池验证显式 keepCount=1 和默认 2 的自动同步清理。Desktop Sync/Switch E2E 改用统一入口保存 5，确认未创建/清理备份，再进行各操作，清理前保存 2 并确认。

ADR-0026 包体 fixture：`apps/desktop/tests/package-size.test.mjs` 验证 Windows Chromium 许可证完整原文归档、损坏检测、失败保留原文、构建输出路径边界、指定目录/版本尺寸门禁及严格参数解析。生产输出显式 minify，预算 1.5 MiB；Windows ASAR/解包预算 3/280 MiB。压缩后仍需原生 SQLite、生产包 Sync → Restore 和最终容器验证，不用下载大小冒充安装占用。

ADR-0021 增量 fixture：`test/repair-scoped.test.js` 覆盖原生会话范围隔离和修后核验；`test/advanced-repair-facade.test.js` 覆盖公开输入、预览和核验 DTO；`test/history-integrity.test.js` 覆盖只读有界记录检查、未知/超限不伪报完整、显示索引明确未验证。共享 UI 对应专项修复选择、诊断过期与核验回归；所有测试使用合成临时数据，不修复真实会话。

ADR-0034：上述 Repair fixture 增加字段合计/分类/去重会话、固定工作区设置类别与 Prepare 零写入；`plan-review.vitest.tsx` 验证全局只读明细、双语数量标签、100 条截断和独立滚动。`status-coordination.test.js` 验证诊断事实只扫一次、不额外全文哈希、漂移/后 revision 失败保留本次事实及最终真实 Home 锁优先；`request-progress.test.js` 验证每个事实扫描阶段仅完成一次。`advanced-features.vitest.tsx` 验证未完整结果提示、保留结果和不自动重试。

阶段 0 的完成标志是本文场景、预期和安全门槛获得确认。C5 已为 `bidirectional-backup-roundtrip` 与 `foreign-pending-restore` 建立真实跨进程 Windows harness 和 required CI job；这只证明这两个 Phase 2 门槛，不代表 Restore v2、全 crash matrix、WSL 或三平台产物等价。其余 Corpus、Builder 与 CI Matrix 必须在对应 checkpoint 真正通过后才能宣称完成。

ADR-0033：`test/status-coordination.test.js` 使用临时合成 rollout，在首行读取之后确定性追加，验证首次/缓存状态漂移无假 Operation、最多一次重试、首轮/扫描后/重试后 revision 失败均未核验、重试期间真实 Home 锁优先、HTTP DTO 不误健康。`test/cli-json-contract.test.js` 验证降级 Human 不崩溃、不输出假计数、JSON 保留原因不泄漏细节；`packages/app-ui/tests/app-status-gating.vitest.tsx` 与 `apps/web/e2e/web-ui.spec.mjs` 验证中性提示、真实 busy、手动刷新及全部既有写入门控，不添加轮询。
