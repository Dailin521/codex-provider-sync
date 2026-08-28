# vNext 行为兼容 Fixture 清单

> **状态：Accepted（阶段 0 语义清单；C2 动态 CLI Fixture、C4 安全 Runner/Schema、C5 首批跨运行时静态 Corpus、C7/C8 Electron 动态 Fixture 与 C9 候选产物 Fixture 已实现）**
>
> **日期：2026-08-24**
>
> **适用范围：Node、迁移期 .NET、CLI、Web 与 Electron Core Runtime 的行为对照**
>
> **架构基线：[vNext Electron + Node 单核心架构](../VNEXT_ELECTRON_NODE_ARCHITECTURE_ZH.md)**

## 1. 目的与边界

本文冻结需要被共享 Fixture 表达的场景、输入语义和验收结果，用于迁移期间比较 Node 与 .NET，并为 Node 单核心提供长期回归证据。

C4 已建立私有 workspace、严格 schema 和只向临时目录复制的安全 Runner。C5 检入首批完全合成的 `bidirectional-backup-roundtrip` 与 `foreign-pending-restore` 静态输入：只有 fake provider、空正文 thread row、`session_meta` 和 SQLite seed SQL，不提交 SQLite 二进制、锁文件、平台专属二进制或真实用户数据。Driver 每次在临时目录 materialize SQLite，并为四个 Node↔.NET 方向复制同一输入；现有动态 Node/.NET fixtures 继续补充更广的行为矩阵。

Fixture 不是用户数据样本，严禁从真实 `~/.codex`、认证文件或私人会话复制内容。

## 2. 通用 Fixture 合同

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
| `explicit-sync-provider` | `sync --provider ID` 显式给出未在 config 声明的 Provider | v0.5 当前允许直接以该 ID 同步；Public API 提取不得无意增加 Switch 式校验。未来若要收紧，必须独立评估 CLI 兼容性与 SemVer |
| `mixed-provider` | sessions、archived_sessions 与 SQLite 中存在多个 Provider | Status 分布完整；Sync 统一目标字段但不要求两类 inventory 数量相等 |
| `archived-sessions` | 只含或混合归档 rollout/SQLite rows | active/archived 作用域保持正确，Restore 能逐字节还原 |
| `root-model` | 根级 model、Provider section model 与 turn_context model 不同 | Follow/Keep/Explicit 三种 Switch 语义清晰；非目标字段和换行符不变 |
| `encrypted-content` | rollout 含来自原 Provider 的 `encrypted_content` | 只同步可见性元数据；保留加密内容字节并返回明确 warning |
| `large-rollout` | 超大 rollout、超过 64 KiB 的行、Unicode 与特殊 model 字符 | 流式处理且目标字段正确；未修改字节、CRLF 与原 mtime 按合同保持 |
| `status-metadata-boundary` | 大 rollout 首行含 `session_meta`，正文设置禁止读取 sentinel | Web/Electron Facade Status 仅以首行和 stat 完整统计 Provider 并成功；CLI/Prepare 仍触发正文扫描，元数据 revision 在文件 size/mtime/ctime 漂移时变化 |
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
| `shared-sqlite-home-contention` | 两个不同 Codex Home 指向同一 SQLite Home，并发写 | 不能因 Codex Home 锁不同而同时写同一 DB；阶段 1 必须验证/裁决共享资源锁，在通过前不得开放 Electron 写能力 |
| `dual-resource-lock-order` | 两个 Codex Home 与一个 SQLite Home 由 Node/.NET 交叉并发写 | 两层 lock 路径和顺序一致；不死锁；恰有一个 SQLite writer；败方无 Backup、Journal 或业务 mutation |
| `sqlite-resource-lock-unverifiable` | State DB resource lock 的 owner、协议、物理路径 identity 或 ABA 状态不可验证 | fail closed，返回 `LOCK_UNVERIFIABLE` 且范围为 state-db；不得自动删除或降级为 Busy |
| `restore-missing-state-db-parent` | Metadata v1/v2 Restore 指向缺失 DB，且其物理父目录也不存在 | Node/.NET 都在任何 Backup、Journal、config/rollout/DB mutation 前返回 `LOCK_UNVERIFIABLE(state-db)`；不得用 Home lock 代替资源锁 |
| `lock-unverifiable` | future protocol、损坏 owner、进程启动身份不可读、ABA/目录身份变化 | fail closed，返回 `LOCK_UNVERIFIABLE`；不得误报普通 Busy，不得自动删除不可证明归属的锁 |
| `external-write-status-snapshot` | 真实第二进程持有 Home→State 双锁，或另一 Home 只持共享 State DB 锁，并在锁内改变 config/SQLite | Core 与 Local Web 不扫描中间态；有缓存时逐字段保留最后完整 snapshot 并附 operation，无缓存时 `rolloutScanComplete:false`；不可验证锁不得显示 aligned/healthy |
| `plan-ledger-replay-expiry` | Plan 过期、重放、跨 operation、重启失效、篡改 apply payload | 只允许当前进程内 10 分钟单次消费；失效返回 `PLAN_EXPIRED`，附加字段返回 `INVALID_INPUT`，均无 Backup/Journal/mutation |
| `plan-ledger-abandoned-expiry` | Prepare 后调用方离开且没有 consume/waiter；同一 Home 有多个不同 expiry 的人工 intent | 单一最早到期 timer 自治清理并 rearm，不阻止进程退出；Watch 在最后 intent 到期后只恢复一次，不产生每 waiter timer |
| `watch-manual-priority` | 单一文件事件触发 Watch，但人工 Apply 已持有本进程协调器；等待期间继续产生重复事件 | Watch 不并发、不计失败；保留并合并 reasons，人工 operation completion 后恰运行一次 follow-up；stop 后 callback 不再 Apply |
| `watch-physical-scope-dedupe-and-bounded-history` | 同一物理 Codex Home 通过重复、并发或路径别名启动 Watch，并在自动/手工停止后重启 | 只创建一个活动 watcher、返回同一 watchId 且首个 options 生效；停止释放 scope，旧 watch 仍可查询/幂等 stop；最多保留 64 个 stopped 记录 |
| `pending-journal` | Managed Backup 中存在未终结 Journal | Status 可读并暴露恢复证据；Sync/Switch 被 `PENDING_TRANSACTION`/`RECOVERY_REQUIRED` 阻断。Prune 仍可作为 recovery-safe maintenance 执行，但必须保护所有 Pending Journal 引用的备份 |
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
| `journal-crash-matrix` | 在 prepared/applying/applied/commit/rollback 及 ack 窗口真实终止进程 | durable terminal 优先；非 terminal 阻断后续写；不得对 committed 状态补回滚事件；显式 Restore 可收敛 |
| `rollback-recovery-required` | mutation 后使自动 rollback 的一个或多个目标失败 | 原始错误与所有 rollback error 均保留；Backup、completed/uncompleted targets 和 `RECOVERY_REQUIRED` 可用于人工恢复 |

真实跨运行时测试不能用 Mock 代替进程争锁。Node 与 .NET 必须在同一临时目标上运行，并以文件/SQLite 最终效果作为独立证据。

V1/C3 的 executable mapping：Plan/revision 见 `test/plan-ledger.test.js`、`test/operation-revision.test.js`、`test/plan-apply.test.js`；Node 锁与外部 Status 见 `test/state-db-lock.test.js`、`test/status-coordination.test.js`；Watch 见 `test/watch.test.js`；Web transport 见 `test/web-server.test.js`；.NET 与跨运行时锁见 `StateDbLockResourceTests`、`DualResourceLockIntegrationTests`、`CrossRuntimeStateDbLockTests`、`LockServiceTests`。`shared-sqlite-home-contention` 与 `dual-resource-lock-order` 另由 `test-support/cross-runtime-fixtures.mjs`、`test-support/cross-runtime-writer-host.mjs` 和 `.NET FixtureHost` 启动两个方向的真实 Sync writer，winner 在 `before_backup` 持有 Home→State DB 双锁，loser 必须以 `OPERATION_BUSY(state-db)` 且零 Backup/Journal/mutation 退出。C5 的双向 backup/foreign pending executable mapping 为 `test-support/cross-runtime-fixtures.mjs`、`test-support/cross-runtime-node-crash-host.mjs`、`.NET FixtureHost` 与既有 `.NET CrashHost`；完整命令与结果记录在 `evidence/C5_SHARED_UI_WEB_2026-08-26.md`。

C7 的 executable mapping 为 `test-support/desktop-sync-switch-fixture.mjs`、`apps/desktop/e2e/desktop-sync-switch.spec.mjs`、`apps/desktop/tests/ipc-router.test.mjs`、`runtime-supervisor.test.mjs` 与 `test/plan-apply.test.js`。它使用临时 Home、真实 SQLite、Windows `FileShare.None`、受控 test-build Utility 终止和完整目标 Hash/语义快照；生产 Core host control 不包含故障注入能力。`scripts/test-wsl-unc-safety.sh` 以 `CPS_REQUIRE_REAL_WSL=1` 提供严格真实 WSL 门；没有与 source commit 绑定的健康 Windows+WSL 结果时仍是 Pending，不能用 synthetic UNC 或代码开关冒充实证。详见 `evidence/C7_ELECTRON_SYNC_SWITCH_2026-08-26.md`。

C8 的 executable mapping 为 `test/restore-v2-state-machine.test.js`、`RestoreJournalServiceTests`、`RestoreV2IntegrationTests`、`test-support/cross-runtime-fixtures.mjs`、Node/.NET CrashHost、`test/watch.test.js`、`test/operation-coordinator.test.js`、`apps/desktop/tests/updater.test.mjs`、Desktop contracts/unit tests、`apps/desktop/e2e/desktop-restore-relocation.spec.mjs` 与隐藏模式 Electron E2E。跨运行时 harness 覆盖 applying/prepared/committing/rollback-pending、commit ack、foreign pending、unknown schema、Windows 物理路径 alias、manifest/prepared 全量绑定和 persisted physical Home mismatch；Windows alias 用例通过系统返回的实际 8.3 短路径和真实 junction，让 Node 与 .NET 分别以别名创建 pending、由另一运行时以物理长路径恢复，也保留长路径创建/别名恢复矩阵，并校验原 journal bytes 不改写。原 journal 保留与 resolver projection 是显式裁决，不再把 raw 非终态伪报为 `rolled-back`。Updater fixture 只使用注入 port，不访问真实 Release；C9/C10 必须另做获授权签名产物的检查/下载/重启升级 smoke。

历史备份兼容 executable mapping 分为两个证据等级：`test-support/historical-tag-backup-fixtures.mjs` 证明冻结 repository tag-source；`test-support/formal-release-backup-fixtures.mjs` 和 `test-support/formal-release-assets.v1.json` 证明 checksum-bound hosted v0.4.1 Automation Release asset。后者仅在 Windows/Node 24 临时目录、严格环境白名单中执行，先固定并核对 release/tag/asset/archive/executable、解压前拒绝越界条目、执行前二次核对 executable，再执行历史 Plan/Apply 与当前 Node Restore；CI 只上传同一 run/commit 绑定的脱敏 hash evidence，不上传二进制、SQLite、rollout、完整 backup、路径、凭据或正文。fork PR 不执行 hosted binary；同仓库 PR 的 bundle 仍只是未受保护 source 的审查预览，只有受保护 `main` 上由同一 required workflow 重新生成的 artifact 才能成为最终证据。该项不是 vNext/Electron/GUI Release，也不是历史版本自身 Restore；历史 tag 与二进制 `NotSigned`，GitHub API、固定清单和 Hash 只提供完整性与托管来源绑定，真实 Beta、签名和生产升级仍保持独立门禁。

## 6. Restore、Backup 与 Prune

| Fixture ID | 输入语义 | 关键预期 |
| --- | --- | --- |
| `restore-relocation` | Backup 的 SQLite Home 与当前目标不同；Electron Renderer 只看到 `sqliteHomeConfigured` 摘要 | 默认拒绝；只有可信命名 profile 的显式目标和 relocation 确认才允许，且跨 SQLite Home Restore 不恢复 config。隐藏 Electron UI 回归必须证明 source 业务状态不变、目标 DB 恢复 |
| `prune-managed-only` | backup root 同时包含受管备份、普通目录和 Pending Journal 引用 | 只删除超过保留数的受管备份；普通目录和 Pending Journal 所在目录永不删除 |
| `backup-first-no-mutation` | Backup 期间空间、权限或 snapshot 失败 | 返回 `BACKUP_FAILED`；不存在 Journal/目标 mutation；原始 Hash 不变 |

`bidirectional-backup-roundtrip` 与 `foreign-pending-restore` 是迁移期淘汰 .NET 前的强制门槛，不因单向 Restore 成功而视为通过。

## 7. Workspace 与 History

| Fixture ID | 输入语义 | 关键预期 |
| --- | --- | --- |
| `workspace-roots` | global state、rollout cwd 与 SQLite cwd 不一致，含跨平台路径形式 | 只修复合同允许的 workspace/cwd 元数据；路径规范化一致；Backup/Restore 覆盖 global state |
| `history-safe-content` | user/event/response-item 重复消息、无 thread id、同 id 多 rollout，并包含大正文 rollout 与多个大 decoy | 无 query 列表只读受限首行 metadata、返回 `messageCountKnown=false` 且 UI 不显示伪 0；显式搜索仍可全文匹配并返回精确计数；详情定位只深读用户选择的 rollout；列表选择稳定会话；正文不进入日志、诊断包、Query cache 或应用数据库 |
| `desktop-readonly-c6` | 临时 Codex Home 含无标题 rollout、真实 SQLite row、valid pending journal 与正文 marker | production bridge 无测试/Node 能力；列表/Profiles/Diagnostics 无路径和正文；显式详情后才显示 marker；写 IPC 拒绝；Utility crash 后按 profile preflight 并恢复；测试前后 Codex Home 全树 Hash 不变 |
| `desktop-release-candidate-c9` | 四个 host-native target 各自生成两个最终发行容器；容器只含 synthetic build content 与 target-native SQLite binding | 每个容器解包/安装后重新审计 ASAR/Fuse/embedded integrity/native binding，隐藏执行 Status 与 Sync→Restore，正常退出；NSIS 卸载清理；SBOM/manifest/checksum 完整闭包；四目标 aggregate 的 version/commit/lock/tool/policy 一致；任何 source map、fixture、凭据名、真实数据、非目标 binding 或未清单文件均阻断 |

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

## 11. 阶段验收边界

阶段 0 的完成标志是本文场景、预期和安全门槛获得确认。C5 已为 `bidirectional-backup-roundtrip` 与 `foreign-pending-restore` 建立真实跨进程 Windows harness 和 required CI job；这只证明这两个 Phase 2 门槛，不代表 Restore v2、全 crash matrix、WSL 或三平台产物等价。其余 Corpus、Builder 与 CI Matrix 必须在对应 checkpoint 真正通过后才能宣称完成。
