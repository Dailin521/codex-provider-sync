# vNext 行为兼容 Fixture 清单

本分支候选夹具：`test/in-place-transaction.test.js` 验证短写、崩溃、损坏 journal、冲突及活跃追加；`test/fast-sync.test.js` 验证快速范围、无正文流、模型/SQLite/cwd、前置失败和回滚；`test/windows-rewrite-worker.test.js` 与 `test/windows-provider-bytes.ps1` 验证协议和 Windows 原生独占句柄。POSIX 原地路径保留实际写入 mtime，原全量路径仍保留旧 mtime；该区别是待评审合同，不是隐式测试豁免。

> **状态：Accepted（阶段 0 语义清单；共享 Corpus 尚未创建）**
>
> **日期：2026-08-24**
>
> **适用范围：Node、迁移期 .NET、CLI、Web 与 Electron Core Runtime 的行为对照**
>
> **架构基线：[vNext Electron + Node 单核心架构](../VNEXT_ELECTRON_NODE_ARCHITECTURE_ZH.md)**

## 1. 目的与边界

本文冻结需要被共享 Fixture 表达的场景、输入语义和验收结果，用于迁移期间比较 Node 与 .NET，并为 Node 单核心提供长期回归证据。

阶段 0 **不创建** `packages/test-fixtures/`，也不提交伪造的 SQLite、锁文件或平台专属二进制。当前 Node 测试使用临时目录动态构造输入，.NET 测试使用 `TestCodexHomeFixture` 和 GUI E2E `IsolatedFixture`；后续实现共享 Corpus 时应复用这些已验证的构造能力。

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

## 5. 锁、并发与恢复

| Fixture ID | 运行方式 | 关键预期 / 安全门槛 |
| --- | --- | --- |
| `locked-rollout` | 平台真实文件锁 | 锁定文件不被写；其他安全目标可形成 Partial Result；返回 `ROLLOUT_LOCKED` 语义并可重试 |
| `active-rollout-changing` | 扫描后、应用前改变目标 | 变化文件被跳过或 Plan 失效；不覆盖 Codex 新写入，使用 `ROLLOUT_CHANGED`/`PLAN_STALE` |
| `sqlite-busy` | 真实 SQLite 写锁 | 在 rollout mutation 和 Backup 前阻断，返回 `SQLITE_BUSY`，全部原始 Hash 不变 |
| `node-dotnet-lock-contention` | 启动真实 Node 与 .NET 进程争用同一 `<CodexHome>/tmp/provider-sync.lock` | 恰有一个写者获得 protocol v2 锁；另一方为 `OPERATION_BUSY`；败方不创建 Backup、不改任何目标 |
| `shared-sqlite-home-contention` | 两个不同 Codex Home 指向同一 SQLite Home，并发写 | 不能因 Codex Home 锁不同而同时写同一 DB；阶段 1 必须验证/裁决共享资源锁，在通过前不得开放 Electron 写能力 |
| `lock-unverifiable` | future protocol、损坏 owner、进程启动身份不可读、ABA/目录身份变化 | fail closed，返回 `LOCK_UNVERIFIABLE`；不得误报普通 Busy，不得自动删除不可证明归属的锁 |
| `pending-journal` | Managed Backup 中存在未终结 Journal | Status 可读并暴露恢复证据；Sync/Switch 被 `PENDING_TRANSACTION`/`RECOVERY_REQUIRED` 阻断。Prune 仍可作为 recovery-safe maintenance 执行，但必须保护所有 Pending Journal 引用的备份 |
| `foreign-pending-restore` | Node 创建 Pending Journal/Backup 后由 .NET Restore，及反方向 | 两个方向都只按受管清单恢复，清除 Pending 前必须落入合法 terminal；差异需显式裁决 |
| `restore-mid-failure` | Restore 在某一目标已替换后注入失败 | 不能报告成功；必须完整补偿，或保留可操作证据并返回 `RECOVERY_REQUIRED`，不得留下无 Journal 的半恢复状态 |
| `bidirectional-backup-roundtrip` | Node Backup→.NET Restore；.NET Backup→Node Restore | 两个方向恢复到等价语义状态；正文和不应变化字段逐字节一致；Metadata v1/v2 兼容边界明确 |
| `journal-crash-matrix` | 在 prepared/applying/applied/commit/rollback 及 ack 窗口真实终止进程 | durable terminal 优先；非 terminal 阻断后续写；不得对 committed 状态补回滚事件；显式 Restore 可收敛 |
| `rollback-recovery-required` | mutation 后使自动 rollback 的一个或多个目标失败 | 原始错误与所有 rollback error 均保留；Backup、completed/uncompleted targets 和 `RECOVERY_REQUIRED` 可用于人工恢复 |

真实跨运行时测试不能用 Mock 代替进程争锁。Node 与 .NET 必须在同一临时目标上运行，并以文件/SQLite 最终效果作为独立证据。

## 6. Restore、Backup 与 Prune

| Fixture ID | 输入语义 | 关键预期 |
| --- | --- | --- |
| `restore-relocation` | Backup 的 SQLite Home 与当前目标不同 | 默认拒绝；只有显式目标和 relocation 确认才允许，且跨 SQLite Home Restore 不恢复 config |
| `prune-managed-only` | backup root 同时包含受管备份、普通目录和 Pending Journal 引用 | 只删除超过保留数的受管备份；普通目录和 Pending Journal 所在目录永不删除 |
| `backup-first-no-mutation` | Backup 期间空间、权限或 snapshot 失败 | 返回 `BACKUP_FAILED`；不存在 Journal/目标 mutation；原始 Hash 不变 |

`bidirectional-backup-roundtrip` 与 `foreign-pending-restore` 是迁移期淘汰 .NET 前的强制门槛，不因单向 Restore 成功而视为通过。

## 7. Workspace 与 History

| Fixture ID | 输入语义 | 关键预期 |
| --- | --- | --- |
| `workspace-roots` | global state、rollout cwd 与 SQLite cwd 不一致，含跨平台路径形式 | 只修复合同允许的 workspace/cwd 元数据；路径规范化一致；Backup/Restore 覆盖 global state |
| `history-safe-content` | user/event/response-item 重复消息、无 thread id、同 id 多 rollout | 列表选择稳定会话；详情只在用户主动读取时返回安全消息；正文不进入日志、诊断包或应用数据库 |

## 8. 未来 Corpus 建议结构

本节只定义目标，不表示目录已经存在：

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

## 9. Node / .NET 对照与差异登记

每个双运行时 Fixture 使用两份相同输入副本：

1. Node 执行并输出规范化结果与最终 Hash；
2. .NET 执行并输出相同维度证据；
3. 比较 Status、Plan、目标字段、Backup、Restore、Journal 与 Error Code；
4. 差异必须记录“Node 行为 / .NET 行为 / 权威选择 / 安全理由 / 对应测试”；
5. Node 是 vNext 目标核心，但不能以“新实现”为理由静默覆盖更安全的既有行为。

## 10. 阶段 0 验收边界

阶段 0 的完成标志是本文场景、预期和安全门槛获得确认。实际 Corpus、Schema、Builder、跨运行时 Harness 和 CI Matrix 均属于后续阶段；在它们真正通过之前，文档不得宣称行为等价。
