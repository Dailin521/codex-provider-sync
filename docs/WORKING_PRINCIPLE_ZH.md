# codex-provider-sync 工作原理与落盘机制

> 适用于当前 V1 Node Core（CLI / Web / Electron）。Legacy .NET 的历史行为不作为新实现规范。
> 用户安装见 [README](../README.md)；开发约束、模块映射及测试入口以 [Node Core 当前架构](architecture/NODE_CORE_ARCHITECTURE_ZH.md) 为准，本文只解释原理。

## 1. 日常同步只对齐 Provider

切换 Provider 后，会话文件仍在，但 rollout 和 SQLite 线程索引中的 Provider 可能与当前 config 不一致，导致历史不可见。本工具不是登录工具、账号管理器、消息重建器或解密工具。

| 操作 | 读取/修改范围 |
| --- | --- |
| Status | 轻量只读状态；不做完整诊断 |
| Sync | 读取 config 当前根 Provider；只修改 rollout 首行和 SQLite 的 `model_provider` |
| Switch | 按用户选择修改 config 根 Provider/model，然后执行同一 ProviderSync |
| Diagnostics | 用户主动发起的一次完整只读扫描，不自动修复 |
| Repair | 显式选择模型、cwd、用户事件或工作区；不混入 Sync |
| Restore | 从受管备份恢复，独立使用恢复快照和 journal |
| History | 只读浏览/搜索，与 Provider 写入流程独立 |

Sync 的目标来自 `config.toml` 根级 `model_provider`，缺失时为 `openai`。它没有 Provider 覆盖参数或快/慢模式。模型差异不是自动修复指令；Sync 不改历史 model、cwd、user-event、workspace roots、title、ordinal 或线程 `updated_at`。

## 2. 路径与数据来源

Codex Home：显式 CLI 参数或可信存储 Profile → `CODEX_HOME` → `~/.codex`。

SQLite Home：显式参数/Profile → config 根级 `sqlite_home` → `CODEX_SQLITE_HOME` → `<Codex Home>/sqlite`。只有默认布局允许检查旧位置 `<Codex Home>/state_5.sqlite`；显式位置缺 DB 时不能偷偷回退。最终只操作一个选中的 state DB。

Windows 的 WSL UNC SQLite 路径仅诊断，不执行数据库写入。需要在对应 WSL 内使用 Linux 路径运行 CLI，不能靠换一个界面绕过限制。

实际解析见 [storage-layout.js](../src/storage-layout.js)、[sqlite-state.js](../src/sqlite-state.js)。这里不承诺某一版 Codex 的项目首屏数量或显示缓存行为；不通过修改时间把历史顶到前面。

## 3. 普通写的生命周期

`runSync/runSwitch` 是兼容适配器，内部仍连续 Prepare/Apply。Web/Electron 的预览显示 Plan 后确认；“直接同步”点击本身代表授权，内部也使用同一个一次性 planId。

```text
Prepare：解析当前配置和存储，生成有期限的影响摘要
    ↓
Apply：消费 Plan，启动 Operation，获取 Codex Home 锁
    ↓
锁内复核 profile/config/storage/rollout/SQLite revisions
    ↓
扫描首行、检查占用、确定实际目标
    ↓
没有目标：直接返回，不创建备份
    ↓
SQLite 可写预检 → 完成 UndoBackup → 最后取消点
    ↓
Switch 的 config → 可写 rollout → Repair 的 global state → SQLite 提交
    ↓
结果、备份清理与释放 Home 锁
```

只持 `<Codex Home>/tmp/provider-sync.lock`，不另建 Node State DB 资源锁。这个锁协调同一 Home 的工具写者，不等于 Codex 自身的会话锁；仍要检查活动 rollout 与文件变化。

## 4. Provider 的两种落盘方式

### 等长、满足资格：原地字节更新

例如 `openai → prov_a`。两个安全 ASCII ID 的 JSON 字面量都是 8 个 UTF-8 字节（包含双引号）。

原地资格不仅看长度：要求合法首行、字段唯一且可定位、原始字面量匹配、语义只改 Provider、没有模型改写/多硬链接，并通过文件身份、大小、mtime、路径及占用复核。完整条件固定在 [核心架构 PIO-2](architecture/NODE_CORE_ARCHITECTURE_ZH.md#pio-2合格的等长更新必须原地写)。

通过后在已校验句柄上定位并覆盖 Provider 字节，不重建整份 rollout。Windows 复用既有原生 worker/helper；其他平台使用文件句柄定点写入。成功保持文件身份、大小与正文 hash；结果计入 `inPlaceSessionFiles`。

等长判断不能用字符数代替字节数。当前原地实现还限制 ID 为 `[A-Za-z0-9._-]+`；Unicode、转义字面量、歧义键不能因为“看起来等长”就直接覆盖。

### 不等长或不符合资格：有界流式替换

对可合法处理但不适合原地写的首行：

1. 创建同目录临时文件，写入修改后的首行。
2. 保留原始 LF/CRLF 分隔符。
3. 从正文起点流式复制原始字节，不解析/重序列化正文。
4. 复核原文件没有被换档/追加等变更，再原子替换。

正文必须逐字节相同；首行可能重新序列化，文件身份允许变化，结果计入 `rewrittenSessionFiles`。用户无需把所有 Provider ID 改成固定长度。

**计划为原地写后发生失败/锁定/漂移，不得静默降级成整文件重写来绕过校验。** 无效或超过 1 MiB 的 Sync 首行也不会通过扫描正文查找下一条 metadata 来兜底。

### “只读首行”的准确含义

业务扫描只解析首行，不分析聊天正文；底层按 64 KiB 分块，首个含换行的块可能预读少量尾部。Prepare、锁内复核和写前检查会重复读首行，不是整个操作只有一次磁盘读。

变长替换必然读取/复制尾部；备份和哈希另有 I/O。不能宣传“所有 Sync 永远只读写几个字节”。性能门禁看实际策略、正文扫描是否发生、写入范围、文件身份与内容一致性，计时仅供参考。

实现：[session-files.js](../src/session-files.js)、[windows-provider-bytes.cs](../src/windows-provider-bytes.cs)。

### Windows 文件阶段计时

当前 worker 复用一个进程顺序处理文件，通过原有响应携带数值计时，批次结束仅汇总一次。临时文件清理先走 `.NET File.Delete`，异常仍回退 `Remove-Item -LiteralPath -Force`；不取消默认复制缓冲区、独占句柄、Flush(true)、原子替换或 mtime 恢复。

`fileUpdateTiming` 可在 Sync/Switch 结果、CLI JSON 与 Desktop 操作日志中出现，分别记录复制、落盘、替换、清理、时间戳恢复及技术总时间。批次包含请求、请求包含 worker、worker 包含部分文件阶段，不能将嵌套时间相加，也不能将差值全部称为 IPC 开销。旧日志、noop、非 Windows 或未收到计时的操作保持字段缺失，不补造零。

计时 observer 的失败不改变操作结果，partial 可保留已经测到的部分。时间戳使用锁内读取的新鲜值，不回拨真正的聊天追加时间；现有变长路径恢复精度为毫秒，不承诺 NTFS 100 ns 精确。边界见 [ADR-0038](adr/0038-windows-cleanup-and-file-update-timing.md)。

## 5. SQLite 与 partial

写前以 `BEGIN IMMEDIATE → ROLLBACK` 预检可写性；真正修改在后续原生事务中提交。预检不预留未来写锁，之后仍可能发生竞争。

Provider 更新的含义是：

```sql
UPDATE threads
SET model_provider = :target_provider
WHERE COALESCE(model_provider, '') <> :target_provider;
```

普通 Sync 不在这条路径附带更新 model、cwd、has_user_event 或 updated_at。SQLite 自己负责 COMMIT/WAL，不由 Renderer 或 Main 操作数据库文件。

- 首次 mutation 前失败：业务目标零写入。
- 锁定/变化 rollout：跳过并区分原因，其余可继续；结果为 partial。
- mutation 后出错（包括后续 SQLite busy）：返回 partial、UndoBackup、失败阶段和重试建议，不自动全量回滚。
- 重试：重新 Prepare 再执行，逐步收敛；需要撤销时显式 Restore。
- 同一 SQLite 事务的 ROLLBACK、单文件写失败的局部字节恢复，不等于整个 Sync 被回滚。

Human CLI 保持 0/1；JSON partial 使用退出码 3。调用方必须看 outcome，不能把可重试的 partial 说成全部成功。

## 6. 备份与恢复各自负责什么

有实际写入时，先在 `<Codex Home>/backups_state/provider-sync/<timestamp>` 创建 UndoBackup；noop 不创建。默认保留最近 2 份受管备份，显式 keep 可覆盖；Prune 不删除无关目录或 pending journal 引用的证据。

备份按实际目标记录 `undoTargets`。Provider-only rollout 主要保存原首行、分隔符、文件快照及必要字节描述符，不为普通同步复制全部聊天正文；需要时捕获 config、SQLite/sidecar、global state。Repair 与旧备份可以包含各自所需的模型元数据。未捕获的目标不能被 Restore 当成空文件或删除指令。

**Restore 是独立恢复流程，不是普通写的自动 catch 分支。** 它先创建恢复前快照，再以耐久 journal、完整目标清单和 hash 处理恢复/补偿/提交确认。无法证明时进入 recovery required；跨 SQLite Home 恢复需显式目标及 relocation 确认，并禁止恢复旧 config。

旧普通 Sync/Switch journal 保持可读，不阻断新 Sync；未解决 Restore journal 继续阻断普通写。详见 [Restore ADR](adr/0013-restore-v2-recovery-state-machine.md)与 [Core 合同](architecture/contracts/CORE_EXTERNAL_BEHAVIOR_ZH.md)。

## 7. 高级修复与只读能力

- `models`：读取当前 config 根模型，缺失则拒绝 Prepare；只有明确选择才修历史模型。
- `cwd`：依据 rollout metadata 修正对应索引路径表示。
- `userEvent`：仅为该目标读取需要的用户事件信息，修复对应标记。
- `workspaceRoots`：包含 cwd，处理全 Profile 工作区设置，不允许局部会话选择覆盖全局设置。
- 修复支持范围预览与锁内修后核验；剩余差异/未核验明确提示，见 [ADR-0021](adr/0021-scoped-advanced-repair-and-readonly-integrity.md)。
- 加密内容只诊断，不解密/重加密；ordinal/显示索引目前只读观察，不提供改序号或重建索引。

History 优先使用已保存标题及工作区信息，项目→主会话→折叠子任务，项目/父会话独立加载更多；正文只在明确查看或正文搜索时处理，不进入日志/持久缓存。项目显示名只是本地偏好，不回写 Codex，见 [ADR-0022](adr/0022-history-project-roots-and-child-pagination.md)。

Status、History 等保持首次/手动刷新；Diagnostics 必须主动点击。Watch 是用户明确开启的监听，事件合并后调用同一 ProviderSync，每次重新 Prepare/Apply/加锁，不抢占人工操作。更新检查是独立 Host 能力，不触发后台数据扫描。

首页“正在使用的会话”来自当前 Home 的 writer 持有情况（`sessionActivity`），包括已对齐或等待输入的会话；未知不能当成零。它与当前同步中无法写入而跳过的目标不同，不用这个展示计数替代写前锁/快照检查。详见 [ADR-0030](adr/0030-writer-owned-session-count.md)。

## 8. 验证与开发入口

[Node Core 当前架构](architecture/NODE_CORE_ARCHITECTURE_ZH.md)固定文件映射、禁止依赖与 PIO 不变量。开发时运行：

```bash
npm run architecture:check
npm test
```

32 MiB fixture、Facade/CLI 适配器、等长身份/大小/body hash、变长正文 byte 一致、占用/漂移及重试收敛由现有测试证明。所有自动化使用合成临时目录，不能操作真实用户 Codex Home。具体测试及兼容矩阵见 [Fixture 清单](migration/BEHAVIOR_FIXTURES_ZH.md)。

文档修改和本地通过不等于公开发布、签名或全部平台验证；这些状态只以[迁移执行索引](migration/VNEXT_MIGRATION_EXECUTION_INDEX_ZH.md)及同提交证据为准。
