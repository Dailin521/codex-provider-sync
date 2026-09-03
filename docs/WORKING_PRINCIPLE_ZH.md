# codex-provider-sync 工作原理与落盘机制

> 本文是实现与数据边界的技术参考。普通安装和使用请先阅读项目 [README](../README.md)。

## 1. 项目定位

`codex-provider-sync` 不是用于恢复已经被删除的聊天内容，也不负责登录、认证或切换账号。它是一个 Codex 会话元数据一致性修复工具。

当用户切换根级 `model_provider` 后，历史会话的消息内容通常仍保存在磁盘中，但 rollout 文件、SQLite 线程索引或 Desktop 项目路径元数据仍然指向旧 Provider。Codex 在构建会话列表、执行 `/resume` 或按项目筛选会话时，可能因此不再展示这些历史会话。

本项目通过同时修复以下数据，使历史会话重新满足当前 Codex 配置的可见性条件：

- `~/.codex/config.toml`
- `~/.codex/sessions/**/rollout-*.jsonl`
- `~/.codex/archived_sessions/**/rollout-*.jsonl`
- Codex `state_5.sqlite` 中的 `threads` 表
- `~/.codex/.codex-global-state.json`

可以把它概括为：

```text
保留原始会话内容
        +
重新对齐 Provider 和 model 元数据
        +
修复 SQLite 索引与项目路径
        +
通过备份、事务和补偿恢复降低写入风险
```

## 2. 历史会话为什么会不可见

一条 Codex 会话的信息并不只存在于一个文件中，而是分散在多个存储层：

| 数据位置 | 主要用途 | 本项目关注的字段 |
| --- | --- | --- |
| `config.toml` | 当前 Codex Provider 和模型配置 | 根级 `model_provider`、`model`、`sqlite_home` |
| `sessions` | 活跃会话的 rollout 事件历史 | `session_meta.payload.model_provider`、`turn_context.payload.model` |
| `archived_sessions` | 已归档会话的 rollout 事件历史 | 同上 |
| `state_5.sqlite` | 会话列表、索引和筛选依据 | `threads.model_provider`、`model`、`has_user_event`、`cwd` |
| `.codex-global-state.json` | Desktop 的项目和工作区状态 | workspace roots、项目顺序、路径映射 |

例如当前配置已经切换为：

```toml
model_provider = "openai"
```

但旧 rollout 首行仍然是：

```json
{"type":"session_meta","payload":{"id":"thread-id","model_provider":"apigather"}}
```

同时 SQLite 中仍然保存：

```text
threads.model_provider = "apigather"
```

这时消息正文虽然还在，Codex 的 Provider 过滤、会话索引或项目路径匹配仍可能排除这条记录。项目所做的不是重建消息，而是将这些元数据重新对齐。

## 3. 核心数据来源

### 3.1 Codex Home

默认 Codex Home 是：

```text
~/.codex
```

也可以通过以下方式覆盖：

- CLI 的 `--codex-home`
- 环境变量 `CODEX_HOME`

### 3.2 当前 Provider

`sync` 从 `config.toml` 根级读取 `model_provider`。如果根级字段不存在，则使用内置默认值 `openai`。

工具只读取第一个 TOML section 之前的根级字段，不会把下面这种 Provider section 内的配置误认为当前根级配置：

```toml
model_provider = "apigather"
model = "MiniMax-M3"

[model_providers.apigather]
model = "provider-default-model"
base_url = "https://example.com"
```

相关实现位于 [`src/config-file.js`](../src/config-file.js)。

### 3.3 SQLite Home 和数据库选择

SQLite Home 按以下优先级解析：

1. CLI 或 GUI 显式覆盖
2. `config.toml` 根级 `sqlite_home`
3. `CODEX_SQLITE_HOME`
4. `<Codex Home>/sqlite`

只有第 4 种默认布局允许继续检查旧位置：

```text
<Codex Home>/state_5.sqlite
```

如果 SQLite Home 来自显式参数、配置或环境变量，而指定位置缺少 `state_5.sqlite`，工具会报错或显示诊断，不会回退到另一个数据库。这样可以避免修改与当前 Codex 实例无关的状态库。

默认布局下如果新旧两个数据库都存在，工具会综合比较：

- 数据库 thread 数量与 rollout 文件数量的距离
- thread 总数
- 最新 thread 时间戳
- 数据库文件修改时间
- 候选路径优先级

然后选择最可能对应当前 Codex 状态的数据库。相关实现位于 [`src/storage-layout.js`](../src/storage-layout.js) 和 [`src/sqlite-state.js`](../src/sqlite-state.js)。

### 3.4 Windows 与 WSL SQLite

Windows 进程不能安全地通过以下 WSL UNC 路径直接操作 SQLite：

```text
\\wsl.localhost\...
\\wsl$\...
```

这些路径在 Windows GUI 和 CLI 中只用于安全诊断。实际 SQLite 操作需要进入对应 WSL 发行版，使用 Linux 路径运行 CLI，例如：

```bash
codex-provider sync \
  --codex-home /mnt/c/Users/you/.codex \
  --sqlite-home /home/you/.codex/sqlite
```

## 4. 一次 Provider 同步流程

一次 `codex-provider sync` 可以概括为：

```text
读取 config.toml
        │
        ├─确定目标 model_provider
        └─解析 SQLite Home
        │
        ▼
只读取 sessions 和 archived_sessions 的 session_meta 首行
        │
        ├─统计 rollout Provider 分布
        ├─生成需要修改的文件清单
        └─检测 locked 文件
        │
        ▼
检查 rollout 文件锁和 SQLite 可写性
        │
        ▼
存在实际写入时创建 UndoBackup
        │
        ▼
更新可写 rollout 的 Provider
        │
        ▼
BEGIN IMMEDIATE → 更新 SQLite Provider → COMMIT
        │
        ▼
清理旧的托管备份，默认保留最近 5 份
```

Provider 等长时只覆盖目标字节；长度不同时流式生成临时文件并原子替换，聊天正文不解析且逐字节保持。noop 不创建备份；mutation 后故障返回带 UndoBackup 和重试提示的 partial。

模型、cwd、user-event、workspace roots 与加密内容问题由用户手动运行 Diagnostics 完整只读扫描；需要修改时再通过显式 Repair target 执行。加密内容只诊断、不修改。

## 5. Rollout 首行同步与显式修复

### 5.1 扫描范围

工具递归扫描：

```text
~/.codex/sessions/**/rollout-*.jsonl
~/.codex/archived_sessions/**/rollout-*.jsonl
```

每个 rollout 是一个 JSON Lines 文件。首行通常是 `session_meta`：

```json
{
  "type": "session_meta",
  "payload": {
    "id": "thread-id",
    "cwd": "D:\\project",
    "model_provider": "apigather"
  }
}
```

工具只有在首行能够解析为合法 `session_meta` 时才处理该文件。

### 5.2 扫描时收集的信息

工具会从 rollout 中收集：

- 当前 `model_provider`
- thread ID
- 会话工作目录 `cwd`
- 是否出现用户消息
- 是否包含 `encrypted_content`
- 所有 `turn_context` 事件中的模型字段
- 文件大小、mtime、换行格式和首行偏移

这些信息用于生成内存中的变更计划，扫描阶段本身不会修改文件。

### 5.3 Provider 元数据修复

如果 rollout 中的 Provider 与目标 Provider 不一致，工具会把首行：

```json
"model_provider": "apigather"
```

改成：

```json
"model_provider": "openai"
```

只修改 `session_meta` 元数据，不修改后续消息、工具调用、标题或时间戳。

### 5.4 model 元数据修复

如果当前根级 `model` 存在，工具还会检查并重写所有 `turn_context` 事件中的 `model` 字段，包括嵌套的协作模式模型字段：

```json
{
  "type": "turn_context",
  "payload": {
    "model": "MiniMax-M3",
    "collaboration_mode": {
      "settings": {
        "model": "MiniMax-M3"
      }
    }
  }
}
```

即使 Provider 已经正确，只要 `turn_context.model` 与目标模型不一致，也会产生一次“仅模型变更”。这是为了让旧会话在 Codex UI 中显示当前有效模型。

### 5.5 大文件处理

rollout 可能达到数十 MB，单条 `turn_context` 也可能因 `developer_instructions` 等内容超过 64 KB。因此工具采用流式扫描和流式重写，不会为了修改一个字段而把整个文件加载到内存或完整执行 `JSON.parse`/`JSON.stringify`。

这样可以降低以下风险：

- 大文件占用过多内存
- 重新序列化改变大量无关字节
- 复杂转义字符串被意外改写
- 工具输出或 opaque payload 被破坏

## 6. Rollout 的落盘策略

Rollout 的 Provider 修改有两种落盘方式。

### 6.1 等长原地覆盖

如果旧 Provider 和新 Provider 的 JSON 编码后 UTF-8 字节长度相同，工具会：

1. 以读写方式打开原 rollout
2. 再次验证文件大小和 mtime
3. 定位首行 `model_provider` 的字节偏移
4. 直接覆盖对应字节
5. 调用文件同步操作

这种方式不需要复制大型文件。

如果原地覆盖过程中失败，工具会尝试把原字节写回，避免留下部分覆盖结果。

### 6.2 临时文件安全重写

如果 Provider 字节长度不同，或者不满足原地覆盖条件，工具会：

1. 创建同目录临时文件
2. 写入更新后的首行
3. 保留原始 LF 或 CRLF 分隔符
4. 从原文件首行之后的位置开始流式复制剩余内容
5. 再次验证原文件在处理期间没有变化
6. 将临时文件重命名为原文件

### 6.3 `turn_context.model` 重写

模型字段通过另一遍逐行流式处理完成。只修改 `type = "turn_context"` 的行，并替换该行内所有符合条件的 `model` 字段。

工具会保留：

- 原有 LF/CRLF
- 原有末尾换行状态
- 原始文件 mtime
- 非 `turn_context` 行的内容

### 6.4 并发变更检测

扫描阶段会记录文件大小和 mtime。正式写入前和替换临时文件前都会重新检查。

如果 Codex 在此期间追加了新内容，快照会失配，工具会跳过该 rollout，而不是覆盖最新内容。活跃会话锁住文件时，该文件也会被列入 `Skipped locked rollout files`。

相关实现位于 [`src/session-files.js`](../src/session-files.js)。

## 7. SQLite 修复与落盘

### 7.1 写入前检查

在备份和修改 rollout 之前，工具先尝试：

```sql
BEGIN IMMEDIATE;
ROLLBACK;
```

这用于验证目标数据库是否可写，以及是否能取得写锁。

如果出现 `state_5.sqlite is currently in use`，同步在修改 rollout 之前停止。用户需要关闭 Codex、Codex App 和 app-server，再重新执行相同命令。

### 7.2 更新 Provider 和模型

实际同步使用 SQLite 事务：

```sql
BEGIN IMMEDIATE;

UPDATE threads
SET model_provider = :target_provider
WHERE COALESCE(model_provider, '') <> :target_provider;

COMMIT;
```

如果指定了目标模型，且当前 schema 存在 `model` 列，则相当于：

```sql
UPDATE threads
SET model_provider = :target_provider,
    model = :target_model
WHERE COALESCE(model_provider, '') <> :target_provider
   OR COALESCE(model, '') <> :target_model;
```

### 7.3 修复 `has_user_event`

扫描 rollout 时，如果工具确认某个 thread 存在用户消息，而 SQLite 中存在 `has_user_event` 列，则执行等价更新：

```sql
UPDATE threads
SET has_user_event = 1
WHERE id = :thread_id
  AND COALESCE(has_user_event, 0) <> 1;
```

这可以修复由于标记不正确造成的会话可见性问题。

### 7.4 修复 `cwd`

工具以 rollout 首行的 `session_meta.payload.cwd` 为依据，修复对应 SQLite thread 的 `cwd`：

```sql
UPDATE threads
SET cwd = :rollout_cwd
WHERE id = :thread_id
  AND COALESCE(cwd, '') <> :rollout_cwd;
```

Windows 扩展路径会规范为 Desktop 更容易匹配的形式，例如：

```text
\\?\D:\project
```

规范为：

```text
D:\project
```

### 7.5 SQLite 真正写入磁盘的位置

工具提交事务后，SQLite 自己负责持久化。根据数据库日志模式，修改可能先进入：

```text
state_5.sqlite-wal
```

之后再由 SQLite checkpoint 合并到：

```text
state_5.sqlite
```

因此备份时工具会同时复制主数据库、`-wal` 和 `-shm` 文件（如果存在），不能只复制主数据库。

相关实现位于 [`src/sqlite-state.js`](../src/sqlite-state.js) 和 [`src/sqlite.js`](../src/sqlite.js)。

## 8. Desktop 项目可见性修复

有些会话可以通过 `/resume` 找到，但在 Desktop 的某个项目下不显示。这通常不是 Provider 的唯一问题，还可能是以下路径表示不一致：

```text
\\?\D:\GitHubProject\demo
```

与：

```text
D:\GitHubProject\demo
```

工具读取 SQLite 中 thread 的 `cwd` 分布，并规范化 `.codex-global-state.json` 中的：

- `electron-saved-workspace-roots`
- `project-order`
- `active-workspace-roots`
- `electron-workspace-root-labels`
- `open-in-target-preferences.perPath`

更新时会同时写入：

```text
~/.codex/.codex-global-state.json
~/.codex/.codex-global-state.json.bak
```

`status` 还会计算每个项目的会话数量、全局排序 rank、诊断窗口中前 50 条的命中数和路径精确匹配情况。这用于解释 Desktop 可能未展示窗口范围外旧会话的现象；工具不会修改 `updated_at` 将旧会话强行顶到前面。

相关实现位于 [`src/workspace-roots.js`](../src/workspace-roots.js)。

## 9. 备份的落盘内容

每次 `sync` 或 `switch` 在正式修改前都会创建备份：

```text
<Codex Home>/backups_state/provider-sync/<timestamp>/
```

典型内容如下：

```text
<timestamp>/
├── metadata.json
├── config.toml
├── session-meta-backup.json
├── .codex-global-state.json
├── .codex-global-state.json.bak
└── db/
    └── sqlite-home/
        ├── state_5.sqlite
        ├── state_5.sqlite-wal
        └── state_5.sqlite-shm
```

### 9.1 `metadata.json`

记录：

- metadata 版本
- 备份命名空间
- Codex Home
- SQLite Home
- 目标 Provider
- 创建时间
- 已复制的 SQLite 文件
- 计划或实际修改的 rollout 数量

### 9.2 `session-meta-backup.json`

Rollout 通常不会整文件复制。工具只记录恢复所需的元数据：

- rollout 的绝对路径
- 原始首行
- 原始换行分隔符
- 原始 mtime
- 原始 `turn_context.model` 值及其行位置
- 是否属于仅模型变更

这样做的好处是：恢复时只撤销工具改过的 metadata，不会把同步之后新追加的聊天消息一并删除。

### 9.3 自动保留策略

默认只保留最近 5 份由本工具创建的托管备份。可以使用：

```bash
codex-provider sync --keep 10
codex-provider prune-backups --keep 5
```

备份清理只处理 `backups_state/provider-sync` 下带有正确 metadata 命名空间的目录，不会删除其它备份。

相关实现位于 [`src/backup.js`](../src/backup.js)。

## 10. 原子性、事务和失败恢复

这个工具同时修改 SQLite 和普通文件系统文件。二者无法加入同一个真正的跨存储原子事务，因此项目使用以下组合：

```text
进程级互斥锁
    +
修改前备份
    +
SQLite 事务
    +
rollout 并发快照检查
    +
失败后的补偿式文件恢复
```

### 10.1 进程级互斥锁

执行 `sync`、`switch`、`restore` 或备份清理时，工具会在 Codex Home 的 `tmp` 目录下创建锁目录，并写入：

- 当前 PID
- 启动时间
- 操作名称
- 当前工作目录

这可以防止两个本工具实例同时修改同一份 Codex 状态。实现位于 [`src/locking.js`](../src/locking.js)。

### 10.2 正常提交路径

正常情况下的写入顺序是：

```text
1. 扫描并生成内存变更计划
2. 检查 rollout 锁定状态
3. 验证 SQLite 可写
4. 将原状态备份落盘
5. 打开 SQLite BEGIN IMMEDIATE 事务
6. 更新 SQLite threads
7. 重写可写 rollout
8. 更新 global state
9. COMMIT SQLite
10. 清理旧备份
```

### 10.3 失败路径

如果事务打开后的某一步失败：

1. SQLite 执行 `ROLLBACK`
2. 已经修改的 rollout 根据备份 manifest 恢复
3. 已经修改的 global state 从备份恢复
4. `switch` 如果已经修改 `config.toml`，还会恢复原配置
5. 如果补偿恢复本身失败，错误信息会同时报告原始错误和恢复错误

因此它不是一个理论上的全局原子事务，而是“SQLite 原子事务 + 普通文件补偿恢复”。

## 11. CLI 操作

### 11.1 `status`

```bash
codex-provider status
```

只读检查：

- 当前 Provider
- rollout Provider 分布
- SQLite Provider 分布
- SQLite 实际路径和来源
- 待修复的 `has_user_event`、`cwd`
- 锁定 rollout
- `encrypted_content`
- 项目会话 rank 和首屏可见性
- 备份数量和空间占用

### 11.2 `sync`

```bash
codex-provider sync
```

使用当前根级 `model_provider` 和 `model` 对齐历史会话。它不修改登录状态，也不管理 `auth.json`。

### 11.3 `switch`

```bash
codex-provider switch apigather
```

在 Provider 已存在于 `config.toml` 的前提下：

1. 计算新的根级 Provider/model 配置
2. 完成同步前备份
3. 修改 `config.toml`
4. 执行与 `sync` 相同的元数据修复流程
5. 失败时恢复原配置

模型选择规则：

- `--model <name>`：显式设置根级 model
- `--keep-root-model`：只切换 Provider，不改根级 model
- 默认：尝试采用目标 Provider section 中的 `model`

### 11.4 `restore`

```bash
codex-provider restore <backup-dir>
```

可以恢复：

- `config.toml`
- SQLite 数据库及 sidecar 文件
- rollout metadata
- global state

也可以通过 `--no-config`、`--no-db` 或 `--no-sessions` 排除部分内容。

metadata v2 备份会记录原 SQLite Home。恢复到不同 SQLite Home 默认被拒绝；CLI 必须显式提供目标 SQLite Home、允许 relocation，并禁止同时恢复旧配置，以免旧配置再次指向原数据库。

### 11.5 `watch`

```bash
codex-provider watch
```

监听以下变化：

- `config.toml`
- `state_5.sqlite`
- `state_5.sqlite-wal`
- `state_5.sqlite-shm`

事件经过默认 750ms 防抖后触发同步。SQLite 临时忙碌会跳过本次并等待后续事件；连续出现非 busy 错误时，watcher 会在达到阈值后退出，避免无限刷错误日志。

实现位于 [`packages/core/src/application/watch-runtime.js`](../packages/core/src/application/watch-runtime.js)；[`src/watch.js`](../src/watch.js) 仅保留兼容转发。

### 11.6 `web` 与 `prune-backups`

```bash
codex-provider web
codex-provider prune-backups --keep 5
```

`web` 启动只监听本机回环地址的 Local Web UI，并复用 Node service 的状态、同步、备份和恢复逻辑。`prune-backups` 只清理由本工具管理的旧备份。

## 12. 用户入口与核心架构

仓库包含三类用户入口：

- Node.js CLI
- CLI 提供的 Local Web UI
- Windows/macOS 桌面 GUI

CLI 入口是 [`src/cli.js`](../src/cli.js)，Web 服务位于 [`src/web-server.js`](../src/web-server.js)。业务编排按 Status、ProviderSync、ProviderSwitch、Repair、Backups、Restore 等用例分布在 [`packages/core/src/application/`](../packages/core/src/application/)；[`src/service.js`](../src/service.js) 与 [`service-runtime.js`](../packages/core/src/application/service-runtime.js) 仅保留兼容转发，Electron 通过 CoreFacade 调用。

桌面 GUI 不是简单启动 Node CLI，而是在 `desktop/CodexProviderSync.Core` 中用 C# 实现了相同的核心流程，包括：

- 配置读取和 Provider 发现
- SQLite Home 解析
- rollout 扫描与重写
- SQLite 事务
- global state 修复
- 备份和恢复
- 锁定文件与 WSL UNC 安全检查

C# 主编排逻辑位于 [`desktop/CodexProviderSync.Core/CodexSyncService.cs`](../desktop/CodexProviderSync.Core/CodexSyncService.cs)。

## 13. 明确不会修改的内容

正常同步不会修改：

- 用户消息正文
- assistant 消息正文
- 工具调用内容
- 会话标题
- `updated_at`
- 登录状态
- `auth.json`
- Provider 凭据
- 账号认证信息

它也不会在不同设备之间复制会话文件，只修复当前 Codex Home 中已有数据的元信息。

## 14. 限制与边界情况

### 14.1 锁定的 rollout

活跃会话可能正在持有 rollout 文件。工具会跳过该文件并继续同步其它可写文件及 SQLite，因此结果可能是部分成功。

会话结束后再次运行：

```bash
codex-provider sync
```

即可尝试完成剩余重写。

### 14.2 锁定的 SQLite

SQLite 无法取得写锁时，同步会在修改 rollout 之前停止。关闭 Codex、Codex App 和 app-server 后重试。

### 14.3 `encrypted_content`

含有 `encrypted_content` 的历史会话可能与原 Provider、账号或密钥上下文绑定。

本工具能够修复列表可见性元数据，但不能重新加密或解密这些内容。跨 Provider/account 后，继续会话或执行 compact 仍可能出现：

```text
invalid_encrypted_content
```

### 14.4 Desktop 首屏限制

Codex Desktop 当前可能只加载最近 50 条会话。即使 Provider 和路径都已修复，较旧的会话也可能不在首屏。`status` 会提供 rank 诊断，但工具不会篡改时间戳绕过该限制。

## 15. 最终总结

这个仓库的核心不是“复制聊天记录”，而是让 Codex 对同一条会话的多个数据视图重新一致：

```text
config.toml 当前 Provider/model
                │
                ▼
rollout session_meta 与 turn_context
                │
                ▼
SQLite threads 索引
                │
                ▼
Desktop workspace/project metadata
```

落盘层面，它采用：

```text
先扫描
  → 先验证写锁
  → 先备份
  → SQLite 事务
  → rollout/global state 文件写入
  → 事务提交
  → 失败时补偿恢复
```

因此，它是一套针对 Codex 会话 Provider 可见性问题的、有备份和回滚能力的元数据同步机制。
