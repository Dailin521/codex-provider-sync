# CLI 命令兼容合同

> 状态：Accepted（Phase 0 Human 兼容基线；C2 JSON 合同已实现）
>
> 基线版本：`@dailin521/codex-provider-sync` v0.5.0
>
> 冻结日期：2026-08-24；C2 增量：2026-08-25
>
> 适用入口：`codex-provider`

## 1. 文档目的

本文冻结 vNext 迁移开始时已经存在的 Node CLI 外部行为，防止提取 Node Core、增加 Electron、重组仓库或迁移到 TypeScript 时无意破坏现有用户和自动化脚本。

本文同时明确区分四类内容：

- **v0.5 当前合同**：当前已发布且迁移期间必须兼容的行为；
- **legacy tolerated 行为**：当前宽松实现碰巧接受，但不升级为长期公开承诺的行为；
- **vNext C2 当前合同**：V1 分支已实现并由真实子进程测试冻结、但尚未公开发布的 opt-in JSON 行为；
- **vNext 目标**：需要后续独立设计、测试和发布说明才能生效的新增合同。

目标架构文档中的建议不自动覆盖本文记录的 v0.5 事实。只有对应迁移阶段通过退出条件并更新合同测试后，才能修改当前合同。

## 2. npm 与运行时入口

### 2.1 v0.5 当前合同

| 项目 | 当前值 |
| --- | --- |
| npm 包 | `@dailin521/codex-provider-sync` |
| CLI 命令 | `codex-provider` |
| 可执行文件 | `src/cli.js` |
| 模块类型 | ESM，`"type": "module"` |
| 最低 Node.js | `16.20.2` |
| npm engine | `>=16.20.2` |

除帮助路径外，CLI 在执行命令前检查 Node.js 版本。不满足最低版本时，向 stderr 输出一行升级提示并以失败退出。

包级脚本的当前名称与含义如下：

| 脚本 | 当前命令 |
| --- | --- |
| `npm test` | `node --test` |
| `npm run web:build` | `vite build --config web/vite.config.js` |
| `npm run web:start` | `node src/cli.js web` |
| `npm run publish:npm` | `node scripts/publish-npm.js` |

当前包没有声明 `main` 或 `exports`。正式用户入口是 `bin` 中的 `codex-provider`；包内 `src/*.js` 可以被物理深导入不等于这些内部路径已经成为稳定 npm Public API。

### 2.2 兼容要求

- vNext 引入 Electron 不得迫使现有 CLI 用户额外安装 Electron。
- 在 v0.x 兼容期内，不得仅因 Electron 构建工具需要更高版本 Node.js，就提高 CLI 的最低 Node.js 版本。
- 若未来提高最低 Node.js 版本，必须独立评估 SemVer、发布说明和安装失败体验。
- npm 包仍应提供可直接运行的 JavaScript；CLI 用户不需要安装 TypeScript。

### 2.3 vNext C4 安装入口兼容

根 npm tarball 仍以 `src/cli.js` 作为 bin 目标。判断该模块是否为直接执行入口时，必须分别规范化 `process.argv[1]` 与 `import.meta.url` 对应文件的物理路径；Windows 比较不区分大小写。这样可兼容 npm shim、临时安装目录、8.3 短路径与符号链接解析后的长路径。

该规范化只用于判断是否启动 CLI，不参与 Codex Home、SQLite Home 或任意业务目标解析。无法取得物理路径时可退回绝对词法路径，但不得因为两侧一种是短路径、另一种是长路径而静默跳过 CLI。已打包 tarball 的 `help` 和显式临时 Codex Home `status --json` 是该行为的可执行合同。

## 3. Codex Home 与 SQLite Home

### 3.1 Codex Home

CLI 解析 Codex Home 的优先级固定为：

1. 当前命令的 `--codex-home PATH`；
2. 环境变量 `CODEX_HOME`；
3. 当前用户主目录下的 `~/.codex`。

相对路径按当前工作目录解析为绝对路径。

### 3.2 SQLite Home

CLI 和共享 Core 解析 SQLite Home 的优先级固定为：

1. 当前命令的 `--sqlite-home PATH`；
2. `config.toml` 根级 `sqlite_home`；
3. 环境变量 `CODEX_SQLITE_HOME`；
4. `<Codex Home>/sqlite`。

只有第 4 种默认布局可以回退到旧位置 `<Codex Home>/state_5.sqlite`。显式参数、配置或环境变量选中的 SQLite Home 中缺少 `state_5.sqlite` 时，写操作必须失败，不得静默改用 Codex Home 下的旧数据库。

Windows 下的 `\\wsl.localhost\...` 和 `\\wsl$\...` SQLite Home 仅用于诊断。涉及 SQLite 的操作必须阻止写入，并提示在相应 WSL 发行版内使用 Linux 路径运行 CLI。

## 4. 命令总表

### 4.1 v0.5 当前合同

| 命令 | 主要作用 | 写入 | 关键默认值 |
| --- | --- | --- | --- |
| `status` | 检查 Provider、rollout、SQLite、备份与恢复状态 | 否 | 当前存储布局 |
| `sync` | 将历史元数据同步到目标 Provider | 是 | 当前 Provider；保留 5 份备份 |
| `switch` | 修改根级 Provider，并同步历史元数据 | 是 | 跟随目标 Provider 的 model；保留 5 份备份 |
| `watch` | 监听配置和 SQLite 变化并自动同步 | 是 | 750 ms；监听 state DB；持续运行 |
| `web` | 启动本地 Web UI | Web 操作可写 | 端口 8791；自动打开浏览器 |
| `prune-backups` | 清理较旧的托管备份 | 是，删除托管备份 | 保留 5 份 |
| `restore` | 从托管备份恢复 | 是 | 恢复配置、数据库和 rollout |
| `install-windows-launcher` | 安装 Windows 双击启动脚本 | 是 | 当前用户 Desktop |

### 4.2 帮助

以下调用打印帮助到 stdout，并以 `0` 退出：

```text
codex-provider
codex-provider help
codex-provider --help
codex-provider <command> --help
```

当前没有稳定的 `--version` 或短选项 `-h` 合同。

### 4.3 vNext C2 JSON 入口

以下有限命令支持 opt-in `--json`：

- `status`、`sync`、`switch`、`restore`；
- `prune-backups`、`install-windows-launcher`；
- 全局帮助或命令帮助。

`--json` 可置于命令前或后。`watch` 与 `web` 是长运行接口，不适用“stdout 只写一个终态 JSON 文档”的合同；C2 在创建 watcher、server、runtime descriptor 或浏览器进程前，以结构化 `INVALID_INPUT` 拒绝二者。若未来需要流式机器接口，必须使用独立协议，不能把日志行混入本合同。

## 5. `status`

### 5.1 语法

```text
codex-provider status [--codex-home PATH] [--sqlite-home PATH]
```

### 5.2 当前行为

- 读取 `config.toml`、rollout 元数据、选中的 SQLite state DB、托管备份摘要和未完成事务；
- 不修改配置、rollout、SQLite 或备份；
- 根级 `model_provider` 缺失时，将当前 Provider 报告为 `openai`，并标记为隐式默认；
- 状态扫描遇到锁定 rollout 时跳过该文件，并在结果中报告数量；
- SQLite 缺失、损坏、不可读或暂时 busy 时尽量降级为诊断结果，而不是把状态读取变成写操作；
- 配置明确选中的 SQLite Home 缺少数据库时，报告所检查的位置，不回退到其他数据库。

### 5.3 人类可读输出

成功输出保持以下顶层顺序：

1. `Codex home`；
2. `SQLite home` 及其来源；
3. `Current provider`；
4. `Configured providers`；
5. 托管备份数量、大小与根目录；
6. 可选的 `Recovery required`；
7. `Rollout files`；
8. `SQLite state`；
9. 可选的 `Project visibility`。

动态诊断行可以按实际数据缺省，但不得把错误堆栈写入普通输出。

## 6. `sync`

### 6.1 语法

```text
codex-provider sync [--provider ID] [--keep N] [--codex-home PATH] [--sqlite-home PATH]
```

### 6.2 参数与默认值

| 参数 | 当前含义 | 默认值 |
| --- | --- | --- |
| `--provider ID` | 显式指定目标 Provider | 根级 `model_provider`；缺失时 `openai` |
| `--keep N` | 成功后保留最近 N 份托管备份 | `5` |
| `--codex-home PATH` | 覆盖 Codex Home | 第 3 节规则 |
| `--sqlite-home PATH` | 覆盖 SQLite Home | 第 3 节规则 |

`--keep` 必须是十进制整数且 `N >= 1`。

### 6.3 当前行为

- CLI 每次执行前读取 `config.toml` 的根级 `model`，并把它传给 Core；
- 根级 `model` 读取失败时继续同步 Provider，且不要求重写每线程 model；
- `sync` 不修改根级 `model_provider`；
- 写入前获取同一 Codex Home 的跨进程锁；
- 未完成事务存在时阻止新的写操作；
- SQLite 可写检查发生在创建备份和重写 rollout 之前；
- 每次执行都先创建托管备份，再进行写入；
- 锁定的 rollout 可以被跳过，其他可写 rollout 和 SQLite 仍继续处理；
- 锁定 rollout 导致的是部分成功，而不是事务整体失败；
- 成功提交后自动按 `--keep` 清理旧托管备份；
- 备份 inventory 刷新或自动清理失败，在主事务已提交后降级为 warning。

### 6.4 成功时的进度输出

CLI 使用以下六个编号阶段：

```text
[1/6] Scanning rollout files...
[2/6] Checking locked rollout files...
[3/6] Creating backup...
[4/6] Updating SQLite...
[5/6] Rewriting rollout files...
[6/6] Cleaning backups...
```

备份完成后还会输出备份路径和耗时。

### 6.5 成功摘要

摘要的稳定核心标签为：

- `Synchronized provider`；
- `Codex home`；
- `SQLite home` 及来源；
- `Backup`；
- `Backup creation time`；
- `Updated rollout files`；
- `Updated SQLite rows`。

以下信息按实际结果出现：

- SQLite user-event 修复数量；
- SQLite cwd 修复数量；
- workspace roots 更新数量；
- 被跳过的锁定 rollout 数量和路径预览；
- encrypted content 警告；
- 自动备份清理结果或 warning。

## 7. `switch`

### 7.1 语法

```text
codex-provider switch <provider-id> [--model NAME] [--keep-root-model] [--keep N] [--codex-home PATH] [--sqlite-home PATH]
```

### 7.2 参数与默认值

| 参数 | 当前含义 | 默认值 |
| --- | --- | --- |
| `<provider-id>` | 目标 Provider | 必填 |
| `--model NAME` | 显式设置根级 model，并同步每线程 model | 未设置 |
| `--keep-root-model` | 保留现有根级 model | `false` |
| `--keep N` | 成功后保留最近 N 份托管备份 | `5` |

`--model` 与 `--keep-root-model` 互斥。`--model` 必须是非空字符串。

### 7.3 Provider 校验

- 内置 `openai` 始终是可选 Provider；
- 自定义 Provider 必须在 `config.toml` 的 `[model_providers.<id>]` 中声明；
- 未声明 Provider 必须在任何配置写入之前失败。

### 7.4 model 规则

优先级固定为：

1. 指定 `--model NAME` 时使用显式值；
2. 指定 `--keep-root-model` 时保留根级值；
3. 否则，自定义 Provider section 中存在 `model` 时，将其复制到根级；
4. 自定义 Provider section 没有 `model` 时，保留根级值并输出 warning；
5. 切换到内置 `openai` 时，没有 Provider section model 可自动复制，保留根级值。

最终根级 model 也是本次 rollout 与 SQLite 每线程 model 同步所使用的目标 model。根级 model 缺失时不强制新增线程 model。

### 7.5 事务边界

- 切换前先创建包含原始 `config.toml` 的备份；
- 备份成功后才允许修改根级 Provider/model；
- 配置更新与后续同步处于同一补偿流程；
- 后续失败时恢复原始配置和已观察到的其他变更；
- 成功结果在 `sync` 摘要后输出根级 model 的应用来源、保留原因或 warning。

## 8. `watch`

### 8.1 语法

```text
codex-provider watch [--codex-home PATH] [--sqlite-home PATH] [--debounce-ms N] [--once] [--no-state-db]
```

### 8.2 参数与默认值

| 参数 | 当前含义 | 默认值 |
| --- | --- | --- |
| `--debounce-ms N` | 变化后等待 N 毫秒再同步 | `750` |
| `--once` | 第一次成功同步后退出 | `false` |
| `--no-state-db` | 仅监听 `config.toml` | `false` |

`--debounce-ms` 必须是非负整数，允许 `0`。

### 8.3 当前行为

- 启动时要求 Codex Home 与 `config.toml` 已存在；
- 启动前按物理 Codex Home 建立唯一 Watch scope；重复或并发启动同一 `realpath` Home 不创建第二个 watcher，Windows 比较不区分大小写；物理路径无法可靠解析时 fail closed；
- 默认监听 `config.toml`、当前活动 `state_5.sqlite` 以及 `-wal`、`-shm` sidecar；
- 配置变化后重新解析 SQLite Home，并重新绑定 DB watcher；
- 每次触发同步时重新读取根级 model；
- SQLite busy 是暂态软跳过，等待下一次变化重试；
- 明确配置的 SQLite Home 暂时缺 DB 时暂停同步，等待配置修复；
- 连续 5 次非 busy 同步失败后自动停止 watcher；
- `--once` 只在第一次成功同步后停止，不在 busy、暂停或普通失败后停止；
- 停止时等待正在执行的同步到达完成状态；
- `SIGINT`、`SIGTERM`、`--once` 和内部自动停止都走清理流程。

## 9. `web`

### 9.1 语法

```text
codex-provider web [--port N] [--no-open] [--reset-access] [--codex-home PATH] [--sqlite-home PATH]
```

### 9.2 参数与默认值

| 参数 | 当前含义 | 默认值 |
| --- | --- | --- |
| `--port N` | 回环监听端口 | `8791` |
| `--no-open` | 不自动打开系统浏览器 | `false` |
| `--reset-access` | 使已配对浏览器失效并创建新配对 | `false` |

端口必须是 `0..65535` 的整数；`0` 表示由操作系统选择可用端口。

### 9.3 当前行为

- 服务只监听 `127.0.0.1`；
- 默认尝试打开带一次性配对 token 的 URL；
- 无桌面 Linux 环境或浏览器打开失败时，服务继续运行并输出配对 URL；
- 输出 Web UI URL；在 `--no-open` 或打开失败时输出一次性配对链接；
- 同一状态文件记录的现有实例只有在 effective Codex Home 与 SQLite Home 一致且内部认证通过时才能复用；
- 复用成功后当前 CLI 进程退出，已有服务继续运行；
- 非复用实例输出仅监听回环和按 `Ctrl+C` 停止的提示；
- 默认状态文件为 `<Codex Home>/provider-sync-web.json`；
- 默认运行时描述文件为 `<Codex Home>/provider-sync-web.runtime.json`。

## 10. `prune-backups`

### 10.1 语法

```text
codex-provider prune-backups [--keep N] [--codex-home PATH]
```

### 10.2 当前行为

- `--keep` 默认 `5`，必须是非负整数，允许 `0`；
- 只处理 `<Codex Home>/backups_state/provider-sync` 中具有本工具 metadata namespace 的托管备份；
- 忽略不具有托管 metadata 的手工目录；
- 永不删除未完成事务正在引用的恢复备份；
- 使用同一 Codex Home 的跨进程锁；
- 输出 backup root、删除数量、剩余数量和释放空间。

## 11. `restore`

### 11.1 语法

```text
codex-provider restore <backup-dir> [--no-config] [--no-db] [--no-sessions] [--allow-sqlite-home-relocation] [--codex-home PATH] [--sqlite-home PATH]
```

### 11.2 默认值

| 内容 | 默认是否恢复 | 关闭参数 |
| --- | --- | --- |
| `config.toml` 与 global state | 是 | `--no-config` |
| SQLite state DB | 是 | `--no-db` |
| rollout 元数据 | 是 | `--no-sessions` |

### 11.3 当前行为

- 备份路径必填；
- 使用同一 Codex Home 的 restore 锁；
- 备份 metadata 与 session manifest 必须合法并绑定当前 Codex Home；
- 恢复 SQLite 时校验备份 SQLite Home 与当前目标；
- `--allow-sqlite-home-relocation` 必须与显式 `--sqlite-home PATH` 一起使用；
- 真正跨 SQLite Home 恢复时禁止同时恢复旧 `config.toml`，必须使用 `--no-config`；
- 未完成事务需要的内容不得通过 `--no-*` 形成不完整恢复；
- 成功恢复后标记相应 transaction journal 已回滚；
- 输出备份绝对路径、Codex Home、备份时 Provider，以及可选 inventory warning。

## 12. `install-windows-launcher`

### 12.1 语法

```text
codex-provider install-windows-launcher [--dir PATH] [--codex-home PATH] [--sqlite-home PATH]
```

### 12.2 当前行为

- `--dir` 缺失时使用当前用户 Desktop；
- 创建 `Codex Provider Sync.cmd`；
- 创建 `Codex Provider Sync.vbs`；
- 两个启动器最终执行 `codex-provider sync`；
- 显式 Codex Home 与 SQLite Home 被固化到启动器；
- cmd 保留 CLI 退出码；
- vbs 以消息框显示成功或失败，并限制展示文本长度。

## 13. stdout、stderr 与退出码

### 13.1 Human Mode 兼容合同

未传入 `--json` 时继续使用 v0.5 人类可读接口，并保持原有 `0/1` 退出行为。

| 场景 | stdout | stderr | Exit Code |
| --- | --- | --- | --- |
| 帮助 | 帮助文本 | 空 | `0` |
| 命令成功 | 进度和/或摘要 | 空 | `0` |
| 无需修改 | 正常摘要 | 空 | `0` |
| 锁定 rollout 导致部分成功 | 摘要含 skipped locked | 空 | `0` |
| 参数、存储、busy、锁、恢复或普通失败 | 可能已有提交前进度 | 单行错误 message | `1` |
| Web/Watch 在信号处理器安装后收到 SIGINT/SIGTERM | 正常清理输出 | 空 | `0` |
| Watch 连续 5 次非 busy 失败后自动停止 | 失败与停止日志 | 空 | `0` |

错误默认不输出 JavaScript stack。

Human Mode 不采用 JSON 模式的 `2`、`3`、`4`、`5`、`130`；partial 仍为 `0`，其余既有失败仍为 `1`。这一区分防止现有脚本因 opt-in JSON 能力而改变行为。

### 13.2 vNext C2 JSON 合同

JSON Mode 的 stdout 必须恰好包含一个 UTF-8 JSON 文档和结尾换行；进度、运行时 warning 与诊断只能进入 stderr。顶层字段始终全部存在、顺序固定，且不得增加未版本化字段：

```json
{
  "schemaVersion": 1,
  "command": "sync",
  "ok": true,
  "outcome": "completed",
  "result": {},
  "warnings": [],
  "error": null
}
```

字段合同：

| 字段 | 合同 |
| --- | --- |
| `schemaVersion` | 固定为整数 `1` |
| `command` | 规范化命令名；全局帮助为 `help` |
| `ok` | 业务调用是否得到成功 Result；`partial` 仍为 `true` |
| `outcome` | `completed`、`noop`、`partial`、`failed`、`failed_rolled_back`、`recovery_required`、`cancelled` 或 `stale` |
| `result` | 成功时为命令结果对象，失败时为 `null` |
| `warnings` | 字符串数组；没有 warning 时为空数组 |
| `error` | 失败时为安全的 `CoreErrorDto`，成功时为 `null`；不得包含 stack、cause、凭据、Token 或消息正文 |

所有 Canonical Error Code 在 JSON 中使用固定安全 message；未知异常统一输出稳定的 `INTERNAL_ERROR`，不回显参数值或底层 message。Error details 只允许经审计且枚举/格式受限的 scope、reason、SQLite source/cause 字段；operationId 只接受 UUID，suggestedAction 不直接透传。成功 result 按命令使用字段 allowlist，只保留产品 DTO 中已审计的状态、计数、Provider/model 与路径字段，并移除凭据、Token、secret、stack/cause、prompt 和消息正文；底层 warning message 归一为稳定摘要。JSON 参数解析为严格模式：未知 flag、重复 flag、缺值、多余位置参数、布尔 flag 带值和 `--json=<value>` 均以 `INVALID_INPUT` 失败；第 14 节的宽松行为只为 Human Mode 保留。

JSON Mode 退出码固定为：

| Exit Code | 含义 |
| --- | --- |
| `0` | 成功或 noop |
| `1` | 普通失败，包含已安全回滚的失败 |
| `2` | 输入无效、Plan 过期或状态漂移 |
| `3` | partial success |
| `4` | recovery required 或 pending transaction |
| `5` | operation busy、SQLite busy 或 lock unverifiable |
| `130` | cancelled |

CLI Exit Code 与 Error Code 是两层合同：多个 Canonical Error Code 可以映射到同一退出码。`--help --json` 返回 `ok:true` 的 schema v1 帮助结果；不存在稳定的 `--version` JSON 合同。

### 13.3 后续变更规则

以后新增或重映射退出码前必须：

1. 调查现有脚本是否依赖 `0/1`；
2. 明确 Human Mode 与 JSON Mode 是否改变各自退出码；
3. 增加真实子进程 Contract Test；
4. 在 CHANGELOG 和发布说明中声明；
5. 不把 partial、busy 或 recovery 的退出码变化混入无关重构。

## 14. legacy tolerated 行为

以下行为由当前手写参数解析器接受，但不属于长期稳定命令合同：

- `switch --provider ID` 可代替位置参数；
- `restore --backup PATH` 可代替位置参数；
- 使用 `--flag=value`；
- 重复 flag 时最后一个值覆盖前值；
- 未识别 flag 被静默保留或忽略；
- 多余位置参数在多数命令中被忽略；
- 布尔 flag 后的普通 token 可能被当作该 flag 的值吞掉；
- `--no-open=false` 等非空字符串仍会被解释为启用；
- 没有标准 `--` 参数终止符。

这些行为的处理原则：

- 不为它们新增长期快照测试；
- vNext 不能在不知情的情况下依赖它们；
- 未来改用严格参数解析器时，应先保留所有已文档化语法；
- 若决定移除被真实用户使用的 tolerated 形式，应给出兼容警告或版本化迁移说明；
- 安全修复可以拒绝危险或歧义输入，但必须单独说明。

## 15. vNext 目标合同

以下内容是 V1 后续目标；其中 C2 已实现的合同是后续阶段必须保持的不变量：

- CLI、Local Web UI 与 Electron Desktop 调用同一个 Core Public API；
- Desktop 不启动 CLI，也不解析 CLI 人类文本；
- 保持 C2 已实现的 opt-in JSON schema、stdout/stderr 分工和退出码矩阵；
- 在 Prepare/Apply 落地后，让 JSON 命令使用相同的 Plan/Result DTO，而不暴露内部适配器；
- 使用稳定 Error Code，同时保留现有人类错误提示的核心语义；
- 支持取消时使用安全取消点，而不是强制中止事务。

任何目标行为只有在实现、测试和本文更新后才成为当前合同。

## 16. 最低 Contract Test 清单

Phase 0 之后的 CLI 改造至少必须覆盖：

- 帮助文本可运行且 exit `0`；
- 未知命令和非法 `--keep` 的 stderr 与 exit `1`；
- `status` 的顶层输出顺序；
- `sync` 默认 Provider、默认 keep=5 与六阶段进度；
- `switch` 的三种 model 策略与互斥校验；
- partial sync 当前仍 exit `0`；
- recovery/busy/lock 当前仍 exit `1`；
- restore 三类默认恢复和三个 `--no-*`；
- SQLite Home 优先级与 default-only legacy fallback；
- Web 默认端口、回环绑定与复用；
- Watch 默认 750 ms、once 成功退出与连续失败停止；
- npm `bin`、`engines` 与发布包包含 CLI/Web 运行所需文件；安装态 tarball 必须经真实 bin shim 完成 `sync --json → managed backup → drift → restore --json`，逐字节恢复 config/rollout、恢复 SQLite Provider 且不留下 pending recovery；
- JSON help、输入错误、成功、noop、partial、rolled-back、stale、recovery、busy、lock unverifiable 与 cancelled 均为单一 stdout 文档；
- JSON progress 只进入 stderr，未知异常、底层 warning 与序列化失败不泄漏 stack、cause、凭据、Token、secret 或消息正文；
- `watch --json` 与 `web --json` 在任何长运行副作用前被拒绝；
- Human Mode 的帮助、进度、partial 和 `0/1` 行为保持既有回归。

## 17. 变更控制

修改以下任一内容，必须同时更新本文、Contract Test 和发布说明：

- 命令名称或参数名称；
- 参数默认值；
- Provider/model 选择规则；
- Codex Home 或 SQLite Home 优先级；
- 人类输出的稳定核心标签；
- stdout/stderr 分工；
- 退出码；
- partial、busy、recovery 或取消语义；
- 备份、锁和恢复边界；
- npm bin 或最低 Node.js 版本。
