# CLI 使用指南

适用于 V1 Node Core。CLI、Web 和 Electron 使用同一套同步业务；CLI 不启动 Electron，也不依赖桌面安装。npm 安装得到的是已发布版本，仓库 V1 的新增能力以本页和实际安装版本的 `codex-provider help` 为准。

## 安装与检查

已发布根包支持 Node.js `16.20.2+`；从仓库构建完整工作区使用 Node 24。

```bash
npm install -g @dailin521/codex-provider-sync
codex-provider help
codex-provider status
```

`status` 是只读检查。先核对当前 Provider、Codex Home、SQLite Home 和实际数据库文件。会话文件数量与索引行数不一定相等，Provider 分布才是同步判断的主要依据。状态尚未完整读取时，不能把命令退出 0 当作“已同步”。

## 同步和切换

### 已通过其他工具切换 Provider

```bash
codex-provider sync
```

目标始终来自 `config.toml` 根级 `model_provider`，缺失时使用内置 `openai`；自定义 Provider 必须已在配置中定义。Sync 不修改 config，不调整历史模型、目录或消息标记。没有 `sync --provider` 或 `--fast` 参数。

### 由本工具切换 Provider

```bash
codex-provider switch openai
codex-provider switch my-provider --keep-root-model
codex-provider switch my-provider --model model-name
```

`my-provider` 和 `model-name` 是示例值；自定义 Provider 需要预先配置，不会由此命令创建。

| 模型策略 | 对根级 `model` 的影响 |
| --- | --- |
| 不传模型选项 | 目标 Provider 配置了 `model` 时采用该值，否则保留当前根模型 |
| `--keep-root-model` | 保留当前根模型 |
| `--model NAME` | 设置为指定名称；不与 `--keep-root-model` 同用 |

三种方式都先修改配置，再执行同一个 ProviderSync，**不会修改历史会话记录的模型**。

CLI 的 Sync/Switch/Repair/Restore 命令直接执行，不提供交互式计划确认。内部仍连续 Prepare → Apply，保留锁、复核和备份；想先查看影响再确认，请使用桌面或 Web 界面。

## 指定存储位置

Windows 示例（按实际目录替换）：

```powershell
codex-provider status --codex-home "D:\CodexData\.codex"
codex-provider status --codex-home "D:\CodexData\.codex" --sqlite-home "D:\CodexData\sqlite"
```

- Codex Home：`--codex-home` → `CODEX_HOME` → `~/.codex`。
- SQLite Home：`--sqlite-home` → config 根级 `sqlite_home` → `CODEX_SQLITE_HOME` → `<Codex Home>/sqlite`。
- 只有默认布局能回退到 `<Codex Home>/state_5.sqlite`。明确指定的位置缺失数据库时会报错，不会静默操作另一份数据。
- 后续 Sync/Switch/Repair/Restore 应使用同一组路径参数；CLI 不读取桌面端选中的存储配置。

Windows 对 `\\wsl.localhost\...` / `\\wsl$\...` SQLite Home 只做诊断。需要写入时进入对应 WSL，在 Linux 环境运行已安装的 CLI，例如：

```bash
codex-provider status --codex-home /mnt/d/CodexData/.codex --sqlite-home /home/example/.codex/sqlite
```

先核对输出，再在同一环境执行需要的写命令。不要从 Windows 直接操作 WSL SQLite。

## 备份、恢复与清理

存在实际写入时先创建受管备份，默认保留最近 2 份；noop 不产生备份。备份位于 `<Codex Home>/backups_state/provider-sync/`，同一 Home 的操作共用备份池，不是每个命令各保留 2 份。CLI 不共享桌面或浏览器的备份偏好。

```bash
codex-provider sync --keep 2
codex-provider prune-backups --keep 2
```

`--keep` 对 Sync/Switch/Repair 要求至少 1。Prune 允许 0，表示删除全部可清理的受管备份；它是直接删除操作，不要把 0 当作“关闭自动清理”。恢复依赖的受保护备份不会被强制裁剪。

使用操作输出或状态中提供的备份位置恢复，例如：

```powershell
codex-provider restore "D:\CodexData\.codex\backups_state\provider-sync\BACKUP_ID" --codex-home "D:\CodexData\.codex"
```

将 `BACKUP_ID` 替换为已有备份目录名。默认恢复备份实际包含的配置、索引和会话元数据；`--no-config`、`--no-db`、`--no-sessions` 可排除对应内容。它不能恢复备份中未保存的数据。

跨 SQLite Home 恢复必须明确给出 `--sqlite-home`、`--allow-sqlite-home-relocation` 和 `--no-config`；先核对目标。建议通过桌面/Web 恢复预览完成迁移操作，避免误选路径。

Restore 会先保存目标当前状态，再使用独立 journal 和补偿机制恢复。未解决的恢复状态不能靠删除锁文件或备份来绕过。

## 诊断与专项修复

```bash
codex-provider diagnostics
```

这会主动运行一次完整只读检查，不会自动修复。模型差异、加密内容数量等不一定表示损坏，日常同步无需执行这些检查。

`repair` 的 targets 用逗号分隔，大小写如下：

| 目标 | 实际作用 |
| --- | --- |
| `models` | 将历史模型名称调整到 config 当前根模型；缺少根模型时拒绝执行 |
| `cwd` | 按会话文件记录修正索引中的会话目录，不移动文件 |
| `userEvent` | 补全索引中已有用户消息的标记，不增删消息 |
| `workspaceRoots` | 整理全存储配置的工作区设置，自动包含 `cwd`；不是删除项目目录 |

仅在明确需要上述变更时执行，例如 `codex-provider repair cwd,userEvent`。CLI 直接执行所选目标；需要按会话预览/选择时使用桌面/Web。当前不支持加密内容修改、记录序号改写或历史显示索引重建。

## 自动同步和 Web

```bash
codex-provider watch
codex-provider watch --no-state-db
codex-provider web --no-open
```

Watch 是主动开启的长期监听，默认同时监听配置和 SQLite 状态事件；`--no-state-db` 仅监听配置。每次仍调用同一 Sync，获取 Home 锁并备份实际目标，忙时给人工操作让路。默认防抖 750 ms，可用 `--debounce-ms N` 调整。按 Ctrl+C 停止。

Watch 的 CLI 入口没有 `--keep` 参数，使用默认保留 2 份；桌面/Web 在开启自动同步时使用各自保存的统一备份设置。Web 配对和 SSH 用法见 [Web 指南](README_WEB_UI_ZH.md)。

## JSON 与退出码

```bash
codex-provider sync --json
```

有限命令的 stdout 严格只有一个终态对象：`{schemaVersion, command, ok, outcome, result, warnings, error}`。进度与运行时诊断进入 stderr。不要把 stderr 合并进待解析的 JSON；`watch` / `web` 不支持此单文档模式。

| JSON 退出码 | 含义 |
| --- | --- |
| 0 | 成功或无需修改 |
| 1 | 普通失败；兼容结果也可能表示已回滚，结合 `outcome` 判断 |
| 2 | 输入无效、计划过期或状态变化，需修正输入/重新准备 |
| 3 | 部分完成，查看失败阶段、备份和重试建议 |
| 4 | 需要恢复处理 |
| 5 | 正忙或无法验证锁 |
| 130 | 已取消 |

Human 模式保留旧的 0/1 行为，其中可跳过文件的 partial Sync 仍可能退出 0。自动化务必使用 JSON，并检查 `outcome`，不能只看命令是否结束。

Windows 新 Sync/Switch 结果可带 `fileUpdateTiming`，包含复制、落盘、替换、清理、时间戳恢复等数值；没有该字段时表示未记录，不是耗时为零。它不是进度流，不记录文件路径或消息内容。

## 遇到失败

- **Provider 未定义**：先修复配置；Sync 不会擅自切回 OpenAI。
- **数据已变化/计划过期**：重新检查并运行命令；不要重放旧 planId。
- **会话占用/部分完成**：结束相关写入后再次同步；重试只处理仍未对齐的目标。若要撤销，选择受管备份恢复。
- **SQLite busy**：预检阶段 busy 会在修改前停止；写入后才发生 busy 则可能 partial，以结果为准。
- **恢复未完成**：保留备份和 journal，在桌面/Web 查看恢复状态；不要删除保护文件。
- **历史可见但无法继续**：可能需要原 Provider/账号才能解密，不能用重复 Sync 或 Repair 解密。

普通写入不会自动全量回滚。当前 CLI 的 Sync/Switch/Repair/Restore 没有提供受控取消入口；请等待终态结果，不要用 Ctrl+C 或强制结束进程代替恢复流程。JSON 的 130 是已取消结果的映射，不保证终端中断会输出该对象。Watch 和 Web 单独处理 Ctrl+C/SIGTERM，按其清理流程停止。

[返回首页](../README.md) · [工作原理](WORKING_PRINCIPLE_ZH.md) · [精确 CLI 合同](architecture/contracts/CLI_CONTRACT_ZH.md)
