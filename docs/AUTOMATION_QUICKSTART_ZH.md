# 自动化接口快速开始

`CodexProviderSync.Automation.exe` 是供脚本、持续集成（CI）和 AI Agent 调用的实验性 Windows 接口。普通桌面用户不需要它，直接使用 `CodexProviderSync.exe` 即可。

自动化接口包包含：

- `CodexProviderSync.Automation.exe`
- `automation-protocol-v0.4.schema.json`
- 本说明文件

接口每次运行只向标准输出写入一份协议 `0.4` JSON；诊断信息写入标准错误。所有路径都必须是绝对路径。

## 只读命令

```powershell
# 查看协议能力和安全要求
.\CodexProviderSync.Automation.exe describe

# 查看当前 Provider、rollout 和 SQLite 状态
.\CodexProviderSync.Automation.exe status `
  --codex-home C:\Users\you\.codex
```

## 先生成计划

写命令默认不会修改数据。以下示例生成一份同步计划：

```powershell
$planResponse = .\CodexProviderSync.Automation.exe plan `
  --operation sync `
  --codex-home C:\Users\you\.codex `
  --provider openai | ConvertFrom-Json

$planPath = 'C:\Temp\codex-provider-sync-plan.json'
$planJson = $planResponse.data | ConvertTo-Json -Depth 100 -Compress
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($planPath, $planJson, $utf8NoBom)
$planDigest = $planResponse.data.digest
```

请先检查返回的计划、警告和目标列表。计划会绑定当前输入与目标状态，具有有效期，并且只能使用一次。

## 明确执行计划

确认计划无误后，使用同一组操作参数，并同时提供 `--apply`、计划文件和计划摘要：

```powershell
.\CodexProviderSync.Automation.exe sync `
  --codex-home C:\Users\you\.codex `
  --provider openai `
  --apply `
  --plan $planPath `
  --plan-digest $planDigest
```

如果目标状态在计划生成后发生变化，执行会被拒绝，需要重新生成计划。同步和切换仍遵循 Core 的备份、事务、回滚、锁和 WSL UNC 安全规则。

## 支持的命令

| 命令 | 用途 |
| --- | --- |
| `describe` | 查看协议能力和安全要求 |
| `status` | 只读检查当前状态 |
| `plan` | 为写操作生成计划 |
| `sync` | 同步历史会话元数据 |
| `switch` | 切换 Provider/model 后同步 |
| `restore` | 恢复托管备份 |
| `prune` | 清理旧的托管备份 |

完整参数、安全限制和返回结构见项目 [README](../README.md) 与 `automation-protocol-v0.4.schema.json`。协议 `0.4` 仍处于 1.0 之前的实验阶段，未来可能发生不兼容变更。
