# vNext/ADR-0003：保留 Node CLI 兼容合同

- Status: Accepted
- Date: 2026-08-24
- Scope: vNext

## Context

`codex-provider` 是现有 npm、自动化、诊断和 WSL 用户的正式入口。Electron 不能成为使用 Core 的前置条件，也不能以桌面迁移为理由破坏脚本。

## Decision

保留现有命令名称、参数语义、默认值和主要人类输出。CLI 与 Electron 调用同一 Core，但各自拥有 Presenter。后续 `--json` 是 opt-in 加法，不替换默认文本模式。

完整冻结面见 [CLI 合同](../architecture/contracts/CLI_CONTRACT_ZH.md)。

## Decision Drivers

- 保护 npm/脚本/WSL 用户；
- 避免 Electron 依赖进入 CLI 安装；
- 为自动化提供可版本化的结构化输出；
- 让人类输出和机器协议独立演进。

## Invariants

- `status`、`sync`、`switch`、`watch`、`web`、`prune-backups`、`restore`、`install-windows-launcher` 不得无迁移方案地移除或改名；
- 当前 v0.5 退出码只有成功 `0` 与失败 `1`；架构建议的细分退出码尚未实现；
- 新增 `--json` 时 stdout 必须是单一机器文档，诊断写 stderr；
- 默认人类文本不是 Electron 或第三方可以解析的 IPC；
- WSL SQLite 操作继续由运行在 WSL 内的 CLI 支持；
- npm CLI 不能要求用户安装 Electron。

## Consequences

兼容变化必须有契约测试、SemVer 判断和发布说明。当前解析器对未知 flag 等宽松行为只记为 legacy tolerated，不自动升级为永久 API。

## Rejected Alternatives

- **桌面端执行 CLI 并解析文本**：脆弱且丢失结构化错误/进度。
- **Electron 替代 npm 包**：破坏服务器、脚本和 WSL 场景。
- **直接改变默认输出为 JSON**：会破坏现有人类使用和脚本。

## Migration and Validation

Public API 提取阶段保持命令快照与行为测试；结构化错误和 `--json` 分别通过独立 PR 落地。任何新退出码都必须先更新合同并提供兼容说明。

## Related

- [CLI 合同](../architecture/contracts/CLI_CONTRACT_ZH.md)
- [错误码合同](../architecture/contracts/ERROR_CODES_ZH.md)
