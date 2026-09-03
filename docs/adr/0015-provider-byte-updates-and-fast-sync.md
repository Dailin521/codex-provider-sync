# ADR-0015: Provider 字节原地更新与统一快速同步模式

- 状态：Partially superseded by ADR-0016
- 日期：2026-09-03
- 接收来源：[PR #92](https://github.com/Dailin521/codex-provider-sync/pull/92)（head `782a901`，作者 `cccat6`）
- 适用范围：V1 Node Core、CLI、Local Web UI、Electron Desktop

> 2026-09-03 范围说明：ADR-0016 保留“等长原地更新”和“不等长时正文逐字节保持”的性能决策；显式 full/fast 模式、`syncMode`、`--fast`、`FAST_MODE_UNSUPPORTED`、ProviderSync 全文扫描及相关公开 DTO 已被取代。以下内容保留为当时接收 PR #92 的历史决策记录。

## 背景

rollout 通常远大于需要修改的 `session_meta.payload.model_provider`。完整重写几十 MiB 的正文会产生与实际业务字段不成比例的读取、写入和文件替换成本。PR #92 恢复了等长 Provider 字节更新，并提出只读取首行的显式快速模式。

V1 已使用统一 Core、Plan/Apply、共享 UI 和 Restore v2，因此接收该优化时必须进入同一公共能力，而不能保留为 CLI 私有分支。

## 决策

1. 普通 `full` 模式保持既有业务语义，并自动优先使用原地 Provider 更新：
   - Provider JSON 字面量编码后等长；
   - `model_provider` 位置唯一且可确定；
   - 本次不需要历史模型重写。
   不满足条件时继续使用现有完整文件重写。用户不需要、也不应为了优化而把所有 Provider ID 改成固定长度。

2. 完整模式把加密标记、用户事件和 `turn_context.model` 采集合并为一次正文流扫描，不对同一 rollout 重复扫描正文。

3. 公共 Prepare 输入新增可选 `syncMode: "full" | "fast"`，省略时为 `full`。CLI 的 `--fast` 和共享 Web/Electron UI 只是该字段的适配器；Apply 仍只接受一次性 `planId`。

4. `fast` 模式只读取 rollout 首行（上限 1 MiB），保留根模型、历史模型和用户事件标志。所有待改 rollout 都必须满足原地更新条件；任一文件不满足时，在备份和业务写入前返回 `FAST_MODE_UNSUPPORTED`，不自动回退完整模式。

5. `PlanSummary.providerSync` 报告模式、扫描范围、写入策略、未检查项和预期原地/完整重写数量；`OperationResult.providerSync` 报告实际原地/完整重写数量。

6. POSIX 使用同一打开句柄定位并写入 Provider 字节；Windows 使用复用的 PowerShell worker 和 `windows-provider-bytes.cs` helper。备份 manifest v2 可带可选 `mutation` 描述，标准 v2 字段与 v1/v2 读取兼容继续保留。

## 性能验收

- 大正文确定性测试验证写入字节数等于 Provider JSON 字面量长度、正文 tail hash 不变、文件大小与文件身份不变。
- 快速模式测试验证同步期间不打开 rollout 正文流。
- 完整模式测试验证每个 rollout 正文只扫描一次。
- `scripts/benchmark-provider-io.mjs` 仅生成手工性能证据；CI 不使用 wall-clock、吞吐或 hosted runner I/O 数值作为门禁。

## 结果

- 常见等长 Provider 切换避免重写大型聊天正文。
- 明确的 Provider-only 工作流同时减少正文读取和写入。
- 不等长 Provider 仍可直接使用普通模式，不形成命名约束。
- CLI、Web 和 Electron 共享同一 Core 语义、计划展示与结果统计。
