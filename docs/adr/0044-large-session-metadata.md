# ADR-0044：大首行兼容与元数据错误分类

> 后续修订：[ADR-0045](0045-isolated-provider-data-skips.md) 取代 Provider 单条问题全局拒绝及集合漂移整体失效规则，保留其余边界。

- 状态：Accepted
- 日期：2026-09-15

## 背景

Issue #102 报告关闭 Codex 后仍在准备阶段立即提示会话变化。隔离复现证明首行超限和格式无效都会被错误归类为 ROLLOUT_CHANGED；尚未确认该用户现场原因。用户选择以 128 MiB 为候选支持上限，并要求验证实际读写，而非仅修改提示。

## 决策

1. Node Status、Provider Prepare/revision/Sync/Switch 支持至多 134217728 字节的首行 UTF-8 内容，不含 LF/CRLF。无末尾换行时全部内容即首行。保持 64 KiB 分块及最多一个块的尾部预读；边界只需两字节判定 CRLF，不扫描正文寻找替代 metadata。Repair 保留独立 1 MiB 上限，Diagnostics/History 保持原有边界。Legacy .NET 不在此次范围。
2. JS 与 Windows 首行读取只扫描新块，避免每块复制/扫描已收集前缀；Windows 写前校验受原首行长度约束，漂移继续跳过。原地资格使用线性 JSON 字符串 token 遍历，保留转义/重复键、字节等长、语义及身份校验。原地 worker 不传无用的新首行。变长替换后的首行也必须在上限内，否则 Prepare 在任何业务写入前拒绝；等长原地路径按实际保留的原首行长度校验。保持 PIO-2～PIO-6、锁、备份、时间戳和正文 byte preservation。
3. 新增 ROLLOUT_METADATA_TOO_LARGE（超出支持上限）、ROLLOUT_METADATA_INVALID（不能解析为既有定义的 session_meta），severity=error、retryable=false、recoveryRequired=false。不可自动重试不表示用户处理原因后不能再次执行。真实漂移继续使用原错误/partial 规则；Prepare 拒绝仍为零业务写入、无备份。
4. Contracts、CLI JSON、Web、Electron 与操作日志使用相同安全错误。Web 返回 422；CLI JSON 保持 schemaVersion=1 和一般失败退出码。不新增公开参数、路径字段或原始异常文本。保留固定 failureStage，中英文说明实际原因。Status 超限仍报告不完整，不能伪造健康或忙碌。
5. 128 MiB 是单文件读取上限，不是整个操作的内存预算；计划仍可包含多个待改文件。不得宣称任意规模 Home 都有固定内存开销。测试记录近上限的耗时与进程峰值；本 ADR 不构成正式安装包、其他平台或用户现场验收。

## 验证

- test/large-session-metadata.test.js：默认 8 MiB 有效首行，Status、原地 Sync、变长 Switch、各自 Restore、SQLite 字段及正文 hash；虚拟 128 MiB LF/CRLF/EOF 边界与超一字节，读取/合并字节计数；真实无效/超限 PrepareSync/PrepareSwitch 零写入、HTTP 422 和安全 DTO。
- CPS_LARGE_HEADER_MIB=128 配合 --test-name-pattern="large valid metadata" 运行真实近上限流程并输出父进程 peak RSS；只使用临时合成 Home。
- provider-preparation-facts、status-coordination、in-place-transaction、windows-rewrite-worker 等原门禁继续保持；修改的预期以本 ADR 的新分类及上限为依据。
- 生产 Electron 大首行 Sync→Restore 检查、architecture:check、npm test；CI 跨平台及实际安装验收仍独立。
