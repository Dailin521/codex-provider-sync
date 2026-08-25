# C1 Public API 与结构化错误证据（2026-08-25）

状态：本地门禁通过，等待远端 CI。输入 checkpoint 为 `29c84ec`；C1 最终 commit SHA 在本 checkpoint 提交后及 C10 evidence bundle 中索引。

## 边界

- `src/public-api.js` 是产品入口使用 Node Core 的唯一公开入口；CLI 与 Web 不再深度导入 Core 实现模块，Watch 的延迟同步调用也经该入口。
- `runSync`、`runSwitch`、`runRestore`、`runWatch` 仅作为已标记弃用的迁移兼容适配器保留；本 checkpoint 不把它们定义为 Renderer API，也不声称 C3 Prepare/Apply 已实现。
- Status 的 Human 呈现从业务 service 移到 `cli-presenter.js`，避免公共 Core 方法携带 CLI 输出职责。
- 未移动同步算法，未改变备份优先、事务、回滚、locked rollout partial 或 WSL UNC 阻断规则。
- 未读取真实 Codex Home、认证信息、凭据、token 或消息正文；未创建 tag 或执行发布动作。

## 公共错误边界

- `CoreError` 固化 canonical code、severity、retryable、recoveryRequired、operationId、details 和 suggestedAction；DTO 不包含 stack、cause 或任意异常属性。
- 新增并测试 `PLAN_EXPIRED`、`STALE_STATE`、`LOCK_UNVERIFIABLE`；`OPERATION_BUSY` 必须携带可信 `busyScope`，`LOCK_UNVERIFIABLE` 必须携带可信 `lockScope`。
- 普通异常即使伪造 canonical `code` 也不能把任意 `details`、operationId、token 或消息正文注入 DTO；details 在规范化后递归冻结。
- 公共写入兼容入口、History 与 Watch 的确定输入错误使用 `INVALID_INPUT`；既有确认快照漂移暂用合同中的 `PLAN_STALE`，C3 的正式 Apply revision 复核使用 `STALE_STATE`。
- SQLite 分类只依赖 driver primary result code 或明确 symbolic code，不依赖英文 message；Web 保持旧 `{error, code?}` 形状，仅对真实 `CoreError` 追加安全 `coreError` DTO。
- Restore 的未完成事务覆盖不足使用 `RECOVERY_REQUIRED`；文件/目录访问拒绝使用 `PERMISSION_DENIED`。

## 锁协议增量

- 协议 v2 owner 可选记录 `scope`；当前 Home 锁为 `codex-home`，资源锁入口可显式使用 `state-db`，旧读取器可忽略该字段。
- 已确认 live owner 返回 `OPERATION_BUSY`；进程代际、目录身份、owner 或 cleanup 无法可靠证明时 fail-closed 返回 `LOCK_UNVERIFIABLE`。
- 本 checkpoint 只建立 scope/error 基础和跨运行时不确定性判定；真实 State DB identity、固定顺序双层获取与 Node/.NET 同时持锁仍属于 C3，本文不声称已完成。

## 本地验证

环境：Windows x64，Node `v24.11.1`，npm `11.10.0`。

| 命令 | 结果 |
| --- | --- |
| `npm test` | 283 passed，0 failed，0 skipped |
| `npm run web:build` | Vite 8.2.2 production build 成功，21 modules transformed |
| `npm audit --omit=dev --audit-level=moderate` | 0 vulnerabilities |
| `npm audit --audit-level=high` | 0 vulnerabilities |
| `npm pack --dry-run --json` | 成功；新 public API、CoreError 与 presenter 均进入根 npm tarball |
| `dotnet test desktop/CodexProviderSync.Core.Tests/CodexProviderSync.Core.Tests.csproj --filter FullyQualifiedName~LockServiceTests --configuration Release` | 28 passed，0 failed，0 skipped |
| `git diff --check` | 通过，仅有既有 Windows CRLF 工作区提示 |

另进行了三轮独立只读复审；最终结论无 P0/P1/P2 阻断。复审提出的 DTO 任意属性泄漏、跨运行时 owner 不确定性误分 busy、Restore/Web 未类型化、lock scope 丢失、cleanup AggregateError 丢码及权限错误分类均已修复并有回归覆盖。

## 已知未闭合项

- CLI 严格单对象 JSON stdout 与退出码矩阵属于 C2，当前 Human CLI 兼容行为保持不变。
- Plan ledger、10 分钟 TTL、单次消费、revision、双层锁和 operation snapshot 属于 C3。
- Node 16.20.2 tarball 的 Windows/Ubuntu 真正安装执行仍由后续兼容 CI 证明；本地 Node 24 结果不能替代该证据。
- 当前 npm/Web dist 仍包含既有 source map；Electron/发布产物必须在 C9 排除并扫描证明。
- 只有最终 PR 的所有必需 job 成功并合入受保护分支后，对应 Phase 才能标记 Completed。
