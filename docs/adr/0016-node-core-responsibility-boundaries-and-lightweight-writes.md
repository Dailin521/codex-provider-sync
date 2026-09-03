# ADR-0016: Node Core 职责边界与轻量写路径

- 状态：Accepted
- 日期：2026-09-03
- 适用范围：V1 Node Core、CLI、Local Web UI、Electron Desktop
- 取代范围：ADR-0015 的显式 `fast` 模式与普通 Sync 完整正文扫描决策；Provider 等长原地更新继续有效

## 背景

现有 Node Core 已具备统一 Facade、Plan/Apply、锁、备份、事务与恢复能力，但普通 Provider 同步同时承担模型、cwd、user-event、workspace roots 修复，并把跨文件 journal、自动回滚和 State DB 资源锁应用到所有写路径。职责和保护范围都超过了 Provider 同步本身。

本 ADR 固定最终依赖方向：`CoreFacade → 业务用例 → OperationRuntime / Storage`。`ProviderSwitch` 和 `Watch` 可以调用内部 `ProviderSync`，但任何用例都不得反向调用 `CoreFacade`；Storage 只提供读写端口，不包含业务编排。

## 决策

1. Node Core 业务用例拆为 `Status`、`ProviderSync`、`ProviderSwitch`、`Diagnostics`、`Repair`、`Backups`、`Restore`、`History`、`Watch` 与 `OperationRuntime`。
2. `CodexStorage` 由 `ConfigStore`、`SessionStore`、`StateDbStore`、`GlobalStateStore` 四个独立端口组成；`SqliteTransaction`、`UndoBackup`、`RestoreRecovery` 保持独立基础设施职责。
3. `ProviderSync` 只同步 config 当前 Provider。默认只解析 rollout 首行：等长 Provider 原地更新；不等长 Provider 以有界流式复制生成临时文件并原子替换，正文不解析且字节保持一致。
4. 删除公开 `sync --provider`、`--fast`、`syncMode` 和 `FAST_MODE_UNSUPPORTED`。显式模型、cwd、user-event、workspace roots 修复进入 `prepareRepair/applyRepair`；加密内容只诊断、不修改。
5. `ProviderSwitch` 只修改根 config，并在同一 Operation、Home 锁和 UndoBackup 中调用内部 `ProviderSync`；历史模型修复必须由用户显式执行 Repair。
6. Status 保持轻量。Diagnostics 只在用户主动触发时做一次完整只读扫描，不后台刷新；Repair 只读取所选 targets 所需的数据，且 `workspaceRoots` 自动包含 `cwd`。
7. 新 Sync、Switch、Repair 只持有 Codex Home 锁。SQLite 并发由原生事务裁决；普通写不创建跨文件 transaction journal，也不自动全量回滚。mutation 后失败返回带 backupId、阶段和重试提示的 `partial`，重复执行相同操作负责收敛。
8. Restore 继续使用独立 `RestoreRecovery`、恢复前快照、journal、哈希验证与补偿，但同样只持有 Home 锁。旧 v0.5 Sync/Switch journal 只读兼容，不阻断新普通写；Restore 和 Prune 继续识别其关联证据。

## 分阶段实施

- C1 只提取 Plan/Apply、进度/取消、运行状态和四个 Storage 端口，现有 DTO、锁、journal、备份、回滚与业务行为不变。
- C2 缩小 ProviderSync，建立显式 Diagnostics/Repair，并更新 CLI、Web、Electron 与共享 UI。
- C3 将普通写切换到 Home-lock + SQLite transaction + UndoBackup 的轻量模型；Restore 保留自己的恢复状态机。

每个 checkpoint 都必须独立通过相应合同与行为测试。目标态文档不能作为已实现证据；只有对应 checkpoint commit 及其测试通过后，相关行为才视为落地。

## 验收重点

- 默认 Provider Sync 不解析正文；等长更新保持文件身份、大小和正文 hash，不等长更新保持正文逐字节一致。
- Repair targets 相互隔离并使用单次 SQLite 事务；Diagnostics 只读且仅手动执行。
- no-op 不创建备份；备份失败时零写入；占用 session 和 mutation 后故障形成可重试的 partial。
- Restore crash matrix、旧 journal/backup 读取兼容和 Node 16 根包兼容继续通过。
