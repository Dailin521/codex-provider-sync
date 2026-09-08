# ADR-0032：高级功能预览与诊断的请求进度

- Status: Accepted
- Date: 2026-09-07
- Scope: V1 Core / Web / Electron；仅本地实现，不授权发布

## 决策

1. 用户明确执行诊断、修复/模型调整预览或更新所选会话预览后，显示当前阶段、已耗时和已检查文件数。不新增自动扫描、轮询或隐式修复。
2. `prepareRepair/getDiagnostics` 的可信 Host control 可传 `signal/onProgress`；JSON 产品输入及 Apply `{schemaVersion,planId}` 不变。Prepare 请求 control 不保存在 Plan 中，不影响之后的 Apply。
3. 复用 `ProgressEvent` 的 `stage/status/progress/count`。枚举后按当前目录实际文件数计数，百分比仅代表当前阶段，不是整体完成度；未知数量的配置、revision、索引阶段显示不定进度，不估算 ETA。保留文件枚举顺序及既有扫描，不为进度额外扫描正文。
4. 新增 `{protocolVersion:1,requestId,event:"request-progress",progress}` envelope，无 `operationId`，只允许上述两个方法。HTTP NDJSON、Desktop 固定 operation-event 通道和 Mock 同语义；普通 JSON 响应不变。
5. 不生成伪 `operation-started`，不改变 Status `operationInProgress`。Desktop 按 generation/dispatchId/requestId 路由；终态后忽略迟到进度，Apply 校验不放宽。
6. observer 异常/异步拒绝不影响结果。取消在请求、文件和阶段边界检查；单次文件读取/SQLite 调用可能先返回，不声称立即中止。完成、失败、卸载或 Profile revision 切换后清理进度；旧请求不得覆盖新请求。
7. UI 单调时钟仅在当前等待组件激活时每秒更新，结束后清理；不请求 Core，不持久化进度或正文。
8. 保留 Diagnostics 和 Prepare/Apply 合并日志。Host 按事件到达时间记录阶段；重复阶段进度不得重置起点，阶段切换/请求终态闭合耗时。日志不包含路径、消息正文或凭据。

## 不变边界与验证

- 普通 Sync PIO-1 至 PIO-6、等长原地替换、非等长正文流式复制、Home 锁、UndoBackup、Restore journal、CLI 输出不变。
- `test/request-progress.test.js`：临时文件进度、无写入/伪 Operation、取消、observer 异常及 Prepare control 不泄漏到 Apply。
- `packages/app-ui/tests/request-progress.vitest.tsx`：真实阶段进度、本地计时清理、晚到事件、Profile 隔离、诊断错误收束及修复预览。
- contracts/core-client/Desktop Runtime/日志/Web 服务测试覆盖白名单、NDJSON、请求关联及阶段时间；产物隐藏窗口证据另存 `output/`，不等同于发布或全平台验收。
