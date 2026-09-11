# ADR-0041：失效 Home 锁的只读识别与正常同步恢复

- Status: Accepted
- Date: 2026-09-11
- Scope: Node Status、Diagnostics、共享 UI 与 CLI；Issue #95，修订 ADR-0033 的失效锁分类。

## 问题

v1.0.0 将已证明进程退出的锁也归为 LOCK_UNVERIFIABLE，Status 构造 operationInProgress，桌面禁用同步，无法进入已有 acquireLock 的安全回收流程；重启仍阻断。临时夹具可复现，不等于已确认反馈者的具体锁来源。

## 决策

1. inspectPathLock 增加内部 `stale`：canonical（若存在）和全部 claims 均为合法且已证明失效的 owner 才返回。复用 PID/进程启动身份验证，不按时间或 PID 数字单独推断。
2. 遍历全部 claims；检查 regular file、名称/instanceId、owner 与目录/文件身份，末尾复查 claim 集合和 canonical 代际。超过 1024 个 claim、符号链接、未知格式、权限或身份查询失败均保持不可验证。dead canonical/首条 dead claim 不得遮蔽后续 live/unknown claim。
3. Status 三次锁检查均接受 absent/stale，保持 revision、只读、手动刷新和 pending Restore 门禁。可选 `staleLockDetected:boolean` 仅为观察提示，不是写授权；Diagnostics safety/脱敏导出与 CLI JSON 同步投影，不输出 owner 原文或路径。
4. 正常 Preview/Direct Sync 作为恢复入口，不新增 Core 方法或强制解锁按钮。Apply 的 acquireLock 重新验证全部 owner、唯一 claim 与代际后回收失效锁；再次出现活动/未知 owner 就拒绝。Status/Diagnostics 自身不删锁、不改用户数据。
5. active 显示“操作执行中”；unverifiable 显示“存储锁需要检查”，仍禁写，提供手动重新检查与诊断包指引。stale 且状态可用时显示“上次操作已结束”，不伪称锁已清理。
6. Windows 查询启动时间期间进程退出，仅 PowerShell 精确 `NoProcessFoundForGivenId` 结果作为 absent；权限和其他查询错误继续 fail-closed，不解析本地化错误文本。
7. 诊断已观察到 active/unverifiable 操作时，不继续扫描聊天完整性，省略可选 historyIntegrity、标记未完整；仍能导出锁状态与日志。没有锁证据的 revision 漂移维持原有独立完整性观察。

## 不变与验证

PIO-1～PIO-6、等长原地更新、首行扫描、时间戳恢复、备份默认 2、Plan/Apply、Restore journal、Node 16 和 .NET Legacy 业务不变。不新增锁协议版本或 State DB 锁。

`test/lock-inspection.test.js` 覆盖只读 hash 不变、dead/live/unknown、全部 claims、代际竞争、跨运行时身份语义与 acquire 回收；`test/status-coordination.test.js` 覆盖 Status→Plan/Apply 收敛及受锁阻断时诊断不读 rollout；`app-status-gating.vitest.tsx` 覆盖提示和同步门控；production packaged E2E 从遗留锁开始执行 Preview→Direct Sync→Restore。发布仍要求最终 SHA 的完整 CI 与 Windows 安装/便携容器验收。
