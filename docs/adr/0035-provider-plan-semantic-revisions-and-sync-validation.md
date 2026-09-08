# ADR-0035：Provider 计划的相关状态校验与同步失败详情

- Status: Accepted
- Date: 2026-09-07
- Scope: V1 本地修复；不授权真实数据同步、提交或发布

## 问题

普通 Sync/Switch 原先以 rollout stat 与 SQLite/WAL/SHM 的物理内容绑定计划。合成复现证明，预览后仅追加聊天消息也会触发 STALE_STATE；无活动的首次执行则正常，不能据此断言所有首次失败都是 SQLite 冷启动。另一个已复现问题是根 Provider 未声明也可同步，随后 Codex 不能加载该 Provider。已有失败日志未保留 reason，无法追溯每次历史失败的具体原因，也没有证据证明 Sync 删除了 Provider 定义。

## 决策

1. 仅 Sync/Switch（含 Direct Sync、Watch 内部 Sync）的计划使用 Provider 相关 revision。Profile/config/storage 路径和来源仍严格绑定；config 任意变化仍须重新预览。
2. rollout 绑定路径集合、物理文件身份/链接数、有限首行及分隔符 hash，并保留内部最小文件大小。普通正文增长不使计划过期；替换、缩短、首行变化、新增/删除/迁移文件仍拒绝。此校验不是正文完整性证明，不扫描正文；真实 Apply 的占用、快照与写入前检查保持原样，发生竞争仍跳过或返回 partial。
3. State DB 绑定物理文件身份、threads 表结构及有序的线程 ID/model_provider，在一个只读 SQLite 事务中取得。无 id 的旧最小表按 rowid 兼容。聊天标题/预览/活动时间等非 Provider 列变化、WAL/SHM 生命周期和 checkpoint 不单独使计划失效；真实 Provider、行集合、表结构或数据库替换仍拒绝。SQLite 原生写事务和 busy 语义不变。
4. Repair/Restore 不使用此模式，仍按原内容 revision/恢复 journal/hash 合同执行。单次消费、10 分钟 TTL、Apply 窄输入、Home 锁与备份优先不变。
5. Sync 的目标取当前 config；内置 openai 有效，其他 Provider 必须有匹配定义。Prepare 与锁内写入程序构建均检查；缺失时 INVALID_INPUT / details.reason=provider-not-configured，零业务写入。根 Provider 支持 TOML 单引号和行尾注释；显式空/非法类型不可默默回退为 openai。读取 Provider section 支持引号、空格和注释。本检查不验证认证、远端可用性或完整 TOML schema。
6. Desktop 日志可选记录 targetProvider、previewCounts、errorReason，仍为 schema v1。预计数量不冒充实际写入数量；旧记录不补造详情。Main 用单调时钟记录 validate_plan 阶段（Apply 发出至首个业务进度或失败），不扩张 Core ProgressEvent 或 CLI 六阶段。Prepare/Apply 同一记录，原因仅白名单，不存配置原文、路径、异常原文或聊天正文。

## 验证

`plan-apply.test.js`：正文追加/非 Provider WAL 更新及 checkpoint 后首次成功、真实 Provider/结构/集合/替换/截断/config 漂移零写入、缺失 Provider 零写入后配置完成可执行。`config-file.test.js`：合法 TOML 字符串/section 与非法根值。现有 PIO、partial/retry、Restore crash matrix 继续原断言。日志测试验证阶段耗时、原因、预计/实际计数隔离、白名单与重启读取。验收结果另记 output evidence，不将 ADR 当作测试通过证明。
