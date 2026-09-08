# ADR-0031：按用户用途呈现专项修复与可选模型调整

- Status: Accepted
- Date: 2026-09-07
- Scope: V1 共享 Web/Electron 界面；不授权提交、推送或发布
- Supersedes: ADR-0019 中四种 targets 同组呈现的规则；ADR-0021 的核心范围/预览/核验规则不变

## 决策

普通用户不应从 `models/cwd/userEvent/workspaceRoots` 推断修复用途。日常 Provider 同步仍不包含这些功能。

1. **专项修复**默认折叠，包含“修正会话所属目录”“补全用户消息标记”“整理项目目录记录”。每项说明适用情况、写入位置和不改变的内容。
2. **高级调整**独立、默认折叠，仅包含“统一历史模型名称”。模型差异不是故障，不根据诊断模型计数推荐修复；只在用户确需统一到当前配置模型时选择。
3. 两组选择独立且默认全不选；无全选、无自动勾选。切换 Profile ID/revision 重置选择及折叠状态。任一组只传自身 targets，复用现有 `prepareRepair/applyRepair`，无新增业务 API。
4. 手动诊断的当前成功结果可以提供“查看修复”：按 cwd/userEvent 对应 SQLite 行数、workspaceRoots 设置项数映射；点击只展开并聚焦对应复选框，不勾选、不扫描、不 Prepare/Apply。模型差异、加密字段、记录序号和显示索引不产生该入口。
5. 失败、刷新中、写后过期、缺数据、扫描不完整、占用、pending recovery 或不可写状态不提供推荐。SQLite 来源必须存在且支持、读计数非空；缺失或非法计数不是零，也不据此宣称健康。沿用按 Profile/revision 的查询隔离和写后过期标记，不增加 Core DTO 字段或后台刷新。
6. Repair 仍可手动选择后直接 Prepare，不强制先扫描所有数据。控件和提交处理同时检查可写状态/capability；推荐不能绕过这些检查。
7. 预览增加“将修改什么”，根据实际 Plan targets 展示作用和不变项；保留全配置/所选会话、前后值及重新预览流程。workspaceRoots 仍作用于整个 Profile，并隐含 cwd。模型取当前根模型，缺失时 Core Prepare 仍拒绝。

## 不变边界

- 不修改 Core 算法、CLI/HTTP/IPC DTO、目标字段、计数、扫描、锁、备份和 Apply 契约。
- 正常 Provider I/O 的首行有界读取、等长原地替换、非等长正文逐字节复制不变。
- 默认保留两份备份。正文、消息顺序和时间不变；不新增序号修正、显示索引重建或解密。
- workspaceRoots 的设置待调整项包括缺失设置备份，不是损坏会话数或目录数。

## 回归证据入口

- `packages/app-ui/tests/repair-clarity.vitest.tsx`：双语分组、独立选择、描述关联、推荐仅定位、不完整/旧结果隔离、禁用提交、跨 Profile 重置。
- `advanced-features.vitest.tsx`：只读手动诊断、无自动升级修复；`plan-review.vitest.tsx`：真实 targets 描述、范围与预览确认。
- Desktop production E2E：隐藏窗口、三项修复与独立模型调整可见性、真实临时数据模型调整/Restore hash 回环。
- 当前轮次具体命令和结果记录于 `output/repair-clarity-20260907.md`，不是发布或全平台验收声明。
