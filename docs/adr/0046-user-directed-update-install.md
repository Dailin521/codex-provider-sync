# ADR-0046：更新安装由用户决定

- Status: Accepted
- Date: 2026-09-15
- Scope: Electron Main 更新器与共享设置页。

## 原因

已下载的更新会因存储 Profile 状态无法验证而被禁用，用户也无法再次点击安装。安装应用不应以会话数据、Watch 或恢复状态检查通过为前提。维护者明确要求取消这些安装限制。

## 决策

- 已下载的安装版更新始终提供“重启并安装”。用户点击后直接调用安装器，不刷新 Profile Status，不查询 Watch，不检查 pending recovery 或写操作，不等待安装前写队列排空。
- Main 的 restart lease 只用于安装开始后阻止新增业务操作；无法取得 lease 也不否决安装。保留正常退出清理、重复点击去重和安装器失败处理，失败时释放已取得的 lease。
- 设置页不因写操作、恢复状态、Watch 或 Status 读取失败禁用安装按钮。下载完成前和安装进行中仍按更新状态显示操作。
- 保留现有安装版/便携版渠道、包下载与完整性校验、用户显式安装、窄 IPC 和错误脱敏。Core 的同步、备份、恢复合同不变。
- Host Update schema v2 不变；旧 `installBlockedReason` 字段保留解析兼容，新 Main 不再生成存储状态拦截原因。

本决策取代 ADR-0014、ADR-0020、ADR-0042 中将写操作、Watch、Profile 恢复复核用作更新安装前置条件的条款。

## 验证

`updater.test.mjs` 覆盖写操作、Watch、恢复阻塞、不可验证/查询失败、不可取得 lease 时仍安装，且不调用存储检查或写队列等待；覆盖重复点击和同步/异步安装失败。`settings-updates.vitest.tsx` 覆盖对应状态下安装按钮可用。

旧版二进制保留旧行为，需手动运行新版安装包完成首次迁移；本地源码修改不意味着新版已发布或真实跨版本升级已经验收。

### 本地验证（2026-09-15）

- `npm run workspaces:check` 通过；更新器/策略/真实模块定向测试 23 项通过，设置页含四类状态的安装回归通过。
- `npm run desktop:build`、`npm run desktop:verify-production-bundle` 通过。
- `TEMP/TMP=D:\Temp` 下 `npm run desktop:test:e2e:production` 六项通过，包括真实 SQLite、Sync → Restore、128 MiB 首行与混合问题数据。隐藏窗口截图触发既有重试后通过；未改截图逻辑。
- 本次未运行真实安装器升级、跨平台 CI 或发布；当前安装版未被修改。
