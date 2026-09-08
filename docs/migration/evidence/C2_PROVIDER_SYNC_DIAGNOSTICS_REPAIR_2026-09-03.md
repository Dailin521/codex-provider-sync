# C2 ProviderSync、Diagnostics 与 Repair 证据（2026-09-03）

状态：本地候选门禁通过，等待 checkpoint commit 与远端 CI。输入 checkpoint 为 `c76c551`；输出 SHA 由包含本文的 C2 commit 固定。

## 已实现边界

- Node Core 的普通 Sync 目标只能来自当前 `config.toml` 根 `model_provider`；`provider`、`model`、`fast`、`syncMode`、CLI `sync --provider` 与 `--fast` 均被移除或明确拒绝。
- ProviderSync 只读取 rollout 的首行 `session_meta`。Provider 等长时原地更新字节；不等长时有界流式生成临时文件并原子替换；两条路径都不解析或修改正文。
- Diagnostics 是用户主动触发的一次完整只读扫描，不随 Status、后台 timer 或页面刷新自动执行；加密内容只报告计数，不提供修改。
- `prepareRepair/applyRepair` 支持 `models`、`cwd`、`userEvent`、`workspaceRoots`。Repair 只扫描所选目标，`workspaceRoots` 自动包含 `cwd`，模型修复要求 config 存在根模型，SQLite 目标在一次原生事务内提交。
- ProviderSwitch 保留 provider-default、keep-root-model、explicit 三种根模型策略；它修改 config 后调用同一内部 ProviderSync，不再隐式改写历史模型。历史模型只由显式 Repair 修复。
- CLI、HTTP、Desktop IPC、CoreClient 与共享 React UI 使用同一 Repair/Diagnostics 契约；Renderer 仍只能提交固定产品输入，Apply 仍只接受 `{schemaVersion, planId}`。
- CoreFacade 继续作为产品唯一入口；根 `src/public-api.js`、`src/service.js`、`src/watch.js` 只是兼容转发。Core application 按 Status、ProviderSync、ProviderSwitch、Diagnostics、Repair、Backups、Restore、History、Watch 组合，并通过四个只读/读写 Storage 端口访问现有 Node 存储实现。

## 行为与性能证据

- 32 MiB equal-length rollout：`33,554,509` bytes，`inPlace=1`、`sameInode=true`，文件大小、文件身份与正文 tail SHA-256 不变；耗时 `1356 ms`。
- 32 MiB unequal-length rollout：`33,554,515` bytes，`inPlace=0`、`sameInode=false`，正文 tail SHA-256 不变；耗时 `1431 ms`。
- 上述耗时只作本机参考；门禁是首行读取、写入路径、文件身份和正文 byte/hash，不以绝对毫秒数判定。

## 本地验证

环境：Windows x64，Node `v24.11.1`，npm `11.10.0`；Node 16 兼容门禁使用一次性 Node `v16.20.2` / npm `8.19.4` 运行时。

| 命令 | 结果 |
| --- | --- |
| `npm run workspaces:check` | workspace build、74 Desktop、18 Core、21 CoreClient、12 Contracts、20 App UI Vitest 及全部边界检查通过 |
| `node scripts/benchmark-provider-io.mjs 32` | equal/unequal 两条 32 MiB byte/hash/identity 门禁通过 |
| `npm run web:build` | Vite production build 成功；无 source map 输出 |
| `npm run web:test:e2e` | 真实 headless Desktop Chrome，2 passed，0 failed |
| `npm run runtime:verify-node16`（Node 16.20.2） | 通过 |
| `npm run package:smoke` 与 `npm run package:smoke:lifecycle`（Node 16.20.2） | 根 tarball 安装、CLI、生命周期与 SQLite 冒烟通过；未发布 |
| `npm audit --omit=dev --audit-level=moderate` | 0 vulnerabilities |
| `npm audit --audit-level=high` | 0 vulnerabilities |

## 未闭合项

- 本地没有可运行的真实 WSL distribution；Windows WSL UNC Electron 用例按条件跳过，不能宣称真实 WSL 已验证。
- macOS/Linux 当前 source head、签名、公证、真实更新和发布均未在本 checkpoint 执行，仍由远端 CI/C10 证明。
- 本 checkpoint 不创建 tag、不发布 npm/GitHub Release、不写更新通道，也不改变 Draft PR #90 的禁止合并状态。
