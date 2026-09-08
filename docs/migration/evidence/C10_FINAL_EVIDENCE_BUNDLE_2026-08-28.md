# C10 最终候选证据快照（2026-08-28）

> 历史 source-head 快照：本文绑定 `c63a403`，其中普通 Sync/Switch 的 State-DB lock/journal 证据已由 ADR-0016 取代，不可用来证明当前 V1 source head 的轻量写合同。

状态：`c63a403688b6d148afa65fba9e1461c7ebcd3331` checkpoint 的本地门禁、四目标原生候选、required CI、aggregate、C10 脱敏 bundle 和 source `1.0.0` 已闭合，CI 结论为 `ci-verified-not-release`。Draft PR #90 仍为 Open/Draft、未合入 `main`；真实 WSL UNC、真实 Beta、合入后 `main` 复验、签名、公证、生产更新升级和独立发布授权未闭合。因此 Phase 6/C10 仍为 Pending，不构成 Beta、Stable、公开替代或发布声明。

本文是 `c63a403` 的静态候选证据快照，不追踪后续 source head；PR #90 的最新 source-head C10 证据只以最新成功 `c10-evidence-bundle` artifact 为准。这样可避免为记录新 SHA 再产生新 commit、进而再次改变被证明的 SHA。[2026-08-27 证据](C10_FINAL_EVIDENCE_BUNDLE_2026-08-27.md) 同样只保留当时本地 Windows checkpoint 的历史事实。

## 绑定对象

| 项目 | 值 |
| --- | --- |
| Draft PR | [#90 `V1` → `main`](https://github.com/Dailin521/codex-provider-sync/pull/90)，Open/Draft，禁止合并 |
| V1 source head | `c63a403688b6d148afa65fba9e1461c7ebcd3331` |
| 事件基线 | `origin/main@c7ff85218a07a8e5f14132c582cad1239c52865e`；source head 包含该 commit |
| CI tested merge commit | `10047581a46f67993c809bb8fb3b58a89fb42d09` |
| CI run | [33142610556](https://github.com/Dailin521/codex-provider-sync/actions/runs/33142610556)，`pull_request`，26/26 jobs `success` |
| Source version | 根包与 Desktop manifest 均为 `1.0.0` |
| 注入候选版本 | `1.0.0-rc.204` |
| C10 outcome | `ci-verified-not-release` |

CI 测试的是 GitHub 为 PR #90 生成的 merge ref；C10 bundle 同时记录 source head、事件基线和 tested merge commit。最终合入后的实际 `main` commit 可能不同，因此必须在获准合并后对该 SHA 重新运行全量门禁，不能用本快照替代。

## 远端 CI 与候选产物

- 13 个 required jobs、Windows x64、macOS x64、macOS arm64、Linux x64 原生 candidate jobs、`electron-candidate-set`、`c10-evidence-bundle` 和最终 `ci-gate` 全部成功；没有 failed、cancelled 或 skipped job。各 job 内按宿主不适用而条件跳过的步骤不属于跳过适用 job。
- 四目标最终容器验证覆盖 Windows NSIS/portable ZIP、macOS x64/arm64 DMG/ZIP、Linux AppImage/deb，并包含 native SQLite fallback、Status、Sync→Restore、正常退出、资产清单、SBOM 和 checksum 审计。候选构建固定 `--publish never`。
- [C10 evidence artifact 9674705528](https://github.com/Dailin521/codex-provider-sync/actions/runs/33142610556/artifacts/9674705528) 的 bundle 内容 SHA-256 为 `fd0e4fa6fbdb2e6f09bfc19906fa27d1836ae62a3f1fb7a0367ed39c6135b92f`。
- [四目标 candidate-set artifact 9674663683](https://github.com/Dailin521/codex-provider-sync/actions/runs/33142610556/artifacts/9674663683) 的 index SHA-256 为 `a631340011f87481000d5bdf289ce3eb3b9f1cf4e0a2bea2a603dd60f523adf6`。
- 上述 GitHub Actions artifacts 是审查期内的 unsigned candidate evidence，按 workflow 保留 30 天；不是 GitHub Release 下载资产。

## c63 本地门禁

环境：Windows 11 x64，Node `24.11.1`，npm `11.10.0`，Git `2.52.0.windows.1`，PowerShell `7.6.4`，.NET SDK `10.0.400`。所有测试使用临时 fixture；Electron 通过 `CPS_DESKTOP_WINDOW_DISPLAY=hidden` 运行，未显示或占用主屏窗口。

| 门禁 | 结果 |
| --- | --- |
| `npm test` | 375/375 passed |
| `npm run workspaces:check` | 全部 workspace build/contract/test 通过；Desktop 65/65、App UI 14/14 |
| `npm run web:build` + `npm run web:test:e2e` | production build 通过；2/2 E2E passed，History detail 可见读取闭环通过 |
| `npm run desktop:test:e2e`（hidden） | 15 passed，1 skipped；唯一 Skip 为不可用的真实 WSL UNC 环境 |
| `npm run fixtures:cross-runtime` | 12/12 passed |
| `.NET FixtureHost` Release build | 0 warning，0 error |

跨运行时矩阵新增两个不同 Codex Home 共用同一个 State DB 的真实 writer 争锁：Node Sync 持有 State-DB 锁时 .NET Sync fail-fast，以及反方向 .NET→Node。两方向都断言败方返回 `OPERATION_BUSY` / `busyScope=state-db`，并且在竞争期间不创建 Backup/Journal、不修改败方 Home 或共享数据库；释放 winner 后再验证真实写入成功。这补齐了仅以裸锁持有者证明协议兼容、却未证明两个真实 Sync writer 写入位置的缺口。

## V1 候选角色与公开发行边界

- V1 候选按批准的 C10 目标，在 Electron 界面和迁移文档中显示“新版主桌面端候选”，并把保留且继续构建/测试的 .NET Windows/macOS 实现标记为交接后的 Legacy fallback。
- 该候选标识不等于 Electron 已经公开替代 .NET，也不把 Phase 6 标为 Completed。本快照生成时，[GitHub Release v0.4.1](https://github.com/Dailin521/codex-provider-sync/releases/tag/v0.4.1) 仍只提供 .NET Windows 资产；Electron 没有公开下载或生产更新通道。
- .NET 实现未删除、未停止关键 CI，是本快照生成时的公开桌面兼容依据。即使未来稳定版交接完成，也至少保留两个维护周期；删除属于后续独立 Phase 7/PR。

## 尚未闭合与停止边界

1. **真实 WSL UNC：**本快照环境只注册 Ubuntu WSL2，但其 `ext4.vhdx` 缺失，启动报 `CreateInstance/MountDisk/HCS/ERROR_FILE_NOT_FOUND`。没有与 source commit 绑定的健康 Windows+WSL strict artifact 时，该项保持 Pending；不得用模拟 UNC 或代码开关替代，也不得在未授权时 unregister/reinstall 发行版。
2. **真实 Beta 与历史发布物：**本静态 `c63a403` 快照生成时尚无真实用户 Beta 反馈或受控历史正式 Release binary backup 实物回归，不能外推为用户环境稳定性。后续 source head 新增了固定 v0.4.1 hosted Automation Release asset 的 checksum-bound synthetic backup→当前 Node Restore fixture；只有该 source head 最新成功 C10 artifact 才能关闭“本快照缺少 hosted formal binary”这一项。它仍不能替代真实用户 Beta、真实用户数据、代码签名或生产跨版本升级。
3. **受保护 `main`：**PR #90 未合并且明确禁止合并。获授权合并后，必须在实际 `main` SHA 重跑全部 applicable jobs 并生成新的 C10 bundle。
4. **签名与更新：**Windows signing、Apple Developer ID/Notarization、生产 update metadata/download/restart 和跨版本升级验证均未执行。
5. **发布授权：**未创建 tag，未发布 npm/GitHub Release，未签名、公证或写生产更新通道；本证据不授予这些权限。

任何上述边界未闭合时，都不得把 `1.0.0` source manifest、`1.0.0-rc.204` CI candidate、26/26 green jobs 或 V1 候选角色标识表述为 Stable 已发布。
