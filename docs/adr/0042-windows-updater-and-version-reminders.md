# ADR-0042：Windows 安装版更新与按版本提醒

- Status: Accepted
- Date: 2026-09-11
- Scope: Desktop Host、共享设置界面、显式 Windows 发布渠道；Core/CLI/Provider I/O 不变。

## 决策

- 新增显式 `stable-updater` Windows x64 渠道，严格匹配源码/tag 的 `1.0.x`；默认 RC 和 `stable-manual` 不启用应用内安装。只有该渠道编译授权，且实际安装目录有 NSIS uninstaller 和 `app-update.yml`，才进入 Main electron-updater。便携版继续打开官方页面。
- 每天首次启动检查一次；下载和重启安装由用户分别点击，不后台下载、不退出时静默安装。写操作、Watch、未解决或无法验证的恢复状态继续阻止安装，保留 Main restart lease。
- 设置页及原生新版弹窗提供“不再提醒此版本”。Main 在 userData 原子保存一个 `ignoredVersion`；仅忽略精确版本，后续不同新版正常提醒。手动检查/下载/安装不受影响，设置页可恢复提醒。Esc/关闭/稍后不改变偏好；保存失败明确提示。
- HostClient 新增 `setUpdateReminder(version, ignored)`；Preload 窄接口 `updates.setReminder({schemaVersion:1,version,ignored})`，IPC `cps:v1:update:reminder` 验证 sender 和精确字段。版本必须匹配 Main 当前发现版本，旧弹窗不能忽略另一版本。该参数只用于提醒，不允许指定下载版本、URL、路径、channel 或安装参数。
- Update schema v2 可选 `reminderIgnored:boolean` 仅在含版本状态有效。它不是 Core DTO，不增加轮询或日志操作。
- 同次构建审核 `latest.yml` 的版本/路径/大小/SHA512、安装器 blockmap 和包内固定公开 GitHub 配置；暂存/SHA256 清单包含两项更新附件。保留固定依赖的默认签名校验，不覆盖 verifier；未签名包不宣称有发布者签名验证。

## 发布边界

最初用户要求本地源码仍为 `1.0.1`，不更改公开版本。2026-09-11 维护者随后明确授权“提交，覆盖 1.0.1 版本，完善新说明”：允许本次同版本重新发布的单次例外，先备份原标签对象、附件和哈希，保留原 SHA `30c276a585344fa3f4f8fee4066565c97981d038`，再在新 main SHA 的 CI 和安装/便携容器验收通过后替换。具体恢复与门禁见 Windows 发布说明；不扩张为默认覆盖策略，不放宽工作流现有拒绝重复创建 Release 的检查。

同版本不会触发升级，旧用户需手动重装包含更新能力的包；安装版提供后续更高版本的显式下载和安装入口。首批上线不宣称已完成线上跨版本升级验证；实际升级、重启与数据保持需在一次性 Windows 环境独立验收，未验证事项必须在公告保留，不以单元测试或同版本重装代替。保持未签名状态，不修改真实用户安装以补造证据。

## 验证

`updater.test.mjs`、`update-reminder.test.mjs`、`ipc-router.test.mjs`、共享 `settings-updates.vitest.tsx` 覆盖持久化/重启、精确版本、手动检查不弹窗、失败/关闭、顺序保存、白名单、分开确认、无轮询。

`windows-update-artifacts.test.mjs`、`release-candidate.test.mjs` 覆盖默认渠道隔离、非法目标/版本、SHA512/路径/配置/blockmap 及发布流程。生产 bundle 与真实安装升级仍是独立门禁。
