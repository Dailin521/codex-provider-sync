# main 分支保护

2026-10-28 已通过 GitHub API 更新并回读确认：[main-protection Ruleset](https://github.com/Dailin521/codex-provider-sync/rules/20265235)。

## 当前规则

| 项目 | 设置 |
| --- | --- |
| 状态与范围 | Active，仅默认分支 main |
| 合并入口 | 必须通过 PR |
| 必过检查 | GitHub Actions 的 `ci-gate` |
| 外部批准数 | 0 |
| 必须解决全部审查对话 | 关闭，由维护者判断评论 |
| 禁止强推、禁止删除 | 开启 |
| 必须与 main 保持最新 | 关闭 |
| Code Owner / 最新推送批准 | 关闭 |
| 管理员绕过 | 仅通过 PR，可用于明确的紧急情况 |

本次只把 `required_review_thread_resolution` 从 `true` 改为 `false`，其他规则字段保留。未配置重复的旧式 Branch Protection Rule。

审查对话可以继续讨论，不必为建议性评论逐条点击 Resolve 才能合并。已核实的 P0 Codex 问题仍按仓库政策阻断交付；维护者负责判断其他评论是否需要在合并前处理。

## CI 如何汇总

只将名称稳定的 `ci-gate` 绑定到 Ruleset，内部任务与平台矩阵可以按改动范围变化：

- 普通文档检查链接；正式发布说明额外校验版本与格式。
- Web、桌面 Renderer、共享 UI、Electron 运行时/打包和 Legacy .NET 分别运行相关检查。
- 普通 UI 使用 Linux / Node 24 工作区/Web 验证与 Windows 桌面生产包验证。
- Core/存储、共享合同、依赖、CI、未知路径及跨领域改动保留完整代码检查。
- main push 执行完整平台矩阵、四目标候选包、历史正式 Release 恢复取证和 C10。发布仍须通过最终 main SHA 验收并获得授权。

`ci-gate` 根据事件和范围核对必需成功与预期跳过，不能把失败或取消当作通过。具体执行规则见 [ci.yml](../.github/workflows/ci.yml)、[范围分类](../scripts/ci-docs.mjs) 和 [发布流程](WINDOWS_ELECTRON_RELEASE_ZH.md)。

## 维护与恢复

Actions 故障或门禁配置错误时，先诊断原因。需要紧急使用管理员 PR-only bypass 时记录原因，并检查合入后的 main CI；失败时修复或回滚。常规修改继续经过 PR 和相关检查。

修改 Ruleset 时先读取现有设置，只改目标字段，回读确认其余规则保留。检查名称调整必须先确保工作流已经提供对应检查，避免要求不存在的状态。不要为解决 CI 故障开启强推。

## 验证记录与限制

本次 API 回读确认：Ruleset 保持 Active，PR、`ci-gate`、禁止删除与强推仍启用，对话解决要求已关闭。该记录证明设置已应用；没有为了验证而实际尝试删除、强推或合并测试 PR。
