# `main` 分支保护方案

> 状态：`ci-gate` 已通过 PR #63 合并到 `main`；`main-protection`
> Ruleset 已启用，等待测试 PR 验证。
>
> 适用仓库：`Dailin521/codex-provider-sync`

## 1. 目标

`main` 是发布和日常开发的基准分支。保护规则需要解决以下风险：

- 误用强制推送覆盖提交历史
- 误删默认分支
- 未经 PR 和 CI 就把代码写入 `main`
- 合并仍有未解决审查意见的 PR
- CI Job 改名或矩阵变化后，分支保护永久等待一个不存在的检查

仓库目前以单人维护为主，因此规则还必须避免：

- 因必须由他人批准而无法合并自己的 PR
- 因每次 `main` 更新而重复运行所有旧 PR 的 CI
- 在 GitHub Actions 故障时完全失去紧急修复能力

## 2. 当前状态

截至 2026-08-03：

- `main` 没有重复配置旧式 Branch Protection Rule
- [`main-protection`](https://github.com/Dailin521/codex-provider-sync/rules/20265235)
  Repository Ruleset 已创建并处于 `Active`
- Ruleset 只匹配 Default branch；当前即 `main`
- `Repository admin` 的 Bypass 模式为 `For pull requests only`
- Required approvals 为 `0`，并要求解决所有 review conversation
- Required status check 只有来源为 GitHub Actions 的 `ci-gate`
- Restrict deletions 和 Block force pushes 已开启，Strict / Up-to-date 已关闭
- [PR #63](https://github.com/Dailin521/codex-provider-sync/pull/63) 已把稳定的
  `ci-gate` 合并到 `main`
- PR #63 的七项检查全部成功；合并后的 `main` push 工作流也成功运行
- GitHub Actions 工作流 `ci` 当前包含六个实际检查：
  - `test (windows-latest, 16)`
  - `test (windows-latest, 24)`
  - `test (ubuntu-latest, 16)`
  - `test (ubuntu-latest, 24)`
  - `desktop-test`
  - `desktop-macos`
- 六项实际检查由第七个稳定检查 `ci-gate` 汇总
- 工作流顶层权限已限制为 `contents: read`
- Secret scanning 和 Push protection 已开启
- 仓库仍以单一主要维护者为主

## 3. 推荐策略

使用 GitHub Repository Ruleset，不同时维护一份功能重复的旧式 Branch
Protection Rule。

建议创建：

| 项目 | 建议值 |
| --- | --- |
| Ruleset 名称 | `main-protection` |
| Enforcement status | `Active` |
| Target branches | 仅 `main`，或 GitHub 的 default branch 条件 |
| Bypass actors | `Repository admin` |
| Bypass mode | `For pull requests only` |
| Restrict deletions | 开启 |
| Block force pushes | 开启 |
| Require a pull request before merging | 开启 |
| Required approving reviews | `0` |
| Require review from Code Owners | 关闭 |
| Require approval of the most recent push | 关闭 |
| Require conversation resolution | 开启 |
| Require status checks to pass | 开启 |
| Required status check | 仅稳定的 `ci-gate` |
| Expected source | `GitHub Actions` |
| Require branches to be up to date | 暂时关闭 |
| Require signed commits | 暂时关闭 |
| Require linear history | 暂时关闭 |
| Require deployments to succeed | 暂时关闭 |
| Merge queue | 暂时不启用 |

### 为什么审批数设为 0

GitHub 不会把 PR 作者对自己 PR 的批准计为有效外部批准。当前主要由单人维护，
如果把 Required approvals 设为 1，就可能需要管理员绕过每一个正常 PR。

审批数为 0 仍然要求所有变更经过 PR，并保留：

- PR 差异和提交记录
- CI 门禁
- 未解决对话门禁
- GitHub 合并记录

项目有稳定的第二位维护者后，再把审批数提高到 1。

### 为什么暂时不开 Strict / Up-to-date

Strict 模式要求 PR 分支在合并前包含最新 `main`。当 `main` 有新提交时，其他
已通过 CI 的 PR 需要更新分支并重新运行检查。

当前阶段暂时关闭，原因是：

- 单人维护时间有限
- 六项 CI 有一定运行成本
- 外部贡献者不应因为无关 PR 先合并而频繁重跑

当并发 PR 明显增加、CI 时间稳定且 Merge Queue 已准备好时，再开启 Strict。

## 4. 使用稳定的 `ci-gate`

不要长期把六个具体检查名称全部写入分支保护规则。

矩阵操作系统、Node 版本或 Job 名称以后可能变化。如果 Ruleset 仍要求一个已经
删除或改名的检查，GitHub 会一直等待它，导致所有 PR 无法正常合并。

PR #63 已在 `.github/workflows/ci.yml` 增加名称固定的汇总 Job：

```yaml
  ci-gate:
    name: ci-gate
    if: ${{ always() }}
    needs:
      - test
      - desktop-test
      - desktop-macos
    runs-on: ubuntu-latest
    steps:
      - name: Verify required jobs
        env:
          NODE_TEST_RESULT: ${{ needs.test.result }}
          DESKTOP_TEST_RESULT: ${{ needs.desktop-test.result }}
          MACOS_TEST_RESULT: ${{ needs.desktop-macos.result }}
        run: |
          if [ "$NODE_TEST_RESULT" != "success" ] ||
             [ "$DESKTOP_TEST_RESULT" != "success" ] ||
             [ "$MACOS_TEST_RESULT" != "success" ]; then
            echo "One or more required CI jobs did not succeed."
            exit 1
          fi
```

行为：

- `test` 的四个矩阵实例全部成功后，其汇总结果才是成功
- Windows Desktop 测试必须成功
- macOS 构建和 Core 测试必须成功
- 任一依赖失败、取消或跳过，`ci-gate` 都失败
- Ruleset 只绑定名称稳定的 `ci-gate`

未来可以修改矩阵版本或增加内部检查，而不必同步修改 `main` 的 Required status
checks。`ci-gate` 的名称属于仓库治理接口，不应随意改名。

## 5. 管理员 Bypass

Bypass actor 仅选择 `Repository admin`，不授予普通 Write 或 Maintain 角色。
Bypass mode 选择 `For pull requests only`，不要选择 `Always allow`。

这样管理员可以在明确的紧急情况下通过 PR 绕过部分门禁，但仍不能直接推送
`main`。Ruleset 配置错误导致 PR 流程也无法恢复时，应临时把 Ruleset 设为
`Disabled`，修复后再恢复为 `Active`。

允许使用的情况：

- GitHub Actions 服务故障导致所有 PR 无法合并
- 紧急安全修复
- 紧急回滚破坏性提交或 Release
- 分支保护自身配置错误，需要恢复正常合并能力

不应使用的情况：

- 为了省略正常 PR
- CI 真实失败但暂时不想修
- PR 仍有未解决审查意见
- 一般文档、小功能或常规 Release

每次使用 Bypass 后：

1. 在 Issue、PR 或提交说明中记录原因。
2. 确认 `main` 的 push CI 已运行。
3. CI 失败时立即修复或回滚。
4. 必要时补建一个事后 PR，保留审查和变更说明。

如果以后形成维护团队，应把 Bypass 缩小到极少数 Release/Security
维护者，并定期检查绕过记录。

## 6. 启用顺序

必须按以下顺序操作，避免提前把 `main` 锁住：

1. [已完成] 在独立 PR 中向 CI 添加 `ci-gate`。
2. [已完成] 确认该 PR 的六项现有检查和 `ci-gate` 全部通过。
3. [已完成] 合并 CI PR。
4. [已完成] 确认 `main` push 工作流中也出现成功的 `ci-gate`。
5. [已完成] 打开仓库 `Settings → Rules → Rulesets`。
6. [已完成] 创建 `main-protection` Branch Ruleset，并只匹配 Default branch。
7. [已完成] 添加 `Repository admin` Bypass，模式选择
   `For pull requests only`。
8. [已完成] 只把来源为 `GitHub Actions` 的 `ci-gate` 设为 Required
   status check。
9. [已完成] 启用 PR、对话解决、禁止删除和禁止强推。
10. [已完成] 把 Enforcement status 设为 `Active` 并创建 Ruleset。
11. [待完成] 使用一个无风险测试 PR 验证规则。

不要在 `ci-gate` 进入 `main` 之前就把它设为 Required check，否则 GitHub 没有
可运行该检查的默认分支工作流，可能导致 PR 卡住。

## 7. 验收检查

使用一个测试分支和 PR 验证：

- 直接向 `main` 推送的普通维护者被拒绝
- PR 创建后会运行 `ci-gate`
- `ci-gate` 未完成或失败时不能合并
- `ci-gate` 成功后可以合并
- 有未解决 review conversation 时不能合并
- Required approvals 为 0，不需要寻找第二个人批准
- 合并后 `main` push CI 正常运行
- 管理员仍能在明确的紧急情况下通过 PR 使用 Bypass
- 管理员直接推送 `main` 仍会被拒绝

不需要为了验证而实际尝试删除 `main` 或强制推送。

## 8. 配置错误或锁死时的恢复

如果 CI Job 改名、Actions 故障或 Ruleset 配置错误导致所有 PR 卡住：

1. 先确认是否为真实测试失败。
2. 如果是 Required check 名称错误，修正 Ruleset 中的检查名称。
3. 必要时把 `main-protection` 临时设为 Disabled，而不是删除整个 Ruleset。
4. 通过正常 PR 修复 `ci-gate`。
5. 在 `main` 上确认新 gate 成功。
6. 重新把 Ruleset 设为 Active。

不要通过开启强制推送解决 CI 或 Ruleset 配置问题。

## 9. 多人维护后的增强规则

出现稳定的第二位维护者或团队后，逐步增加：

- Required approvals 从 0 提高到 1
- 增加 `CODEOWNERS`
- 要求 Code Owner review
- 要求最新推送由非推送者批准
- 开启 Strict / Require branches to be up to date
- 并发 PR 较多时启用 Merge Queue
- 评估 Require signed commits
- 将 Bypass 限定为少数 Release/Security 维护者
- 将 lint、协议兼容、发布包验证和安全检查接入 `ci-gate`

这些增强不应在缺少对应人员、流程或 CI 能力时提前开启。

## 10. 最小可执行结论

当前最合适的保护方案是：

> 所有常规改动通过 PR；不要求第二人批准；稳定的 `ci-gate` 必须成功；所有
> review conversation 必须解决；禁止删除和强推 `main`；仅仓库所有者保留有
> 记录的 PR-only 紧急 Bypass；`ci-gate` 的 Expected source 固定为
> GitHub Actions；暂不开 Strict、CODEOWNERS、签名提交和 Merge Queue。

## 11. GitHub 官方参考

- [Creating rulesets for a repository](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/creating-rulesets-for-a-repository)
- [Available rules for rulesets](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/available-rules-for-rulesets)
- [Managing rulesets for a repository](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/managing-rulesets-for-a-repository)
- [Troubleshooting rules](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/troubleshooting-rules)
- [GitHub Actions contexts and `needs.<job_id>.result`](https://docs.github.com/en/actions/reference/workflows-and-actions/contexts#needs-context)
