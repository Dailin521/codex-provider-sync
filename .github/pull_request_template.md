## 目的 / Why

<!-- 为什么需要这项修改？它解决了什么问题或使用场景？ / Why is this change needed? -->

## 关联 Issue / Related issue

<!-- 如适用，请使用 Closes #123 等格式关联 Issue。 / If applicable, link the issue with "Closes #123" or similar. -->

## 改动 / Changes

<!-- 列出关键改动，保持范围明确。 / List the key changes and keep the scope focused. -->

## 影响范围 / Impact

<!-- 勾选所有适用项。 / Check all that apply. -->

- [ ] Node.js CLI
- [ ] Shared .NET Core
- [ ] Windows GUI
- [ ] macOS GUI
- [ ] WSL / SQLite paths
- [ ] Backup / restore
- [ ] CI / GitHub Actions
- [ ] Documentation

### 数据写入 / Data writes

<!-- 是否修改 config.toml、rollout、SQLite、备份或其它用户数据？没有则写“无”。 / Describe any writes to user data, or write "None". -->


## 验证 / Validation

### Automated

<!-- 写出实际运行的命令和结果，例如 npm test: 100 passed。 / List the commands actually run and their results. -->

### Manual

<!-- 写出平台、操作步骤和结果；GUI 改动还请注明缩放比例并附前后截图。 / Include platform, steps, and results; for GUI changes, also include display scaling and screenshots. -->

### Not run

<!-- 未运行的测试、原因以及剩余风险。没有则写“无”。 / List tests not run, why, and any remaining risk; otherwise write "None". -->

## 检查清单 / Checklist

- [ ] PR 只包含相关修改 / This PR contains only related changes
- [ ] 已补充相关测试，或说明不需要测试的原因 / Tests were added or the reason they are unnecessary is explained
- [ ] 如有用户可见变化，已更新相关文档 / Relevant documentation was updated for user-facing changes
- [ ] 未提交未脱敏的凭据、会话、SQLite、备份、日志或个人信息 / No unredacted credentials, sessions, databases, backups, logs, or personal data are included
