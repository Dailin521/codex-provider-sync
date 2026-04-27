# Codex Provider Sync GUI

## 适用场景

这是面向普通 Windows 用户的图形界面版本。

如果你不想装 Node、不想打开 PowerShell，也不想记命令，直接下载发布页里的 `CodexProviderSync.exe` 双击运行即可。

## 它能做什么

- 检测当前 `.codex` 下的 root provider
- 统计 rollout files 和 SQLite 里的 provider 分布
- 自动汇总当前可见的全部 provider
- 支持手动补充 provider，并持久化保存
- 选择目标 provider 后一键执行同步
- 可选同时改写 `config.toml` 的 root `model_provider`
- 默认自动保留最近 5 份由本工具生成的备份，并支持自定义保留数
- 支持手动清理旧备份
- 支持从 backup 目录恢复
- 恢复时可分别选择 config、SQLite、rollout metadata
- 如果 EXE 双击无反应，查看 `%AppData%\codex-provider-sync\startup-error.log`，或在 PowerShell 中运行 `./CodexProviderSync.exe` 获取错误
- 含 `encrypted_content` 的历史会话跨 provider/account 后可能只能恢复可见性，继续对话或 compact 仍可能报 `invalid_encrypted_content`

## 能力边界

- GUI 只同步历史会话可见性相关 metadata，不会处理登录、认证或第三方 provider 切换
- GUI 不会修改消息历史、会话标题、对话内容、认证信息或 `auth.json`
- GUI 不会修改会话 `updated_at`，也不会通过改变历史排序来修复 Desktop 显示问题
- 含 `encrypted_content` 的旧会话不能由本工具重新加密到另一个 provider / account
- 如果 CLI 能看到历史会话但 Desktop 项目侧仍不显示，请优先复制并反馈 Refresh 后的完整状态文本

## 使用方式

1. 打开 `CodexProviderSync.exe`
2. 确认顶部 `Codex Home` 路径
3. 点击 `Refresh`
4. 在中间列表里选择目标 Provider
5. 如果你希望同时改写 `config.toml` 根级 provider，勾选右侧复选框
6. 根据需要调整“自动保留最近 N 份备份”
7. 点击 `Execute`
8. 如需回滚，点击 `Restore Backup`
9. 如需立刻清理旧备份，点击 `清理旧备份`

## 持久化位置

- GUI 设置：`%AppData%\codex-provider-sync\settings.json`
- 备份目录：`%USERPROFILE%\.codex\backups_state\provider-sync\`

## 注意事项

- 如果 `state_5.sqlite` 被占用，请先关闭 Codex / Codex App / app-server 再重试
- 如果某个 rollout 文件仍被活跃会话占用，程序会跳过它并在日志区列出来
- 自动清理和手动清理都只会处理由本工具创建的备份目录
- 手动清理旧备份前会弹确认框
- GUI 不会处理登录、认证或第三方 provider 切换，只负责同步可见性相关元数据
