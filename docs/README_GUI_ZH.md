# Codex Provider Sync GUI

## 适用场景

这是 Windows 用户可用的图形界面版本。

如果你不想装 Node、不想打开 PowerShell，也不想记命令，直接下载发布页里的 `CodexProviderSync.exe` 双击运行即可。

macOS 桌面版说明见 [README_MAC_GUI_ZH.md](README_MAC_GUI_ZH.md)。

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
- 支持为每个 Codex Home 单独指定 SQLite Home，适配 Windows Codex App + WSL app-server 等分离布局
- 当前状态、执行结果和常用提示使用中文显示
- 常规执行日志按天写入本地并自动保留最近 30 天
- 支持直接打开日志目录
- 每天首次启动会后台检查更新，版本查询最多等待 10 秒；失败不弹窗且不影响使用
- “检查更新”按钮不受每日限制，可随时手动重试
- 如果 EXE 双击无反应，查看 `%AppData%\codex-provider-sync\startup-error.log`，或在 PowerShell 中运行 `./CodexProviderSync.exe` 获取错误
- 含 `encrypted_content` 的历史会话跨 provider/account 后可能只能恢复可见性，继续对话或 compact 仍可能报 `invalid_encrypted_content`

## 能力边界

- GUI 只同步历史会话可见性相关 metadata，不会处理登录、认证或第三方 provider 切换
- GUI 不会在多台设备之间复制配置或会话文件，只处理当前 Codex Home
- GUI 不会修改消息历史、会话标题、对话内容、认证信息或 `auth.json`
- GUI 不会修改会话 `updated_at`，也不会通过改变历史排序来修复 Desktop 显示问题
- 含 `encrypted_content` 的旧会话不能由本工具重新加密到另一个 provider / account
- 如果 CLI 能看到历史会话但 Desktop 项目侧仍不显示，请优先复制并反馈“刷新”后的完整状态文本

## Codex Desktop 最近 50 条限制

Codex Desktop 当前首屏只拉取最近 `50` 条会话。如果某个项目的旧会话排在全局最近 50 条之后，CLI `/resume` 可能能看到，但 Desktop 项目侧仍显示“暂无对话”。

GUI“刷新”会显示项目可见性诊断，例如 `first page 0/50`、`ranks 64-77`。这表示会话存在，但没有进入 Desktop 首屏最近 50 条。本工具不会修改 `updated_at` 或历史排序来绕过这个限制。

## 使用方式

1. 打开 `CodexProviderSync.exe`
2. 确认顶部 `Codex Home` 路径
3. 如果 SQLite 不在 Codex Home 中，在 `SQLite Home` 填写或选择包含 `state_5.sqlite` 的目录；留空时按配置自动解析
4. 点击“刷新”，核对状态中的有效 SQLite Home、来源和数据库路径
5. 在中间列表里选择目标 Provider
6. 如果你希望同时改写 `config.toml` 根级 provider，勾选右侧复选框
7. 根据需要调整“自动保留最近 N 份备份”
8. 点击“立即同步”
9. 如需回滚，点击“恢复备份”
10. 如需立刻清理旧备份，点击“清理旧备份”
11. 如需复制或查看历史执行信息，点击“打开日志目录”
12. 软件每天首次启动会自动检查一次更新，也可以点击“检查更新”立即重试

GUI 中的 SQLite Home override 按 Codex Home 保存在 GUI settings 中，不会写入 `config.toml`。解析优先级为：GUI override → `config.toml` 根级 `sqlite_home` → `CODEX_SQLITE_HOME` → `<Codex Home>\sqlite`。只有最后一种默认布局会检查旧路径 `<Codex Home>\state_5.sqlite`。

Windows GUI 可直接选择 `\\wsl.localhost\Ubuntu\home\you\.codex\sqlite` 一类 UNC 路径。显式位置缺少 `state_5.sqlite` 时，“刷新”只显示诊断，写操作会停止，不会回退到 Codex Home 中可能过期的数据库。从 metadata v2 备份恢复到不同 SQLite Home 时，GUI 会显示来源与目标并要求二次确认。

## 更新与日志

Windows GUI 每天首次启动会在后台检查一次最新的稳定版 GitHub Release，也可以随时点击“检查更新”手动重试。自动和手动版本查询共用 10 秒总时限；网络或代理异常不会阻止软件启动，自动检查失败也不会弹窗。

确认更新后，程序会下载 EXE 和对应 SHA-256，完成校验后退出，由临时更新器再次校验并原子替换原 EXE，然后自动重启。如果 EXE 所在目录没有写入权限，旧版本不会被覆盖，提示中会保留新版本下载路径供手动安装。

项目目前未做 Windows 代码签名，从浏览器下载后可能出现 SmartScreen 提示。SHA-256 可以检测文件损坏，但不能替代代码签名。

## 持久化位置

- GUI 设置：`%AppData%\codex-provider-sync\settings.json`
- 每日执行日志：`%AppData%\codex-provider-sync\logs\execution-YYYY-MM-DD.log`
- 启动失败日志：`%AppData%\codex-provider-sync\startup-error.log`
- 备份目录：`%USERPROFILE%\.codex\backups_state\provider-sync\`

## 注意事项

- 如果 `state_5.sqlite` 被占用，请先关闭 Codex / Codex App / app-server 再重试
- 如果某个 rollout 文件仍被活跃会话占用，程序会跳过它并在日志区列出来
- 每日执行日志使用 UTF-8，可在程序运行期间读取；超过 30 天的同类日志会自动清理
- 自动清理和手动清理都只会处理由本工具创建的备份目录
- 手动清理旧备份前会弹确认框
- GUI 不会处理登录、认证或第三方 provider 切换，只负责同步可见性相关元数据
