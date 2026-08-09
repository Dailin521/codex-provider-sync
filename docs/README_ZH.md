# codex-provider-sync 中文说明

`codex-provider-sync` 是一个本机运行的 Codex 元数据一致性工具。切换根级 `model_provider` 后，历史会话通常仍在磁盘中，但 rollout、SQLite 线程索引或项目路径 metadata 可能仍指向旧 Provider，导致会话从 Codex 列表、项目视图或 `/resume` 中消失。

项目现在以本地 Web UI 为主要入口，CLI 用于自动化和 WSL 场景。Desktop GUI 已弃用，仅保留兼容性参考。

## 快速开始：Web UI

需要 Node.js 16.20.2 或更高版本。

```bash
npm install
npm run web:build
npm run web:start
```

或者全局安装：

```bash
npm install -g git+https://github.com/Dailin521/codex-provider-sync.git
codex-provider web
```

默认地址：`http://127.0.0.1:8791`

```bash
codex-provider web --no-open
codex-provider web --port 8792
codex-provider web --reset-access
```

服务只监听 `127.0.0.1`。启动命令通过短时一次性链接自动配对浏览器，浏览器保存设备凭证，服务端只保存哈希；写操作按实际回环 Host 校验 Origin，并从服务端存储配置解析路径。同步、切换、恢复和清理等写操作会串行执行。

## Web UI 功能

### 概览

- 当前 Provider 和 model。
- `sessions`、`archived_sessions` rollout 分布。
- SQLite `threads` 分布和 rollout/SQLite 对齐状态。
- 项目可见性、CWD 匹配、ranks 和首屏 50 条诊断。
- locked rollout、`encrypted_content`、SQLite repair、数据库损坏和 WSL UNC 安全提示。

### 聊天记录

聊天记录页面只读扫描 rollout JSONL，不修改本地数据：

- 查看会话列表和用户/agent 消息。
- 搜索标题、项目路径、Provider 和消息正文。
- 按 Provider、项目、活跃/归档状态筛选。
- 服务端分页，默认每页 50 个会话。
- 详情默认显示最近 200 条可读消息。
- 支持安全的受限 Markdown 和代码块。
- 不向浏览器返回原始 JSONL、token、工具调用参数或 `encrypted_content`。

### 同步与切换

- **仅同步元数据**：使用当前根级 Provider，不修改 `config.toml`。
- **切换 Provider 并同步**：更新根级 `model_provider` 后同步历史元数据。
- model 策略：跟随 Provider 配置、保留当前根级 model 或自定义 model。
- 执行前确认关闭 Codex CLI、Codex App 和 app-server。

### 备份与恢复

- 每次 sync / switch 前创建 metadata v2 备份。
- 默认保留最近 5 份托管备份。
- 可分别恢复 config、SQLite 和 rollout metadata。
- SQLite Home 不同时显示来源与目标。
- SQLite Home relocation 需要额外确认，并禁止不安全的 config/database 组合。

## CLI：自动化和 WSL

CLI 与 Web UI 复用相同的 `src/service.js` 核心逻辑：

```bash
codex-provider status
codex-provider sync
codex-provider sync --keep 5
codex-provider sync --provider openai
codex-provider switch apigather
codex-provider switch apigather --model "MiniMax-M3"
codex-provider switch apigather --keep-root-model
codex-provider prune-backups --keep 5
codex-provider restore C:\Users\you\.codex\backups_state\provider-sync\20260319T042708906Z
codex-provider watch
codex-provider watch --once
```

指定 Codex Home 或 SQLite Home：

```bash
codex-provider status --codex-home C:\Users\you\.codex
codex-provider sync --codex-home C:\Users\you\.codex --sqlite-home C:\Users\you\.codex\sqlite
```

Windows Codex Home + WSL SQLite 时，在 WSL 内执行：

```bash
codex-provider status --codex-home /mnt/c/Users/you/.codex --sqlite-home /home/you/.codex/sqlite
codex-provider sync --codex-home /mnt/c/Users/you/.codex --sqlite-home /home/you/.codex/sqlite
```

## SQLite Home 解析

优先级为：CLI/GUI override → `config.toml` 根级 `sqlite_home` → `CODEX_SQLITE_HOME` → `<Codex Home>/sqlite`。

只有默认布局允许检查旧路径 `<Codex Home>/state_5.sqlite`。显式、配置或环境变量指定的 SQLite Home 缺少数据库时，不会回退到其它路径。

## 安全与限制

同步和切换前的备份位置：

```text
~/.codex/backups_state/provider-sync/<timestamp>
```

- 不修改消息正文、会话标题、认证信息、`auth.json` 或 `updated_at`。
- 只修复当前 Codex Home 的 metadata，不在设备之间复制会话。
- SQLite 被占用时先关闭 Codex CLI、Codex App 和 app-server。
- locked rollout 会跳过，活动会话结束后可重新同步。
- `encrypted_content` 会话可能恢复列表可见性，但跨 Provider/account 后继续对话或 compact 仍可能失败。
- Windows 不能安全通过 `\\wsl.localhost\...` 或 `\\wsl$\...` 操作 SQLite，应在 WSL 内使用 Linux 路径。

## Desktop GUI 状态

Desktop GUI 已弃用，不再作为推荐入口。现有 Windows/macOS 版本仅保留兼容性参考；新功能优先在 Web UI 中实现。

- [Windows GUI 旧版说明](README_GUI_ZH.md)
- [macOS GUI 旧版说明](README_MAC_GUI_ZH.md)
- [Web UI 使用说明](README_WEB_UI_ZH.md)
- [工作原理与落盘机制](WORKING_PRINCIPLE_ZH.md)
- [English README](../README.md)

## 开发与测试

```bash
npm install
npm run web:build
npm test
git diff --check
```

### 发布 npm package

登录 npm 后，可以使用仓库自带的跨平台发布脚本：

```bash
npm run publish:npm -- --dry-run
npm run publish:npm -- --otp 123456
```

脚本会依次检查 npm 登录、构建 Web UI、运行测试、预览包内容，然后发布当前版本。也可以使用 `NPM_OTP=123456 npm run publish:npm` 传入一次性验证码；OTP 不会写入仓库文件。

## License

MIT
