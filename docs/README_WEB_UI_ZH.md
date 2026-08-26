# Web UI 使用说明

Web UI 是 CLI 提供的本地浏览器界面。共享 React UI 通过版本化 `HttpCoreClient` 调用本地 Web Host，再进入与 CLI 相同的 Node Core 公开边界；页面不会解析 CLI 输出或复制同步逻辑。

## 启动

Web UI 由 CLI 提供。安装 Node.js `16.20.2+` 后，安装本项目官方 npm 包并启动：

```bash
npm install -g @dailin521/codex-provider-sync
codex-provider web
```

默认地址：

```text
http://127.0.0.1:8791
```

默认会打开系统浏览器。可使用：

```bash
codex-provider web --port 8792
codex-provider web --no-open
codex-provider web --reset-access
codex-provider web --codex-home /path/to/.codex --sqlite-home /path/to/sqlite
```

运行中的服务按 `Ctrl+C` 停止。升级已安装的 CLI/Web UI：

```bash
npm install -g @dailin521/codex-provider-sync@latest
```

从仓库开发时也可以运行：

```bash
npm ci
npm run web:build
npm run web:start
```

## 典型同步流程

本工具只同步本地元数据，不负责登录或切换账号。已经通过其他工具切换 Provider 时：

1. 使用 CCSwitch 等常用工具切换 Provider，并确认 Codex 可以正常对话。
2. 回到 Web UI，在“概览”检查当前 Provider、rollout/SQLite 分布和安全状态。
3. 进入“同步”，生成十分钟内有效的一次性计划，核对影响数量、警告与备份预期后确认。
4. 显示“Provider 元数据已对齐”即完成。切回原 Provider 时重复相同步骤。

rollout 与 SQLite 的会话总数可能因活动会话写入和索引时序短暂相差 1；这不表示 Provider 元数据未对齐。以两侧的 Provider 分布和页面对齐状态为准。

> **注意：** 元数据同步只能恢复历史可见性。跨供应商继续旧会话时，目标后端可能无法解密会话中的 `encrypted_content` 推理内容，导致继续对话或压缩（compact）失败。遇到这种情况请切回原 Provider/account，或新建会话。

## 页面功能

- 概览（Overview）：显示当前 Provider、rollout/SQLite 分布、对齐状态、备份数、locked rollout 数和存储来源；公共状态不显示本机绝对路径。
- 同步（Sync）：设置备份保留数，先生成计划，再在确认对话框中 Apply。
- 切换 Provider（Switch Provider）：只允许已配置 Provider，并明确选择跟随 Provider 默认模型、保留根 model 或显式 model 三种模式。
- 备份/恢复（Backups/Restore）：只展示受管 `backupId`；Restore 可选择 config/SQLite/session 范围，跨 SQLite Home 必须选择目标 Profile 并确认 relocation；同页可按保留数清理受管备份。
- 历史（History）：进入页面后才读取会话列表；只有点击某个会话才延迟读取详情。当前列表最多读取 100 项，不提供搜索/筛选承诺；消息正文不进入 Query cache，离开详情即清空并取消未完成请求。
- Profiles：管理服务端受信任的 Codex/SQLite Home 配置；Core 业务请求只提交 profile ID/revision，不传递任意路径。
- Diagnostics：只读显示 Core 返回的有界安全诊断字段，不读取凭据、token、消息正文或原始异常。
- Settings：切换 `zh-CN` / `en`、`system` / `light` / `dark` 主题，管理 Watch 与撤销当前浏览器授权。

全局区域显示 Recovery、正在进行的 Operation、结构化错误与 Toast。界面支持键盘操作、可见焦点、reduced motion 和 200% 缩放等效窄视口。

## 本地安全边界

- 服务只监听 `127.0.0.1`，不要直接暴露到局域网或公网。
- 首次启动使用短时、一次性的配对链接；服务端只保存设备凭证哈希。使用“忘记此浏览器”或 `--reset-access` 可撤销授权。
- Core API 请求/响应使用共享版本化 envelope；服务端验证 Origin、64 KiB 请求上限、requestId、Profile/Storage revision 和产品输入 schema。
- Production HTML 使用每响应随机 nonce 的严格 CSP；不允许远程脚本或跨源 Core 请求。
- 存储路径由服务端配置管理，写操作 fail-fast 串行；Sync、Switch、Restore 使用 Prepare/Apply，Apply 只接收不透明 `planId`；恢复只能选择受管备份。
- Web UI 不能绕过共享核心逻辑中的锁、SQLite Home、WSL UNC、备份和恢复限制。

## SSH、无桌面和远程浏览器

服务不能直接暴露到公网或局域网。远程机器上使用时，只转发回环端口：

```bash
ssh -L 8791:127.0.0.1:8791 user@server
```

在远程 shell 中启动：

```bash
codex-provider web --no-open
```

命令会输出可点击的一次性配对链接。无桌面或纯 SSH 环境不会强制调用 `xdg-open`；即使浏览器打开失败，服务也会继续运行并输出配对链接。

## 注意事项

建议在同步或恢复前关闭 Codex CLI、Codex App、app-server 和相关终端。保持会话运行时，未锁定的数据仍可正常同步；如果报告跳过锁定的 rollout，操作属于部分成功，请在该会话结束后再次同步。SQLite 正在使用时，核心服务会在修改 rollout 前停止，此时必须关闭占用进程后重试。
