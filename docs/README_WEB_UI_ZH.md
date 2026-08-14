# Web UI 使用说明

Web UI 是现有 CLI 和 Desktop GUI 的浏览器界面。它不会在浏览器中重新实现同步算法，而是通过只监听本机回环地址的 Node 服务调用 `src/service.js` 中的同一套核心逻辑。

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
npm install
npm run web:build
npm run web:start
```

## 典型同步流程

本工具只同步本地元数据，不负责登录或切换账号。已经通过其他工具切换 Provider 时：

1. 使用 CCSwitch 等常用工具切换 Provider，并确认 Codex 可以正常对话。
2. 在 Web UI 点击“读取状态”（可跳过）。
3. 在“执行同步”中保持“仅同步元数据”，选择目标 Provider（供应商）。
4. 核对确认页中的 Codex Home、SQLite Home 和目标 Provider，然后点击“确认执行同步”。
5. 完成后再次读取状态。两侧元数据都归到当前 Provider，并显示“Provider 元数据已对齐”即为成功。
6. 以后切回原 Provider 时重复相同步骤；`openai → 自定义 Provider → openai` 两个方向没有区别。

rollout 与 SQLite 的会话总数可能因活动会话写入和索引时序短暂相差 1；这不表示 Provider 元数据未对齐。以两侧的 Provider 分布和页面对齐状态为准。

> **注意：** 元数据同步只能恢复历史可见性。跨供应商继续旧会话时，目标后端可能无法解密会话中的 `encrypted_content` 推理内容，导致继续对话或压缩（compact）失败。遇到这种情况请切回原 Provider/account，或新建会话。

## 页面功能

- 概览：显示当前 Provider、rollout/SQLite 分布、修复项和项目可见性。
- 聊天记录：从 rollout 文件只读读取会话列表和用户/助手消息，支持搜索、Provider/项目/归档筛选、分页和会话详情。
- 执行同步：区分“仅同步元数据”和“切换 Provider 并同步”。
- 切换模型：支持跟随 Provider section、保留根级 model 或显式指定 model。
- 备份：查看当前 Codex Home 下由本工具管理的备份，并按内容恢复。
- 恢复保护：SQLite Home 不同时显示来源与目标；迁移数据库时禁止同时恢复旧配置。
- 活动：显示当前 Web UI 进程中的同步阶段和操作结果。
- 清理：按保留数量删除较旧的托管备份。

## 本地安全边界

- 服务只监听 `127.0.0.1`，不会绑定局域网地址。
- 首页和静态资源不包含 API token。启动时生成高熵、短时、一次性的 `#pair=...` 配对链接；配对后立即清除 fragment。
- 浏览器把长期设备凭证保存在 `localStorage`，服务端只持久化 SHA-256 哈希。刷新页面和重启服务后仍保持授权；“忘记此浏览器”或 `--reset-access` 会使对应凭证失效。
- 浏览器写请求必须携带设备凭证，并按实际回环 Host/端口校验 `Origin`；支持 `localhost`、自定义端口和 SSH 隧道两端端口不同的情况。
- `codexHome` 和 `sqliteHome` 由服务端存储配置管理。普通 API 只提交 `profileId`；新增配置时服务端规范化和验证路径。
- 写操作串行执行，同一时间只允许一个 sync、switch、restore 或 prune。
- 恢复只能选择后端枚举出的当前 Codex Home 托管备份，不能提交任意目录路径。
- Web UI 不能绕过核心层的锁、SQLite Home、WSL UNC、备份和恢复限制。

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

## 与 Desktop GUI 的对应关系

Windows 桌面用户默认推荐原生 GUI；Web UI 适合需要浏览器界面或跨平台使用的场景。下表用于说明两种界面的操作对应关系：

| Desktop GUI | Web UI |
| --- | --- |
| Refresh | 读取状态/刷新 |
| Execute，不修改配置 | 仅同步元数据 |
| Execute，同时修改配置 | 切换 Provider 并同步 |
| Model 自动/保留/自定义 | 根级 model 三种策略 |
| Restore Backup | 备份页中的恢复 |
| Clean Old Backups | 清理旧备份 |
| 状态文本框 | 结构化概览、分布和项目表格 |
| 执行日志 | 活动日志页 |

## 注意事项

建议在同步或恢复前关闭 Codex CLI、Codex App、app-server 和相关终端。保持会话运行时，未锁定的数据仍可正常同步；如果报告跳过锁定的 rollout，操作属于部分成功，请在该会话结束后再次同步。SQLite 正在使用时，核心服务会在修改 rollout 前停止，此时必须关闭占用进程后重试。
