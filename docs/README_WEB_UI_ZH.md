# Web UI 使用说明

Web UI 是现有 CLI 和 Desktop GUI 的浏览器界面。它不会在浏览器中重新实现同步算法，而是通过只监听本机回环地址的 Node 服务调用 `src/service.js` 中的同一套核心逻辑。

## 启动

安装依赖时会自动构建 Web 资源。之后运行：

```bash
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
```

从仓库开发时也可以运行：

```bash
npm install
npm run web:build
npm run web:start
```

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
- HTML 启动时注入随机会话令牌；所有 API 请求必须携带令牌。
- API 校验同源 `Origin`。
- 写操作串行执行，同一时间只允许一个 sync、switch、restore 或 prune。
- 恢复只能选择后端枚举出的当前 Codex Home 托管备份，不能提交任意目录路径。
- Web UI 不能绕过核心层的锁、SQLite Home、WSL UNC、备份和恢复限制。

## 与旧版 Desktop GUI 的对应关系

Desktop GUI 已弃用。下表仅用于帮助旧用户理解 Web UI 与原操作名称的对应关系：

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

Web UI 不能替代操作系统级关闭 Codex 的要求。执行同步或恢复前仍应关闭 Codex CLI、Codex App、app-server 和相关终端。

如果输出报告锁定的 rollout，操作属于部分成功；会话结束后再次执行同步即可。如果 SQLite 正在使用，核心服务会在修改 rollout 前停止。
