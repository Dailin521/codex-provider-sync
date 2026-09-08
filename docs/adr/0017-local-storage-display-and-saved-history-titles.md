# ADR-0017：本地存储路径展示与已保存的会话标题

- Status: Accepted
- Date: 2026-09-04
- Scope: V1 本地桌面手测反馈；不授权提交、推送或发布

## Context

用户需要确认桌面端实际使用的 Codex/SQLite 位置。仅显示来源枚举不能定位数据；只读 rollout 首行标题也会遗漏 Codex 保存到 SQLite `threads.title` 的会话名称。

## Decision

- Desktop Utility 在创建 CoreFacade 时启用可信构造选项 `includeLocalDisplayPaths`；同一完整 Status snapshot 可包含 `displayPaths={codexHome,sqliteHome,stateDbPath}`。无数据库时 `stateDbPath=null`；不能重算或猜测路径。
- 此选项不是 Renderer/HTTP/IPC 请求字段。默认 Core/Web 输出继续移除路径；Profile 列表仍脱敏，目录选择仍只能提交 token。显示路径不授予任意文件读写能力。
- 本地 Overview 显示完整路径，包括实际选中的 DB 文件，以区分默认 SQLite Home 与旧布局 DB 回退。复用首次读取和手动刷新，不增加轮询。
- History 标题优先取当前 Codex Home 的 `session_index.jsonl` 中匹配 ID 的非空 `thread_name`（按追加顺序取最后一个有效名称），再取当前解析 DB 的非空 `threads.title`，最后回退到 rollout 的显式 `session_meta.title/name`。只按已发现会话 ID 查询；已保存名称超过 1024 字符时截短显示，不整条丢弃；不读取首条用户消息来生成名称。
- 名称索引只读、单次流式扫描，固定读取开始时的文件范围，每条记录最多 64 KiB，仅保留所需 ID 的名称。忽略损坏、超长、空名称记录；缺失、不可读或链接文件不阻断 DB/metadata 回退。不将索引名称写回 SQLite。
- DB 缺失、不可读或没有 title 列时保持 metadata 回退；不能因此换读其他显式存储位置。标题索引不可用不阻断现有 History 浏览。
- 路径和标题不进入操作日志、诊断导出或遥测。消息正文仍只在用户明确打开详情/提交搜索时按现有边界读取。
- 2026-09-04 手测补充：不隐藏无标题记录。确认是子代理的首行 metadata 可提供 `subagentName`（agent_path 的最后一段，或 agent_nickname，最多 160 字符）；不得返回完整 agent_path。Core 的 `title` 仍表示已保存标题。共享 UI 在空标题时显示“子任务 · 名称”，无子任务名时显示“无标题会话 · 创建日期时间 · ID 末 8 位”；旧记录缺创建时间才回退更新时间。列表、详情及无障碍名称共用同一格式，支持中英文，不读取正文生成标题、不写回数据、不增加刷新。

## Validation

覆盖默认/config/env/Profile 路径、legacy DB、缺失数据库、默认 Core 无路径、Renderer 无法打开路径选项、列表与详情标题一致、无 title 列回退，以及 packaged Electron 的路径/已保存标题显示。测试只使用合成临时数据。
