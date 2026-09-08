# ADR-0028：实测前反馈可靠性与验证边界

- 状态：Accepted
- 日期：2026-09-07
- 范围：V1 本地优化；不授权提交、发布、签名或更新通道
- 补充：ADR-0020、0023、0024、0027；不改变 PIO-1～PIO-6

## 决策

1. Desktop 操作日志的持久读取和 Preload 共用一个严格 entry 校验器。允许字段清单与 DTO 的 `keyof` 编译关联，新增字段不可只改生产者。保留 Profile revision、失败阶段、失败码、partial 原因与重试提示。JSONL 轮转同时移除内存中已淘汰的终态记录，运行中/等待确认的活动记录继续保留。
2. Watch 所有终态通过同一 finalize 发出一次可信 Host 回调；observer 失败不影响停止。Utility/Main 私有协议升级为 v3，加入有界 `watch-stopped` 事件；公开 Core protocol 仍为 v1。Main 按 runtime generation/watchId 清除所有权并推送，至多保存 256 个终态以处理停止早于启动响应的竞争。Renderer 在页面外订阅、按 watchId 更新已缓存的同 Home 别名状态；不重新轮询，不把迟到的 running 响应复活。
3. History 增加进程内短期定位表，按物理 Codex Home 隔离，闲置 TTL 15 秒，最多 8 个 Home、每个 8192 个候选。只保留 ID、路径与文件身份，不保留标题、消息正文或查询内容。列表始终新读；详情重新枚举目录和核对文件身份，稳定时只重读目标首行并打开目标正文，候选新增/删除/替换/移动/变化时重新选择 canonical 文件。它减少元数据文件打开次数，不保证恒定时间或零全量 stat。
4. Core 存储端口泛型保留实际方法和参数类型。所有 Core JS 文件必须显式选择 `@ts-check`，13 个现存迁移编排文件由固定 `@ts-nocheck` 清单保护、只允许逐步减少。全局 `checkJs:false` 暂留用于不把传递导入的根存储整体纳入类型迁移；不能宣称已全面类型化。该清单属于现有 workspace contract，不新建第二套 Provider 规则引擎。
5. 手动 npm 发布备用脚本必须执行 Node 24 构建/架构/根测试/Web E2E、Node 24 与精确 Node 16.20.2 + npm 8 的已安装 tarball 生命周期/SQLite/Web 验证、审计和包检查。取消 `--skip-tests`，失败不认证、不发布；dry-run 无发布写入。测试 tarball 位于自身临时目录，不覆盖已有包或现代开发依赖。完整跨平台 CI 仍是独立门禁。
6. 本地打包的 Windows/macOS/AppImage 已具有手动查更回退，不改其逻辑；补平台回归。文档明确本地版本、公开资产、签名与安装通道之间的区别，不暗示 macOS/Linux 产物已经发布。

## 不改变

Provider 仅首行扫描、合格等字节原地覆盖、变长正文逐字节复制、备份优先、默认保留 2、普通写 partial 收敛和 Restore 独立 journal 均不变。CLI 兼容适配器仍调用共享用例，不改成多个平台各自实现。

## 证据

参见 fixture 清单 ADR-0028 条目；测试只使用临时合成数据。平台 skip、未执行平台和实际包体数字需写入本轮 evidence，不能用本机通过代替 release-ready 声明。
