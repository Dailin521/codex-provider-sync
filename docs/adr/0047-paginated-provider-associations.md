# ADR-0047：分页 rollout 的受锚定同线程关联

- 状态：Accepted
- 日期：2026-09-28
- 范围：共享 Node Core 的 Provider Sync、Switch、Watch；不改变公开 API、CLI 参数、协议版本、错误码或 Legacy .NET。

## 背景

同一 Codex 线程可留下多个 rollout 文件。已有逐文件跳过规则会在缺少明确归属时保留冲突保护，但不能把“同 ID”本身当作多文件可写的证明。特别是回退可创建新的同线程文件，`ordinal` 只是记录位置，可能重复，不能用于身份、唯一性或重建历史继承链。

## 决策

同一 ID 的多个 rollout 只有同时满足下列条件，才作为一个分页组参与 Provider 同步：每个成员首行的 `payload.history_mode` 都明确为 `paginated`；可信首行 ID 与 SQLite `threads.id` 相同；该 SQLite 行的 `rollout_path` 经过 Windows 等价路径规范化和 Home 边界验证后，锚定组内恰好一个有效成员；不存在跨 ID 路径或多个 SQLite owner 对同一文件/组的争用。该组内所有成员是同一 SQLite 行的写前提，任何成员跳过、变化、删除、锁定或写失败，都保留该行并返回 partial；健康的其他组仍按既有规则继续。

缺少 marker、legacy/非 paginated、多文件但没有路径锚点、跨 ID，或多个 owner 的情形仍是 `association-conflict`/`association-unknown`，不推测归属。单文件候选沿用 ADR-0045 的旧选择规则。`ordinal` 不参与上述判断。

Windows 比较路径时先统一普通 DOS 与 `\\?\` DOS、普通 UNC 与 `\\?\UNC\` 的等价表示，再做现有 Home 边界检查。数据库保存的原始 `rollout_path` 不改写；设备路径、越界路径和 WSL 限制不放宽。

首行扫描、冻结计划与跳过事实在内部保留可信 ID 和 `historyMode`。原计划成员在锁定、变化、消失等后续事实中保持原归属，以便准确排除 SQLite 行与 Restore 范围。计划后新增且 deferred 的文件仍无写资格，但提交前必须再次有界读取首行确认当前归属；新读取失败或首行无效时清除缓存归属，按未知新增保护分页候选，不能因首次 Apply 已见过该文件而复用旧 ID。SQLite 路径锚点可以保护其自身索引行，但不能替代新增文件当前首行的归属证据，即使该索引行已经对齐也如此。

Apply 新发现的文件只读取有界首行确认归属，始终 deferred，不能扩大本轮写集合。已知同 ID 的新增或变化成员阻止对应行更新；未知/坏新增文件保守阻止所有已知 paginated 候选（包括单文件候选）的 SQLite 更新。传统单文件已有 unknown 语义保持。SQLite 提交前复核整个相关组，包括原本已对齐、无需改写的成员；区分本轮流式替换后的新身份与外部并发修改。该复核降低竞态，但不承诺最后检查后与外部写者的原子隔离。

SQLite 路径归属在备份前按当前快照重新选择，并在既有原生写事务中按全部当前索引行再次核验，包含已对齐行和执行期间新增行。新增或变更 owner 争用分页成员时保留受影响行；写候选只能从冻结集合缩减，健康组仍可继续。

PIO-1～PIO-6、原地与流式写策略、UndoBackup、partial、原生 SQLite 事务及既有写前/写后校验保持。SQLite-only 的半同步状态在组内全部文件已对齐时可于重试补齐行；随后 noop 不创建备份。

## 验证与文档边界

新增 `test/provider-paginated-groups.test.js` 以及 Provider facts/现有 I/O 单元回归，Windows 使用 `D:\Temp` 合成 Home，其他平台使用系统临时目录：正常多文件、SQLite-only 半同步、合法重复 ordinal、传统重复 ID/错锚/多 owner、锁定/删除/首行变化/写失败、新增/未知归属、普通与归档路径别名及越界反例。覆盖等长原地、变长流式、Restore 的正文、其他 metadata、文件身份与恢复范围。生产 Electron fixture 还应执行隐藏窗口的 Sync → noop → Restore。

实施变更须运行 `npm run architecture:check` 与 `npm test`，并重建生产 Electron、检查 production bundle 后进行该合成 fixture。以上是要求的验证，不构成已通过、跨平台 CI、实际安装或真实用户现场的声明。
