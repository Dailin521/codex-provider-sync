# ADR-0023：当前 Core 约束入口与防漂移门禁

- 状态：Accepted
- 日期：2026-09-04
- 范围：V1 文档、贡献规范、Provider I/O 回归和 CI
- 关联：ADR-0002、0014、0015、0016、0019、0021、0022
- 行为影响：不修改 Provider 算法、公开 DTO、存储数据或发布/阶段状态

## 背景

总体架构保留了迁移开始时的目录、API 和保护模型；后续职责拆分、普通写轻量化与日常/高级功能分层已另有 Accepted 裁决。如果开发仅参考旧段落，会把全文扫描、全量回滚、双资源锁或重复业务实现重新带回 Sync。README 也不能兼任细节不断复制的第二套架构合同。

## 决策

1. [Node Core 当前架构与开发约束](../architecture/NODE_CORE_ARCHITECTURE_ZH.md) 作为日常开发入口：记录真实模块归属、依赖方向、CLI 过渡适配例外及 PIO-1～PIO-6。总体架构负责产品路线，合同负责外部行为，ADR 负责有意变更的裁决，执行索引独立管理阶段/发布状态。
2. 合格等长 Provider 更新必须保留既有原地字节覆盖。资格按当前实现的安全 ID、JSON UTF-8 字节长度、唯一字面量、文件身份/边界和占用检查判定，不承诺任意“字符数相同”都原地写。失败不得降级强制替换。
3. 不满足原地资格的有效首行使用流式尾部复制并原子替换；正文逐字节保持。首行扫描允许底层有界块预读，不允许为了 Provider 业务打开全文扫描。不得把备份或变长复制的 I/O 宣称为零。
4. 复用已有 `workspaces:check` 作为唯一架构依赖检查；新增 `core:test:provider-io` 聚合既有 Provider、原地事务兼容与 Windows worker 测试。`architecture:check` 顺序执行这两项，CI workspace-contract 使用同一命令，不另建重复规则引擎。
5. 新增 Facade Prepare/Apply 层的字节不变量回归，避免只测底层而上层重构选错写入策略。Windows 与 POSIX 专属测试分别执行/标记 skip，不能把一台机器的结果当成跨平台完成。
6. 有意改变不变量必须先裁决，同时更新 ADR、合同、测试及面向用户的说明。意外回归按 bug 处理，不许修改断言追认退化。全套测试失败仍是未闭合门禁；文档和局部测试通过不代表 release-ready。

## 不包含

- 不进行 Core 重构、普通写保护模型变更或 .NET 业务迁移。
- 不变更默认保留 2 份备份，不增加后台刷新/诊断。
- 不自动提交、推送、更新 PR、合并、打 Tag 或公开发布。

## 验证

```bash
npm run architecture:check
npm test
```

Provider 测试检查 32 MiB fixture 的正文 hash、文件身份/大小、无正文解析、SQLite 非 Provider 字段不变、变长字节保持、跳过变化文件与重试收敛。耗时仅供参考。平台构建、Node 16 根包安装和 packaged E2E 仍是独立验收，不由本 ADR 替代。
