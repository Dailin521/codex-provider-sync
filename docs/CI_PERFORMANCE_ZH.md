# CI 与打包提速记录

## 本次范围

- 普通文档 PR 使用白名单轻量检查，规则见 [Windows 发布流程](WINDOWS_ELECTRON_RELEASE_ZH.md)。按整个 PR 判断，代码 PR 后续只补文档仍完整检查。
- 同一 PR 新提交取消旧运行；main 和独立发布运行保留。
- electron-builder 使用 `compression: normal`，保留原包大小限制、产物审计和容器验收。
- main 最终源码的跨平台检查与发布门禁不变。RC 与正式更新渠道的产物复用、不稳定测试诊断留待后续处理。

## Windows ZIP 压缩实测

2026-09-11，在同一台 Windows 开发机上，使用锁定的 electron-builder 26.15.7 内部 `archive('zip', ..., { compression, withoutDir: true, preserveSymlinks: false })`，对同一份本地 1.0.2 程序内容分别压缩一次。未设置压缩级别环境变量。该测量用于选择压缩配置，不是新源码的正式发布证据；测试期间机器也执行了其他验证，不能据此承诺 CI 的固定耗时。

| 配置 | 耗时 | ZIP 字节数 |
| --- | ---: | ---: |
| maximum | 310.14 秒 | 128,959,840 |
| normal | 42.48 秒 | 129,224,083 |

本次测量耗时下降约 86%，大小增加 264,243 字节（约 258 KiB，0.20%）。普通压缩 ZIP 低于现有 130 MiB 上限。该版本打包器的 maximum ZIP 使用 `-mx=9 -mfb=258 -mpass=15`，normal 使用 `-mx=7`。

两份 ZIP 解压后的 18 个文件逐个 SHA-256 相同。共同的 `resources/app.asar` SHA-256：`914b88f24d954c1ad9a2f5e4db82f1c52b9db67bfa9020e7e36efacf82e7ee9e`。

normal ZIP 解压后的生产包 4 项测试通过，包含真实公开更新检查、SQLite、Provider 切换、隔离数据 Sync → Restore。普通压缩 NSIS 通过 prepackaged 构建，大小 104,601,532 字节，低于 105 MiB 上限；未在本机执行实际安装/卸载。macOS/Linux 新压缩配置和真实安装验收仍需后续 CI 验证。

复测时使用固定输入目录及新的输出路径，记录打包器版本、源文件哈希、环境变量、耗时、大小及解压后的逐文件哈希。不要复用已存在 ZIP，以免打包器按时间戳跳过压缩；不要放宽包大小限制来使测量通过。
