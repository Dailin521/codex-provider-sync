# npm 发布维护指南

官方 CLI/Web 包名为 `@dailin521/codex-provider-sync`：

```bash
npm install -g @dailin521/codex-provider-sync
```

npm 包只包含 Node CLI、Web UI 和相关文档。它与 Windows GUI 的 GitHub Release 独立发布；仅更新 CLI/Web 时，不创建 Git tag 或 Windows Release。

## 常规发布

1. 更新 `package.json` 和 `package-lock.json` 中的版本号；npm 不允许覆盖已发布版本。
2. 在目标提交上完成本地验证：

   ```bash
   npm ci
   npm run web:build
   npm test
   npm run publish:npm -- --dry-run --node16 /absolute/path/to/node16 --node16-npm /absolute/path/to/npm8/bin/npm-cli.js
   ```

3. 合并到 `main` 并确认 `ci-gate` 成功。
4. 在 GitHub Actions 手动运行 `publish npm`。工作流仅允许从 `main` 运行，使用 npm Trusted Publisher 的短期 OIDC 凭据，不需要长期 npm token。
5. 发布后核对：

   ```bash
   npm view @dailin521/codex-provider-sync version
   npm install -g @dailin521/codex-provider-sync@latest
   codex-provider --help
   ```

Trusted Publisher 配置：

- GitHub 用户或组织：`Dailin521`
- 仓库：`codex-provider-sync`
- 工作流文件：`publish-npm.yml`
- Environment：`npm`
- Allowed actions：`npm publish`

## 手动备用流程

只有 Trusted Publisher 暂时不可用时，才由 npm 组织 owner 在可信本机手动发布：

```bash
npm login
npm whoami
npm run publish:npm -- --dry-run --node16 /absolute/path/to/node16 --node16-npm /absolute/path/to/npm8/bin/npm-cli.js
npm run publish:npm -- --node16 /absolute/path/to/node16 --node16-npm /absolute/path/to/npm8/bin/npm-cli.js
```

发布脚本必须在 Node 24 上运行，并提供精确 Node 16.20.2 和 npm 8 的绝对路径。也可设置 `CPS_NODE16_EXECUTABLE` / `CPS_NODE16_NPM_CLI`；Windows 路径包含空格时用引号包围。npm 8 安装在 Node 16 旁的常规目录时可省略 `--node16-npm`。

脚本先验证兼容工具链，再构建 Web、执行架构/Core 测试、Web E2E、Node 24 和 Node 16 两套已安装 tarball 的 CLI/SQLite/Restore/Web smoke、依赖审计与包内容检查；最后才登录校验并发布。E2E 需要预先安装 Playwright Chromium（`npx playwright install chromium`）。兼容安装在临时目录进行，不覆盖开发工作区的现代依赖；打包临时文件也只放在该临时目录。`--dry-run` 运行相同门禁但不登录或发布；不再支持 `--skip-tests`，任何门禁失败都禁止继续。它不替代完整跨平台 CI，也不授予发布权限。

不要把 npm token、密码、恢复码或一次性验证码写入仓库、Issue、PR 或日志。
