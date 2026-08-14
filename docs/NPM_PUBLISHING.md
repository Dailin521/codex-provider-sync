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
   npm run publish:npm -- --dry-run
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
npm run publish:npm -- --dry-run
npm run publish:npm
```

发布脚本会构建 Web UI、运行 Node 测试、预览包内容，再以公开包发布。不要把 npm token、密码、恢复码或一次性验证码写入仓库、Issue、PR 或日志。
