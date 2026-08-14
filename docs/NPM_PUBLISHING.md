# npm 发布维护指南

官方 CLI/Web 包名为 `@dailin521/codex-provider-sync`，公开安装命令为：

```bash
npm install -g @dailin521/codex-provider-sync
```

npm 包只包含 Node CLI、Web UI 和相关文档。它与 Windows GUI 的 GitHub Release 独立发布；仅更新 CLI/Web 时，不需要创建 Git tag 或 GUI Release。

## 首次发布

首次发布需要 npm 用户 `codexsync` 拥有 `dailin521` 组织，并在本机完成 npm 登录和双重验证：

```bash
npm login
npm whoami
npm run publish:npm -- --dry-run
npm run publish:npm
```

发布脚本会依次构建 Web UI、运行 Node 测试、预览 npm 包内容，并以公开包发布。不要把 npm token、密码或一次性验证码写入仓库。

## 后续发布

首次发布成功后，在 npm 包设置中为 GitHub Actions 配置 Trusted Publisher：

- GitHub 用户或组织：`Dailin521`
- 仓库：`codex-provider-sync`
- 工作流文件：`publish-npm.yml`
- Environment：`npm`
- Allowed actions：`npm publish`

之后可在 GitHub Actions 中手动运行 `publish npm`。工作流只允许从 `main` 分支运行，使用短期 OIDC 凭据发布，不需要在 GitHub 保存长期 npm token。

每次发布前先更新 `package.json` 和 `package-lock.json` 中的版本号，并确保目标版本尚未发布。npm 不允许覆盖已经发布的同名同版本包。
