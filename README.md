<div align="center">

# codex-provider-sync

### Keep Codex history visible after switching Providers

[![CI](https://github.com/Dailin521/codex-provider-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/Dailin521/codex-provider-sync/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/Dailin521/codex-provider-sync)](https://github.com/Dailin521/codex-provider-sync/releases/latest)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

[中文](docs/README_ZH.md) · [日本語](docs/README_JA.md) · [한국어](docs/README_KO.md) · [Web UI guide](docs/README_WEB_UI_ZH.md)

</div>

## What it is

`codex-provider-sync` is a local metadata-consistency tool for Codex. After changing the root `model_provider`, older sessions may still be on disk while rollout files, the SQLite thread index, or project metadata still points to the previous Provider. Codex can then hide those sessions from its list, project view, or `/resume`.

The primary interface is a browser-based Web UI running on localhost. It reuses the same Node.js core service as the CLI and does not reimplement synchronization in the browser.

The tool does not sign you in, manage `auth.json`, switch accounts, or modify message bodies.

## Quick start: Web UI

Requires Node.js 16 or newer.

From the repository:

```bash
npm install
npm run web:build
npm run web:start
```

Or install the CLI globally:

```bash
npm install -g git+https://github.com/Dailin521/codex-provider-sync.git
codex-provider web
```

The default address is:

```text
http://127.0.0.1:8791
```

Options:

```bash
codex-provider web --no-open   # do not open a browser automatically
codex-provider web --port 8792 # use another localhost port
```

The server binds only to `127.0.0.1`. Each process gets a random API session token, validates the request Origin, and serializes write operations (`sync`, `switch`, `restore`, and `prune`).

## Web UI features

### Overview

- Current root Provider and model.
- Rollout distribution under `sessions` and `archived_sessions`.
- SQLite `threads` distribution.
- Rollout/SQLite alignment status.
- Project visibility diagnostics: CWD matches, ranks, and the first-page 50-session limit.
- Warnings for locked rollouts, `encrypted_content`, SQLite repairs, malformed databases, and WSL UNC safety boundaries.

### Chat history

The Chat History page reads rollout JSONL files read-only and never changes local data.

- Browse sessions and open user/agent messages.
- Search titles, project paths, Providers, and message text.
- Filter by Provider, project, and active/archived state.
- Server-side pagination, 50 sessions per page by default.
- Session details show the most recent 200 readable messages.
- Safe, restricted Markdown rendering with code-block support.
- Raw JSONL, tokens, tool-call arguments, and `encrypted_content` are not returned to the browser.

### Sync and switch

- **Sync metadata only**: use the current root Provider without changing `config.toml`.
- **Switch Provider and sync**: update the root `model_provider`, then synchronize history.
- Model policy: follow the Provider section, keep the current root model, or set a custom model.
- An explicit confirmation dialog reminds you to close Codex CLI, Codex App, and app-server first.

### Backups and restore

- Create a metadata v2 backup before every `sync` or `switch`.
- Keep the newest five managed backups by default.
- Restore `config.toml`, SQLite, and rollout metadata independently.
- Show source and target when SQLite Homes differ.
- Require an extra confirmation for SQLite Home relocation and prevent unsafe config/database combinations.

## CLI for automation and WSL

The CLI and Web UI call the same `src/service.js` core logic.

```bash
codex-provider status
codex-provider sync
codex-provider sync --keep 5
codex-provider sync --provider openai
codex-provider switch apigather
codex-provider switch apigather --model "MiniMax-M3"
codex-provider switch apigather --keep-root-model
codex-provider prune-backups --keep 5
codex-provider restore C:\Users\you\.codex\backups_state\provider-sync\20260319T042708906Z
codex-provider watch
codex-provider watch --once
```

All main commands accept `--codex-home <PATH>` and `--sqlite-home <PATH>`. For a Windows Codex Home with app-server and SQLite in WSL, run the CLI inside WSL:

```bash
codex-provider status --codex-home /mnt/c/Users/you/.codex --sqlite-home /home/you/.codex/sqlite
codex-provider sync --codex-home /mnt/c/Users/you/.codex --sqlite-home /home/you/.codex/sqlite
```

## Storage and SQLite resolution

SQLite Home precedence is:

1. CLI or GUI override.
2. Root-level `sqlite_home` in `config.toml`.
3. `CODEX_SQLITE_HOME`.
4. `<Codex Home>/sqlite`.

Only the default layout may check the legacy `<Codex Home>/state_5.sqlite`. An explicit, configured, or environment-provided SQLite Home never falls back to another database.

## Safety and limitations

Before each `sync` or `switch`, a backup is created under:

```text
~/.codex/backups_state/provider-sync/<timestamp>
```

- Messages, titles, authentication, `auth.json`, and `updated_at` are not modified.
- The tool repairs metadata only within the selected Codex Home; it does not copy sessions between devices.
- If SQLite is locked, close Codex CLI, Codex App, and app-server and retry.
- Locked live rollout files are skipped and can be synchronized after the active session ends.
- Sessions containing `encrypted_content` may become visible but still fail to continue or compact across Providers/accounts.
- Windows processes cannot safely operate on SQLite through `\\wsl.localhost\...` or `\\wsl$\...`; use the corresponding Linux path inside WSL.

## Desktop GUI status

The Desktop GUI is deprecated and is no longer the recommended or primary interface. Existing Windows/macOS builds remain available for compatibility, but new features—especially Chat History—are implemented in the Web UI first.

Legacy GUI references:

- [Windows GUI guide](docs/README_GUI_ZH.md)
- [macOS GUI guide](docs/README_MAC_GUI_EN.md)

## Documentation

- [中文说明](docs/README_ZH.md)
- [日本語](docs/README_JA.md)
- [한국어](docs/README_KO.md)
- [Web UI guide](docs/README_WEB_UI_ZH.md)
- [Working principle](docs/WORKING_PRINCIPLE_ZH.md)
- [AI / Agent guide](AGENTS.md)
- [Contributing guide](CONTRIBUTING.md)

## Development and tests

```bash
git clone https://github.com/Dailin521/codex-provider-sync.git
cd codex-provider-sync
npm install
npm run web:build
npm test
git diff --check
```

## License

MIT
