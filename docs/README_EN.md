<div align="center">

# codex-provider-sync

### Keep Codex history visible after switching Providers

[![CI](https://github.com/Dailin521/codex-provider-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/Dailin521/codex-provider-sync/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/Dailin521/codex-provider-sync)](https://github.com/Dailin521/codex-provider-sync/releases/latest)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](../LICENSE)

[Download Windows GUI](https://github.com/Dailin521/codex-provider-sync/releases/latest) · [中文](../README.md) · English

</div>

## When You Need It

After switching `model_provider`, older Codex sessions may disappear from Desktop or `/resume`. The sessions are usually still present, but their rollout, SQLite, or project-visibility metadata still points to the previous Provider.

Use this tool when:

- switching between an official subscription (whose internal Provider is `openai`) and a custom relay;
- switching configurations that must use different `model_provider` IDs;
- rollout and SQLite Provider or model metadata has become inconsistent; or
- you want changes to `config.toml`, SQLite, or its WAL to trigger synchronization automatically.

If all of your relays can reliably reuse one `model_provider` ID and history remains visible, using that shared ID is simpler and no synchronization is needed. This project is mainly useful when Provider IDs cannot be unified or when switching between official and custom Providers.

The tool does not sign in, manage accounts, or switch authentication. Switch Provider using your normal workflow first, then synchronize history.

## What It Updates

- Rollout metadata under `~/.codex/sessions` and `~/.codex/archived_sessions`.
- Codex SQLite thread records. It prefers `~/.codex/sqlite/state_5.sqlite` and supports the legacy `~/.codex/state_5.sqlite` location.
- Project-visibility path information and related model metadata when required.
- Managed backups before each synchronization, with restore and pruning support.
- Large rollout files in place when safe, with automatic fallback to a full safe rewrite.
- Automatic CLI synchronization after `config.toml`, SQLite, or WAL changes.

## Quick Start

### Windows GUI

For normal Windows use, download and extract `CodexProviderSync.exe` from [Releases](https://github.com/Dailin521/codex-provider-sync/releases/latest):

1. Open `CodexProviderSync.exe`.
2. Click `刷新` (Refresh).
3. Select the target Provider.
4. Click `立即同步` (Sync Now).

The GUI keeps backups and displays the synchronization result. It checks for a stable release in the background on the first launch of each local day, with a 10-second lookup deadline. Manual update checks remain available. Execution logs are stored under `%AppData%\codex-provider-sync\logs`.

The Windows executable is currently unsigned, so browser downloads may trigger a SmartScreen warning. Download it only from this project's Releases and verify the matching SHA-256 when needed.

See [README_GUI_ZH.md](README_GUI_ZH.md) for the full Windows guide. A self-built Avalonia macOS app is also available; see [README_MAC_GUI_ZH.md](README_MAC_GUI_ZH.md).

### CLI

The CLI requires Node.js `16+`:

```bash
npm install -g git+https://github.com/Dailin521/codex-provider-sync.git
codex-provider status
codex-provider sync
```

Common commands:

| Command | Purpose |
| --- | --- |
| `codex-provider status` | Inspect the current Provider, rollout files, SQLite, and project visibility |
| `codex-provider sync` | Synchronize history to the current Provider without changing authentication |
| `codex-provider switch <provider-id>` | Change the root `model_provider`, then synchronize |
| `codex-provider restore <backup-dir>` | Restore a selected backup |
| `codex-provider prune-backups --keep 5` | Keep only the five newest managed backups |
| `codex-provider watch` | Watch config, SQLite, and WAL changes and synchronize automatically |
| `codex-provider watch --once` | Exit after the first change is synchronized successfully |

`switch` accepts `--model <NAME>` to set the root model explicitly, or `--keep-root-model` to change only the Provider. All main commands accept `--codex-home <PATH>`.

Node.js 24+ uses the built-in `node:sqlite` module. Older supported Node.js releases use the optional `better-sqlite3` dependency.

## Safety and Limitations

Before each `sync` or `switch`, the tool creates a backup under:

```text
~/.codex/backups_state/provider-sync/<timestamp>
```

- It does not modify messages, session titles, authentication, `auth.json`, or `updated_at`.
- It does not copy configuration or session files between devices; it only repairs metadata in the current Codex Home.
- If SQLite is in use, close Codex, Codex App, and app-server before retrying.
- If a live session locks a rollout file, the tool skips that file and continues. Run sync again after the session ends for a complete update.
- Sessions containing `encrypted_content` may become visible across Providers/accounts but still fail to continue or compact with `invalid_encrypted_content`.
- Codex Desktop currently shows only the latest 50 sessions on its first page. If `/resume` can see a session but the project view cannot, inspect the `first page` / `ranks` diagnostics. This tool does not alter timestamps to bypass that upstream limit.

## Documentation

- [Windows GUI guide](README_GUI_ZH.md)
- [macOS GUI guide](README_MAC_GUI_ZH.md)
- [中文说明](../README.md)
- [AI / Agent guide](../AGENTS.md)

## Development

```bash
git clone https://github.com/Dailin521/codex-provider-sync.git
cd codex-provider-sync
npm test
dotnet test desktop/CodexProviderSync.Core.Tests/CodexProviderSync.Core.Tests.csproj
pwsh ./scripts/publish-gui.ps1
./scripts/publish-gui-macos.sh
```

## License

MIT
