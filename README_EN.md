<div align="center">

# codex-provider-sync

### Keep Codex history visible after switching between providers

[![CI](https://github.com/Dailin521/codex-provider-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/Dailin521/codex-provider-sync/actions/workflows/ci.yml)
[![Platform](https://img.shields.io/badge/platform-Windows-lightgrey.svg)](https://github.com/Dailin521/codex-provider-sync)
[![Node](https://img.shields.io/badge/node-24%2B-brightgreen.svg)](https://nodejs.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Community](https://img.shields.io/badge/community-LINUX%20DO-2ea043.svg)](https://linux.do/)

English | [中文](README.md)

</div>

## What It Solves

Codex session visibility can break after you switch `model_provider`.

Typical symptom:

- old sessions are visible under one provider
- then disappear after switching to another provider
- `codex resume` and Codex App may disagree because session metadata is stored in both rollout files and SQLite

`codex-provider-sync` fixes that by updating both:

- `~/.codex/sessions` and `~/.codex/archived_sessions`
- `~/.codex/state_5.sqlite`

## GUI For Windows

If you want a normal Windows app instead of Node/npm, download `CodexProviderSync.exe` from Releases.

The GUI app:

- scans the current `.codex` home
- shows provider distribution from rollout files and SQLite
- lets you choose a target provider from detected and saved providers
- can optionally update root `model_provider` in `config.toml`
- keeps the latest 5 managed backups by default, with a configurable retention count
- can manually clean old managed backups from the app
- can restore from backup without using a terminal

For GUI-specific usage notes, see [README_GUI_ZH.md](README_GUI_ZH.md).

## Install

```bash
npm install -g github:Dailin521/codex-provider-sync
```

Requirements:

- Node.js `24+`
- standard `~/.codex` layout
- Windows is the primary tested target for now

For end users, the GUI EXE is the recommended path. The npm CLI remains available for power users and automation.

## Quick Start

GUI:

- download `CodexProviderSync.exe` from Releases
- open it and click `Refresh`
- choose the target provider
- click `Execute`

If you already switched auth/provider using your usual method:

```bash
codex-provider sync
```

If you want to change the root `model_provider` and sync history in one step:

```bash
codex-provider switch openai
codex-provider switch apigather
```

If you want a different automatic backup retention count for one run:

```bash
codex-provider sync --keep 5
codex-provider switch apigather --keep 10
```

Check current state first:

```bash
codex-provider status
```

Install a Windows double-click launcher (placed on your Desktop by default):

```bash
codex-provider install-windows-launcher
```

Rollback from a backup:

```bash
codex-provider restore C:\Users\you\.codex\backups_state\provider-sync\<timestamp>
```

Clean old managed backups manually:

```bash
codex-provider prune-backups --keep 5
```

Delete one or more sessions by id (with backup):

```bash
codex-provider delete <session-id>
codex-provider delete <session-id-1> <session-id-2>
```

## AI Quick Run

If you want an AI assistant to handle this in one shot, copy this prompt:

```text
Help me fix Codex session visibility with codex-provider-sync.

Steps:
1. Run `codex-provider status`.
2. If my current provider is already correct, run `codex-provider sync`.
3. If I explicitly want to switch provider, run `codex-provider switch <provider-id>` instead.
4. If `state_5.sqlite` is currently in use, tell me to close Codex / Codex App / app-server and retry.
5. If sync skips locked rollout files, tell me which files were skipped and remind me to rerun `codex-provider sync` later.
6. Summarize the final provider counts in rollout files and SQLite.
```

If the user prefers the GUI, the AI can instead guide these steps:

1. Open `CodexProviderSync.exe`
2. Confirm the `.codex` path
3. Click `Refresh`
4. Pick the target provider from the list
5. Enable the config checkbox only if root `model_provider` should also change
6. Click `Execute`
7. Read the log panel for backup path, updated rollout files, SQLite rows, and skipped locked files

Quick mapping:

- inspect only: `codex-provider status`
- fix visibility under current provider: `codex-provider sync`
- switch provider and sync: `codex-provider switch openai`
- delete a specific session: `codex-provider delete <session-id>`
- install a desktop double-click launcher: `codex-provider install-windows-launcher`
- roll back a mistake: `codex-provider restore <backup-dir>`

## Commands

- `codex-provider status`
  - shows current provider and provider distribution in rollout files and SQLite
- `codex-provider sync`
  - syncs history to the current provider
  - `--provider <id>` overrides the target provider
  - if root `model_provider` is missing, it falls back to `openai`
- `codex-provider switch <provider-id>`
  - updates root `model_provider` in `config.toml`
  - immediately runs a sync
  - `--keep <n>` overrides how many managed backups are retained after the run
- `codex-provider delete <session-id> [more-session-ids...]`
  - deletes sessions by id
  - removes matching rollout session files and matching rows in SQLite `threads`
  - skips sessions whose rollout files are currently locked, and reports them
  - creates a backup before deletion so `restore` can roll back
- `codex-provider prune-backups`
  - manually removes older managed backups and keeps the newest `n`
- `codex-provider restore <backup-dir>`
  - restores a previous backup
- `codex-provider install-windows-launcher`
  - creates two files on the Desktop by default
  - `Codex Provider Sync.vbs`: hidden double-click launcher with a result popup
  - `Codex Provider Sync.cmd`: visible console version for troubleshooting
  - use `--dir <path>` to choose another install directory
  - use `--codex-home <path>` to bake a fixed `CODEX_HOME` into the launcher

```bash
codex-provider status
codex-provider sync
codex-provider sync --keep 5
codex-provider sync --provider openai
codex-provider switch openai
codex-provider switch apigather
codex-provider delete 019d95ee-abd6-7de3-b1f8-63b13c725f17
codex-provider delete 019d95ee-abd6-7de3-b1f8-63b13c725f17 019d95ef-c7fd-74b0-956c-6110fd8ff314
codex-provider prune-backups --keep 5
codex-provider install-windows-launcher
codex-provider install-windows-launcher --dir D:\Tools
codex-provider install-windows-launcher --codex-home C:\Users\you\.codex
codex-provider restore C:\Users\you\.codex\backups_state\provider-sync\20260319T042708906Z
codex-provider status --codex-home C:\Users\you\.codex
codex-provider sync --codex-home C:\Users\you\.codex
codex-provider switch apigather --codex-home C:\Users\you\.codex
codex-provider restore C:\Users\you\.codex\backups_state\provider-sync\20260319T042708906Z
```

## Safety

Before each sync, the tool creates a backup under:

```text
~/.codex/backups_state/provider-sync/<timestamp>
```

It also uses:

```text
~/.codex/tmp/provider-sync.lock
```

- It does not replace official `codex`.
- It does not manage `auth.json` or third-party login tools.
- It does not rewrite message history, titles, cwd, or timestamps.
- It keeps the newest 5 managed backups by default; GUI retention settings or CLI `--keep <n>` can override that.
- Manual cleanup and auto-prune only touch backups created by this tool inside `backups_state/provider-sync`.
- `Codex Provider Sync.vbs` assumes the `codex-provider` command is already available.
- If `state_5.sqlite` is in use, close Codex / Codex App / app-server and retry.
- If a live session keeps one rollout file open, `sync` skips that file and reports it. Rerun later.

## For AI Agents

For a fuller machine-oriented version, see [AGENTS.md](AGENTS.md).

## Development

```bash
git clone https://github.com/Dailin521/codex-provider-sync.git
cd codex-provider-sync
npm test
dotnet test desktop/CodexProviderSync.Core.Tests/CodexProviderSync.Core.Tests.csproj
pwsh ./scripts/publish-gui.ps1
node ./src/cli.js status --codex-home C:\path\to\.codex
```

## License

MIT
