# AI Operator Guide

This file is for AI assistants, coding agents, and automation tools.

## Goal

Help the user make historical Codex sessions visible again after switching `model_provider`.

For normal Windows users, prefer the GUI app when it is available. Use the CLI when:

- the user explicitly wants commands
- the task is automated
- the GUI EXE is unavailable

The tool works by updating both:

- rollout metadata under `~/.codex/sessions` and `~/.codex/archived_sessions`
- SQLite thread metadata in the resolved Codex state database

## Architecture Direction

`docs/AUTOMATION_DESIGN_NOTES.md` records the experimental 0.x direction.
It is not a public compatibility contract. No Automation executable, stable
JSONL protocol, or UI probe is currently shipped.

For Windows GUI work:

- move UI-independent state, validation, and Core request construction into the
  Application/controller layer incrementally
- keep Core authoritative for config, rollout, SQLite, backup, restore, and
  storage-safety behavior
- keep WinForms handlers focused on presentation and platform interaction
- preserve observable behavior and add controller tests for each migrated slice
- prefer controller tests over adding new reflection-based MainForm business tests

Resolve SQLite Home in this order: explicit CLI/GUI override, root `sqlite_home` in `config.toml`, `CODEX_SQLITE_HOME`, then `<codex-home>/sqlite`. Only the default layout may fall back to `<codex-home>/state_5.sqlite`. Never fall back when an explicit/config/environment SQLite Home is missing.

On Windows, `\\wsl.localhost\...` and `\\wsl$\...` SQLite Homes are diagnostic-only. SQLite operations for these paths run inside WSL and use the corresponding Linux path.

Do not solve this by manually editing rollout files only unless the user explicitly asks for manual intervention.

## Preferred Flow

Use this order by default:

1. If the GUI is available and the user is not asking for terminal commands, open `CodexProviderSync.exe`
2. Refresh and inspect the current provider plus rollout/SQLite distribution
3. Decide whether the user needs sync, switch-like behavior, or restore
4. Execute the action
5. Report whether the result is complete or partially skipped due to locked files

CLI fallback flow:

1. Run `codex-provider status`
2. Read `Current provider`, the displayed SQLite database path, and compare rollout/SQLite distribution
3. Decide whether the user needs `sync`, `switch`, or `restore`
4. Run the command
5. Report whether the result is complete or partially skipped due to locked files

## Command Selection

Use `codex-provider sync` when:

- the user already switched auth/provider using another tool
- the current `config.toml` root `model_provider` is already correct
- the user says things like:
  - "make my old sessions visible again"
  - "resync my Codex history"
  - "I already switched provider"

Use `codex-provider switch <provider-id>` when:

- the user wants to change the root `model_provider`
- the user wants one command to both switch provider and resync history

By default `switch` also aligns the root-level `model` with the new
provider section's `model`. Use `switch <provider-id> --keep-root-model`
to leave the root-level `model` untouched, or
`switch <provider-id> --model <name>` to set it explicitly (e.g. when
the new provider section has no `model` field of its own, or when the
user wants to call a non-default model through a relay provider).

Use `codex-provider restore <backup-dir>` when:

- the user wants to roll back a previous sync
- the user synced to the wrong provider

Use `codex-provider status` only when:

- the user asks for inspection only
- you need a safe first step before deciding what to do

## GUI Selection

Use the GUI app when:

- the user wants a double-click tool
- the user does not want to install Node/npm
- the user wants to visually inspect providers and backups

GUI mapping:

- `Refresh` = inspect current status
- `Execute` without config checkbox = `sync --provider <selected>`
- `Execute` with config checkbox = switch-like behavior
- `Restore Backup` = restore a previous backup
- backup retention defaults to 5 and can be customized in the GUI
- SQLite Home overrides are stored per Codex Home in app settings and are passed to refresh, sync, switch, and restore
- Windows GUI refresh reports WSL UNC SQLite Homes as diagnostic-only paths; Execute and Restore are disabled for that layout
- restoring a metadata v2 backup to a different SQLite Home requires a second confirmation showing source and target
- `Clean Old Backups` = prune managed backups down to the selected retention count

## Important Behavior

- `sync` uses the current root `model_provider` from `~/.codex/config.toml`
- if root `model_provider` is missing, `sync` falls back to `openai`
- `switch` changes root `model_provider`, then runs a sync
- built-in `openai` is always valid
- custom providers must already exist in `config.toml`
- the tool does not log the user in and does not manage `auth.json`
- sync and switch create a backup first, then automatically prune older managed backups
- backup pruning only touches backups created by this tool under `backups_state/provider-sync`

## Error Handling

If the output says `state_5.sqlite is currently in use`:

- tell the user to close Codex, Codex App, and app-server
- then rerun the same command

If the output says Windows cannot safely access SQLite through a WSL UNC path:

- identify the message as a WSL UNC path safety diagnostic
- open the corresponding WSL distribution
- run the CLI there with the Windows Codex Home mounted under `/mnt/<drive>/...` and SQLite Home expressed as a Linux `/home/...` path

If sync reports `Skipped locked rollout files`:

- treat the sync as mostly successful
- explain that an active session either still holds one or more rollout files open, or appended to one while it was being scanned
- tell the user to rerun `codex-provider sync` after that session ends if they want a full rewrite

If `switch <provider-id>` fails because the provider is missing:

- tell the user to define it in `config.toml` or switch via their existing provider tool first
- then run `codex-provider sync`

## Safe Defaults

- default Codex home: `~/.codex`
- detect the SQLite DB before reasoning about SQLite counts; recent Codex uses
  `~/.codex/sqlite/state_5.sqlite`, while older layouts may use
  `~/.codex/state_5.sqlite`
- prefer `status` before destructive-looking operations, even though this tool only edits metadata
- by default the tool keeps the most recent 5 managed backups
- use GUI retention settings or CLI `--keep <n>` when the user wants a different retention count
- do not edit `state_5.sqlite` or rollout files manually if the tool can do it
- classify WSL UNC messages as path safety diagnostics and route SQLite operations through WSL with Linux paths
- metadata v2 backups record `sqliteHome` and `sqliteDbFiles`; a missing default-layout database may be rebuilt from a valid backup, but a missing explicit/config/environment database remains an error
- CLI restore to a different SQLite Home requires `--sqlite-home`, `--allow-sqlite-home-relocation`, and `--no-config`; desktop apps must reject relocation while config restore is selected
- GUI settings live in `%AppData%\codex-provider-sync\settings.json`

## Recommended Commands

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
```

With an explicit Codex home:

```bash
codex-provider status --codex-home C:\Users\you\.codex
codex-provider sync --codex-home C:\Users\you\.codex
codex-provider switch openai --codex-home C:\Users\you\.codex
```

From WSL when Codex Home is on Windows and SQLite Home is in WSL:

```bash
codex-provider status --codex-home /mnt/c/Users/you/.codex --sqlite-home /home/you/.codex/sqlite
codex-provider sync --codex-home /mnt/c/Users/you/.codex --sqlite-home /home/you/.codex/sqlite
```

## One-Shot Prompt Template

Use this prompt in another AI tool if the user wants one-step handling:

```text
I use codex-provider-sync. Please help me fix Codex session visibility under my current provider.

Steps:
1. Run `codex-provider status`.
2. If my current provider is already correct, run `codex-provider sync`.
3. If I explicitly tell you to switch provider, run `codex-provider switch <provider-id>` instead.
4. If SQLite is locked, tell me to close Codex / Codex App / app-server and retry.
5. If rollout files are skipped because they are locked, tell me which ones were skipped and remind me to rerun sync later.
6. Summarize the final state of rollout files and SQLite after the command finishes.
```

## User-Facing Summary Style

When reporting results back to the user:

- state the current provider
- state whether rollout files and SQLite are aligned
- mention backup location if a sync or switch was executed
- call out partial success clearly if locked rollout files were skipped
