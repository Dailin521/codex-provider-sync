# macOS GUI Guide

[中文](README_MAC_GUI_ZH.md) · English

`CodexProviderSync.app` is the macOS desktop GUI. It is built with Avalonia and
reuses the status, synchronization, switching, restore, and backup-cleanup
logic from `desktop/CodexProviderSync.Core`.

## Build

The app requires the .NET 10 SDK and macOS 12 or later.

Build the default Apple Silicon (`osx-arm64`) app:

```bash
./scripts/publish-gui-macos.sh
```

Build the Intel (`osx-x64`) app:

```bash
./scripts/publish-gui-macos.sh --runtime osx-x64 --output artifacts/osx-x64
```

The default output is:

```text
artifacts/osx-arm64/CodexProviderSync.app
```

To use a specific .NET SDK:

```bash
DOTNET=/path/to/dotnet ./scripts/publish-gui-macos.sh
```

Open the locally built app:

```bash
open artifacts/osx-arm64/CodexProviderSync.app
```

## Features

- Select or enter a Codex Home; the default is `~/.codex`.
- Use `Refresh` to inspect the current Provider, rollout and SQLite Provider
  counts, managed backups, project visibility, and `encrypted_content` risks.
- See where each Provider was discovered: `config`, `rollout`, `SQLite`, or
  `manual`.
- Add or remove manual Providers.
- Run `Sync Metadata Only`.
- Run `Switch config.toml and sync`.
- Use `Restore Backup` to restore selected `config.toml`, SQLite, and rollout
  metadata.
- Use `Clean Old Backups` and `Open Backup Folder`.
- Review operation logs and error messages in the app.

## Backups and Safety

- Launching the app or using `Refresh` does not modify sessions, SQLite, or
  `config.toml` metadata in the selected Codex Home.
- The app asks for confirmation before write operations.
- The Core creates a managed backup before `sync` or `switch` changes metadata.
  Backups are stored under
  `~/.codex/backups_state/provider-sync/<timestamp>`.
- The app does not manage `auth.json`, sign in, authenticate, change
  conversation content, or modify `updated_at`.
- `encrypted_content` is reported as a risk; the app does not promise to repair
  encrypted conversations across Providers or accounts.

## Before Write Operations

Before using `Sync Metadata Only`, `Switch config.toml and sync`,
`Restore Backup`, or `Clean Old Backups`, close:

- Codex CLI;
- Codex App;
- app-server; and
- terminal tasks that are still using the selected Codex Home.

If the app reports `state_5.sqlite is currently in use`, close those processes
and retry.

If the log reports skipped locked rollout files, an active session usually
still holds those files open. Most of the synchronization may already be
complete. Run sync again after the active session ends to update the skipped
files.

## Build and Distribution Notes

The macOS desktop project currently uses Avalonia 12.0.4.

`scripts/publish-gui-macos.sh` publishes an `osx-arm64` self-contained `.app`
bundle by default. When `codesign` is available, the script applies ad-hoc
signing. Ad-hoc signing is not Apple Developer ID signing or notarization.

The project currently provides the macOS build script rather than a prebuilt
macOS asset in GitHub Releases. Build the app locally using the commands above.
The macOS script does not modify the Windows GUI publishing script at
`scripts/publish-gui.ps1`.
