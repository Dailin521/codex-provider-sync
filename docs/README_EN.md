<div align="center">

# codex-provider-sync

### Help reuse existing Codex sessions after switching providers

[![CI](https://github.com/Dailin521/codex-provider-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/Dailin521/codex-provider-sync/actions/workflows/ci.yml)
[![CLI / Web](https://img.shields.io/npm/v/%40dailin521%2Fcodex-provider-sync?label=CLI%20%2F%20Web)](https://www.npmjs.com/package/@dailin521/codex-provider-sync)
[![Releases](https://img.shields.io/github/v/release/Dailin521/codex-provider-sync?label=Releases)](https://github.com/Dailin521/codex-provider-sync/releases)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](../LICENSE)

[中文](../README.md) · **English** · [日本語](README_JA.md) · [한국어](README_KO.md)

</div>

After a Provider switch, existing sessions may still reference the previous Provider. This tool aligns **Provider metadata in session files and the SQLite chat index** with the current configuration.

**It does not guarantee cross-provider/account continuation or compaction**, handle authentication, or decrypt content. Already aligned sessions do not need another sync.

## Download for Windows

**Windows x64**. No Node.js installation required.

[Download the latest stable release: installer / portable ZIP, release notes and checksums](https://github.com/Dailin521/codex-provider-sync/releases/latest).

Extract the entire portable folder; do not copy only the EXE.

This release is unsigned and requires manual installation for updates. The old .NET updater cannot migrate to Electron; download the full new package. macOS/Linux Electron packages are not yet published. CLI / Web npm versions are released independently and do not match the desktop version automatically.

## Everyday use

1. Open Overview and check the **Provider, storage paths and sync status**.
2. After switching with CCSwitch or another tool, choose **Preview sync**, or **Sync now** to execute immediately.
3. Inspect the result. If partially completed, end the relevant session activity and retry; use **Backups / Restore** to undo changes.

To change the configuration in this app, use **Switch Provider separately** at the bottom of Overview. It **changes configuration and synchronizes historical Provider metadata**, not historical models. Custom Providers must already be defined in `config.toml`.

Actual changes are backed up first. Retention defaults to the **two most recent backups**, managed in Backups / Restore. No-op operations create no backup; recovery-protected backups may exceed the limit.

You can also browse chats by project, right-click to copy session IDs/resume commands, inspect operation logs and configure storage locations. Advanced diagnostics and repair are optional: **ordinary sync never performs them automatically**. Data is not polled in the background; Watch requires explicit activation.

[Full desktop guide](README_DESKTOP_EN.md) · [Migration notes (Chinese)](release-notes/v1.0.0-zh.md)

## Local Web UI

Install Node.js `16.20.2+`, then get the currently published CLI / Web npm version:

```bash
npm install -g @dailin521/codex-provider-sync
codex-provider web
```

By default it listens only on `127.0.0.1:8791` and opens a browser for pairing. See the [Web and SSH guide (Chinese)](README_WEB_UI_ZH.md) for remote access.

## CLI

After installing the same npm package, inspect before syncing:

```bash
codex-provider status
codex-provider sync
```

CLI write commands execute directly. See the [CLI guide (Chinese)](README_CLI_ZH.md) for switching, restore, Watch, paths and JSON exit codes. Check your installed version's `--help` for available commands.

## Before using

- **Scope:** Sync changes Provider metadata, not message bodies, historical models or `threads.updated_at`. It does not read or modify `auth.json`.
- **Performance:** Eligible equal-byte-length Providers update in place. Other valid headers use streamed replacement with byte-identical bodies. Backups, flush checks and timestamp restoration remain; no manual fast mode is needed.
- **Partial results:** Not everything failed, and there is no automatic whole-operation rollback. Inspect skipped items, the failed stage and backup before retrying or restoring.
- **Still unable to continue:** For encrypted-content or model compatibility errors, return to the original Provider/account or start a new session. Sync cannot fix these.
- **Storage:** Check the actual paths in Overview. Windows WSL UNC SQLite paths are diagnostic-only; run writes inside the corresponding WSL distribution.

## Documentation

- [Documentation index (Chinese)](README_ZH.md) · [Changelog](../CHANGELOG.md) · [Report an issue](https://github.com/Dailin521/codex-provider-sync/issues)
- [How it works (Chinese)](WORKING_PRINCIPLE_ZH.md) · [Current Node Core architecture and I/O invariants](architecture/NODE_CORE_ARCHITECTURE_ZH.md)
- [Contributing and builds](../CONTRIBUTING.md) · [Migration and release gates](migration/VNEXT_MIGRATION_EXECUTION_INDEX_ZH.md) · [AI / Agent guide](../AGENTS.md)

CLI, Web and Electron share Node Core; installing the CLI does not install Electron. The .NET desktop apps remain separate Legacy implementations with build and compatibility maintenance.

## Acknowledgements and license

Thanks to [@tangquanwei](https://github.com/tangquanwei) for the Local Web UI, history browsing and multilingual documentation foundation, brought into v0.5.0 through [PR #80](https://github.com/Dailin521/codex-provider-sync/pull/80), and to everyone contributing code, documentation and issue investigation.

[Contributors](../CONTRIBUTORS.md) · [GitHub Contributors](https://github.com/Dailin521/codex-provider-sync/graphs/contributors) · [LINUX DO community](https://linux.do/) · [MIT License](../LICENSE)
