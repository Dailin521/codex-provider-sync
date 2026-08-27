# vNext Electron Desktop Candidate

> **Release status: unreleased candidate.** The currently published desktop entry point remains the Windows .NET GUI. This page is for code review and internal acceptance; it provides no Stable download and does not authorize a tag, npm/GitHub Release, signing, notarization, or update-channel publication.

The vNext desktop host lives in `apps/desktop` and shares the React UI and Node Core across Windows x64, macOS x64/arm64, and Linux x64. Electron becomes the primary desktop product, and the retained .NET GUI becomes a Legacy fallback, only after the Phase 6 exit gates pass. Neither claim is valid yet.

## Capabilities

- Overview shows Provider/model distributions, SQLite/Codex Home sources, backups, pending state, the active operation, and locked rollouts.
- Sync and Switch Provider use Prepare, present a plan, and confirm Apply with the same `planId`. Switch supports provider-default, keep-root-model, and explicit-model policies.
- Backups/Restore accepts only a managed `backupId`. Restore snapshots the current target before any target write and uses a durable journal to acknowledge, compensate, or enter recovery required.
- History loads only after explicit navigation and reads details lazily. Message bodies do not enter logs, caches, diagnostics bundles, or bulk exports.
- Profiles exposes only managed profile identifiers and revisions; the Renderer cannot submit arbitrary paths.
- Diagnostics and Settings keep destination selection in Main, support system/light/dark themes, and provide `zh-CN` and `en`.
- Watch reacquires both locks for every Apply and yields to manual operations. Main alone owns updates, and installation is blocked by writes, Watch, or unresolved journals.

## Security data flow

```text
Electron Renderer
  → DesktopCoreClient
  → narrow Preload IPC
  → Main (window, lifecycle, picker, update, supervision)
  → Utility Process
  → Node Core public API
  → original Codex storage
```

`BrowserWindow` keeps context isolation, sandboxing, and web security enabled while Node integration is disabled. A local protocol uses a strict CSP. The Renderer has no Node, file-system, arbitrary-channel, or arbitrary-path access. The Utility Process completes an app/core/protocol handshake before any business call; a crash rejects pending requests and permits at most one restart after checking unfinished journals.

Writes preserve backup-first behavior, the fixed Codex Home → State DB two-lock order, transaction journals, rollback/recovery, and the diagnostic-only WSL UNC boundary. The application does not read or export authentication data and does not modify message bodies, session titles, or `updated_at`.

## Internal acceptance

The modern workspace and Electron build use Node 24. Automated tests use only temporary directories and redacted fixtures; never use a real user's Codex Home for development tests.

```powershell
npm ci
npm run desktop:test
$env:CPS_DESKTOP_WINDOW_DISPLAY = "hidden"
npm run desktop:test:e2e
```

The hidden policy does not display or occupy the primary screen. Any visual acceptance must explicitly use a controlled test environment; automation remains hidden.

The C9 matrix fixes Windows x64 NSIS/portable ZIP, macOS x64/arm64 DMG/ZIP, and Linux x64 AppImage/deb as the candidate containers. Every platform must build natively and pass ASAR/native SQLite audits, final-container Status, Sync→Restore, graceful exit, checksum, and SBOM verification before the four-target aggregate and redacted C10 evidence bundle can close.

## Release and handoff boundary

- `1.0.0-alpha|beta|rc.<run>` is CI-candidate metadata only, and builds always use `--publish never`.
- Until required CI, the four-target aggregate, and final C10 evidence all pass, do not write source `1.0.0` or call Electron Beta, Stable, default, or released.
- The current candidate is unsigned, not notarized, and has no production Release update channel.
- Public release, signing, notarization, update metadata, and cross-version upgrade validation require separate authorization.
- Removing .NET is outside this PR. Even after Electron takes over, removal waits at least two maintenance cycles after the stable release and requires a separate project.

The [Accepted architecture baseline](VNEXT_ELECTRON_NODE_ARCHITECTURE_ZH.md) and [vNext migration execution index](migration/VNEXT_MIGRATION_EXECUTION_INDEX_ZH.md) remain authoritative.
