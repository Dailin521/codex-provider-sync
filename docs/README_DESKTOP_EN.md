# V1 Electron Primary Desktop Candidate

> **V1 candidate designation: new primary desktop candidate; release status: unreleased.** This PR labels Electron as the new primary desktop candidate and the retained .NET Windows/macOS implementations as post-handoff Legacy fallbacks. Public Releases still provide only the Windows .NET GUI; Electron is not merged into `main`, signed, notarized, downloadable, or on a production update channel. This does not claim that Electron has publicly replaced .NET and authorizes no tag, npm/GitHub Release, signing, notarization, update-channel publication, or merge.

The new desktop host lives in `apps/desktop` and shares the modern React UI and Node Core across Windows x64, macOS x64/arm64, and Linux x64. `V1` displays those handoff targets in the candidate source and UI; they do not become a public entry-point switch before the final PR and release gates pass, and they do not mark Phase 6 Completed or claim real Beta use, signing/notarization, a public Release, or update validation.

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

Sync, Switch, and Repair use only the Codex Home lock, native SQLite transactions, and an UndoBackup. A failure after mutation is a retryable partial result; ordinary writes do not create journals or auto-roll back. Restore keeps its independent pre-restore snapshot, journal, and compensation state machine. Diagnostics performs a full scan only when the user runs it and never refreshes in the background.

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
- Source manifests are `1.0.0`; this denotes a CI-verified source candidate only, not Beta, Stable, a public download, a default update entry point, or a released product.
- The current candidate is unsigned, not notarized, and has no production Release update channel.
- Public release, signing, notarization, update metadata, and cross-version upgrade validation require separate authorization.
- The .NET implementation remains buildable and tested, and the V1 candidate labels it as the post-handoff Legacy fallback target. Removal is outside this PR, waits at least two maintenance cycles after the stable release, and requires a separate project.

The [Accepted architecture baseline](VNEXT_ELECTRON_NODE_ARCHITECTURE_ZH.md) and [vNext migration execution index](migration/VNEXT_MIGRATION_EXECUTION_INDEX_ZH.md) remain authoritative.
