# Automation Design Notes

> Status: implemented for the v0.4 integration branch, experimental and
> non-normative.
>
> The v0.4 source and Windows Release build contain an Automation executable,
> protocol schema, GUI manifest, and isolated GUI bridge. Implementation commit
> `7545b5d` passed the visible Headful Windows release gate; v0.4 has not been
> tagged or published as a formal Release. Protocol `0.4` is a pre-1.0
> compatibility boundary.

## Motivation

Business scripting and GUI regression tests must exercise the same behavior as
the real Windows application without duplicating Core workflows or exposing a
production control port. The design therefore separates two automation
surfaces:

- Business Automation invokes shared, UI-independent Application use cases.
- GUI Automation drives real WinForms controls and observes their real event
  path into those use cases.

## Implemented Architecture

- `CodexProviderSync.Core` remains authoritative for config, rollout, SQLite,
  backup, restore, pruning, storage resolution, locking, transaction recovery,
  and WSL safety behavior.
- `CodexProviderSync.Application` exposes immutable use cases for
  `describe`, `status`, `plan`, `sync`, `switch`, `restore`, and `prune`.
- The production Windows GUI and `CodexProviderSync.Automation.exe` use the
  same `IApplicationService` implementation.
- WinForms remains responsible for rendering, native dialogs, confirmation,
  focus, shell launch, update UI, and other Windows-specific presentation.
- The GUI bridge may only manipulate registered controls on the UI thread. It
  may not call Application directly while claiming to cover a GUI action.

## Business Automation Protocol

The Windows Release build contains:

- `CodexProviderSync.Automation.exe`
- `automation-protocol-v0.4.schema.json`

Each invocation emits exactly one protocol `0.4` JSON response on stdout;
diagnostics use stderr. Supported commands are `describe`, `status`, `plan`,
`sync`, `switch`, `restore`, and `prune`.

Every write command is dry-run by default. Mutation requires explicit
`--apply`, a plan document containing the `data` object returned by `plan`, and
its exact lowercase SHA-256 digest. Plans are expiry-bound, bind normalized
inputs and target fingerprints, and are single-use through a durable ledger.

## GUI Automation Surface

The versioned manifest is
`desktop/CodexProviderSync.App/Automation/gui-automation-manifest.v0.4.json`.
Runtime enumeration checks the manifest against real interactive controls and
stable Automation IDs.

The bridge supports `ui.describe`, `ui.snapshot`, `ui.get`, `ui.set`,
`ui.invoke`, `ui.wait`, and `ui.shutdown`; launch and authentication belong to
the external harness/bootstrap. `ui.set` and `ui.invoke` use real control APIs
and WinForms events. A schema-2 causal trace records the Automation ID, GUI
event, Application operation ID/kind/lifecycle, timestamps, and redacted
values.

The bridge is disabled during normal launches. An automation launch requires
a protected descriptor beneath a sentinel-bearing disposable root, a random
current-user-only named pipe, and a one-time token. The bridge permits one
authenticated client, bounds messages and wait times, rejects replay, and
confines all paths to the canonical isolated root. The external Headful driver
operates real native dialogs while the bridge waits for the real GUI event
path.

## Safety Invariants

- Automation never reads, copies, logs, or modifies `auth.json` or credentials.
- Tests use explicit disposable fixture homes and never infer the runner's real
  Codex Home, SQLite Home, AppData, settings, logs, or backup roots.
- Absolute path validation rejects escapes, symbolic links, and reparse-point
  ancestors.
- Writes remain backup-first and preserve Core transaction, rollback, crash
  recovery, locking, and WSL UNC safety rules.
- Partial results, rollback failures, recovery-required state, cancellation,
  timeout, stale plans, and duplicate execution remain machine-distinct.
- A hidden window, mock control, skipped scenario, or direct Application call
  cannot be reported as a real GUI pass.

## Release Verification Status

The intended one-command gate is:

```powershell
pwsh ./scripts/run-windows-gui-e2e.ps1
```

It publishes the Release GUI and Headful driver, creates an isolated fixture,
launches the visible executable, traverses the manifest, and produces
machine-readable evidence. Acceptance requires real controls and events,
native dialogs, independent file/SQLite effects, state and busy behavior,
restart persistence, and GUI-to-Application traces. Commit `7545b5d` passed with
40/40 manifest entries, 53/53 required scenarios, zero errors/blockers, and a
matching published-EXE SHA-256. Lower-level tests alone are not a PASS claim.

## Compatibility Boundary

Protocol and manifest version `0.4` may change incompatibly before 1.0. The
project does not promise a public network service, arbitrary reflection,
arbitrary file access, GUI automation in normal mode, stable native-dialog
internals, or macOS GUI automation. Full macOS Application migration is not a
v0.4 Windows release gate, although cross-platform Core/Application builds
remain compatibility checks.

Stabilization beyond `0.4` requires exact-head schema and package validation,
the complete Headful Windows gate, transaction/recovery evidence, independent
review, and CI at the same final commit.
