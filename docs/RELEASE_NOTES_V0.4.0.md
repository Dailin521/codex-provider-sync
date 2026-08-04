# v0.4.0 Release Notes (Draft)

> This document describes the current v0.4 integration branch. v0.4.0 has not
> been tagged or published. All v0.4 engineering gates are complete on the
> integration branch; creating a tag or formal Release remains a separate
> maintainer action.

## Highlights

- Issue #69 reliability work adds backup-bound transaction journals, atomic
  file replacement, compensating rollback, rollback-failure evidence, and
  explicit crash-recovery diagnostics for rollout and global-state writes.
- Ambiguous SQLite COMMIT acknowledgement is treated conservatively: once a
  mutating COMMIT is attempted, any unconfirmed outcome restores the database
  from the transaction's bound backup; compensation failure remains
  `recoveryRequired` rather than being reported as rolled back.
- The Windows GUI and Business Automation host now share the same Application
  use cases for status, planning, sync, switch, restore, and backup pruning.
- `CodexProviderSync.Automation.exe` provides experimental protocol `0.4`
  commands: `describe`, `status`, `plan`, `sync`, `switch`, `restore`, and
  `prune`.
- The WinForms application has stable Automation IDs, a versioned manifest,
  an isolated named-pipe bridge, and schema-2 GUI-to-Application causal traces.
- The Windows publish build includes the GUI, Business Automation executable,
  protocol schema, and GUI automation manifest.

## Business Automation Safety

Write commands are dry-run by default. An actual mutation requires all three
of the following:

1. explicit `--apply`;
2. a plan file containing the `data` object returned by a matching `plan`
   response; and
3. the plan's exact lowercase SHA-256 digest through `--plan-digest`.

Plans expire, bind normalized inputs and target fingerprints, and are claimed
only once through a durable ledger. Automation accepts absolute paths only,
rejects symbolic-link/reparse-point traversal, and never reads or modifies
`auth.json`.

## GUI Automation Safety

Normal GUI launches do not create an Automation listener. Test launches require
a protected descriptor under a sentinel-bearing disposable root, a random
current-user-only named pipe, and a one-time token. The bridge drives registered
real controls and WinForms events on the UI thread; it cannot bypass the GUI
and call Application directly while claiming GUI coverage. Native dialogs are
operated by the external visible-desktop driver.

## Compatibility and Migration

- Existing CLI command behavior remains available. Business Automation is an
  additional Windows companion executable, not a replacement for the Node CLI.
- Protocol and manifest `0.4` are experimental pre-1.0 contracts and may
  change incompatibly in a later protocol family.
- Core remains authoritative for storage resolution, backup/restore, locking,
  transaction safety, and WSL UNC diagnostics.
- Full macOS GUI migration to the shared Application layer is outside this
  Windows v0.4 gate; Core/Application compatibility builds remain required.

## Windows Release GUI Gate Evidence

Run the intended visible Windows Release GUI gate with:

```powershell
pwsh ./scripts/run-windows-gui-e2e.ps1
```

Acceptance requires the published real EXE, all manifest entries and declared
actions, real events and dialogs, independent file/SQLite differences, state
and busy behavior, restart persistence, and GUI-to-Application traces. Mock,
hidden, skipped, or direct-Application runs do not count.

Implementation commit `7545b5d` passed this gate on a visible interactive
Windows desktop. Machine-readable evidence recorded 40/40 manifest entries,
53/53 required Headful scenarios, zero errors, zero blockers, a matching
published-EXE SHA-256, real native dialogs, independent sync/switch/restore/
prune effects, physical prune deletion with an unchanged unmanaged sentinel,
restart persistence, and linked GUI-to-Application operation/lifecycle traces.

At `7545b5d`, the complete Node suite passed 183/183, the .NET Core suite passed
182 tests with one expected environment-gated WSL test skipped, the remaining
.NET suites and package/version checks passed, independent final-diff review
found no unresolved P0/P1/P2, and exact-head GitHub CI run `30887305572` passed
all eight jobs. The material SQLite plan-fingerprint reliability decision
received the single policy-authorized independent Claude challenge; ordinary
final-diff review remained local under the current policy. No formal tag or
Release was created by this integration work.
