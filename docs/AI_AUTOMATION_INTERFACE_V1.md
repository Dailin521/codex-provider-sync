# AI Automation Interface v1

> Status: approved design; not implemented yet.
>
> This document defines the first stable automation contract. Until the
> Automation executable is shipped, the existing GUI and CLI remain the
> supported entry points.

## 1. Purpose

`codex-provider-sync` needs a machine-readable interface that can:

- inspect the same state shown by the desktop GUI
- exercise input linkage and validation without private-field reflection
- run the real `sync`, `switch`, `restore`, and backup-pruning workflows
- render the real Windows UI for AI-assisted layout review
- remain safe when an agent is pointed at a real Codex home

The first release is a Windows companion executable:

```text
codex-provider-sync-win-x64.zip
├─ CodexProviderSync.exe
├─ CodexProviderSync.Automation.exe
└─ automation-protocol-v1.schema.json
```

The standalone GUI executable remains available. The Automation executable is
local-only: it uses UTF-8 JSON Lines over standard input and standard output,
does not listen on a network port, and does not install a background service.

## 2. Architecture

The dependency direction is fixed:

```text
CodexProviderSync.Core
          ↓
CodexProviderSync.Application
          ↓
WinForms / Automation / macOS
```

### Core

`CodexProviderSync.Core` continues to own filesystem, TOML, rollout, SQLite,
backup, restore, and provider-sync behavior. The automation protocol must not
reimplement these operations.

### Shared application layer

`CodexProviderSync.Application` is a cross-platform project. It owns:

- application state and state transitions
- field linkage and validation
- action availability
- settings semantics
- construction of Core requests
- operation planning and plan lifetime
- structured progress, warning, and result mapping

Its public entry point is `IAppController`. The intended operations are:

```text
InitializeAsync
GetState
ApplyPatchAsync
RefreshStatusAsync
AddManualProviderAsync
RemoveManualProviderAsync
CheckUpdatesAsync
CreatePlanAsync
ExecutePlanAsync
```

Concrete types should be immutable records where practical. UI frameworks must
not be referenced by this project.

### Windows frontend

The Windows implementation is split into:

- `CodexProviderSync.App.WinForms`: MainForm, state binding, platform dialogs,
  and the UI probe host
- `CodexProviderSync.App`: the thin GUI entry point and single-instance guard
- `CodexProviderSync.Automation`: the JSONL host

The Automation process may create a hidden WinForms probe host, but it must not
trigger the normal GUI single-instance guard.

### macOS frontend

The macOS GUI will migrate to the same application layer in a later PR. It must
not receive a second implementation of controller rules. Real macOS visual
smoke testing remains a separate release check.

## 3. Application state

`AppState` is the only business-state source for all frontends. It contains at
least:

- a monotonically increasing `revision`
- resolved Codex Home, SQLite Home, and SQLite database path
- detected and manually added providers
- current and selected provider
- root-model behavior
- backup retention and restore selection
- field-level validation
- action availability and disabled reasons
- busy state and operation progress
- status snapshot, backups, warnings, and latest result
- update-check state

Stable field identifiers:

| Field ID | Type | Meaning |
| --- | --- | --- |
| `storage.codexHome` | string | Selected Codex Home |
| `storage.sqliteHomeOverride` | string/null | Explicit SQLite Home override |
| `provider.selectedId` | string | Target provider |
| `provider.manualId` | string | Manual-provider input |
| `execution.updateConfig` | boolean | Whether to switch root `model_provider` |
| `execution.modelMode` | enum | `followProvider`, `keepCurrent`, or `custom` |
| `execution.customModel` | string/null | Root model used by custom mode |
| `backup.retentionCount` | integer | Managed backups to keep |
| `restore.backupDirectory` | string/null | Backup selected for restore |
| `restore.includeConfig` | boolean | Restore `config.toml` |
| `restore.includeDatabase` | boolean | Restore SQLite data |
| `restore.includeSessions` | boolean | Restore rollout/session data |

`state.patch` applies one or more field updates atomically. The returned state
must already contain all resulting linkage, validation, and enabled-state
changes. A rejected batch does not partially modify state.

In an Automation session, patches are in-memory by default. Persisting this
tool's own settings requires `settings.save`. Modifying Codex data always uses
the operation plan/execute flow.

## 4. JSONL protocol

### Process contract

Read-only and planning mode:

```text
CodexProviderSync.Automation.exe --jsonl
```

Write-enabled mode:

```text
CodexProviderSync.Automation.exe --jsonl --allow-write
```

Rules:

- stdin and stdout are UTF-8 without a byte-order mark
- each non-empty input line is exactly one JSON request
- each response is emitted as one JSON line
- stdout contains protocol messages only
- diagnostics and human-readable logs go to stderr
- request failures do not terminate the process unless the stream or process
  is no longer usable
- every response and progress event carries the originating request ID

Request:

```json
{"protocolVersion":"1.0","id":"req-1","method":"state.get","params":{}}
```

Successful response:

```json
{"protocolVersion":"1.0","id":"req-1","ok":true,"result":{}}
```

Error response:

```json
{
  "protocolVersion":"1.0",
  "id":"req-1",
  "ok":false,
  "error":{
    "code":"VALIDATION_FAILED",
    "message":"The selected provider is invalid.",
    "field":"provider.selectedId",
    "retryable":false,
    "details":{}
  }
}
```

Progress event:

```json
{
  "protocolVersion":"1.0",
  "id":"req-1",
  "event":"progress",
  "data":{"operationId":"op-1","stage":"backup","percent":25}
}
```

Machine-readable codes and enum values are locale-independent. Human-readable
messages may be localized and must not be parsed as protocol state.

### Stable v1 methods

| Method | Writes data | Purpose |
| --- | --- | --- |
| `system.describe` | no | Describe protocol version, capabilities, fields, actions, and enum values |
| `session.initialize` | no | Initialize with detected or explicit storage paths |
| `state.get` | no | Return the complete current `AppState` |
| `state.patch` | memory only | Apply a batch of field changes |
| `settings.save` | tool settings | Persist the tool's settings; requires `--allow-write` |
| `provider.addManual` | memory only | Add a provider to the current state |
| `provider.removeManual` | memory only | Remove a manual provider from current state |
| `status.refresh` | no | Re-read config, rollouts, SQLite, and distribution |
| `backups.list` | no | List managed and selectable backups |
| `updates.check` | network read | Check for an available release without installing it |
| `operation.plan` | no | Validate and describe a real operation |
| `operation.execute` | yes | Execute one valid plan |
| `ui.inspect` | no | Return the actual WinForms control tree |
| `ui.capture` | probe artifact | Render an internal PNG and return its path plus UI metadata |

The v1 interface deliberately excludes presentation-only desktop actions:

- showing a native folder picker
- opening Explorer, the log directory, or a backup directory
- focusing or moving desktop windows
- downloading or installing an update

Automation callers set paths directly and receive structured paths in results.

## 5. Planning and write safety

`operation.plan` accepts these operation kinds:

- `sync`
- `switch`
- `restore`
- `pruneBackups`

For GUI parity, `execution.updateConfig=false` leads to sync behavior and
`execution.updateConfig=true` leads to switch behavior. An explicit Automation
operation kind must be consistent with the current state or validation fails.

A successful plan contains:

- an unpredictable `planId`
- operation kind and normalized input
- current and target provider
- root-model behavior
- absolute Codex Home, config, SQLite, and backup paths
- expected affected data and backup retention
- validation findings and warnings
- confirmation requirements for an interactive frontend
- creation and expiration timestamps

Plan IDs are:

- held in memory only
- scoped to one process and Automation session
- valid for five minutes
- single-use, including when execution begins but later fails
- bound to the normalized request, `AppState.revision`, resolved targets, and
  relevant observed storage state

Any input, target, or relevant external-state change makes the plan stale.
Execution repeats provider, target, SQLite relocation, and lock checks before
calling Core.

`operation.execute` succeeds only when:

1. the process was started with `--allow-write`
2. the plan exists, is unused, unexpired, and not stale
3. all execution-time validation passes

The interactive GUI does not use a command-line write flag, but it uses the
same plan object to render confirmation and the same execute path after the
user confirms.

Required stable error codes include:

- `INVALID_REQUEST`
- `METHOD_NOT_FOUND`
- `VALIDATION_FAILED`
- `WRITE_NOT_ENABLED`
- `PLAN_NOT_FOUND`
- `PLAN_EXPIRED`
- `PLAN_STALE`
- `PLAN_ALREADY_USED`
- `PROVIDER_NOT_FOUND`
- `TARGET_LOCKED`
- `INTERNAL_ERROR`

Skipped locked rollout files are a successful result with `status: "partial"`,
a skipped-file collection, and structured warnings. They are not reported as
complete success and are not converted into a generic protocol error.

Every successful write result includes:

- operation status: `complete` or `partial`
- backup path when a backup was created
- affected and skipped item counts
- structured warnings
- refreshed final application/status state

Automation does not log in, edit `auth.json`, or handle credentials.

## 6. Target-path policy

`session.initialize` may detect the current user's default Codex Home. An
explicit Codex Home and SQLite Home override are also supported.

Before a real operation, the plan always reports and binds:

- absolute Codex Home
- absolute `config.toml` path
- resolved SQLite Home and database path
- source and destination backup paths
- whether SQLite Home came from override, config, environment, or default

The existing storage-resolution and WSL safety rules remain authoritative.
Automation must not add a shortcut that bypasses Core path validation.

Automated tests always use temporary fixture homes. They must never plan or
execute against the test runner's real `~/.codex`.

## 7. Internal Windows UI probe

`ui.inspect` creates the actual WinForms view bound to the current `AppState`
and returns:

- stable control ID
- WinForms type and semantic role
- text and current value
- checked/selected state
- enabled, visible, and focusable state
- parent control ID
- bounds and preferred size
- tab order
- accessible name and description

`ui.capture` renders the same view to PNG. It accepts optional output
directory, viewport, DPI, language, and artifact name parameters.

Default output:

```text
%TEMP%\codex-provider-sync\ui-probe\<session>\
```

The result contains:

- absolute PNG path
- width and height
- SHA-256
- matching control-tree snapshot
- state revision used for rendering

The image is returned by path instead of Base64 so an AI agent can load it as
an image without inflating every JSONL message.

The probe runs with platform dialogs, update installation, and settings writes
disabled. It is a deterministic layout and interaction probe, not a substitute
for the final packaged-app Windows smoke test.

## 8. Compatibility policy

This protocol is public and stable beginning with v1:

- new optional fields and new advertised capabilities may be added to v1
- existing v1 fields, methods, enum values, and error meanings are not renamed
  or removed
- callers ignore unknown optional fields
- breaking changes require a new major protocol version
- `system.describe` is the feature-negotiation source of truth
- protocol fixtures and the published JSON Schema are release-gated

## 9. Delivery phases

1. Add the shared Application state/controller and protocol models.
2. Migrate WinForms to the controller without visible behavior changes.
3. Ship the Automation JSONL host and plan/execute protection.
4. Ship the WinForms UI probe, CI artifacts, and two-EXE Windows ZIP.
5. Migrate macOS to the shared application layer in a later PR.

Automation v1 is considered delivered after phases 1-4. macOS migration does
not block the Windows v1 release.
