# AI / Agent Operator Guide

This file is for AI assistants and automation working in this repository. User-facing setup and usage belong in [README.md](README.md) and `docs/`.

## vNext architecture baseline

Read [Current Node Core architecture and invariants](docs/architecture/NODE_CORE_ARCHITECTURE_ZH.md) first for V1 development, then the [vNext Electron + Node architecture](docs/VNEXT_ELECTRON_NODE_ARCHITECTURE_ZH.md) for the overall target and staged migration. The current guide maps real modules, the CLI compatibility exception, and Provider I/O invariants PIO-1 through PIO-6.

The current Core guide maps the V1 implementation and accepted invariants; the larger vNext baseline also contains target-state and historical migration sections. Do not treat a target as shipped evidence or a superseded contract as the current behavior. Do not delete the .NET implementation, break the CLI/Web contract, or perform a big-bang rewrite ahead of those gates.

Phase 0 decisions and frozen behavior are indexed in the [vNext migration execution index](docs/migration/VNEXT_MIGRATION_EXECUTION_INDEX_ZH.md). Before changing an external behavior, read the applicable [Core behavior contract](docs/architecture/contracts/CORE_EXTERNAL_BEHAVIOR_ZH.md), [CLI contract](docs/architecture/contracts/CLI_CONTRACT_ZH.md), [error-code contract](docs/architecture/contracts/ERROR_CODES_ZH.md), and [behavior fixture catalog](docs/migration/BEHAVIOR_FIXTURES_ZH.md), plus the Accepted ADRs in `docs/adr/`.

An external behavior change must update its contract and add or update a fixture/test in the same PR. Target-state text is not proof that a feature exists: keep current behavior, transitional adapter behavior, and the vNext target explicitly separated.

## Goal

Help reuse Codex sessions affected by `model_provider` metadata mismatches by keeping rollout metadata and the resolved SQLite thread index aligned. This does not guarantee cross-provider continuation or compaction. Do not treat this as an authentication or account-management tool.

## Choose the interface

- Prefer the Windows Electron desktop app for users who want a double-click tool and do not want Node.js.
- Prefer the Local Web UI for browser-based or cross-platform use: `codex-provider web`.
- Use the CLI for explicit command requests, automation, diagnostics, WSL paths, or when a GUI is unavailable.
- Use `CodexProviderSync.Automation.exe` only for repository development or explicit Automation work. It ships with the v0.4 Windows Release, but protocol 0.4 is experimental and is not a stable public API or a production GUI control port.

## Safe operating flow

1. Inspect with the UI status action or `codex-provider status`.
2. Confirm the current Provider, effective SQLite Home/database, and rollout/SQLite Provider distributions.
3. Choose `sync`, `switch`, or `restore` from the rules below.
4. Execute once; do not manually edit rollout files or SQLite when the tool can perform the operation.
5. Report the final Provider alignment, backup location, and any skipped or blocked data.

## Choose the operation

- `sync`: the user already changed Provider/account with CCSwitch or another tool, and `config.toml` already contains the intended root `model_provider`.
- `switch <provider-id>`: the user explicitly wants this tool to change the root Provider and synchronize history. Custom providers must already exist in `config.toml`; built-in `openai` is always valid.
- `switch <provider-id> --keep-root-model`: preserve the root `model`.
- `switch <provider-id> --model <name>`: explicitly set the root `model`.
- `restore <backup-dir>`: roll back a mistaken operation. Cross-SQLite-Home restore requires an explicit target, relocation confirmation, and no config restore.
- `prune-backups --keep <n>`: remove only older managed backups.
- `diagnostics`: explicitly requested full read-only scan; never run automatically after status or sync.
- `repair`: explicitly selected non-Provider metadata repair; not part of sync or switch.

`sync` uses the current root `model_provider`, falling back to `openai` when it is absent. Sync and switch create an UndoBackup for actual write targets before mutation and prune only tool-managed backups (default retention: 2). A noop does not create a backup. Direct Sync still prepares and consumes a plan internally; the button click authorizes execution without a second confirmation dialog.

## Storage and path rules

Resolve SQLite Home in this order:

1. explicit CLI, desktop GUI, or Web profile override
2. root `sqlite_home` in `config.toml`
3. `CODEX_SQLITE_HOME`
4. `<Codex Home>/sqlite`

Only the default layout may fall back to legacy `<Codex Home>/state_5.sqlite`. A missing explicit, config, or environment SQLite Home is an error; never silently fall back elsewhere.

On Windows, `\\wsl.localhost\...` and `\\wsl$\...` SQLite Homes are diagnostic-only. Run SQLite operations inside the matching WSL distribution, using Linux paths. Metadata v2 backups record `sqliteHome` and `sqliteDbFiles`; do not bypass relocation checks.

## Safety boundaries

- Never read, copy, log, or modify `auth.json`, credentials, or tokens. Message display/content search requires explicit History interaction in Web/Desktop. Explicit Diagnostics and selected Repair may scan only the data required by their contract; never put bodies into logs, diagnostics exports, list DTOs or persistent caches. Provider Sync does not parse bodies; its unequal-length path may stream-copy unchanged tail bytes.
- Do not change thread `updated_at` or reorder history to force visibility.
- Preserve Home locking, backup-first, native SQLite transactions, WSL and path boundaries. Ordinary Sync/Switch/Repair use retryable partial outcomes after mutation, not cross-file journals or automatic full rollback. Restore alone retains its durable recovery journal and compensation. Do not reintroduce a Node State DB resource lock.
- Rollout/SQLite counts may differ briefly because of an active session; Provider distributions are the alignment signal.
- Metadata synchronization only addresses Provider alignment, not every cause of an unusable session. Another Provider/account may be unable to decrypt existing `encrypted_content`; advise the user to return to the original Provider/account or start a new session if continuation or compact fails.
- Tests and reproduction scripts must use temporary directories or fixtures, never a real user Codex Home.

## Handle common outcomes

- SQLite busy during preflight: stop before mutation. A later transaction can still become busy; after mutation report partial with backup and retry guidance, never falsely claim zero writes or full rollback.
- Skipped locked rollout files: classify as partial success. List the skipped files and recommend another sync after the active session ends.
- Missing custom Provider: define it in `config.toml` or switch with the user's normal Provider tool, then run `sync`.
- Missing explicit SQLite database: keep the explicit path authoritative and report the error; do not use the legacy database.
- WSL UNC diagnostic: run the CLI in WSL with the Windows Codex Home under `/mnt/<drive>/...` and a Linux SQLite Home such as `/home/...`.

## Current implementation and migration direction

- The V1 Web `/api/core` entry and Electron Utility call CoreFacade; CLI and retained legacy Web write routes use `src/public-api.js` / service adapters into the same `packages/core/src/application` use cases. Do not describe every HTTP route as Facade-backed or expand the transitional adapter into another public API.
- Switch and Watch call internal ProviderSync, never CoreFacade. Storage ports do not call business use cases. Main/Renderer must not implement SQL or rollout algorithms.
- Mature storage algorithms remain in root `src/`, statically wired by Core infrastructure. Moving folders is not permission to rewrite high-risk JavaScript or duplicate it.
- V1 Electron is the primary desktop interface in this branch. .NET Windows/macOS remain Legacy fallback implementations with their own supported behavior; preserve their build, do not port new Node business capabilities back into them. Public release and phase completion still require their independent gates.
- Legacy Windows uses Application/controllers; Legacy macOS calls .NET Core directly. Do not claim both use an Application layer.

## Mandatory Provider I/O regression gate

- Follow PIO-1 through PIO-6 in the current Core guide. Sync reads bounded first-line metadata, not full chat streams. No public `sync --provider`, `--fast` or `syncMode`.
- Eligible equal-byte-length JSON Provider literals must use the existing in-place descriptor/handle path. Preserve file identity, size and body hash; character-count equality alone is insufficient. Do not fall back to whole-file replacement after an in-place failure.
- Ineligible but valid headers use streaming tail copy and atomic replacement with byte-identical bodies. Invalid/changed/locked targets must not be forcibly rewritten.
- Run `npm run architecture:check` and `npm test` for Core changes; platform skips and failures must be reported. The first command reuses workspace boundary checks and Provider I/O tests, not a second rule engine.
- Behavior changes require an explicit ADR/contract/fixture update and matching user docs. Do not weaken an invariant test to bless accidental drift. README is not a substitute for the current Core guide.

## Reporting

State the current Provider, whether rollout and SQLite metadata are aligned, the resolved database path, the backup created by a write operation, and whether the result was complete, partial, or blocked. Distinguish automated tests from real-machine validation and list anything not run.
