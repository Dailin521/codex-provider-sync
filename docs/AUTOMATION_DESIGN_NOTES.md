# Automation Design Notes

> Status: exploratory and non-normative.
>
> These notes describe a possible 0.x direction. They do not define a shipped
> API, protocol, executable, schema, or compatibility commitment.

## Motivation

The Windows GUI currently combines presentation, application state,
validation, request construction, and Core service orchestration in
`MainForm`. Before adding an automation surface, the project needs a
UI-independent seam that is exercised by the real GUI.

## Current Direction

- `CodexProviderSync.Core` remains the only owner of config, rollout, SQLite,
  backup, restore, storage resolution, locking, and WSL safety behavior.
- A UI-independent Application/controller layer owns migrated application
  state, validation, action availability, and Core request construction.
- WinForms remains responsible for rendering, native dialogs, confirmation,
  accessibility, and other Windows-specific presentation.
- Existing frontends migrate incrementally. Unmigrated behavior may remain in
  the frontend until its own tested vertical slice is moved.

## Phase 1

Phase 1 introduces the Application/controller boundary and migrates one
complete Windows GUI behavior slice through it.

Requirements:

- preserve observable GUI behavior
- reuse Core rather than reimplementing data operations
- keep the Application project free of WinForms dependencies
- add controller tests for migrated state, validation, and request mapping
- keep existing Core, CLI, GUI, and packaging tests green

This phase does not ship an Automation executable.

## Possible Later Experiment

After the controller has been exercised by the real GUI, a local automation
host may be prototyped as an experimental 0.x interface. Its transport,
messages, versioning, write opt-in, planning model, and packaging remain open
questions and may change incompatibly during 0.x development.

## Safety Invariants

Any future automation path must:

- use the same Core storage-resolution and WSL safety rules
- never read, write, expose, or manage `auth.json` or credentials
- identify write targets clearly and retain the existing backup-first behavior
- use temporary fixture homes in automated tests, never the runner's real
  Codex home
- report partial results such as locked rollout files without presenting them
  as complete success

## Deliberately Undecided

The project currently makes no commitment to:

- JSON Lines or any other transport
- public method, field, enum, or error-code names
- a `CodexProviderSync.Automation.exe` binary or published JSON schema
- stable v1 compatibility
- plan IDs, expiration rules, or a particular plan/execute protocol
- stable control IDs, `ui.inspect`, `ui.capture`, or a UI probe
- bundling multiple executables in the Windows release

A UI probe, if needed, will be designed and reviewed separately from the
Application/controller extraction.

## Stabilization Criteria

A stable public automation contract should be proposed only after an
experimental 0.x host has been used against the shared controller, its safety
model has been tested, and the required compatibility surface is understood.
