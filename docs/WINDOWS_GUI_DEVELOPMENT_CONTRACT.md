# Windows GUI Development Contract

> Status: approved development contract; application-layer migration is not
> implemented yet.

This contract applies to future Windows GUI work in
`codex-provider-sync`. Its purpose is to keep the WinForms application,
Automation interface, and shared application behavior testable without relying
on local mouse-and-keyboard automation.

The companion protocol is defined in
[`AI_AUTOMATION_INTERFACE_V1.md`](AI_AUTOMATION_INTERFACE_V1.md).

## 1. Ownership boundaries

### Core owns data operations

`CodexProviderSync.Core` owns:

- config parsing and writing
- provider discovery
- rollout metadata reads and rewrites
- SQLite discovery and updates
- backup, restore, and pruning
- storage safety and WSL path behavior

No WinForms control or event handler may duplicate these implementations.

### Application owns behavior

`CodexProviderSync.Application` owns:

- state and state transitions
- field linkage
- validation
- action availability
- operation planning
- Core request construction
- structured progress, warnings, and results

This is shared code and must not reference WinForms, Cocoa, native dialogs, or
other platform UI types.

### WinForms owns presentation

WinForms owns:

- rendering `AppState`
- converting control events into controller commands
- confirmation and error presentation
- native folder dialogs
- opening folders and external URLs
- Windows-specific accessibility and layout

Event handlers must not call `CodexSyncService`, edit config or SQLite, persist
business state, or independently decide validation rules.

## 2. State flow

The required flow is:

```text
Control event
  → typed controller command or field patch
  → new AppState revision
  → render controls from AppState
```

Rules:

- `AppState` is the only business-state source.
- Rendering must not cause a second user-input command.
- A multi-field update is atomic.
- Busy, enabled, visible, selected, validation, and progress state comes from
  the controller.
- MainForm must not inspect controls later to reconstruct an operation request.
- Async work accepts cancellation and reports progress through application
  state.
- UI-thread marshaling belongs to the WinForms adapter, not the controller.

## 3. Stable identifiers and accessibility

Every interactive or test-relevant control has:

- a stable `Name`
- a stable `AccessibleName`
- an appropriate accessible role/description
- an intentional tab index
- a mapping to a public field ID or action ID

Automation and tests select controls by stable ID, never by visible text,
screen coordinates, or reflection over private MainForm fields.

Identifier rules:

- use semantic names, not layout positions
- do not include localized text
- do not reuse an old ID for different behavior
- v1 IDs are not renamed without a compatibility alias and deprecation period
- container controls that define layout or radio grouping also receive IDs

Expected action IDs include:

- `status.refresh`
- `provider.addManual`
- `provider.removeManual`
- `operation.execute`
- `restore.execute`
- `backups.openDirectory`
- `backups.prune`
- `updates.check`

Presentation-only actions may lack an Automation operation, but they still need
a stable control ID for UI inspection.

## 4. Input and selection rules

- Text boxes, combo boxes, numeric inputs, check boxes, and radio buttons map
  to typed fields in the controller.
- Radio-button semantics use an application enum. Visual grouping must match
  that enum and use an explicit common container.
- Exactly one model mode is selected whenever model-mode controls are enabled.
- Custom model input is enabled only for `custom` mode.
- Disabled controls still render their authoritative value from state.
- Provider display names are presentation only; provider IDs are used in state
  and commands.
- Browse dialogs only obtain a path. The chosen path is sent to the same field
  patch used by Automation.
- Validation appears next to or is associated with the relevant field; a
  generic message alone is insufficient when a field can be identified.

## 5. Layout rules

- Prefer `AutoSize`, `TableLayoutPanel`, and `FlowLayoutPanel`.
- Text-bearing controls must not use an unexplained fixed height.
- Use shared padding, margin, and section-spacing constants.
- Allow labels to wrap when translated or when paths/provider names are long.
- Do not overlap a warning banner, option group, action row, or restore section.
- Minimum window size must preserve access to every action, using scrolling
  where necessary.
- A change that adds or lengthens visible text must add or update layout-probe
  coverage.

Required layout scenarios:

- Simplified Chinese and English
- default and minimum supported window width
- 100%, 125%, 150%, and 200% DPI
- long provider ID
- long custom model
- long Codex and SQLite paths
- warning and validation text visible
- all restore options visible

The stable CI gate is based on control geometry, preferred size, state, and
overlap assertions. PNG screenshots are emitted as review artifacts; broad
pixel-perfect snapshots must not become the only layout test.

## 6. Commands, confirmation, and writes

Real `sync`, `switch`, `restore`, and backup-pruning operations use the shared
plan/execute path.

The GUI:

1. requests a plan from the controller
2. renders the plan summary and confirmation requirements
3. executes that exact plan after confirmation
4. renders the structured final result

MainForm must not recalculate affected paths or warnings while composing its
confirmation message.

Required result presentation:

- current and target provider
- complete versus partial outcome
- backup location
- resolved SQLite database
- skipped locked rollout files
- actionable lock/retry guidance

The application must not edit or manage `auth.json`.

## 7. Platform services

Platform-only behavior is hidden behind narrow interfaces, including:

- settings storage
- native folder selection
- opening folders or URLs
- update checking/presentation
- clock and environment information when needed for deterministic tests

The controller returns confirmation requirements and structured messages; it
does not show dialogs.

Tests use deterministic fakes. They must not open Explorer, a browser, native
dialogs, or the real updater.

## 8. Testing contract

### Application tests

Every field or business behavior change adds controller tests for:

- state transition
- validation
- dependent enabled/selected state
- action availability
- normalized Core request
- success, partial success, and error mapping

Provider and model tests must cover:

- follow provider model
- keep current root model
- custom model
- switching the `config.toml` update checkbox
- provider addition/removal
- missing and invalid providers

### WinForms tests

WinForms tests use the public test host and UI probe. New tests must not use
reflection to read or mutate private MainForm fields.

Required assertions include:

- control IDs are present and unique
- field values match `AppState`
- radio grouping and custom-input enabled state are correct
- tab order and accessibility metadata are present
- text is not clipped by preferred bounds
- visible controls do not overlap
- busy state disables the correct actions

### End-to-end tests

- Use a temporary Codex Home and temporary SQLite fixture.
- Exercise Automation JSONL as a subprocess.
- Verify read-only mode rejects execution.
- Verify plan expiration, reuse, stale state, and target binding.
- Verify sync, switch, restore, prune, backup path, and partial rollout result.
- Never write to the test account's real `~/.codex`.
- Windows GUI/Automation validation must not require WSL.
- Existing WSL-specific storage tests may remain conditional on WSL
  availability.

## 9. Pull request requirements

A Windows GUI PR is complete only when:

- business behavior is represented in the shared controller
- new fields/actions have stable IDs and protocol descriptions
- WinForms contains presentation logic only
- controller, protocol, and UI-probe tests pass
- probe PNG/control-tree artifacts have been reviewed for changed screens
- Chinese and English layouts pass at required widths and scaling
- the self-contained Windows publish succeeds
- the Windows ZIP packaging test confirms both executables when Automation is
  part of the release
- documentation is updated for any public v1-compatible protocol addition

Breaking protocol or field-ID changes require a new major protocol version and
must not be hidden inside a routine GUI PR.

## 10. Migration rule

Until MainForm has fully migrated, touched behavior should move toward the
controller rather than adding another direct service call. Existing
reflection-based tests may be removed only after equivalent controller or
UI-probe coverage exists.

The macOS migration follows the same rule: platform views are adapters over the
shared application state, not an independent implementation.
