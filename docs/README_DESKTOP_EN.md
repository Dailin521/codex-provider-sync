# Codex Provider Sync Desktop

V1 Electron is the primary desktop interface for aligning Provider metadata in local Codex session files and the chat index. Build targets are Windows, macOS, and Linux; available downloads depend on actual Release assets. Local builds do not imply public release, signing, or an enabled update channel.

## First use

1. Install the matching package or extract the complete program folder. On Windows, run `Codex Provider Sync.exe`; do not copy the EXE alone.
2. Check the Provider, Codex data folder, chat index folder, and actual database file in **Overview**.
3. After switching with another tool, choose **Preview sync** to review first, or **Sync now** to execute immediately.
4. Read the result. For partial completion, end the relevant active sessions and retry; to undo an operation, select its backup in **Backups / Restore**.
5. Everyday sync does not require Advanced features.

Wide windows place storage details beside the Sync controls, below the status and Provider distributions. **Switch Provider separately** stays at the bottom. Narrow windows stack the controls.

## Sync and switch Provider

### Sync current Provider

Sync follows the root Provider already in `config.toml`. It aligns session-file and SQLite Provider metadata without editing config, historical models, or unrelated metadata.

- **Preview sync:** review the impact and backup notice, then confirm.
- **Sync now:** the click authorizes execution without a second confirmation; the same internal plan, locking, validation, and backup checks still run.

Custom Providers must already be defined in config. An undefined Provider stops the operation; the app never silently switches to OpenAI. A no-op creates no backup.

### Switch Provider separately

This is not a config-only action: it changes config, then runs the same Provider sync. Choose a target, select **Preview switch**, review, and confirm.

The target follows the current Provider on initial reads/manual refreshes until edited. Switching storage profiles resets the draft. **Recently used successfully** offers still-configured targets from successful switch logs; clicking only fills the target.

| Model handling | Root-model effect |
| --- | --- |
| Use model configured for this Provider | Use `[model_providers.<id>].model`; if absent, keep the current root model. No online lookup |
| Keep current root model | Leave the root `model` unchanged |
| Specify root model | Write the entered name without checking remote availability |

None of these rewrites historical model names. If explicitly needed, use **Advanced features → Advanced adjustments → Unify historical model names**. Different historical models are not a fault, and adjustment does not regenerate replies.

### Performance and timestamps

The Provider business scan only parses first-line metadata. Safe, uniquely located Provider JSON literals with equal UTF-8 byte lengths use in-place writes. Other valid headers use streamed replacement with byte-identical bodies; large files take longer to copy.

Overview's **How to speed up sync** is optional guidance, not a mode switch. Renaming Providers is unnecessary. If you choose to change an ID, keep config and your Provider manager consistent; changing a display name alone does not help.

Backups, flushing and file modification-time restoration remain enabled. Prepare shares bounded header facts within that request; Apply checks files afresh. Message timestamps and thread-index update times are not changed. See [working principles](WORKING_PRINCIPLE_ZH.md).

## Backups and Restore

Set retention only in **Backups / Restore → Backup settings**, default **2**. Sync, Switch, Repair and automatic sync share this app preference, with a separate pool per Codex Home—not a quota per operation.

Saving does not delete backups. Stop automatic sync before changing the setting; it survives restarts. Manual cleanup requires confirmation. **Delete all eligible backups** is separate and does not set automatic retention to zero. Recovery-protected backups may exceed the limit.

Desktop installations, Web browsers and CLI do not share preferences. Failed reads show Retry, not an empty backup list.

Restore only offers content actually captured by the selected backup. Review before confirming. It saves the target's current state first and uses independent recovery checks, rather than ordinary Sync's retryable-partial path.

Relocating the chat index requires an explicit target profile and confirmation, and excludes old config/workspace settings. If session records are also selected, they restore to the source Codex Home; this does not move all chat files to the target profile.

## Chats

- Project groups contain main chats and collapsed child tasks, with independent Load more controls. List and messages scroll separately; narrow windows switch between list and detail.
- Select a row to open it. Right-click for session ID/resume-command copying, information, file location and parent navigation. Project aliases are local display preferences and do not change Codex data.
- Messages load only after explicit selection, showing at most the latest 200 with a truncation notice. Assistant content supports Markdown, code blocks and copying.
- Search defaults to title/ID/project metadata. Select content search and submit with Enter or Search to scan bodies; typing alone does not search.
- Clear filters discards unsubmitted filters without starting content search. A failed detail can be retried without losing expanded groups.
- Names prefer saved title-index entries, database titles and header metadata. Missing titles use type/identifier fallbacks, not the first message body.
- File modification time is not the last chat time.

Copying `codex resume <session-id>` does not launch a terminal. Use the matching Codex Home environment and original project directory. An internal list ID cannot replace a missing original session ID. Cross-Provider encrypted history may still be impossible to continue.

## Operation logs

Select an operation on the left to see details on the right; both panes scroll independently. Narrow windows provide **Back to operations**. Filter by profile, operation and result.

Logs cover Sync, Switch, Repair, Restore, backup cleanup, diagnostics/export, Watch, updates, profile management and Core Runtime failures—not routine status reads or chat browsing/search. They live in application userData, outside Codex Home.

Details include target/impact, actual counts, skipped work, warnings, failed stages, backups, active processing time, wall time including confirmation waits, and the stage timeline. Switch plans show Provider/root-model transitions separately from final outcomes; planned changes alone do not prove they were applied.

New Windows Sync/Switch/automatic-sync records expose copying, flushing, replacement, cleanup and timestamp-restoration timing. Technical totals are nested, not additive. Missing old or unmeasured data is never reconstructed as zero. Reference IDs can be copied for investigation.

Logs load initially and refresh manually; select Refresh if a recent operation is not listed. Dismissed confirmations and interrupted operations are distinguished from success. Archives rotate across at most five 5 MiB files. Diagnostics exports include recent structured logs, never chat bodies.

## Storage profiles

Overview shows the full active Codex Home, SQLite Home and actual database path from its snapshot. Refresh to read again. The SQLite folder and chosen database file can differ under the legacy default-layout fallback.

The default profile follows the startup environment and cannot be edited or deleted. To use another location, create a named profile:

1. Enter a name and choose a Codex data folder with the directory picker.
2. Optionally choose a chat index folder. Otherwise resolution follows config `sqlite_home` → `CODEX_SQLITE_HOME` → `<Codex Home>/sqlite`.
3. Saving selects the profile and updates the relevant status/history/backup queries. Named profiles can be edited, deleted or revealed in the file manager.

Only the default SQLite layout can fall back to `<Codex Home>/state_5.sqlite`. A missing explicitly selected database never silently redirects elsewhere. Writes or active Watch block profile changes; revision changes invalidate old plans. Windows WSL UNC SQLite paths are diagnostic-only; run writes from the CLI inside WSL.

## Advanced features

Everyday sync needs none of these options:

| Action | What changes |
| --- | --- |
| Correct chat project folders | Correct index folders from chat metadata; no files moved |
| Complete user-message markers | Complete existing index markers; no messages added/deleted |
| Organize project directory records | Organize saved workspace settings with a settings backup; no project folders deleted. Applies to the whole profile and includes folder correction |
| Advanced adjustment: unify historical model names | Use config's current root model; preparing fails if no root model is configured |

Run diagnostics manually or select the needed targets, then preview and confirm. Nothing is selected by default. **View repair** only focuses the option; Sync errors never trigger repairs automatically.

Diagnostics and repair/model previews show the current step, checked count and elapsed time. Percentages describe that step, not the whole request. One full facts pass is followed by a separate bounded integrity check. Changed data is reported as not fully verified without repeatedly rescanning. Apply independently checks the data again.

Index field-change totals can count several fields per chat; affected-chat counts are deduplicated. Workspace counts represent setting categories, not folders. Whole-profile repair can show up to 100 read-only chat details but cannot be limited to those chats.

Record renumbering, history display-index rebuilding and encrypted-content modification are not supported.

## Status, results and retry

- **Not verified / Refresh needed:** retry the status read. This is not evidence of an active write; do not delete lock files.
- **Sessions currently in use:** Codex writer-held sessions in this Home, including idle/child/already-aligned chats. Currently available for the Windows writer mechanism; unavailable observations show Unknown, not zero. It is not a count of replies being generated.
- **Sessions to skip this time:** blocked targets for this sync, separate from session activity.
- **Partial:** inspect the failed stage, error and backup, then prepare/retry or manually Restore. Ordinary writes do not automatically roll back everything.
- **Final Provider check incomplete:** manually refresh Overview rather than assuming alignment.
- **Old chat cannot continue:** return to its original Provider/account or start a new chat when encryption is incompatible.

Cancellation is accepted only before mutation; wait once writing starts. Retry links navigate rather than replaying consumed plans. Changed profiles or deleted backups require a new selection.

## Watch, language and window preferences

Data reads are initial or explicit. Relevant writes refresh status/history/backups; logs refresh separately on request. There is no background data polling or automatic Diagnostics scan.

Watch must be enabled explicitly in Settings. Profiles for the same physical Codex Home share one watcher; stopping it stops that shared watcher. Options and run/stop logs belong to the profile that first enabled it; use All profiles in logs when needed.

The UI supports Chinese/English, system/light/dark themes, keyboard navigation, visible focus, reduced motion and 200% scaling. Window size, position and maximized state persist in userData; a disconnected display moves the window back into view.

## Installation, updates and size

**Settings → Updates** checks for higher stable Electron versions, not Legacy .NET EXEs.

- The first launch each local date checks once and only announces an available update. Later launches do not check again, nor does continuous running schedule a new-day check. Manual retry remains available; no automatic download/install.
- Portable/local packaged builds open the official download page. Close the app and extract the complete new version into a new directory.
- Installer builds with an enabled channel can download on request, then restart on confirmation. Writes, Watch and unresolved recovery block installation.
- **Legacy .NET's single-EXE updater cannot directly upgrade to Electron.** Initial migration needs the full new package. An NSIS installer must not masquerade as the old EXE; online cross-version upgrade validation is separate.
- Updating the program does not replace Codex data. Actual release notes determine availability, signing and update support.

Windows budgets are ASAR ≤ 3 MiB, extracted folder ≤ 280 MiB, NSIS ≤ 105 MiB and portable ZIP ≤ 130 MiB. Actual artifacts must be measured; this Electron route does not promise a 50 MiB application.

Complete Chromium notices ship in `LICENSES.chromium.zip`; extract the HTML for offline reading. `THIRD-PARTY-NOTICES.txt` gives instructions and the original checksum. Keep notices and runtime dependencies with the EXE.

## Data and development boundaries

The app never reads, copies or logs `auth.json`, credentials or tokens. Bodies are processed only for explicit History interaction, diagnostics or selected repair within their scopes—not logs, diagnostics bundles or persistent caches. Provider sync does not alter messages, titles, ordering or thread `updated_at`.

Workspace development uses Node 24 and synthetic temporary data only. See [Contributing](../CONTRIBUTING.md). Hidden-window verification:

```powershell
npm ci
npm run architecture:check
npm run desktop:test
$env:CPS_DESKTOP_WINDOW_DISPLAY = "hidden"
npm run desktop:test:e2e
```

[Home](../README.md) · [CLI guide (Chinese)](README_CLI_ZH.md) · [Current Core architecture](architecture/NODE_CORE_ARCHITECTURE_ZH.md)
