# Proposed: transactional provider byte updates and explicit fast scope

- Status: Proposed, not maintainer-approved or released
- Date: 2026-08-28
- Base: upstream main c7ff852 (v0.5.0)

## Context

[PR #51](https://github.com/Dailin521/codex-provider-sync/pull/51), by cccat6
(7231881, cdcde35, 84a60d3), avoided whole-rollout writes for equal-length IDs.
[PR #71](https://github.com/Dailin521/codex-provider-sync/pull/71) removed that
path while adding durable per-target recovery for #69. Restore the optimization
inside that journal, not around it. A separate fast scope addresses body reads.

## Proposal

- Keep current default synchronization semantics and full-rewrite fallback.
- Add a manifest-bound provider-byte strategy, durable before `applying`.
- Revalidate identity/header/bytes, write through the same handle, flush and
  verify; undo through that handle/strategy without rename or tail truncation.
- Preserve original mtime for unchanged-size files. If an append races POSIX
  stat/utimes, reassert only the guarded bytes for a fresh kernel write time;
  never backdate again. Recovery also repairs this interrupted timestamp step.
  Windows preserves timestamps under its exclusive handle. `updated_at` stays
  unchanged; History's existing ordering and duplicate selection remain intact.
- Keep official v2 metadata/manifests and v1/v2 restore support. `mutation` is
  optional: all standard original header/model/time/DB fields remain intact.
  Older readers may ignore it and use their existing restore path; updated
  readers use it for guarded byte-level undo. No private format version is added.
- Opt-in `--fast` narrows diagnostics/model scope, never durability or recovery.
  Unsupported headers fail preflight rather than silently copying the body.
- Full mode fuses encryption, user-event and model checks in one body pass.
- Fast mode caps headers at 1 MiB, preserves root/history models and user-event
  flags, retains provider/cwd/workspace repair, and reports unchecked diagnostics.
  `--model` conflicts with `--fast`. Busy/changed targets retain partial outcomes.
- No credentials, account tooling, process management, new database or cache.

## Recovery boundaries

Only a unique unescaped ASCII provider value, equal encoded length and no
model rewrite qualifies. Validate file identity, size, header and target bytes
before writing; a stale precondition is skipped, never a full-rewrite fallback.
Normal undo and crash recovery use the same byte strategy. Old/new bytes or
one contiguous run of new bytes among old bytes are recoverable under the
append-only writer model. Unknown/disjoint edits, truncation and replaced
identities fail closed. Hardlinked targets are ineligible. Inode identity is
not a permanent proof against reuse; writes are not atomic to concurrent readers.

Cross-store restore preflights byte conflicts before changing config/SQLite;
rollout-only compensation attempts all targets even when one conflicts. Applying
and applied targets recover from the immutable manifest; damaged journals use
all candidates. Failure retains recoveryRequired and evidence. Full rewrite and
old-backup paths retain their existing active-writer limitations.

## Acceptance questions

Format interoperability does not retrofit in-place recovery into older tools:
their original full-rewrite and active-writer restrictions still apply. Verify
both directions with unmodified upstream Node/.NET readers and producers, as
well as recovery from applying-only partial writes. This does not claim that
old binaries implement the new inode-preserving strategy.

Fast scope is new behavior: discuss it separately from restoring #51. If the
unmerged #90 becomes main, integrate scope into its Plan/Revision/Restore v2
contracts before claiming support. No new public method, lock protocol or
second transaction system is necessary on the current base.

## Evidence

Focused tests: `in-place-transaction.test.js`, `fast-sync.test.js`, and the
Windows worker/native-helper tests. They cover short writes, flush failure,
crashes, torn journals, rollback conflicts, append/mtime races, inode and tail
preservation, fast preflight and default behavior. `scripts/benchmark-provider-io.mjs`
measures disposable fixtures; report logical I/O separately from kernel writes.
Native, simulated and unrun coverage belong in the PR validation report.
Process-exit tests do not prove arbitrary power-loss or hostile-writer safety.
