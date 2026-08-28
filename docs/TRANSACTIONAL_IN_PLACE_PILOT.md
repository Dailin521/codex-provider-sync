# Transactional provider byte updates (implementation candidate)

This branch restores the optimization introduced by cccat6 in PR #51
(`7231881`, `cdcde35`, `84a60d3`). PR #71's transaction refactor removed the
production in-place path; v0.5.0 still counts `APPLIED_IN_PLACE` but does not
produce it. This candidate does not manage authentication, profiles or processes.
It is based on upstream main `c7ff852`, not the unmerged V1 migration (#90).

## Implemented and tested

- Node POSIX and the existing Windows exclusive PowerShell worker support the
  byte strategy. The small C# helper is compiled by that worker for native file
  identity and handle operations; it is not a second sync service or SDK dependency.
  The published .NET application is unchanged and rejects metadata v3 before
  writing any restore target. Cross-runtime recovery parity is NOT claimed.
- A non-empty, equal-length ASCII provider ID with one unescaped, unambiguous
  `session_meta.payload.model_provider` field can be replaced in place. A
  `turn_context.model` rewrite or an ineligible header uses the existing path.
- The plan captures device/inode, size, mtime, the original header, byte offset,
  and both byte sequences. An immutable managed backup contains this descriptor
  before the coordinator durably appends `applying` and starts the write.
- Apply revalidates the path, handle identity, snapshot, header and bytes. A
  stale precondition is skipped, never used as a reason for a full rewrite.
  Hardlinked targets are ineligible. Post-write checks include the complete
  header, identity and minimum file size, not just the replacement bytes.
- Short writes loop; write/fsync/read-back failure attempts byte restoration
  through the same handle. There is no post-mutation fallback to rename.
- `applying` and `applied` targets recover from the immutable manifest. A torn
  journal conservatively selects all manifest candidates. Recovery failure
  leaves the existing `recoveryRequired` state and evidence intact.
- Both backup metadata and session manifest use version 3 when any entry is
  in-place, so old readers reject before restoring config or SQLite. Version
  1/2 backups still use their old recovery semantics. Non-in-place backups stay
  version 2. Fast-mode backups always use version 3, including no-op rollouts.
  New backups require this version or a compatible restore implementation.

## Recovery and writer contract

Recovery verifies device/inode **and** all surrounding original header bytes
and requires a file at least as large as the original. Identity alone is not a
permanent guarantee against inode reuse. It accepts original bytes, replacement
bytes, or a single contiguous run of replacement bytes among original bytes
at differing positions (`old* new* old*`). That last case covers a sequential
short write and an interrupted sequential rollback. Unknown bytes, disjoint
tears, replaced paths, or truncation fail closed. This is conditional evidence
under the append-only writer model, not proof against a third party rewriting
the header to an indistinguishable value.

The supported writer leaves existing bytes alone and appends after the guarded
metadata operation. Pre-apply growth is skipped. Later appends remain visible
through the existing fd and survive rollback without truncation. POSIX in-place
apply/restore leaves the actual write mtime: there is no race-free stat/utimes
sequence against an uncooperative appender. Windows restores mtime while holding
the exclusive handle; no-op recovery does not alter it. Thread `updated_at`
is never changed. History views using filesystem mtime may reorder; this
deliberate safety/compatibility tradeoff requires maintainer acceptance.
No POSIX cooperative lock can force Codex to participate: this
does not promise atomic visibility to concurrent readers, or protection from
non-cooperating writers replacing/truncating/editing the header during the
small check/write window. Full replacement paths still have the active-fd risk
identified in PR #71; this pilot removes that risk only for eligible writes.

## Fast scope and reading cost

`sync --fast` and `switch <id> --fast` enumerate both rollout roots but read
only metadata headers (bounded to 1 MiB per header) plus file attributes.
Every changed rollout must qualify for in-place replacement. An ineligible or
invalid header fails preflight, before backup or config/SQLite mutation; there
is no implicit full rewrite. Busy/changed targets retain the existing partial
outcome semantics and must not be described as completely aligned.

The scope preserves root and historical models, leaves `has_user_event`
unchanged, and reports encrypted-content/model/user-event checks as unchecked.
Provider and header-derived cwd/workspace repair retain the existing transaction.
`--model` conflicts with `--fast`; `--keep-root-model` is redundant, allowed.
The managed manifest records `scanScope: metadata`; restore does not invent
historical model snapshots or scan/copy message bodies. Existing config and
SQLite backup/restore behavior is retained, so cost is not strictly header-only.

Default scans keep their diagnostics but compute encryption presence, positive
user-event evidence and model snapshots in one streaming pass. No persistent
cache is added. The default full rewrite remains for noneligible operations.

## Validation and follow-up

Validated on 2026-08-28:

- Linux Node 24.16.0: full suite, 280 passed / 6 platform skips / 0 failed.
- Linux Node 16.20.2 with the existing optional better-sqlite3 8.7.0 driver:
  all 19 test files passed (the older runner reports file-level totals).
- Existing Windows PowerShell: real production worker protocol, busy response,
  native file identity, in-place apply/restore, timestamp retention, short-write
  exception, Flush failure, failed immediate undo followed by recovery,
  idempotence, appended-tail preservation and unknown-byte rejection passed.
- Web production build, package dry-run (including the native helper source),
  and `git diff --check` passed.
- Not run: full Node/SQLite suite on Windows, macOS native tests, real WSL UNC
  tests, and cross-runtime v3 restore (the old .NET reader rejects v3).

`node scripts/benchmark-provider-io.mjs 32` uses disposable data. On ext4 with
warm page cache, one ~32 MiB fixture produced these process-level measurements:

| Mode | Logical reads | Kernel-accounted writes | Elapsed |
| --- | ---: | ---: | ---: |
| Full, equal IDs | 34,044,759 B | 45,056 B | 127 ms |
| Fast, equal IDs | 405,780 B | 45,056 B | 55 ms |
| Full, unequal IDs | 67,689,493 B | 33,681,408 B | 247 ms |

Equal-ID cases retained inode and tail hash. Numbers include managed backup
and journal overhead, not just provider bytes. They exclude SSD-internal write
amplification and are not a prediction for cold storage or a large SQLite DB.

`test/in-place-transaction.test.js` covers eligibility, short/zero writes,
fsync failure, immediate restoration failure, immutable manifests, A/B failure,
crashes before `applied` and before commit, torn journals, idempotence,
conflicts, active fds and appends. A 32 MiB disposable fixture records only
8 rollout bytes written and verifies the unchanged tail hash and inode.
`test/fast-sync.test.js` guards against rollout body streams throughout switch
and restore, tests preflight failures, CLI parsing, models, SQLite and rollback.
`test/windows-provider-bytes.ps1` also tests the native helper using the existing
PowerShell runtime without a Node installation. No real history or API calls
are used. This work does not authorize production deployment.

Before a formal PR: run the full native Windows Node suite; agree the POSIX
mtime policy, metadata v3 transition and fast-scope semantics with the maintainer.
See the [proposed ADR](adr/proposed-transactional-provider-bytes.md). No public
PR/comment/release is authorized by this development work.

If #90 lands first, rebase through its shared Core, plan ledger, dual locks and
Restore v2. Its full-content revisions and final status refresh must become
scope-aware for fast operations, not be bypassed. Bind scope, original header,
file identity, target bytes and config/DB revisions in the plan; retain full
hash verification for full-scope operations (streaming rather than readFile).
Restore-v2 pre-snapshots, target digests and compensation must use the same byte
strategy. Those V1-specific changes are NOT implemented on this main-based branch.

References: [#51](https://github.com/Dailin521/codex-provider-sync/pull/51),
[#71](https://github.com/Dailin521/codex-provider-sync/pull/71),
[#69](https://github.com/Dailin521/codex-provider-sync/issues/69),
[active-fd finding](https://github.com/Dailin521/codex-provider-sync/pull/71#discussion_r3711178450),
[Codex #38149](https://github.com/openai/codex/issues/38149).

For frequent switching, we recommend equal-length ASCII provider IDs, preferably six
characters because `openai` has six (for example `provider_a` as `prov_a`).
Different lengths require whole-file rewriting; large histories can multiply
disk writes and elapsed time. The original user's rollout collection was
approximately 53 GiB. In-place updates do not convert `encrypted_content` or
make histories portable between providers/accounts.
