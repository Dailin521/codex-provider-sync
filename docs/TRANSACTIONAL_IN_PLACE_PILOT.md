# Transactional in-place provider writes: POSIX pilot

This branch restores the optimization introduced by cccat6 in PR #51
(`7231881`, `cdcde35`, `84a60d3`). PR #71's transaction refactor removed the
production in-place path; v0.5.0 still counts `APPLIED_IN_PLACE` but does not
produce it. This pilot does not change authentication or deploy to other hosts.

## Implemented and tested

- Node POSIX only; Windows retains the existing exclusive replacement worker.
  The .NET implementation is unchanged. Neither is claimed to have parity yet.
- A non-empty, equal-length ASCII provider ID with one unescaped, unambiguous
  `session_meta.payload.model_provider` field can be replaced in place. A
  `turn_context.model` rewrite or an ineligible header uses the existing path.
- The plan captures device/inode, size, mtime, the original header, byte offset,
  and both byte sequences. An immutable managed backup contains this descriptor
  before the coordinator durably appends `applying` and starts the write.
- Apply revalidates the path, handle identity, snapshot, header and bytes. A
  stale precondition is skipped, never used as a reason for a full rewrite.
- Short writes loop; write/fsync/read-back failure attempts byte restoration
  through the same handle. There is no post-mutation fallback to rename.
- `applying` and `applied` targets recover from the immutable manifest. A torn
  journal conservatively selects all manifest candidates. Recovery failure
  leaves the existing `recoveryRequired` state and evidence intact.
- Both backup metadata and session manifest use version 3 when any entry is
  in-place, so old readers reject before restoring config or SQLite. Version
  1/2 backups still use their old recovery semantics. Non-in-place backups stay
  version 2. New backups must be restored using this pilot or a compatible tool.

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
through the existing fd and survive rollback without truncation. Recovery
preserves the newer mtime of an already-appended file rather than applying the
scan-time mtime. No POSIX cooperative lock can force Codex to participate: this
does not promise atomic visibility to concurrent readers, or protection from
non-cooperating writers replacing/truncating/editing the header during the
small check/write window. Full replacement paths still have the active-fd risk
identified in PR #71; this pilot removes that risk only for eligible writes.

## Validation and follow-up

`test/in-place-transaction.test.js` covers eligibility, short/zero writes,
fsync failure, immediate restoration failure, immutable manifests, A/B failure,
crashes before `applied` and before commit, torn journals, idempotence,
conflicts, active fds and appends. A 32 MiB disposable fixture records only
8 rollout bytes written and verifies the unchanged tail hash and inode.
The full Node suite must pass before MOSS installation. No real history is
used by these tests, and no Codex/API calls are needed.

Before a formal PR: add and actually run Windows worker fault/recovery tests;
decide .NET transition support with the maintainer; review recovery portability
and the append-only contract. No public PR/comment/release is authorized yet.

References: [#51](https://github.com/Dailin521/codex-provider-sync/pull/51),
[#71](https://github.com/Dailin521/codex-provider-sync/pull/71),
[#69](https://github.com/Dailin521/codex-provider-sync/issues/69),
[active-fd finding](https://github.com/Dailin521/codex-provider-sync/pull/71#discussion_r3711178450),
[Codex #38149](https://github.com/openai/codex/issues/38149).

For frequent switching, use equal-length ASCII provider IDs, preferably six
characters because `openai` has six (for example `provider_a` as `prov_a`).
Different lengths require whole-file rewriting; large histories can multiply
disk writes and elapsed time. The original user's rollout collection was
approximately 53 GiB. In-place updates do not convert `encrypted_content` or
make histories portable between providers/accounts.
