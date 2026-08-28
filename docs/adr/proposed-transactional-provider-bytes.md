# Proposed: transactional provider byte updates and explicit fast scope

- Status: Proposed, not maintainer-approved or released
- Date: 2026-08-28
- Base: upstream main c7ff852 (v0.5.0)

## Context

PR #51 saved whole-rollout writes for equal-length IDs. PR #71 removed that
path while adding durable per-target recovery. Restore the optimization inside
the same journal, not around it. A separate fast scope addresses body reading.

## Proposal

- Keep current default synchronization semantics and full-rewrite fallback.
- Add a manifest-bound provider-byte strategy, durable before `applying`.
- Revalidate identity/header/bytes, write through the same handle, flush and
  verify; undo through that handle/strategy without rename or tail truncation.
- Preserve POSIX write mtime to avoid racing appenders; Windows can preserve
  original mtime under its exclusive handle. This is an explicit compatibility
  change, not an assertion that all default observable behavior is identical.
- Metadata and manifest v3 prevent old readers from silently applying the
  wrong restore strategy. Old v1/v2 backups retain their existing semantics.
- Opt-in `--fast` narrows diagnostics/model scope, never durability or recovery.
  Unsupported headers fail preflight rather than silently copying the body.
- No credentials, account tooling, process management, new database or cache.

## Acceptance questions

The mtime policy can affect History's filesystem-time fallback, so it needs
explicit acceptance. Do not claim safe concurrent append and unconditional old
mtime restoration simultaneously. Do not replace this policy with a timing
heuristic and call it exclusive access.

Current .NET rejects v3 safely but cannot recover it. Maintainer approval is
required for this migration boundary, or a compatible recovery reader must be
added before release. Windows Node support is not .NET application parity.

Fast scope is new behavior: discuss it separately from restoring #51. If the
unmerged #90 becomes main, integrate scope into its Plan/Revision/Restore v2
contracts before claiming support. No new public method, lock protocol or
second transaction system is necessary on the current base.

## Evidence

See [implementation and tests](../TRANSACTIONAL_IN_PLACE_PILOT.md). Native,
simulated and unrun coverage must be reported separately. Process-exit tests
do not establish arbitrary power-loss or non-cooperating-writer guarantees.
