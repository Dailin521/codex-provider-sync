# GUI E2E harness contract tests

These tests validate only the harness's isolation, manifest-gating, evidence-redaction,
and scenario-asset contracts. They do not launch WinForms and must never be reported as
a real GUI pass.

The only release GUI result is the exit code and evidence JSON produced by the headful
runner against the visible published executable. A missing desktop, blocked required
headful scenario, skipped required headful scenario, or uncovered manifest entry is a
failure. The asset explicitly partitions manifest scenarios that require future isolated
fault injection; those capability scenarios are reported as non-gating and are never
claimed as PASS by the headful run.
