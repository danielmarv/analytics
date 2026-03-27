# HIP Progression Benchmark

This fixture set is the checked-in gold-standard benchmark for HIP progression.

- `catalog_snapshot.json`: official HIP catalog rows used during evaluation.
- `artifact_expectations.json`: artifact payloads plus expected artifact-level statuses.
- `repo_expectations.json`: expected repo-level statuses derived from the same artifact set.

The cases intentionally include:

- `completed` with merged code, tests, changelog, and linked issue closure
- `in_progress` with merged implementation that lacks completion corroboration
- `not_started` with planning or backlog evidence only
- `unknown` with bot-only or weak ambiguous evidence
- `conflicting` with positive implementation evidence mixed with revert or follow-up signals
