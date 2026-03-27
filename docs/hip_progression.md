# HIP Progression

## What the pipeline does

The HIP progression pipeline compares the official Hiero Improvement Proposal catalog against GitHub issues and pull requests in SDK repositories. It extracts HIP candidates, collects evidence, engineers explainable features, infers artifact-level status, aggregates to repo-level status, and exports both reviewer-friendly markdown and machine-readable CSV tables.

## Status model

- `not_started`: no repo evidence for the official HIP, or only mention, planning, or backlog evidence with no implementation proof
- `unknown`: weak or ambiguous evidence exists, but the HIP association or progress state is not proven
- `in_progress`: partial implementation, tests-only work, or merged implementation without enough completion corroboration
- `completed`: merged implementation with code, tests, and corroborating completion evidence
- `conflicting`: strong positive and negative evidence coexist without a clear winner

## Evidence tiers

- Tier 1: direct HIP mention in titles, bodies, comments, commit messages, or linked-artifact propagation
- Tier 2: implementation-oriented code changes or HIP-shaped code paths
- Tier 3: tests that back the implementation
- Tier 4: merge state, linked issue closure, changelog updates, release-note updates, or docs updates
- Tier 5: contradictory or negative evidence such as blocked, reverted, follow-up-only, prep, cleanup-only, or docs-only work

High confidence requires strong agreement across positive tiers and low contradiction. The default thresholds are centralized in `src/hiero_analytics/config/hip_progression.py`.

## Run analysis

Single repository:

```bash
uv run python -m hiero_analytics.run_hip_progression_for_repo --owner hiero-ledger --repo hiero-sdk-js
```

Default SDK batch:

```bash
uv run python -m hiero_analytics.run_hip_progression_batch
```

Batch plus benchmark evaluation:

```bash
uv run python -m hiero_analytics.run_hip_progression_batch --evaluate
```

Lean reviewer bundle with the latest 10 HIPs in the checklist:

```bash
uv run python -m hiero_analytics.run_hip_progression_batch --evaluate --latest-hip-limit 10 --export-profile review --checklist-limit 10
```

Full audit bundle when you need every intermediate table:

```bash
uv run python -m hiero_analytics.run_hip_progression_batch --evaluate --latest-hip-limit 20 --export-profile full
```

## Review outputs

Per-repo outputs land in `outputs/hip_progression/<owner>_<repo>/`.

Batch outputs land in `outputs/hip_progression/batch/`.

The fastest files to review are:

- `hip_repo_summary.md`
- `hip_checklist.md` limited to the newest 10 HIPs per repo by default
- `hip_evidence_detail.md`
- `benchmark_report.md`

Default `review` exports keep the folder small:

- `hip_repo_summary.csv`
- `hip_repo_summary.md`
- `hip_checklist.md`
- `hip_high_confidence_completion.csv`
- `hip_high_confidence_completion.md`
- `hip_evidence_detail.csv`
- `hip_evidence_detail.md`
- `recent_hip_status_counts.csv` and `recent_hip_status_counts.png` when multiple repos are in scope
- `sdk_completion_counts.csv` and `sdk_completion_counts.png` when multiple repos are in scope
- `manual_accuracy_review.csv`
- `manual_accuracy_report.md`
- `benchmark_report.md` and `benchmark_report.json` when evaluation is enabled

Use `--export-profile full` when you also want manual-review sheets, artifact feature tables, artifact-level evidence summaries, and other audit-oriented intermediates.

## Scope control

The pipeline no longer defaults to the full HIP catalog in reviewer-facing runs.

- `--latest-hip-limit 10` keeps the newest 10 official HIPs in scope
- `--latest-hip-limit 20` widens the scope to the newest 20 HIPs
- repo summaries and checklists are emitted in descending HIP-number order

This keeps the analysis focused on recent governance work and avoids overcrowding the output with old HIPs that may not be relevant to the SDK review.

## Feedback cycle

The primary manual-review loop now lives in two files:

- `manual_accuracy_review.csv`: editable review queue with PR/issue links, linked-artifact URLs, `human_observation`, `is_prediction_correct`, `is_overcalled_match`, and `is_missed_match`
- `manual_accuracy_report.md`: one-page summary that separates pull requests, issues, and repo-level calls while surfacing current reviewed accuracy

Artifacts with no HIP prediction are still included in the manual review queue with `prediction_present = false`, so reviewers can explicitly mark missed HIP matches instead of only reviewing predicted rows.

The benchmark split is chronological and conservative:

- issues are split 80/20 by time
- pull requests are split 80/20 by time
- the newest 20% stays untouched for later validation

## Evidence refinements

Recent scoring refinements focus on reviewer trust rather than aggressive recall:

- direct `HIP-1234` references in pull requests carry more weight than issue-only mentions
- PR language such as `feat`, `adds`, and `introduces` adds confidence when it appears alongside a valid HIP reference
- stronger implementation shapes get extra credit when source files and tests both change, especially when new source and test files are added
- substantial source deltas can nudge confidence upward, but never bypass the conservative completion rules
- negative contexts such as `unblock`, `follow-up`, `prep`, `cleanup only`, and `reverted` reduce confidence
- maintainer-linked PR and issue chains provide stronger corroboration than isolated discussion artifacts

## Benchmark evaluation

The checked-in benchmark lives under `tests/fixtures/hip_progression_benchmark/`.

It includes:

- a frozen catalog snapshot
- curated artifact-level expectations
- curated repo-level expectations
- rationale for each labeled case

The benchmark reports:

- coverage
- accuracy
- macro precision
- macro recall
- per-status confusion matrix
- overcall rate
- undercall rate

## Known limitations

- The pipeline is deterministic and conservative, not semantic oracle-grade NLP
- Cross-repo release-note evidence is limited to what appears in fetched artifacts and changed files
- A catalog-wide `not_started` row does not imply a HIP is applicable to every SDK; it only means the pipeline found no repo evidence for that official HIP
