"""Structured exports for HIP progression analysis outputs."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pandas as pd

from hiero_analytics.analysis.hip_evaluation import (
    ARTIFACT_EVALUATION_COLUMNS,
    BENCHMARK_METRIC_COLUMNS,
    CONFUSION_MATRIX_COLUMNS,
    EVALUATION_SUMMARY_COLUMNS,
    LEGACY_MANUAL_REVIEW_COLUMN_ALIASES,
    MANUAL_ACCURACY_COLUMNS,
    MANUAL_REVIEW_COLUMNS,
    PER_STATUS_METRIC_COLUMNS,
    PREDICTION_REVIEW_BREAKDOWN_COLUMNS,
    REPO_EVALUATION_COLUMNS,
    assign_dataset_splits,
    build_artifact_evaluation_rows,
    build_manual_accuracy_rows,
    build_repo_evaluation_rows,
)
from hiero_analytics.domain.hip_progression_models import (
    HipArtifact,
    HipEvidence,
    HipFeatureVector,
    HipRepoStatus,
    hip_sort_key,
)
from hiero_analytics.plotting.bars import plot_stacked_bar
from hiero_analytics.export.save import (
    dataframe_to_markdown_table,
    save_dataframe,
    save_json,
    save_markdown_table,
)

ExportProfile = Literal["review", "full"]
DEFAULT_CHECKLIST_LATEST_LIMIT = 10
STATUS_CHART_COLORS = {
    "completed": "#2E8B57",
    "in_progress": "#D98E04",
    "unknown": "#C96A00",
    "not_completed": "#9AA0A6",
}


def _format_datetime(value) -> str:
    return value.isoformat() if value is not None else ""


def _flatten_sequence(values: list[str]) -> str:
    return " | ".join(values)


def _artifact_rows(artifacts: list[HipArtifact]) -> list[dict[str, object]]:
    return [
        {
            "repo": artifact.repo,
            "artifact_type": artifact.artifact_type,
            "number": artifact.number,
            "title": artifact.title,
            "body": artifact.body,
            "comments_text": artifact.comments_text,
            "commit_messages_text": artifact.commit_messages_text,
            "comments_count": len(artifact.comments),
            "commit_count": len(artifact.commits),
            "author_login": artifact.author_login,
            "author_association": artifact.author_association,
            "state": artifact.state,
            "merged": artifact.merged,
            "created_at": _format_datetime(artifact.created_at),
            "updated_at": _format_datetime(artifact.updated_at),
            "closed_at": _format_datetime(artifact.closed_at),
            "additions": artifact.additions,
            "deletions": artifact.deletions,
            "labels": _flatten_sequence(artifact.labels),
            "changed_files_count": len(artifact.changed_files),
            "changed_file_paths": _flatten_sequence([changed_file.path for changed_file in artifact.changed_files]),
            "linked_artifact_numbers": _flatten_sequence([str(number) for number in artifact.linked_artifact_numbers]),
            "url": artifact.url,
        }
        for artifact in artifacts
    ]


def _feature_rows(feature_vectors: list[HipFeatureVector]) -> list[dict[str, object]]:
    return [
        {
            "repo": feature_vector.repo,
            "artifact_type": feature_vector.artifact_type,
            "artifact_number": feature_vector.artifact_number,
            "hip_id": feature_vector.hip_id,
            "evidence_count": feature_vector.evidence_count,
            "positive_evidence_count": feature_vector.positive_evidence_count,
            "negative_evidence_count": feature_vector.negative_evidence_count,
            "tier_1_count": feature_vector.tier_1_count,
            "tier_2_count": feature_vector.tier_2_count,
            "tier_3_count": feature_vector.tier_3_count,
            "tier_4_count": feature_vector.tier_4_count,
            "tier_5_count": feature_vector.tier_5_count,
            "direct_mention_count": feature_vector.direct_mention_count,
            "semantic_phrase_count": feature_vector.semantic_phrase_count,
            "propagated_mention_count": feature_vector.propagated_mention_count,
            "bot_mention_count": feature_vector.bot_mention_count,
            "src_files_changed_count": feature_vector.src_files_changed_count,
            "test_files_changed_count": feature_vector.test_files_changed_count,
            "integration_test_files_changed_count": feature_vector.integration_test_files_changed_count,
            "docs_files_changed_count": feature_vector.docs_files_changed_count,
            "changelog_files_changed_count": feature_vector.changelog_files_changed_count,
            "new_src_files_count": feature_vector.new_src_files_count,
            "new_test_files_count": feature_vector.new_test_files_count,
            "merged": feature_vector.merged,
            "linked_artifact_numbers": _flatten_sequence([str(number) for number in feature_vector.linked_artifact_numbers]),
            "has_direct_reference": feature_vector.has_direct_reference,
            "has_code_evidence": feature_vector.has_code_evidence,
            "has_test_evidence": feature_vector.has_test_evidence,
            "has_docs_only_change": feature_vector.has_docs_only_change,
            "has_changelog_update": feature_vector.has_changelog_update,
            "top_evidence_types": _flatten_sequence(feature_vector.top_evidence_types),
        }
        for feature_vector in feature_vectors
    ]


def _artifact_assessment_rows(artifact_assessments: list[HipEvidence]) -> list[dict[str, object]]:
    return [
        {
            "repo": assessment.repo,
            "hip_id": assessment.hip_id,
            "artifact_type": assessment.artifact_type,
            "artifact_number": assessment.artifact_number,
            "status": assessment.status,
            "progress_stage": assessment.progress_stage,
            "confidence_score": assessment.confidence_score,
            "confidence_level": assessment.confidence_level,
            "evidence_count": assessment.evidence_count,
            "positive_evidence_count": assessment.positive_evidence_count,
            "negative_evidence_count": assessment.negative_evidence_count,
            "top_reasons": _flatten_sequence(assessment.top_reasons),
            "uncertainty_reasons": _flatten_sequence(assessment.uncertainty_reasons),
        }
        for assessment in artifact_assessments
    ]


def _evidence_detail_rows(artifact_assessments: list[HipEvidence]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for assessment in artifact_assessments:
        for evidence in assessment.evidence_records:
            rows.append(
                {
                    "repo": assessment.repo,
                    "hip_id": assessment.hip_id,
                    "artifact_type": assessment.artifact_type,
                    "artifact_number": assessment.artifact_number,
                    "evidence_type": evidence.evidence_type,
                    "evidence_tier": evidence.evidence_tier,
                    "source_artifact": evidence.source_artifact,
                    "source_kind": evidence.source_kind,
                    "polarity": evidence.polarity,
                    "confidence_contribution": evidence.confidence_contribution,
                    "short_rationale": evidence.short_rationale,
                    "top_reasons": _flatten_sequence(evidence.top_reasons),
                    "uncertainty_reasons": _flatten_sequence(evidence.uncertainty_reasons),
                }
            )
    return rows


def _summary_rows(repo_statuses: list[HipRepoStatus]) -> list[dict[str, object]]:
    return [
        {
            "repo": repo_status.repo,
            "hip_id": repo_status.hip_id,
            "rag_label": repo_status.rag_label,
            "status": repo_status.status,
            "confidence_level": repo_status.confidence_level,
            "confidence_score": repo_status.confidence_score,
            "evidence_count": repo_status.evidence_count,
            "top_artifacts": _flatten_sequence(repo_status.top_artifacts),
            "reviewer_notes": repo_status.reviewer_notes,
            "top_reasons": _flatten_sequence(repo_status.top_reasons),
            "uncertainty_reasons": _flatten_sequence(repo_status.uncertainty_reasons),
            "last_evidence_at": _format_datetime(repo_status.last_evidence_at),
        }
        for repo_status in repo_statuses
    ]


def _high_confidence_completion_rows(repo_statuses: list[HipRepoStatus]) -> list[dict[str, object]]:
    return [
        {
            "repo": repo_status.repo,
            "hip_id": repo_status.hip_id,
            "confidence_score": repo_status.confidence_score,
            "top_artifacts": _flatten_sequence(repo_status.top_artifacts),
            "top_reasons": _flatten_sequence(repo_status.top_reasons),
        }
        for repo_status in repo_statuses
        if repo_status.status == "completed" and repo_status.confidence_level == "high"
    ]


def _recent_hip_status_counts_df(repo_statuses: list[HipRepoStatus]) -> pd.DataFrame:
    grouped: dict[str, dict[str, int]] = {}
    for repo_status in repo_statuses:
        grouped.setdefault(
            repo_status.hip_id,
            {"completed": 0, "in_progress": 0, "unknown": 0},
        )
        if repo_status.status in grouped[repo_status.hip_id]:
            grouped[repo_status.hip_id][repo_status.status] += 1
    rows = [
        {
            "hip_id": hip_id,
            "completed": counts["completed"],
            "in_progress": counts["in_progress"],
            "unknown": counts["unknown"],
        }
        for hip_id, counts in sorted(grouped.items(), key=lambda item: hip_sort_key(item[0]), reverse=True)
    ]
    return pd.DataFrame(rows, columns=["hip_id", "completed", "in_progress", "unknown"])


def _sdk_completion_counts_df(repo_statuses: list[HipRepoStatus]) -> pd.DataFrame:
    grouped: dict[str, dict[str, int]] = {}
    for repo_status in repo_statuses:
        repo_counts = grouped.setdefault(
            repo_status.repo,
            {"completed": 0, "not_completed": 0},
        )
        if repo_status.status == "completed" and repo_status.confidence_level == "high":
            repo_counts["completed"] += 1
        else:
            repo_counts["not_completed"] += 1
    rows = [
        {
            "repo": repo,
            "completed": counts["completed"],
            "not_completed": counts["not_completed"],
        }
        for repo, counts in sorted(grouped.items())
    ]
    return pd.DataFrame(rows, columns=["repo", "completed", "not_completed"])


def _key_for_row(row: dict[str, object], key_columns: list[str]) -> tuple[str, ...]:
    return tuple(str(row.get(column, "")) for column in key_columns)


def _preserve_feedback_rows(
    rows: list[dict[str, object]],
    path: Path,
    *,
    key_columns: list[str],
    columns: list[str],
    preserved_columns: list[str],
    keep_missing_existing_rows: bool = True,
) -> pd.DataFrame:
    existing_rows: dict[tuple[str, ...], dict[str, object]] = {}
    if path.exists():
        existing_df = pd.read_csv(path, keep_default_na=False)
        existing_df = existing_df.rename(columns=LEGACY_MANUAL_REVIEW_COLUMN_ALIASES)
        for existing_row in existing_df.to_dict(orient="records"):
            existing_rows[_key_for_row(existing_row, key_columns)] = existing_row

    merged_rows: list[dict[str, object]] = []
    seen_keys: set[tuple[str, ...]] = set()
    for row in rows:
        key = _key_for_row(row, key_columns)
        existing_row = existing_rows.get(key, {})
        merged_row = {column: row.get(column, "") for column in columns}
        for column in preserved_columns:
            merged_row[column] = existing_row.get(column, merged_row.get(column, ""))
        merged_rows.append(merged_row)
        seen_keys.add(key)
    if keep_missing_existing_rows:
        for key, existing_row in existing_rows.items():
            if key in seen_keys:
                continue
            preserved_row = {column: existing_row.get(column, "") for column in columns}
            if "prediction_present" in preserved_row:
                preserved_row["prediction_present"] = False
            merged_rows.append(preserved_row)
    return pd.DataFrame(merged_rows, columns=columns)


def _parse_manual_bool(value: object) -> bool | None:
    if value is None or pd.isna(value):
        return None
    normalized = str(value).strip().lower()
    if not normalized:
        return None
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    return None


def _has_manual_feedback(row: dict[str, object]) -> bool:
    return any(str(row.get(column, "")).strip() for column in MANUAL_REVIEW_COLUMNS)


def _percentage(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round((numerator / denominator) * 100, 2)


def _balance_score_percentage(
    confirmed_match_rows: int,
    overcalled_match_rows: int,
    missed_match_rows: int,
) -> float | None:
    return _percentage(
        2 * confirmed_match_rows,
        (2 * confirmed_match_rows) + overcalled_match_rows + missed_match_rows,
    )


def _resolve_review_outcome_counts(rows: list[dict[str, object]]) -> dict[str, int]:
    counts = {
        "confirmed_match_rows": 0,
        "overcalled_match_rows": 0,
        "missed_match_rows": 0,
        "confirmed_non_match_rows": 0,
        "unclear_review_rows": 0,
    }
    for row in rows:
        positive_labels = [
            column
            for column in [
                "is_confirmed_match",
                "is_overcalled_match",
                "is_missed_match",
                "is_confirmed_non_match",
            ]
            if _parse_manual_bool(row.get(column)) is True
        ]
        if len(positive_labels) > 1:
            counts["unclear_review_rows"] += 1
            continue
        if len(positive_labels) == 0:
            continue
        label = positive_labels[0]
        if label == "is_confirmed_match":
            counts["confirmed_match_rows"] += 1
        elif label == "is_overcalled_match":
            counts["overcalled_match_rows"] += 1
        elif label == "is_missed_match":
            counts["missed_match_rows"] += 1
        elif label == "is_confirmed_non_match":
            counts["confirmed_non_match_rows"] += 1
    return counts


def _subset_rows(rows: list[dict[str, object]], dataset_split: str) -> list[dict[str, object]]:
    if dataset_split == "all":
        return rows
    return [row for row in rows if str(row.get("dataset_split", "")) == dataset_split]


def _review_breakdown_rows(scope: str, df: pd.DataFrame) -> list[dict[str, object]]:
    rows = df.to_dict(orient="records")
    breakdown_rows: list[dict[str, object]] = []
    for dataset_split in ["all", "train", "test"]:
        subset = _subset_rows(rows, dataset_split)
        counts = _resolve_review_outcome_counts(subset)
        confirmed_match_rows = counts["confirmed_match_rows"]
        overcalled_match_rows = counts["overcalled_match_rows"]
        missed_match_rows = counts["missed_match_rows"]
        confirmed_non_match_rows = counts["confirmed_non_match_rows"]
        reviewed_rows = (
            confirmed_match_rows
            + overcalled_match_rows
            + missed_match_rows
            + confirmed_non_match_rows
        )
        breakdown_rows.append(
            {
                "scope": scope,
                "dataset_split": dataset_split,
                "confirmed_match_rows": confirmed_match_rows,
                "overcalled_match_rows": overcalled_match_rows,
                "missed_match_rows": missed_match_rows,
                "confirmed_non_match_rows": confirmed_non_match_rows,
                "reviewed_rows": reviewed_rows,
                "unclear_review_rows": counts["unclear_review_rows"],
                "accuracy_percent": _percentage(confirmed_match_rows + confirmed_non_match_rows, reviewed_rows),
                "match_quality_percent": _percentage(
                    confirmed_match_rows,
                    confirmed_match_rows + overcalled_match_rows,
                ),
                "match_coverage_percent": _percentage(
                    confirmed_match_rows,
                    confirmed_match_rows + missed_match_rows,
                ),
                "non_match_accuracy_percent": _percentage(
                    confirmed_non_match_rows,
                    confirmed_non_match_rows + overcalled_match_rows,
                ),
                "balance_score_percent": _balance_score_percentage(
                    confirmed_match_rows,
                    overcalled_match_rows,
                    missed_match_rows,
                ),
            }
        )
    return breakdown_rows


def _evaluation_summary_rows(
    scope: str,
    df: pd.DataFrame,
    *,
    review_breakdown_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    rows = df.to_dict(orient="records")
    breakdown_lookup = {
        str(row["dataset_split"]): row
        for row in review_breakdown_rows
    }
    summary_rows: list[dict[str, object]] = []
    for dataset_split in ["all", "train", "test"]:
        subset = _subset_rows(rows, dataset_split)
        total_rows = len(subset)
        prediction_rows = sum(_parse_manual_bool(row.get("prediction_present")) is not False for row in subset)
        reviewed_rows = sum(_has_manual_feedback(row) for row in subset)
        breakdown_row = breakdown_lookup.get(dataset_split, {})
        evaluated_review_rows = int(breakdown_row.get("reviewed_rows", 0) or 0)
        confirmed_match_rows = int(breakdown_row.get("confirmed_match_rows", 0) or 0)
        overcalled_match_rows = int(breakdown_row.get("overcalled_match_rows", 0) or 0)
        missed_match_rows = int(breakdown_row.get("missed_match_rows", 0) or 0)
        confirmed_non_match_rows = int(breakdown_row.get("confirmed_non_match_rows", 0) or 0)
        summary_rows.append(
            {
                "scope": scope,
                "dataset_split": dataset_split,
                "total_rows": total_rows,
                "prediction_rows": prediction_rows,
                "reviewed_rows": reviewed_rows,
                "review_coverage_percent": _percentage(reviewed_rows, total_rows),
                "evaluated_review_rows": evaluated_review_rows,
                "correct_reviewed_rows": confirmed_match_rows + confirmed_non_match_rows,
                "accuracy_percent": breakdown_row.get("accuracy_percent"),
                "confirmed_match_rows": confirmed_match_rows,
                "overcalled_match_rows": overcalled_match_rows,
                "missed_match_rows": missed_match_rows,
                "confirmed_non_match_rows": confirmed_non_match_rows,
                "match_quality_percent": breakdown_row.get("match_quality_percent"),
                "match_coverage_percent": breakdown_row.get("match_coverage_percent"),
            }
        )
    return summary_rows

def _select_checklist_rows(
    repo_statuses: list[HipRepoStatus],
    *,
    latest_limit: int,
) -> list[HipRepoStatus]:
    grouped: dict[str, list[HipRepoStatus]] = {}
    for repo_status in repo_statuses:
        grouped.setdefault(repo_status.repo, []).append(repo_status)

    selected: list[HipRepoStatus] = []
    for repo, repo_rows in grouped.items():
        newest_rows = sorted(
            repo_rows,
            key=lambda row: hip_sort_key(row.hip_id),
            reverse=True,
        )[:latest_limit]
        selected.extend(sorted(newest_rows, key=lambda row: (repo, row.status, hip_sort_key(row.hip_id))))
    return selected


def _write_checklist(
    repo_statuses: list[HipRepoStatus],
    path: Path,
    *,
    latest_limit: int = DEFAULT_CHECKLIST_LATEST_LIMIT,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    grouped: dict[str, list[HipRepoStatus]] = {}
    for repo_status in _select_checklist_rows(repo_statuses, latest_limit=latest_limit):
        grouped.setdefault(repo_status.repo, []).append(repo_status)
    status_order = ["conflicting", "in_progress", "not_started", "unknown", "completed"]
    lines: list[str] = []
    for repo in sorted(grouped):
        lines.append(f"## {repo}")
        lines.append(f"_Latest {min(latest_limit, len(grouped[repo]))} HIPs by catalog number_")
        for status in status_order:
            matching = sorted(
                [repo_status for repo_status in grouped[repo] if repo_status.status == status],
                key=lambda repo_status: hip_sort_key(repo_status.hip_id),
                reverse=True,
            )
            if not matching:
                continue
            lines.append(f"### {status}")
            for repo_status in matching:
                checkbox = "[x]" if repo_status.status == "completed" else "[ ]"
                artifacts = ", ".join(repo_status.top_artifacts) or "no supporting artifacts"
                reasons = "; ".join(repo_status.top_reasons[:2]) or "no supporting reasons"
                notes = f" | reviewer_notes: {repo_status.reviewer_notes}" if repo_status.reviewer_notes else ""
                lines.append(
                    f"- {checkbox} {repo_status.hip_id} | {repo_status.rag_label} | "
                    f"{repo_status.confidence_level} ({repo_status.confidence_score:.0f}) | "
                    f"{artifacts} | {reasons}{notes}"
                )
        lines.append("")
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _write_benchmark_report(
    benchmark_exports: dict[str, list[dict[str, object]]],
    *,
    markdown_path: Path,
    json_path: Path,
) -> None:
    metrics_df = pd.DataFrame(benchmark_exports.get("metrics", []), columns=BENCHMARK_METRIC_COLUMNS)
    confusion_df = pd.DataFrame(benchmark_exports.get("confusion", []), columns=CONFUSION_MATRIX_COLUMNS)
    per_status_df = pd.DataFrame(benchmark_exports.get("per_status", []), columns=PER_STATUS_METRIC_COLUMNS)

    save_json(
        {
            "metrics": metrics_df.to_dict(orient="records"),
            "confusion_matrix": confusion_df.to_dict(orient="records"),
            "per_status_metrics": per_status_df.to_dict(orient="records"),
        },
        json_path,
    )
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    sections = [
        "# Benchmark Evaluation",
        "",
        "## Overall Metrics",
        "",
        dataframe_to_markdown_table(metrics_df).rstrip(),
        "",
        "## Confusion Matrix",
        "",
        dataframe_to_markdown_table(confusion_df).rstrip(),
        "",
        "## Per-Status Metrics",
        "",
        dataframe_to_markdown_table(per_status_df).rstrip(),
        "",
    ]
    markdown_path.write_text("\n".join(sections), encoding="utf-8")


def _write_status_chart(
    df: pd.DataFrame,
    *,
    x_col: str,
    stack_cols: list[str],
    labels: list[str],
    output_path: Path,
    title: str,
    colors: dict[str, str],
) -> None:
    if df.empty:
        return
    plot_stacked_bar(
        df,
        x_col=x_col,
        stack_cols=stack_cols,
        labels=labels,
        title=title,
        output_path=output_path,
        colors=colors,
        rotate_x=20,
        annotate_totals=True,
        sort_categorical=False,
    )


def _accuracy_snapshot_rows(
    *,
    pr_review_breakdown_rows: list[dict[str, object]],
    issue_review_breakdown_rows: list[dict[str, object]],
    repo_review_breakdown_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for scope, breakdown_rows in [
        ("pull_request", pr_review_breakdown_rows),
        ("issue", issue_review_breakdown_rows),
        ("repo", repo_review_breakdown_rows),
    ]:
        all_row = next((row for row in breakdown_rows if row.get("dataset_split") == "all"), None)
        if all_row is None:
            continue
        rows.append(
            {
                "scope": scope,
                "reviewed_rows": all_row.get("reviewed_rows", 0),
                "accuracy_percent": all_row.get("accuracy_percent", ""),
                "confirmed_match_rows": all_row.get("confirmed_match_rows", 0),
                "overcalled_match_rows": all_row.get("overcalled_match_rows", 0),
                "missed_match_rows": all_row.get("missed_match_rows", 0),
                "confirmed_non_match_rows": all_row.get("confirmed_non_match_rows", 0),
                "match_quality_percent": all_row.get("match_quality_percent", ""),
                "match_coverage_percent": all_row.get("match_coverage_percent", ""),
            }
        )
    return rows


def _review_queue_table(
    df: pd.DataFrame,
    *,
    columns: list[str],
) -> str:
    if df.empty:
        return "_No rows in scope._\n"
    return dataframe_to_markdown_table(df.loc[:, columns].fillna(""))


def _write_manual_accuracy_report(
    *,
    manual_accuracy_df: pd.DataFrame,
    accuracy_snapshot_df: pd.DataFrame,
    path: Path,
) -> None:
    sections = [
        "# Manual Accuracy Report",
        "",
        "Use `human_observation`, `is_prediction_correct`, `is_overcalled_match`, and `is_missed_match` in `manual_accuracy_review.csv` to drive the feedback cycle.",
        "",
        "## Accuracy Snapshot",
        "",
        dataframe_to_markdown_table(accuracy_snapshot_df).rstrip() if not accuracy_snapshot_df.empty else "_No manual review metrics yet._",
        "",
        "## Pull Request Review Queue",
        "",
        _review_queue_table(
            manual_accuracy_df[manual_accuracy_df["review_scope"] == "pull_request"],
            columns=[
                "dataset_split",
                "prediction_present",
                "artifact_link",
                "hip_id",
                "predicted_status",
                "predicted_confidence_level",
                "linked_issue_urls",
                "linked_pr_urls",
                "human_observation",
                "is_prediction_correct",
                "is_overcalled_match",
                "is_missed_match",
            ],
        ).rstrip(),
        "",
        "## Issue Review Queue",
        "",
        _review_queue_table(
            manual_accuracy_df[manual_accuracy_df["review_scope"] == "issue"],
            columns=[
                "dataset_split",
                "prediction_present",
                "artifact_link",
                "hip_id",
                "predicted_status",
                "predicted_confidence_level",
                "linked_issue_urls",
                "linked_pr_urls",
                "human_observation",
                "is_prediction_correct",
                "is_overcalled_match",
                "is_missed_match",
            ],
        ).rstrip(),
        "",
        "## Repo Review Queue",
        "",
        _review_queue_table(
            manual_accuracy_df[manual_accuracy_df["review_scope"] == "repo"],
            columns=[
                "dataset_split",
                "repo",
                "hip_id",
                "predicted_status",
                "predicted_confidence_level",
                "supporting_artifact_links",
                "human_observation",
                "is_prediction_correct",
                "is_overcalled_match",
                "is_missed_match",
            ],
        ).rstrip(),
        "",
        "## Missed Calls",
        "",
        _review_queue_table(
            manual_accuracy_df[
                manual_accuracy_df["is_missed_match"].map(_parse_manual_bool).fillna(False)
            ],
            columns=[
                "review_scope",
                "artifact_link",
                "repo",
                "hip_id",
                "human_expected_outcome",
                "human_observation",
            ],
        ).rstrip(),
        "",
        "## Overcalled Calls",
        "",
        _review_queue_table(
            manual_accuracy_df[
                manual_accuracy_df["is_overcalled_match"].map(_parse_manual_bool).fillna(False)
            ],
            columns=[
                "review_scope",
                "artifact_link",
                "repo",
                "hip_id",
                "predicted_status",
                "human_observation",
            ],
        ).rstrip(),
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(sections), encoding="utf-8")


def export_hip_progression_results(
    output_dir: Path,
    *,
    artifacts: list[HipArtifact],
    feature_vectors: list[HipFeatureVector],
    artifact_assessments: list[HipEvidence] | None = None,
    evidence_records: list[HipEvidence] | None = None,
    repo_statuses: list[HipRepoStatus],
    dataset_splits: dict[tuple[str, int], str] | None = None,
    benchmark_exports: dict[str, list[dict[str, object]]] | None = None,
    export_profile: ExportProfile = "review",
    checklist_latest_limit: int = DEFAULT_CHECKLIST_LATEST_LIMIT,
) -> dict[str, Path]:
    """Export HIP progression outputs as review-friendly CSV and markdown tables."""
    output_dir.mkdir(parents=True, exist_ok=True)
    dataset_splits = dataset_splits or assign_dataset_splits(artifacts)
    artifact_assessments = artifact_assessments or evidence_records or []

    evidence_detail_path = output_dir / "hip_evidence_detail.csv"
    evidence_detail_markdown_path = output_dir / "hip_evidence_detail.md"
    summary_path = output_dir / "hip_repo_summary.csv"
    summary_markdown_path = output_dir / "hip_repo_summary.md"
    checklist_path = output_dir / "hip_checklist.md"
    high_confidence_completion_path = output_dir / "hip_high_confidence_completion.csv"
    high_confidence_completion_markdown_path = output_dir / "hip_high_confidence_completion.md"
    recent_status_counts_path = output_dir / "recent_hip_status_counts.csv"
    recent_status_chart_path = output_dir / "recent_hip_status_counts.png"
    sdk_completion_counts_path = output_dir / "sdk_completion_counts.csv"
    sdk_completion_chart_path = output_dir / "sdk_completion_counts.png"
    manual_accuracy_review_path = output_dir / "manual_accuracy_review.csv"
    manual_accuracy_report_path = output_dir / "manual_accuracy_report.md"
    benchmark_report_markdown_path = output_dir / "benchmark_report.md"
    benchmark_report_json_path = output_dir / "benchmark_report.json"

    evidence_detail_df = pd.DataFrame(_evidence_detail_rows(artifact_assessments))
    summary_rows = _summary_rows(repo_statuses)
    high_confidence_completion_df = pd.DataFrame(
        _high_confidence_completion_rows(repo_statuses),
        columns=["repo", "hip_id", "confidence_score", "top_artifacts", "top_reasons"],
    )
    recent_status_counts_df = _recent_hip_status_counts_df(repo_statuses)
    sdk_completion_counts_df = _sdk_completion_counts_df(repo_statuses)
    unique_repos = sorted({repo_status.repo for repo_status in repo_statuses})
    write_cross_repo_charts = len(unique_repos) > 1
    summary_df = _preserve_feedback_rows(
        summary_rows,
        summary_path,
        key_columns=["repo", "hip_id"],
        columns=list(summary_rows[0].keys()) if summary_rows else [
            "repo",
            "hip_id",
            "rag_label",
            "status",
            "confidence_level",
            "confidence_score",
            "evidence_count",
            "top_artifacts",
            "reviewer_notes",
            "top_reasons",
            "uncertainty_reasons",
            "last_evidence_at",
        ],
        preserved_columns=["reviewer_notes"],
        keep_missing_existing_rows=False,
    )
    artifact_evaluation_rows = build_artifact_evaluation_rows(
        artifacts=artifacts,
        artifact_assessments=artifact_assessments,
        dataset_splits=dataset_splits,
    )
    pr_evaluation_df = _preserve_feedback_rows(
        [row for row in artifact_evaluation_rows if row["artifact_type"] == "pull_request"],
        output_dir / "pr_evaluation.csv",
        key_columns=["repo", "hip_id", "artifact_number"],
        columns=ARTIFACT_EVALUATION_COLUMNS,
        preserved_columns=MANUAL_REVIEW_COLUMNS,
        keep_missing_existing_rows=False,
    )
    issue_evaluation_df = _preserve_feedback_rows(
        [row for row in artifact_evaluation_rows if row["artifact_type"] == "issue"],
        output_dir / "issue_evaluation.csv",
        key_columns=["repo", "hip_id", "artifact_number"],
        columns=ARTIFACT_EVALUATION_COLUMNS,
        preserved_columns=MANUAL_REVIEW_COLUMNS,
        keep_missing_existing_rows=False,
    )
    repo_evaluation_df = _preserve_feedback_rows(
        build_repo_evaluation_rows(
            artifacts=artifacts,
            repo_statuses=repo_statuses,
            dataset_splits=dataset_splits,
        ),
        output_dir / "repo_evaluation.csv",
        key_columns=["repo", "hip_id"],
        columns=REPO_EVALUATION_COLUMNS,
        preserved_columns=MANUAL_REVIEW_COLUMNS,
        keep_missing_existing_rows=False,
    )
    pr_review_breakdown_rows = _review_breakdown_rows("pull_request", pr_evaluation_df)
    issue_review_breakdown_rows = _review_breakdown_rows("issue", issue_evaluation_df)
    repo_review_breakdown_rows = _review_breakdown_rows("repo", repo_evaluation_df)
    prediction_review_breakdown_df = pd.DataFrame(
        [*pr_review_breakdown_rows, *issue_review_breakdown_rows, *repo_review_breakdown_rows],
        columns=PREDICTION_REVIEW_BREAKDOWN_COLUMNS,
    )
    evaluation_summary_df = pd.DataFrame(
        [
            *_evaluation_summary_rows("pull_request", pr_evaluation_df, review_breakdown_rows=pr_review_breakdown_rows),
            *_evaluation_summary_rows("issue", issue_evaluation_df, review_breakdown_rows=issue_review_breakdown_rows),
            *_evaluation_summary_rows("repo", repo_evaluation_df, review_breakdown_rows=repo_review_breakdown_rows),
        ],
        columns=EVALUATION_SUMMARY_COLUMNS,
    )
    manual_accuracy_df = _preserve_feedback_rows(
        build_manual_accuracy_rows(
            artifacts=artifacts,
            artifact_assessments=artifact_assessments,
            repo_statuses=repo_statuses,
            dataset_splits=dataset_splits,
        ),
        manual_accuracy_review_path,
        key_columns=["review_scope", "repo", "artifact_number", "hip_id"],
        columns=MANUAL_ACCURACY_COLUMNS,
        preserved_columns=MANUAL_REVIEW_COLUMNS,
        keep_missing_existing_rows=False,
    )
    accuracy_snapshot_df = pd.DataFrame(
        _accuracy_snapshot_rows(
            pr_review_breakdown_rows=pr_review_breakdown_rows,
            issue_review_breakdown_rows=issue_review_breakdown_rows,
            repo_review_breakdown_rows=repo_review_breakdown_rows,
        )
    )
    save_dataframe(evidence_detail_df, evidence_detail_path)
    save_markdown_table(evidence_detail_df, evidence_detail_markdown_path)
    save_dataframe(summary_df, summary_path)
    save_markdown_table(summary_df, summary_markdown_path)
    _write_checklist(repo_statuses, checklist_path, latest_limit=checklist_latest_limit)
    save_dataframe(high_confidence_completion_df, high_confidence_completion_path)
    save_markdown_table(high_confidence_completion_df, high_confidence_completion_markdown_path)
    if write_cross_repo_charts:
        save_dataframe(recent_status_counts_df, recent_status_counts_path)
        _write_status_chart(
            recent_status_counts_df,
            x_col="hip_id",
            stack_cols=["completed", "in_progress", "unknown"],
            labels=["completed", "in_progress", "unknown"],
            output_path=recent_status_chart_path,
            title="Most Recent HIP Status By Repo Count",
            colors=STATUS_CHART_COLORS,
        )
        save_dataframe(sdk_completion_counts_df, sdk_completion_counts_path)
        _write_status_chart(
            sdk_completion_counts_df,
            x_col="repo",
            stack_cols=["completed", "not_completed"],
            labels=["completed", "not_completed"],
            output_path=sdk_completion_chart_path,
            title="High Confidence HIP Completion By SDK",
            colors=STATUS_CHART_COLORS,
        )
    save_dataframe(manual_accuracy_df, manual_accuracy_review_path)
    _write_manual_accuracy_report(
        manual_accuracy_df=manual_accuracy_df,
        accuracy_snapshot_df=accuracy_snapshot_df,
        path=manual_accuracy_report_path,
    )

    export_paths = {
        "hip_evidence_detail": evidence_detail_path,
        "hip_evidence_detail_markdown": evidence_detail_markdown_path,
        "hip_repo_summary": summary_path,
        "hip_repo_summary_markdown": summary_markdown_path,
        "hip_checklist_markdown": checklist_path,
        "hip_high_confidence_completion": high_confidence_completion_path,
        "hip_high_confidence_completion_markdown": high_confidence_completion_markdown_path,
        "manual_accuracy_review": manual_accuracy_review_path,
        "manual_accuracy_report": manual_accuracy_report_path,
    }
    if write_cross_repo_charts:
        export_paths["recent_hip_status_counts"] = recent_status_counts_path
        export_paths["recent_hip_status_chart"] = recent_status_chart_path
        export_paths["sdk_completion_counts"] = sdk_completion_counts_path
        export_paths["sdk_completion_chart"] = sdk_completion_chart_path

    if benchmark_exports:
        _write_benchmark_report(
            benchmark_exports,
            markdown_path=benchmark_report_markdown_path,
            json_path=benchmark_report_json_path,
        )
        export_paths["benchmark_report_markdown"] = benchmark_report_markdown_path
        export_paths["benchmark_report_json"] = benchmark_report_json_path

    if export_profile == "full":
        artifact_path = output_dir / "artifacts.csv"
        feature_path = output_dir / "artifact_features.csv"
        feature_markdown_path = output_dir / "artifact_features.md"
        assessment_path = output_dir / "hip_evidence.csv"
        assessment_markdown_path = output_dir / "hip_evidence.md"
        legacy_status_path = output_dir / "hip_repo_status.csv"
        legacy_status_markdown_path = output_dir / "hip_repo_status.md"
        pr_evaluation_path = output_dir / "pr_evaluation.csv"
        pr_evaluation_markdown_path = output_dir / "pr_evaluation.md"
        issue_evaluation_path = output_dir / "issue_evaluation.csv"
        issue_evaluation_markdown_path = output_dir / "issue_evaluation.md"
        repo_evaluation_path = output_dir / "repo_evaluation.csv"
        repo_evaluation_markdown_path = output_dir / "repo_evaluation.md"
        prediction_review_breakdown_path = output_dir / "prediction_review_breakdown.csv"
        prediction_review_breakdown_markdown_path = output_dir / "prediction_review_breakdown.md"
        evaluation_summary_path = output_dir / "evaluation_summary.csv"
        evaluation_summary_markdown_path = output_dir / "evaluation_summary.md"

        artifact_df = pd.DataFrame(_artifact_rows(artifacts))
        feature_df = pd.DataFrame(_feature_rows(feature_vectors))
        assessment_df = pd.DataFrame(_artifact_assessment_rows(artifact_assessments))

        save_dataframe(artifact_df, artifact_path)
        save_dataframe(feature_df, feature_path)
        save_markdown_table(feature_df, feature_markdown_path)
        save_dataframe(assessment_df, assessment_path)
        save_markdown_table(assessment_df, assessment_markdown_path)
        save_dataframe(summary_df, legacy_status_path)
        save_markdown_table(summary_df, legacy_status_markdown_path)
        save_dataframe(pr_evaluation_df, pr_evaluation_path)
        save_markdown_table(pr_evaluation_df, pr_evaluation_markdown_path)
        save_dataframe(issue_evaluation_df, issue_evaluation_path)
        save_markdown_table(issue_evaluation_df, issue_evaluation_markdown_path)
        save_dataframe(repo_evaluation_df, repo_evaluation_path)
        save_markdown_table(repo_evaluation_df, repo_evaluation_markdown_path)
        save_dataframe(prediction_review_breakdown_df, prediction_review_breakdown_path)
        save_markdown_table(prediction_review_breakdown_df, prediction_review_breakdown_markdown_path)
        save_dataframe(evaluation_summary_df, evaluation_summary_path)
        save_markdown_table(evaluation_summary_df, evaluation_summary_markdown_path)

        export_paths.update(
            {
                "artifacts": artifact_path,
                "artifact_features": feature_path,
                "artifact_features_markdown": feature_markdown_path,
                "hip_evidence": assessment_path,
                "hip_evidence_markdown": assessment_markdown_path,
                "hip_repo_status": legacy_status_path,
                "hip_repo_status_markdown": legacy_status_markdown_path,
                "pr_evaluation": pr_evaluation_path,
                "pr_evaluation_markdown": pr_evaluation_markdown_path,
                "issue_evaluation": issue_evaluation_path,
                "issue_evaluation_markdown": issue_evaluation_markdown_path,
                "repo_evaluation": repo_evaluation_path,
                "repo_evaluation_markdown": repo_evaluation_markdown_path,
                "prediction_review_breakdown": prediction_review_breakdown_path,
                "prediction_review_breakdown_markdown": prediction_review_breakdown_markdown_path,
                "evaluation_summary": evaluation_summary_path,
                "evaluation_summary_markdown": evaluation_summary_markdown_path,
            }
        )

    return export_paths
