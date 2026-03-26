"""Structured exports for HIP progression analysis outputs."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from hiero_analytics.analysis.hip_evaluation import (
    ARTIFACT_EVALUATION_COLUMNS,
    EVALUATION_SUMMARY_COLUMNS,
    LEGACY_MANUAL_REVIEW_COLUMN_ALIASES,
    MANUAL_REVIEW_COLUMNS,
    PREDICTION_REVIEW_BREAKDOWN_COLUMNS,
    REPO_EVALUATION_COLUMNS,
    assign_dataset_splits,
    build_artifact_evaluation_rows,
    build_repo_evaluation_rows,
)
from hiero_analytics.domain.hip_progression_models import (
    HipArtifact,
    HipEvidence,
    HipFeatureVector,
    HipRepoStatus,
)
from hiero_analytics.export.save import save_dataframe, save_markdown_table


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
            "src_files_changed_count": sum(1 for changed_file in artifact.changed_files if changed_file.is_src),
            "test_files_changed_count": sum(1 for changed_file in artifact.changed_files if changed_file.is_test),
            "integration_test_files_changed_count": sum(
                1 for changed_file in artifact.changed_files if changed_file.is_integration_test
            ),
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
            "extraction_source": feature_vector.extraction_source,
            "text_match_reason": feature_vector.text_match_reason,
            "explicit_hip_mention": feature_vector.explicit_hip_mention,
            "hip_in_title": feature_vector.hip_in_title,
            "hip_in_body": feature_vector.hip_in_body,
            "hip_in_comments": feature_vector.hip_in_comments,
            "hip_in_commit_messages": feature_vector.hip_in_commit_messages,
            "negative_context_flags": _flatten_sequence(feature_vector.negative_context_flags),
            "negative_phrase_unblock": feature_vector.negative_phrase_unblock,
            "negative_phrase_blocked": feature_vector.negative_phrase_blocked,
            "negative_phrase_follow_up": feature_vector.negative_phrase_follow_up,
            "negative_phrase_prep": feature_vector.negative_phrase_prep,
            "negative_phrase_refactor_only": feature_vector.negative_phrase_refactor_only,
            "negative_phrase_cleanup_only": feature_vector.negative_phrase_cleanup_only,
            "has_feat_keyword": feature_vector.has_feat_keyword,
            "has_implement_keyword": feature_vector.has_implement_keyword,
            "has_support_keyword": feature_vector.has_support_keyword,
            "src_files_changed_count": feature_vector.src_files_changed_count,
            "test_files_changed_count": feature_vector.test_files_changed_count,
            "integration_test_files_changed_count": feature_vector.integration_test_files_changed_count,
            "new_src_files_count": feature_vector.new_src_files_count,
            "new_test_files_count": feature_vector.new_test_files_count,
            "total_additions": feature_vector.total_additions,
            "total_deletions": feature_vector.total_deletions,
            "merged": feature_vector.merged,
            "author_is_maintainer_like": feature_vector.author_is_maintainer_like,
            "author_is_committer_like": feature_vector.author_is_committer_like,
            "implementation_score_inputs": _flatten_sequence(feature_vector.implementation_score_inputs),
        }
        for feature_vector in feature_vectors
    ]


def _evidence_rows(evidence_records: list[HipEvidence]) -> list[dict[str, object]]:
    return [
        {
            "repo": evidence.repo,
            "hip_id": evidence.hip_id,
            "artifact_type": evidence.artifact_type,
            "artifact_number": evidence.artifact_number,
            "hip_candidate_score": evidence.hip_candidate_score,
            "implementation_score": evidence.implementation_score,
            "completion_score": evidence.completion_score,
            "confidence_level": evidence.confidence_level,
            "reasons": _flatten_sequence(evidence.reasons),
        }
        for evidence in evidence_records
    ]


def _status_rows(repo_statuses: list[HipRepoStatus]) -> list[dict[str, object]]:
    return [
        {
            "repo": repo_status.repo,
            "hip_id": repo_status.hip_id,
            "status": repo_status.status,
            "confidence_level": repo_status.confidence_level,
            "supporting_artifact_numbers": _flatten_sequence(
                [str(number) for number in repo_status.supporting_artifact_numbers]
            ),
            "rationale": _flatten_sequence(repo_status.rationale),
            "last_evidence_at": _format_datetime(repo_status.last_evidence_at),
        }
        for repo_status in repo_statuses
    ]


def _key_for_row(row: dict[str, object], key_columns: list[str]) -> tuple[str, ...]:
    return tuple(str(row.get(column, "")) for column in key_columns)


def _preserve_feedback_rows(
    rows: list[dict[str, object]],
    path: Path,
    *,
    key_columns: list[str],
    columns: list[str],
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
        for column in MANUAL_REVIEW_COLUMNS:
            if column in merged_row:
                merged_row[column] = existing_row.get(column, merged_row[column])
        merged_rows.append(merged_row)
        seen_keys.add(key)

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


def _subset_rows(
    rows: list[dict[str, object]],
    dataset_split: str,
) -> list[dict[str, object]]:
    if dataset_split == "all":
        return rows
    return [
        row for row in rows if str(row.get("dataset_split", "")) == dataset_split
    ]


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
    review_breakdown_rows: list[dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    rows = df.to_dict(orient="records")
    breakdown_lookup = {
        str(row["dataset_split"]): row
        for row in (review_breakdown_rows or _review_breakdown_rows(scope, df))
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


def export_hip_progression_results(
    output_dir: Path,
    *,
    artifacts: list[HipArtifact],
    feature_vectors: list[HipFeatureVector],
    evidence_records: list[HipEvidence],
    repo_statuses: list[HipRepoStatus],
    dataset_splits: dict[tuple[str, int], str] | None = None,
) -> dict[str, Path]:
    """Export HIP progression outputs as review-friendly CSV tables."""
    output_dir.mkdir(parents=True, exist_ok=True)
    dataset_splits = dataset_splits or assign_dataset_splits(artifacts)

    artifact_path = output_dir / "artifacts.csv"
    feature_path = output_dir / "artifact_features.csv"
    feature_markdown_path = output_dir / "artifact_features.md"
    evidence_path = output_dir / "hip_evidence.csv"
    evidence_markdown_path = output_dir / "hip_evidence.md"
    status_path = output_dir / "hip_repo_status.csv"
    status_markdown_path = output_dir / "hip_repo_status.md"
    artifact_markdown_path = output_dir / "artifacts.md"
    pr_evaluation_path = output_dir / "pr_evaluation.csv"
    pr_evaluation_markdown_path = output_dir / "pr_evaluation.md"
    issue_evaluation_path = output_dir / "issue_evaluation.csv"
    issue_evaluation_markdown_path = output_dir / "issue_evaluation.md"
    repo_evaluation_path = output_dir / "repo_evaluation.csv"
    repo_evaluation_markdown_path = output_dir / "repo_evaluation.md"
    evaluation_summary_path = output_dir / "evaluation_summary.csv"
    evaluation_summary_markdown_path = output_dir / "evaluation_summary.md"
    prediction_review_breakdown_path = output_dir / "prediction_review_breakdown.csv"
    prediction_review_breakdown_markdown_path = output_dir / "prediction_review_breakdown.md"

    artifact_df = pd.DataFrame(_artifact_rows(artifacts))
    feature_df = pd.DataFrame(_feature_rows(feature_vectors))
    evidence_df = pd.DataFrame(_evidence_rows(evidence_records))
    status_df = pd.DataFrame(_status_rows(repo_statuses))
    artifact_evaluation_rows = build_artifact_evaluation_rows(
        artifacts=artifacts,
        feature_vectors=feature_vectors,
        evidence_records=evidence_records,
        dataset_splits=dataset_splits,
    )
    pr_evaluation_df = _preserve_feedback_rows(
        [
            row
            for row in artifact_evaluation_rows
            if row["artifact_type"] == "pull_request"
        ],
        pr_evaluation_path,
        key_columns=["repo", "hip_id", "artifact_number"],
        columns=ARTIFACT_EVALUATION_COLUMNS,
    )
    issue_evaluation_df = _preserve_feedback_rows(
        [
            row
            for row in artifact_evaluation_rows
            if row["artifact_type"] == "issue"
        ],
        issue_evaluation_path,
        key_columns=["repo", "hip_id", "artifact_number"],
        columns=ARTIFACT_EVALUATION_COLUMNS,
    )
    repo_evaluation_df = _preserve_feedback_rows(
        build_repo_evaluation_rows(
            artifacts=artifacts,
            evidence_records=evidence_records,
            repo_statuses=repo_statuses,
            dataset_splits=dataset_splits,
        ),
        repo_evaluation_path,
        key_columns=["repo", "hip_id"],
        columns=REPO_EVALUATION_COLUMNS,
    )
    pr_review_breakdown_rows = _review_breakdown_rows("pull_request", pr_evaluation_df)
    issue_review_breakdown_rows = _review_breakdown_rows("issue", issue_evaluation_df)
    repo_review_breakdown_rows = _review_breakdown_rows("repo", repo_evaluation_df)
    prediction_review_breakdown_df = pd.DataFrame(
        [
            *pr_review_breakdown_rows,
            *issue_review_breakdown_rows,
            *repo_review_breakdown_rows,
        ],
        columns=PREDICTION_REVIEW_BREAKDOWN_COLUMNS,
    )
    evaluation_summary_df = pd.DataFrame(
        [
            *_evaluation_summary_rows(
                "pull_request",
                pr_evaluation_df,
                review_breakdown_rows=pr_review_breakdown_rows,
            ),
            *_evaluation_summary_rows(
                "issue",
                issue_evaluation_df,
                review_breakdown_rows=issue_review_breakdown_rows,
            ),
            *_evaluation_summary_rows(
                "repo",
                repo_evaluation_df,
                review_breakdown_rows=repo_review_breakdown_rows,
            ),
        ],
        columns=EVALUATION_SUMMARY_COLUMNS,
    )

    save_dataframe(artifact_df, artifact_path)
    artifact_markdown_path.unlink(missing_ok=True)
    save_dataframe(feature_df, feature_path)
    save_markdown_table(feature_df, feature_markdown_path)
    save_dataframe(evidence_df, evidence_path)
    save_markdown_table(evidence_df, evidence_markdown_path)
    save_dataframe(status_df, status_path)
    save_markdown_table(status_df, status_markdown_path)
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

    return {
        "artifacts": artifact_path,
        "artifact_features": feature_path,
        "artifact_features_markdown": feature_markdown_path,
        "hip_evidence": evidence_path,
        "hip_evidence_markdown": evidence_markdown_path,
        "hip_repo_status": status_path,
        "hip_repo_status_markdown": status_markdown_path,
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
