"""Structured exports for HIP progression analysis outputs."""

from __future__ import annotations

from pathlib import Path
import shutil
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
    HipCatalogEntry,
    HipEvidence,
    HipFeatureVector,
    HipRepoStatus,
    hip_sort_key,
)
from hiero_analytics.plotting.bars import plot_bar, plot_stacked_bar
from hiero_analytics.export.save import (
    dataframe_to_markdown_table,
    save_dataframe,
    save_json,
    save_markdown_table,
)

ExportProfile = Literal["review", "full"]
ExportScope = Literal["repo", "batch"]
DEFAULT_CHECKLIST_LATEST_LIMIT = 10
STATUS_CHART_COLORS = {
    "completed": "#2E8B57",
    "in_progress": "#D98E04",
    "issue_raised": "#C96A00",
    "not_raised": "#9AA0A6",
}
PRESENTATION_STATUS_ORDER = ["not_raised", "issue_raised", "in_progress", "completed"]


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


def _presentation_status(repo_status: HipRepoStatus) -> str:
    if repo_status.status == "completed":
        return "completed"
    if repo_status.status in {"in_progress", "conflicting"}:
        return "in_progress"
    if repo_status.evidence_count > 0:
        return "issue_raised"
    return "not_raised"


def _repo_aliases(repos: list[str]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    used_aliases: set[str] = set()
    for repo in sorted(repos):
        base_name = repo.split("/", maxsplit=1)[-1]
        alias = base_name
        for prefix in ("hiero-sdk-", "hedera-sdk-", "sdk-"):
            if alias.startswith(prefix):
                alias = alias[len(prefix):]
                break
        alias = alias.replace("-", "_")
        if alias in used_aliases:
            alias = base_name.replace("-", "_")
        aliases[repo] = alias
        used_aliases.add(alias)
    return aliases


def _chart_scope_label(repos: list[str]) -> str:
    aliases = _repo_aliases(repos)
    ordered = [aliases[repo] for repo in sorted(repos)]
    if len(ordered) <= 4:
        return ", ".join(ordered)
    return f"{', '.join(ordered[:4])}, +{len(ordered) - 4} more"


def _repo_status_df(repo_statuses: list[HipRepoStatus]) -> pd.DataFrame:
    rows = [
        {
            "repo": repo_status.repo,
            "hip_id": repo_status.hip_id,
            "development_status": _presentation_status(repo_status),
            "raw_status": repo_status.status,
            "confidence_score": repo_status.confidence_score,
            "confidence_level": repo_status.confidence_level,
            "evidence_count": repo_status.evidence_count,
            "top_artifacts": _flatten_sequence(repo_status.top_artifacts),
            "top_reasons": _flatten_sequence(repo_status.top_reasons[:3]),
            "last_evidence_at": _format_datetime(repo_status.last_evidence_at),
        }
        for repo_status in sorted(repo_statuses, key=lambda row: (row.repo, hip_sort_key(row.hip_id)), reverse=True)
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "repo",
            "hip_id",
            "development_status",
            "raw_status",
            "confidence_score",
            "confidence_level",
            "evidence_count",
            "top_artifacts",
            "top_reasons",
            "last_evidence_at",
        ],
    )


def _repo_issue_likelihood_df(
    *,
    artifacts: list[HipArtifact],
    artifact_assessments: list[HipEvidence],
    repo_statuses: list[HipRepoStatus],
) -> pd.DataFrame:
    artifact_lookup = {(artifact.repo, artifact.number): artifact for artifact in artifacts}
    repo_status_lookup = {(repo_status.repo, repo_status.hip_id): repo_status for repo_status in repo_statuses}
    rows: list[dict[str, object]] = []
    for assessment in sorted(
        artifact_assessments,
        key=lambda row: (hip_sort_key(row.hip_id), row.confidence_score, row.artifact_number),
        reverse=True,
    ):
        if assessment.artifact_type != "issue":
            continue
        artifact = artifact_lookup.get((assessment.repo, assessment.artifact_number))
        if artifact is None:
            continue
        repo_status = repo_status_lookup.get((assessment.repo, assessment.hip_id))
        rows.append(
            {
                "repo": assessment.repo,
                "hip_id": assessment.hip_id,
                "hip_likelihood_score": assessment.confidence_score,
                "hip_likelihood_level": assessment.confidence_level,
                "development_status": _presentation_status(repo_status) if repo_status else assessment.status,
                "raw_issue_status": assessment.status,
                "issue_number": artifact.number,
                "issue_state": artifact.state,
                "issue_title": artifact.title,
                "issue_url": artifact.url,
            }
        )
    return pd.DataFrame(
        rows,
        columns=[
            "repo",
            "hip_id",
            "hip_likelihood_score",
            "hip_likelihood_level",
            "development_status",
            "raw_issue_status",
            "issue_number",
            "issue_state",
            "issue_title",
            "issue_url",
        ],
    )


def _repo_status_chart_df(repo_statuses: list[HipRepoStatus]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for repo_status in sorted(repo_statuses, key=lambda row: hip_sort_key(row.hip_id), reverse=True):
        status = _presentation_status(repo_status)
        rows.append(
            {
                "hip_id": repo_status.hip_id,
                "not_raised": 1 if status == "not_raised" else 0,
                "issue_raised": 1 if status == "issue_raised" else 0,
                "in_progress": 1 if status == "in_progress" else 0,
                "completed": 1 if status == "completed" else 0,
            }
        )
    return pd.DataFrame(rows, columns=["hip_id", *PRESENTATION_STATUS_ORDER])


def _sdk_status_matrix_df(
    repo_statuses: list[HipRepoStatus],
    *,
    catalog_entries: list[HipCatalogEntry],
) -> pd.DataFrame:
    repos = sorted({repo_status.repo for repo_status in repo_statuses})
    aliases = _repo_aliases(repos)
    lookup = {(repo_status.repo, repo_status.hip_id): _presentation_status(repo_status) for repo_status in repo_statuses}
    rows: list[dict[str, object]] = []
    for entry in sorted(catalog_entries, key=lambda item: hip_sort_key(item.hip_id), reverse=True):
        row: dict[str, object] = {"hip_id": entry.hip_id}
        for repo in repos:
            row[aliases[repo]] = lookup.get((repo, entry.hip_id), "not_raised")
        rows.append(row)
    return pd.DataFrame(rows, columns=["hip_id", *[aliases[repo] for repo in repos]])


def _status_rollup_df(
    repo_statuses: list[HipRepoStatus],
    *,
    catalog_entries: list[HipCatalogEntry],
    approved_only: bool = False,
) -> pd.DataFrame:
    grouped: dict[str, dict[str, object]] = {}
    for entry in catalog_entries:
        if approved_only and entry.status.strip().lower() != "approved":
            continue
        grouped[entry.hip_id] = {
            "hip_id": entry.hip_id,
            "catalog_status": entry.status,
            "not_raised_count": 0,
            "issue_raised_count": 0,
            "in_progress_count": 0,
            "completed_count": 0,
            "repos_with_issue_raised_count": 0,
            "completion_rate_percent": 0.0,
            "completed_repos": [],
        }

    for repo_status in repo_statuses:
        row = grouped.get(repo_status.hip_id)
        if row is None:
            continue
        status = _presentation_status(repo_status)
        row[f"{status}_count"] = int(row.get(f"{status}_count", 0) or 0) + 1
        if status != "not_raised":
            row["repos_with_issue_raised_count"] = int(row["repos_with_issue_raised_count"]) + 1
        if status == "completed":
            completed_repos = list(row["completed_repos"])
            completed_repos.append(repo_status.repo)
            row["completed_repos"] = sorted(completed_repos)

    rows: list[dict[str, object]] = []
    for hip_id, row in sorted(grouped.items(), key=lambda item: hip_sort_key(item[0]), reverse=True):
        completed_count = int(row["completed_count"])
        raised_repo_count = int(row["repos_with_issue_raised_count"])
        row["completion_rate_percent"] = round((completed_count / raised_repo_count) * 100, 2) if raised_repo_count else 0.0
        row["completed_repos"] = _flatten_sequence(list(row["completed_repos"]))
        rows.append(row)

    return pd.DataFrame(
        rows,
        columns=[
            "hip_id",
            "catalog_status",
            "not_raised_count",
            "issue_raised_count",
            "in_progress_count",
            "completed_count",
            "repos_with_issue_raised_count",
            "completion_rate_percent",
            "completed_repos",
        ],
    )


def _sdk_status_chart_df(repo_statuses: list[HipRepoStatus]) -> pd.DataFrame:
    repos = sorted({repo_status.repo for repo_status in repo_statuses})
    aliases = _repo_aliases(repos)
    grouped: dict[str, dict[str, int | str]] = {
        repo: {"sdk": aliases[repo], **{status: 0 for status in PRESENTATION_STATUS_ORDER}}
        for repo in repos
    }

    for repo_status in repo_statuses:
        status = _presentation_status(repo_status)
        row = grouped.get(repo_status.repo)
        if row is None:
            continue
        row[status] = int(row[status]) + 1

    rows = [grouped[repo] for repo in repos]
    return pd.DataFrame(rows, columns=["sdk", *PRESENTATION_STATUS_ORDER])


def _sdk_completion_rate_df(repo_statuses: list[HipRepoStatus]) -> pd.DataFrame:
    repos = sorted({repo_status.repo for repo_status in repo_statuses})
    aliases = _repo_aliases(repos)
    grouped: dict[str, dict[str, int | float | str]] = {
        repo: {"sdk": aliases[repo], "raised_hip_count": 0, "completed_count": 0, "completion_rate_percent": 0.0}
        for repo in repos
    }

    for repo_status in repo_statuses:
        row = grouped.get(repo_status.repo)
        if row is None:
            continue
        presentation_status = _presentation_status(repo_status)
        if presentation_status != "not_raised":
            row["raised_hip_count"] = int(row["raised_hip_count"]) + 1
        if repo_status.status == "completed":
            row["completed_count"] = int(row["completed_count"]) + 1

    rows: list[dict[str, object]] = []
    for repo in repos:
        row = grouped[repo]
        raised_hip_count = int(row["raised_hip_count"])
        completed_count = int(row["completed_count"])
        row["completion_rate_percent"] = round((completed_count / raised_hip_count) * 100, 2) if raised_hip_count else 0.0
        rows.append(row)

    return pd.DataFrame(rows, columns=["sdk", "raised_hip_count", "completed_count", "completion_rate_percent"])


def _write_bar_chart(
    df: pd.DataFrame,
    *,
    x_col: str,
    y_col: str,
    output_path: Path,
    title: str,
) -> None:
    if df.empty:
        return
    chart_df = df.copy()
    chart_df[x_col] = pd.Categorical(chart_df[x_col], categories=list(chart_df[x_col]), ordered=True)
    plot_bar(
        chart_df,
        x_col=x_col,
        y_col=y_col,
        title=title,
        output_path=output_path,
        rotate_x=20,
    )


def _filter_rows_with_positive_signal(
    df: pd.DataFrame,
    *,
    value_columns: list[str],
) -> pd.DataFrame:
    if df.empty:
        return df
    mask = df[value_columns].fillna(0).sum(axis=1) > 0
    return df.loc[mask].reset_index(drop=True)


def _cleanup_export_paths(paths: list[Path]) -> None:
    for path in paths:
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
            continue
        if path.exists():
            path.unlink()


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
    catalog_entries: list[HipCatalogEntry],
    feature_vectors: list[HipFeatureVector],
    artifact_assessments: list[HipEvidence] | None = None,
    evidence_records: list[HipEvidence] | None = None,
    repo_statuses: list[HipRepoStatus],
    dataset_splits: dict[tuple[str, int], str] | None = None,
    benchmark_exports: dict[str, list[dict[str, object]]] | None = None,
    export_profile: ExportProfile = "review",
    export_scope: ExportScope = "repo",
    checklist_latest_limit: int = DEFAULT_CHECKLIST_LATEST_LIMIT,
) -> dict[str, Path]:
    """Export minimal end-user HIP outputs plus evaluation/debug CSVs."""
    output_dir.mkdir(parents=True, exist_ok=True)
    evaluation_dir = output_dir / "evaluation"
    debug_dir = output_dir / "debug"
    _cleanup_export_paths(
        [
            output_dir / "hip_repo_summary.csv",
            output_dir / "hip_repo_summary.md",
            output_dir / "hip_checklist.md",
            output_dir / "hip_high_confidence_completion.csv",
            output_dir / "hip_high_confidence_completion.md",
            output_dir / "hip_evidence_detail.csv",
            output_dir / "hip_evidence_detail.md",
            output_dir / "recent_hip_status_counts.csv",
            output_dir / "recent_hip_status_counts.png",
            output_dir / "sdk_completion_counts.csv",
            output_dir / "sdk_completion_counts.png",
            output_dir / "manual_accuracy_review.csv",
            output_dir / "manual_accuracy_report.md",
            output_dir / "benchmark_report.md",
            output_dir / "benchmark_report.json",
            output_dir / "hip_repo_status.csv",
            output_dir / "hip_repo_status.md",
            output_dir / "artifacts.csv",
            output_dir / "artifact_features.csv",
            output_dir / "artifact_features.md",
            output_dir / "hip_evidence.csv",
            output_dir / "hip_evidence.md",
            output_dir / "pr_evaluation.csv",
            output_dir / "pr_evaluation.md",
            output_dir / "issue_evaluation.csv",
            output_dir / "issue_evaluation.md",
            output_dir / "repo_evaluation.csv",
            output_dir / "repo_evaluation.md",
            output_dir / "prediction_review_breakdown.csv",
            output_dir / "prediction_review_breakdown.md",
            output_dir / "evaluation_summary.csv",
            output_dir / "evaluation_summary.md",
            evaluation_dir / "manual_accuracy_report.md",
            evaluation_dir / "benchmark_report.md",
            evaluation_dir / "benchmark_report.json",
        ]
    )
    if export_profile != "full":
        _cleanup_export_paths([debug_dir])
    dataset_splits = dataset_splits or assign_dataset_splits(artifacts)
    artifact_assessments = artifact_assessments or evidence_records or []
    artifact_evaluation_rows = build_artifact_evaluation_rows(
        artifacts=artifacts,
        artifact_assessments=artifact_assessments,
        dataset_splits=dataset_splits,
    )
    artifact_predictions_path = evaluation_dir / "artifact_predictions.csv"
    repo_predictions_path = evaluation_dir / "repo_predictions.csv"
    manual_accuracy_review_path = evaluation_dir / "manual_accuracy_review.csv"
    accuracy_summary_path = evaluation_dir / "accuracy_summary.csv"
    review_breakdown_path = evaluation_dir / "review_breakdown.csv"
    benchmark_metrics_path = evaluation_dir / "benchmark_metrics.csv"
    benchmark_confusion_path = evaluation_dir / "benchmark_confusion_matrix.csv"
    benchmark_per_status_path = evaluation_dir / "benchmark_per_status.csv"

    artifact_predictions_df = _preserve_feedback_rows(
        artifact_evaluation_rows,
        artifact_predictions_path,
        key_columns=["repo", "hip_id", "artifact_number"],
        columns=ARTIFACT_EVALUATION_COLUMNS,
        preserved_columns=MANUAL_REVIEW_COLUMNS,
        keep_missing_existing_rows=False,
    )
    repo_predictions_df = _preserve_feedback_rows(
        build_repo_evaluation_rows(
            artifacts=artifacts,
            repo_statuses=repo_statuses,
            dataset_splits=dataset_splits,
        ),
        repo_predictions_path,
        key_columns=["repo", "hip_id"],
        columns=REPO_EVALUATION_COLUMNS,
        preserved_columns=MANUAL_REVIEW_COLUMNS,
        keep_missing_existing_rows=False,
    )
    pr_evaluation_df = artifact_predictions_df[artifact_predictions_df["artifact_type"] == "pull_request"].copy()
    issue_evaluation_df = artifact_predictions_df[artifact_predictions_df["artifact_type"] == "issue"].copy()
    repo_evaluation_df = repo_predictions_df.copy()
    pr_review_breakdown_rows = _review_breakdown_rows("pull_request", pr_evaluation_df)
    issue_review_breakdown_rows = _review_breakdown_rows("issue", issue_evaluation_df)
    repo_review_breakdown_rows = _review_breakdown_rows("repo", repo_evaluation_df)
    review_breakdown_df = pd.DataFrame(
        [*pr_review_breakdown_rows, *issue_review_breakdown_rows, *repo_review_breakdown_rows],
        columns=PREDICTION_REVIEW_BREAKDOWN_COLUMNS,
    )
    accuracy_summary_df = pd.DataFrame(
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

    save_dataframe(artifact_predictions_df, artifact_predictions_path)
    save_dataframe(repo_predictions_df, repo_predictions_path)
    save_dataframe(review_breakdown_df, review_breakdown_path)
    save_dataframe(accuracy_summary_df, accuracy_summary_path)
    save_dataframe(manual_accuracy_df, manual_accuracy_review_path)

    export_paths: dict[str, Path] = {
        "artifact_predictions": artifact_predictions_path,
        "repo_predictions": repo_predictions_path,
        "review_breakdown": review_breakdown_path,
        "accuracy_summary": accuracy_summary_path,
        "manual_accuracy_review": manual_accuracy_review_path,
    }

    if benchmark_exports:
        benchmark_metrics_df = pd.DataFrame(benchmark_exports.get("metrics", []), columns=BENCHMARK_METRIC_COLUMNS)
        benchmark_confusion_df = pd.DataFrame(benchmark_exports.get("confusion", []), columns=CONFUSION_MATRIX_COLUMNS)
        benchmark_per_status_df = pd.DataFrame(benchmark_exports.get("per_status", []), columns=PER_STATUS_METRIC_COLUMNS)
        save_dataframe(benchmark_metrics_df, benchmark_metrics_path)
        save_dataframe(benchmark_confusion_df, benchmark_confusion_path)
        save_dataframe(benchmark_per_status_df, benchmark_per_status_path)
        export_paths["benchmark_metrics"] = benchmark_metrics_path
        export_paths["benchmark_confusion_matrix"] = benchmark_confusion_path
        export_paths["benchmark_per_status"] = benchmark_per_status_path

    if export_scope == "repo":
        repo_status_path = output_dir / "repo_hip_status.csv"
        repo_issue_path = output_dir / "repo_hip_issues.csv"
        repo_status_chart_path = output_dir / "repo_hip_status.png"
        repo_scope_label = _chart_scope_label(sorted({repo_status.repo for repo_status in repo_statuses}))

        repo_status_df = _repo_status_df(repo_statuses)
        repo_issue_df = _repo_issue_likelihood_df(
            artifacts=artifacts,
            artifact_assessments=artifact_assessments,
            repo_statuses=repo_statuses,
        )
        repo_chart_df = _repo_status_chart_df(repo_statuses)

        save_dataframe(repo_status_df, repo_status_path)
        save_dataframe(repo_issue_df, repo_issue_path)
        _write_status_chart(
            repo_chart_df,
            x_col="hip_id",
            stack_cols=PRESENTATION_STATUS_ORDER,
            labels=PRESENTATION_STATUS_ORDER,
            output_path=repo_status_chart_path,
            title=f"Repo HIP Development Status | {repo_scope_label}",
            colors=STATUS_CHART_COLORS,
        )

        export_paths["repo_hip_status"] = repo_status_path
        export_paths["repo_hip_issues"] = repo_issue_path
        export_paths["repo_hip_status_chart"] = repo_status_chart_path
    else:
        sdk_status_matrix_path = output_dir / "sdk_hip_status_matrix.csv"
        sdk_rollup_path = output_dir / "sdk_hip_rollup.csv"
        sdk_status_chart_path = output_dir / "sdk_hip_development_status.png"
        sdk_completion_chart_path = output_dir / "sdk_hip_completion_rate.png"
        approved_rollup_path = output_dir / "approved_hip_org_rollup.csv"
        approved_rollup_chart_path = output_dir / "approved_hip_org_rollup.png"
        sdk_scope_label = _chart_scope_label(sorted({repo_status.repo for repo_status in repo_statuses}))

        sdk_status_matrix_df = _sdk_status_matrix_df(repo_statuses, catalog_entries=catalog_entries)
        sdk_rollup_df = _status_rollup_df(repo_statuses, catalog_entries=catalog_entries, approved_only=False)
        sdk_status_chart_df = _sdk_status_chart_df(repo_statuses)
        sdk_completion_chart_df = _sdk_completion_rate_df(repo_statuses)
        approved_rollup_df = _status_rollup_df(repo_statuses, catalog_entries=catalog_entries, approved_only=True)
        approved_rollup_chart_df = _filter_rows_with_positive_signal(
            approved_rollup_df,
            value_columns=["issue_raised_count", "in_progress_count", "completed_count"],
        )

        save_dataframe(sdk_status_matrix_df, sdk_status_matrix_path)
        save_dataframe(sdk_rollup_df, sdk_rollup_path)
        save_dataframe(approved_rollup_df, approved_rollup_path)

        _write_status_chart(
            sdk_status_chart_df,
            x_col="sdk",
            stack_cols=PRESENTATION_STATUS_ORDER,
            labels=PRESENTATION_STATUS_ORDER,
            output_path=sdk_status_chart_path,
            title=f"SDK HIP Development Status | {sdk_scope_label}",
            colors=STATUS_CHART_COLORS,
        )
        _write_bar_chart(
            sdk_completion_chart_df,
            x_col="sdk",
            y_col="completion_rate_percent",
            output_path=sdk_completion_chart_path,
            title=f"SDK HIP Completion Rate Across Raised HIPs | {sdk_scope_label}",
        )
        _write_status_chart(
            approved_rollup_chart_df,
            x_col="hip_id",
            stack_cols=["issue_raised_count", "in_progress_count", "completed_count"],
            labels=["issue_raised", "in_progress", "completed"],
            output_path=approved_rollup_chart_path,
            title=f"Approved HIP Rollup Across SDKs | {sdk_scope_label}",
            colors=STATUS_CHART_COLORS,
        )

        export_paths["sdk_hip_status_matrix"] = sdk_status_matrix_path
        export_paths["sdk_hip_rollup"] = sdk_rollup_path
        export_paths["sdk_hip_development_status_chart"] = sdk_status_chart_path
        export_paths["sdk_hip_completion_rate_chart"] = sdk_completion_chart_path
        export_paths["approved_hip_org_rollup"] = approved_rollup_path
        export_paths["approved_hip_org_rollup_chart"] = approved_rollup_chart_path

    if export_profile == "full":
        artifact_path = debug_dir / "artifacts.csv"
        feature_path = debug_dir / "artifact_features.csv"
        assessment_path = debug_dir / "artifact_assessments.csv"
        evidence_detail_path = debug_dir / "evidence_detail.csv"

        artifact_df = pd.DataFrame(_artifact_rows(artifacts))
        feature_df = pd.DataFrame(_feature_rows(feature_vectors))
        assessment_df = pd.DataFrame(_artifact_assessment_rows(artifact_assessments))
        evidence_detail_df = pd.DataFrame(_evidence_detail_rows(artifact_assessments))

        save_dataframe(artifact_df, artifact_path)
        save_dataframe(feature_df, feature_path)
        save_dataframe(assessment_df, assessment_path)
        save_dataframe(evidence_detail_df, evidence_detail_path)

        export_paths.update(
            {
                "artifacts": artifact_path,
                "artifact_features": feature_path,
                "artifact_assessments": assessment_path,
                "evidence_detail": evidence_detail_path,
            }
        )

    return export_paths
