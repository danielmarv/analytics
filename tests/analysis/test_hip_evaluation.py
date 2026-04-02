"""Tests for HIP evaluation helpers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from hiero_analytics.analysis.hip_evaluation import (
    assign_dataset_splits,
    build_artifact_evaluation_rows,
    build_manual_accuracy_rows,
)
from hiero_analytics.analysis.hip_scoring import score_candidate
from hiero_analytics.analysis.hip_status_aggregation import aggregate_hip_repo_status
from tests.hip_progression_fixtures import (
    make_candidate,
    make_catalog_entries,
    make_issue_artifact,
    make_pull_request_artifact,
)


def test_assign_dataset_splits_reserves_latest_twenty_percent_per_artifact_type():
    """Issues and PRs should each keep their newest 20 percent as held-out test data."""
    base_time = datetime(2025, 1, 1, tzinfo=UTC)
    artifacts = []

    for offset in range(5):
        issue = make_issue_artifact(number=100 + offset)
        issue.created_at = base_time + timedelta(days=offset)
        issue.updated_at = base_time + timedelta(days=offset)
        artifacts.append(issue)

        pull_request = make_pull_request_artifact(number=200 + offset)
        pull_request.created_at = base_time + timedelta(days=offset)
        pull_request.updated_at = base_time + timedelta(days=offset)
        artifacts.append(pull_request)

    splits = assign_dataset_splits(artifacts, train_ratio=0.8)

    assert splits[("hiero-ledger/hiero-sdk-js", 104)] == "test"
    assert splits[("hiero-ledger/hiero-sdk-js", 204)] == "test"
    assert splits[("hiero-ledger/hiero-sdk-js", 103)] == "train"
    assert splits[("hiero-ledger/hiero-sdk-js", 203)] == "train"


def test_build_artifact_evaluation_rows_include_unmatched_artifacts():
    """Artifacts without predictions should still appear for manual missed-match review."""
    artifact = make_issue_artifact(title="General issue", body="No HIP prediction here.")

    rows = build_artifact_evaluation_rows(
        artifacts=[artifact],
        assessments=[],
        dataset_splits=assign_dataset_splits([artifact]),
    )

    assert len(rows) == 1
    assert rows[0]["prediction_present"] is False
    assert rows[0]["hip_id"] == ""


def test_build_manual_accuracy_rows_combine_pr_issue_and_repo_scopes():
    """Combined manual accuracy rows should carry review scope labels."""
    artifact = make_pull_request_artifact()
    assessment = score_candidate(make_candidate(artifact))
    repo_status = aggregate_hip_repo_status(
        [assessment],
        artifacts=[artifact],
        catalog_entries=make_catalog_entries("HIP-1234"),
        repos=["hiero-ledger/hiero-sdk-js"],
    )[0]

    rows = build_manual_accuracy_rows(
        artifacts=[artifact],
        assessments=[assessment],
        repo_statuses=[repo_status],
        dataset_splits=assign_dataset_splits([artifact]),
    )

    assert any(row["review_scope"] == "pull_request" for row in rows)
    assert any(row["review_scope"] == "repo" for row in rows)
