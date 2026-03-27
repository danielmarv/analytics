"""Tests for HIP evaluation helpers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from hiero_analytics.analysis.hip_evaluation import (
    assign_dataset_splits,
    build_artifact_evaluation_rows,
    build_manual_accuracy_rows,
    evaluate_status_predictions,
    load_benchmark_dataset,
)
from hiero_analytics.analysis.hip_feature_engineering import engineer_hip_feature_vector
from hiero_analytics.analysis.hip_scoring import score_hip_feature_vector
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


def test_load_benchmark_dataset_reads_checked_in_fixtures():
    """The curated benchmark fixture directory should load all core sections."""
    benchmark_dir = Path("tests/fixtures/hip_progression_benchmark")
    catalog_entries, artifacts, artifact_expectations, repo_expectations = load_benchmark_dataset(benchmark_dir)

    assert len(catalog_entries) == 5
    assert len(artifacts) >= 5
    assert any(expectation.expected_status == "completed" for expectation in artifact_expectations)
    assert any(expectation.expected_status == "conflicting" for expectation in repo_expectations)


def test_evaluate_status_predictions_computes_metrics_and_confusion():
    """Benchmark scoring should export stable metrics and confusion rows."""
    expectations = [
        (("repo", "HIP-1"), "completed"),
        (("repo", "HIP-2"), "in_progress"),
        (("repo", "HIP-3"), "not_started"),
    ]
    predictions = {
        ("repo", "HIP-1"): "completed",
        ("repo", "HIP-2"): "completed",
        ("repo", "HIP-3"): "not_started",
    }

    metrics, confusion, per_status = evaluate_status_predictions(
        scope="repo",
        expectations=expectations,
        predictions=predictions,
    )

    assert metrics[0]["coverage_percent"] == 100.0
    assert metrics[0]["accuracy_percent"] == 66.67
    assert metrics[0]["overcall_rate_percent"] == 33.33
    assert any(row["expected_status"] == "in_progress" and row["predicted_status"] == "completed" for row in confusion)
    assert any(row["status"] == "completed" for row in per_status)


def test_build_artifact_evaluation_rows_include_unmatched_artifacts_for_missed_review():
    """Artifacts without predictions should still appear for manual missed-match review."""
    artifact = make_issue_artifact(title="General issue", body="No HIP prediction here.")

    rows = build_artifact_evaluation_rows(
        artifacts=[artifact],
        artifact_assessments=[],
        dataset_splits=assign_dataset_splits([artifact]),
    )

    assert len(rows) == 1
    assert rows[0]["prediction_present"] is False
    assert rows[0]["hip_id"] == ""
    assert rows[0]["artifact_link"] == "[Issue #77](https://github.com/hiero-ledger/hiero-sdk-js/issues/77)"
    assert rows[0]["top_reasons"] == "No HIP prediction generated for this artifact."


def test_build_manual_accuracy_rows_combine_pr_issue_and_repo_review_data():
    """Combined manual accuracy rows should carry review scope and generated links."""
    artifact = make_pull_request_artifact()
    feature_vector = engineer_hip_feature_vector(make_candidate(artifact))
    assessment = score_hip_feature_vector(feature_vector)
    repo_status = aggregate_hip_repo_status(
        [assessment],
        artifacts=[artifact],
        catalog_entries=make_catalog_entries("HIP-1234"),
        repos=["hiero-ledger/hiero-sdk-js"],
    )[0]

    rows = build_manual_accuracy_rows(
        artifacts=[artifact],
        artifact_assessments=[assessment],
        repo_statuses=[repo_status],
        dataset_splits=assign_dataset_splits([artifact]),
    )

    assert any(row["review_scope"] == "pull_request" and row["artifact_link"] for row in rows)
    assert any(row["review_scope"] == "repo" and row["supporting_artifact_links"] for row in rows)
