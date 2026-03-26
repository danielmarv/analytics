"""Tests for HIP evaluation helpers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from hiero_analytics.analysis.hip_evaluation import assign_dataset_splits
from tests.hip_progression_fixtures import make_issue_artifact, make_pull_request_artifact


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
