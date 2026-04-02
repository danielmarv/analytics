"""Tests for HIP progression CSV and chart exports."""

from __future__ import annotations

import pandas as pd

from hiero_analytics.analysis.hip_evaluation import assign_dataset_splits
from hiero_analytics.analysis.hip_scoring import score_candidate
from hiero_analytics.analysis.hip_status_aggregation import aggregate_hip_repo_status
from hiero_analytics.domain.hip_progression_models import HipRepoStatus
from hiero_analytics.export.hip_progression_export import (
    _sdk_completion_df,
    _sdk_status_chart_df,
    export_hip_progression_results,
)
from tests.hip_progression_fixtures import make_candidate, make_catalog_entries, make_pull_request_artifact


def test_export_repo_scope_writes_status_and_evaluation_outputs(tmp_path):
    """Repo scope should write end-user CSVs and evaluation CSVs."""
    artifact = make_pull_request_artifact()
    candidate = make_candidate(artifact)
    assessment = score_candidate(candidate)
    catalog_entries = make_catalog_entries("HIP-1234")
    repo_statuses = aggregate_hip_repo_status(
        [assessment],
        artifacts=[artifact],
        catalog_entries=catalog_entries,
        repos=["hiero-ledger/hiero-sdk-js"],
    )

    paths = export_hip_progression_results(
        tmp_path,
        artifacts=[artifact],
        catalog_entries=catalog_entries,
        assessments=[assessment],
        repo_statuses=repo_statuses,
        dataset_splits=assign_dataset_splits([artifact]),
        export_scope="repo",
    )

    assert paths["repo_hip_status"].exists()
    assert paths["repo_hip_issues"].exists()
    assert paths["repo_hip_status_chart"].exists()
    assert paths["artifact_predictions"].exists()
    assert paths["repo_predictions"].exists()
    assert paths["manual_accuracy_review"].exists()
    assert "sdk_hip_status_matrix" not in paths

    repo_status_df = pd.read_csv(paths["repo_hip_status"], keep_default_na=False)
    assert "hip_id" in repo_status_df.columns
    assert "status" in repo_status_df.columns
    assert "confidence" in repo_status_df.columns
    assert repo_status_df.loc[0, "hip_id"] == "HIP-1234"


def test_export_batch_scope_writes_sdk_matrix_and_rollup(tmp_path):
    """Batch scope should write the SDK matrix and rollup CSVs."""
    js_artifact = make_pull_request_artifact(repo="hiero-ledger/hiero-sdk-js", number=101)
    java_artifact = make_pull_request_artifact(repo="hiero-ledger/hiero-sdk-java", number=202)
    js_assessment = score_candidate(make_candidate(js_artifact))
    java_assessment = score_candidate(make_candidate(java_artifact))
    catalog_entries = make_catalog_entries("HIP-1234", "HIP-1200")
    repo_statuses = aggregate_hip_repo_status(
        [js_assessment, java_assessment],
        artifacts=[js_artifact, java_artifact],
        catalog_entries=catalog_entries,
        repos=["hiero-ledger/hiero-sdk-js", "hiero-ledger/hiero-sdk-java"],
    )

    paths = export_hip_progression_results(
        tmp_path,
        artifacts=[js_artifact, java_artifact],
        catalog_entries=catalog_entries,
        assessments=[js_assessment, java_assessment],
        repo_statuses=repo_statuses,
        dataset_splits=assign_dataset_splits([js_artifact, java_artifact]),
        export_scope="batch",
    )

    assert paths["sdk_hip_status_matrix"].exists()
    assert paths["sdk_hip_rollup"].exists()
    assert paths["sdk_hip_development_status_chart"].exists()
    assert paths["sdk_hip_completion_rate_chart"].exists()
    assert "repo_hip_status" not in paths

    matrix_df = pd.read_csv(paths["sdk_hip_status_matrix"], keep_default_na=False)
    assert list(matrix_df.columns) == ["hip_id", "java", "js"]
    assert matrix_df.iloc[0]["hip_id"] == "HIP-1234"


def test_export_batch_scope_completion_rate_uses_raised_repos_only(tmp_path):
    """Completion rate should be measured against repos that raised the HIP, not all repos."""
    catalog_entries = make_catalog_entries("HIP-1234")
    repo_statuses = [
        HipRepoStatus(
            repo="hiero-ledger/hiero-sdk-js",
            hip_id="HIP-1234",
            status="completed",
            confidence="high",
            supporting_artifact_numbers=[10],
            top_artifacts=["PR #10"],
        ),
        HipRepoStatus(
            repo="hiero-ledger/hiero-sdk-python",
            hip_id="HIP-1234",
            status="raised",
            confidence="low",
            supporting_artifact_numbers=[20],
            top_artifacts=["Issue #20"],
        ),
        HipRepoStatus(
            repo="hiero-ledger/hiero-sdk-go",
            hip_id="HIP-1234",
            status="not_raised",
            confidence="low",
        ),
    ]

    paths = export_hip_progression_results(
        tmp_path,
        artifacts=[],
        catalog_entries=catalog_entries,
        assessments=[],
        repo_statuses=repo_statuses,
        export_scope="batch",
    )

    rollup_df = pd.read_csv(paths["sdk_hip_rollup"], keep_default_na=False)
    assert rollup_df.loc[0, "repos_with_issue_raised_count"] == 2
    assert rollup_df.loc[0, "completion_rate_percent"] == 50.0


def test_sdk_chart_dfs_use_sdk_aliases():
    """Batch chart helpers should aggregate by SDK alias, not HIP id."""
    repo_statuses = [
        HipRepoStatus(
            repo="hiero-ledger/hiero-sdk-js",
            hip_id="HIP-1234",
            status="completed",
            confidence="high",
            supporting_artifact_numbers=[10],
            top_artifacts=["PR #10"],
        ),
        HipRepoStatus(
            repo="hiero-ledger/hiero-sdk-js",
            hip_id="HIP-1200",
            status="raised",
            confidence="low",
            supporting_artifact_numbers=[20],
            top_artifacts=["Issue #20"],
        ),
        HipRepoStatus(
            repo="hiero-ledger/hiero-sdk-python",
            hip_id="HIP-1234",
            status="not_raised",
            confidence="low",
        ),
    ]

    status_chart_df = _sdk_status_chart_df(repo_statuses)
    completion_df = _sdk_completion_df(repo_statuses)

    assert list(status_chart_df["sdk"]) == ["js", "python"]
    assert status_chart_df.loc[0, "completed"] == 1
    assert status_chart_df.loc[0, "raised"] == 1
    assert status_chart_df.loc[1, "not_raised"] == 1

    assert list(completion_df["sdk"]) == ["js", "python"]
    assert completion_df.loc[0, "raised_count"] == 2
    assert completion_df.loc[0, "completion_rate_percent"] == 50.0
    assert completion_df.loc[1, "completion_rate_percent"] == 0.0


def test_export_repo_scope_drops_rows_outside_current_scope(tmp_path):
    """Scoped reruns should replace the repo status CSV with the current HIP scope only."""
    wide_scope_statuses = aggregate_hip_repo_status(
        [],
        artifacts=[],
        catalog_entries=make_catalog_entries(*[f"HIP-{number}" for number in range(1000, 1012)]),
        repos=["hiero-ledger/hiero-sdk-js"],
    )
    narrow_catalog = make_catalog_entries(*[f"HIP-{number}" for number in range(1002, 1012)])
    narrow_scope_statuses = aggregate_hip_repo_status(
        [],
        artifacts=[],
        catalog_entries=narrow_catalog,
        repos=["hiero-ledger/hiero-sdk-js"],
    )

    first_paths = export_hip_progression_results(
        tmp_path,
        artifacts=[],
        catalog_entries=make_catalog_entries(*[f"HIP-{number}" for number in range(1000, 1012)]),
        assessments=[],
        repo_statuses=wide_scope_statuses,
        export_scope="repo",
    )
    first_df = pd.read_csv(first_paths["repo_hip_status"], keep_default_na=False)
    second_paths = export_hip_progression_results(
        tmp_path,
        artifacts=[],
        catalog_entries=narrow_catalog,
        assessments=[],
        repo_statuses=narrow_scope_statuses,
        export_scope="repo",
    )

    second_df = pd.read_csv(second_paths["repo_hip_status"], keep_default_na=False)

    assert len(first_df) == 12
    assert len(second_df) == 10
    assert "HIP-1001" not in set(second_df["hip_id"])
    assert "HIP-1000" not in set(second_df["hip_id"])


def test_export_preserves_manual_feedback_in_evaluation_csvs(tmp_path):
    """Manual feedback columns in evaluation CSVs should survive reruns."""
    artifact = make_pull_request_artifact()
    assessment = score_candidate(make_candidate(artifact))
    catalog_entries = make_catalog_entries("HIP-1234")
    repo_statuses = aggregate_hip_repo_status(
        [assessment],
        artifacts=[artifact],
        catalog_entries=catalog_entries,
        repos=["hiero-ledger/hiero-sdk-js"],
    )

    first_paths = export_hip_progression_results(
        tmp_path,
        artifacts=[artifact],
        catalog_entries=catalog_entries,
        assessments=[assessment],
        repo_statuses=repo_statuses,
        export_profile="full",
        export_scope="repo",
    )

    manual_df = pd.read_csv(first_paths["manual_accuracy_review"], keep_default_na=False)
    manual_df.loc[0, "human_observation"] = "Reviewed manually."
    manual_df.loc[0, "is_prediction_correct"] = "true"
    manual_df.to_csv(first_paths["manual_accuracy_review"], index=False)

    artifact_predictions_df = pd.read_csv(first_paths["artifact_predictions"], keep_default_na=False)
    artifact_predictions_df.loc[0, "human_observation"] = "Artifact check passed."
    artifact_predictions_df.to_csv(first_paths["artifact_predictions"], index=False)

    second_paths = export_hip_progression_results(
        tmp_path,
        artifacts=[artifact],
        catalog_entries=catalog_entries,
        assessments=[assessment],
        repo_statuses=repo_statuses,
        export_profile="full",
        export_scope="repo",
    )

    rerun_manual_df = pd.read_csv(second_paths["manual_accuracy_review"], keep_default_na=False)
    rerun_artifact_predictions_df = pd.read_csv(second_paths["artifact_predictions"], keep_default_na=False)

    assert rerun_manual_df.loc[0, "human_observation"] == "Reviewed manually."
    assert rerun_manual_df.loc[0, "is_prediction_correct"] in {True, "True", "true"}
    assert rerun_artifact_predictions_df.loc[0, "human_observation"] == "Artifact check passed."
