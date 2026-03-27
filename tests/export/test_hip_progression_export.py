"""Tests for HIP progression markdown exports."""

from __future__ import annotations

import pandas as pd

from hiero_analytics.analysis.hip_evaluation import assign_dataset_splits
from hiero_analytics.analysis.hip_feature_engineering import engineer_hip_feature_vector
from hiero_analytics.analysis.hip_scoring import score_hip_feature_vector
from hiero_analytics.analysis.hip_status_aggregation import aggregate_hip_repo_status
from hiero_analytics.export.hip_progression_export import export_hip_progression_results
from hiero_analytics.export.save import save_markdown_table
from tests.hip_progression_fixtures import make_candidate, make_catalog_entries, make_pull_request_artifact


def test_save_markdown_table_escapes_pipes_and_newlines(tmp_path):
    """Markdown export should escape pipes and preserve line breaks safely."""
    path = tmp_path / "table.md"
    df = pd.DataFrame([{"summary": "line one\nline | two"}])

    save_markdown_table(df, path)

    contents = path.read_text(encoding="utf-8")
    assert "| summary |" in contents
    assert "line one<br>line \\| two" in contents


def test_export_hip_progression_results_writes_new_summary_and_checklist_outputs(tmp_path):
    """Default HIP progression export should emit the lean single-repo reviewer bundle."""
    artifact = make_pull_request_artifact()
    candidate = make_candidate(artifact)
    feature_vector = engineer_hip_feature_vector(candidate)
    assessment = score_hip_feature_vector(feature_vector)
    repo_status = aggregate_hip_repo_status(
        [assessment],
        artifacts=[artifact],
        catalog_entries=make_catalog_entries("HIP-1234"),
        repos=["hiero-ledger/hiero-sdk-js"],
    )[0]

    paths = export_hip_progression_results(
        tmp_path,
        artifacts=[artifact],
        feature_vectors=[feature_vector],
        artifact_assessments=[assessment],
        repo_statuses=[repo_status],
        dataset_splits=assign_dataset_splits([artifact]),
    )

    assert paths["hip_evidence_detail_markdown"].exists()
    assert paths["hip_repo_summary_markdown"].exists()
    assert paths["hip_checklist_markdown"].exists()
    assert paths["hip_high_confidence_completion"].exists()
    assert paths["manual_accuracy_review"].exists()
    assert paths["manual_accuracy_report"].exists()
    assert "recent_hip_status_counts" not in paths
    assert "recent_hip_status_chart" not in paths
    assert "sdk_completion_counts" not in paths
    assert "sdk_completion_chart" not in paths
    assert "artifact_features_markdown" not in paths
    assert "hip_evidence_markdown" not in paths
    assert "pr_evaluation" not in paths

    summary_markdown = paths["hip_repo_summary_markdown"].read_text(encoding="utf-8")
    checklist_markdown = paths["hip_checklist_markdown"].read_text(encoding="utf-8")
    accuracy_report = paths["manual_accuracy_report"].read_text(encoding="utf-8")

    assert "| repo | hip_id | rag_label | status |" in summary_markdown
    assert "confidence_score" in summary_markdown
    assert "## hiero-ledger/hiero-sdk-js" in checklist_markdown
    assert "HIP-1234" in checklist_markdown
    assert "## Pull Request Review Queue" in accuracy_report
    assert "is_prediction_correct" in accuracy_report


def test_export_hip_progression_results_writes_cross_repo_charts_for_multi_repo_batches(tmp_path):
    """Cross-repo chart outputs should appear when more than one repo is in scope."""
    js_artifact = make_pull_request_artifact(repo="hiero-ledger/hiero-sdk-js", number=101)
    java_artifact = make_pull_request_artifact(repo="hiero-ledger/hiero-sdk-java", number=202)
    js_feature_vector = engineer_hip_feature_vector(make_candidate(js_artifact))
    java_feature_vector = engineer_hip_feature_vector(make_candidate(java_artifact))
    js_assessment = score_hip_feature_vector(js_feature_vector)
    java_assessment = score_hip_feature_vector(java_feature_vector)
    repo_statuses = aggregate_hip_repo_status(
        [js_assessment, java_assessment],
        artifacts=[js_artifact, java_artifact],
        catalog_entries=make_catalog_entries("HIP-1234"),
        repos=["hiero-ledger/hiero-sdk-js", "hiero-ledger/hiero-sdk-java"],
    )

    paths = export_hip_progression_results(
        tmp_path,
        artifacts=[js_artifact, java_artifact],
        feature_vectors=[js_feature_vector, java_feature_vector],
        artifact_assessments=[js_assessment, java_assessment],
        repo_statuses=repo_statuses,
        dataset_splits=assign_dataset_splits([js_artifact, java_artifact]),
    )

    assert paths["recent_hip_status_counts"].exists()
    assert paths["recent_hip_status_chart"].exists()
    assert paths["sdk_completion_counts"].exists()
    assert paths["sdk_completion_chart"].exists()


def test_export_hip_progression_results_limits_checklist_to_latest_ten_hips(tmp_path):
    """Checklist output should focus on the newest HIPs only."""
    hip_ids = [f"HIP-{number}" for number in range(1000, 1012)]
    repo_statuses = aggregate_hip_repo_status(
        [],
        artifacts=[],
        catalog_entries=make_catalog_entries(*hip_ids),
        repos=["hiero-ledger/hiero-sdk-js"],
    )

    paths = export_hip_progression_results(
        tmp_path,
        artifacts=[],
        feature_vectors=[],
        artifact_assessments=[],
        repo_statuses=repo_statuses,
        checklist_latest_limit=10,
    )

    checklist_markdown = paths["hip_checklist_markdown"].read_text(encoding="utf-8")
    assert "HIP-1011" in checklist_markdown
    assert "HIP-1010" in checklist_markdown
    assert "HIP-1002" in checklist_markdown
    assert "HIP-1001" not in checklist_markdown
    assert "HIP-1000" not in checklist_markdown


def test_export_hip_progression_results_drops_summary_rows_outside_current_scope(tmp_path):
    """Scoped reruns should not keep stale summary rows from older, wider exports."""
    wide_scope_statuses = aggregate_hip_repo_status(
        [],
        artifacts=[],
        catalog_entries=make_catalog_entries(*[f"HIP-{number}" for number in range(1000, 1012)]),
        repos=["hiero-ledger/hiero-sdk-js"],
    )
    narrow_scope_statuses = aggregate_hip_repo_status(
        [],
        artifacts=[],
        catalog_entries=make_catalog_entries(*[f"HIP-{number}" for number in range(1002, 1012)]),
        repos=["hiero-ledger/hiero-sdk-js"],
    )

    first_paths = export_hip_progression_results(
        tmp_path,
        artifacts=[],
        feature_vectors=[],
        artifact_assessments=[],
        repo_statuses=wide_scope_statuses,
    )
    first_summary_df = pd.read_csv(first_paths["hip_repo_summary"], keep_default_na=False)
    second_paths = export_hip_progression_results(
        tmp_path,
        artifacts=[],
        feature_vectors=[],
        artifact_assessments=[],
        repo_statuses=narrow_scope_statuses,
    )

    second_summary_df = pd.read_csv(second_paths["hip_repo_summary"], keep_default_na=False)

    assert len(first_summary_df) == 12
    assert len(second_summary_df) == 10
    assert "HIP-1001" not in set(second_summary_df["hip_id"])
    assert "HIP-1000" not in set(second_summary_df["hip_id"])


def test_export_hip_progression_results_preserves_reviewer_notes_and_manual_feedback(tmp_path):
    """Editable reviewer notes and manual review columns should survive reruns."""
    artifact = make_pull_request_artifact()
    candidate = make_candidate(artifact)
    feature_vector = engineer_hip_feature_vector(candidate)
    assessment = score_hip_feature_vector(feature_vector)
    repo_status = aggregate_hip_repo_status(
        [assessment],
        artifacts=[artifact],
        catalog_entries=make_catalog_entries("HIP-1234"),
        repos=["hiero-ledger/hiero-sdk-js"],
    )[0]

    first_paths = export_hip_progression_results(
        tmp_path,
        artifacts=[artifact],
        feature_vectors=[feature_vector],
        artifact_assessments=[assessment],
        repo_statuses=[repo_status],
        export_profile="full",
    )

    summary_df = pd.read_csv(first_paths["hip_repo_summary"], keep_default_na=False)
    summary_df.loc[0, "reviewer_notes"] = "Needs maintainer validation."
    summary_df.to_csv(first_paths["hip_repo_summary"], index=False)

    pr_eval_df = pd.read_csv(first_paths["pr_evaluation"], keep_default_na=False)
    pr_eval_df.loc[0, "human_observation"] = "Confirmed by manual review."
    pr_eval_df.loc[0, "is_prediction_correct"] = "true"
    pr_eval_df.loc[0, "is_confirmed_match"] = "true"
    pr_eval_df.to_csv(first_paths["pr_evaluation"], index=False)

    second_paths = export_hip_progression_results(
        tmp_path,
        artifacts=[artifact],
        feature_vectors=[feature_vector],
        artifact_assessments=[assessment],
        repo_statuses=[repo_status],
        export_profile="full",
    )

    rerun_summary_df = pd.read_csv(second_paths["hip_repo_summary"], keep_default_na=False)
    rerun_pr_eval_df = pd.read_csv(second_paths["pr_evaluation"], keep_default_na=False)
    review_breakdown_df = pd.read_csv(second_paths["prediction_review_breakdown"], keep_default_na=False)

    assert rerun_summary_df.loc[0, "reviewer_notes"] == "Needs maintainer validation."
    assert rerun_pr_eval_df.loc[0, "human_observation"] == "Confirmed by manual review."
    assert rerun_pr_eval_df.loc[0, "is_confirmed_match"] in {True, "True", "true"}

    pr_all_breakdown = review_breakdown_df[
        (review_breakdown_df["scope"] == "pull_request")
        & (review_breakdown_df["dataset_split"] == "all")
    ].iloc[0]
    assert pr_all_breakdown["confirmed_match_rows"] == 1
