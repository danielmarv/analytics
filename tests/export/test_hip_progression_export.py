"""Tests for HIP progression markdown exports."""

from __future__ import annotations

import pandas as pd

from hiero_analytics.analysis.hip_feature_engineering import engineer_hip_feature_vector
from hiero_analytics.analysis.hip_scoring import score_hip_feature_vector
from hiero_analytics.analysis.hip_status_aggregation import aggregate_hip_repo_status
from hiero_analytics.export.hip_progression_export import export_hip_progression_results
from hiero_analytics.export.save import save_markdown_table
from tests.hip_progression_fixtures import make_candidate, make_pull_request_artifact


def test_save_markdown_table_escapes_pipes_and_newlines(tmp_path):
    """Markdown export should escape pipes and preserve line breaks safely."""
    path = tmp_path / "table.md"
    df = pd.DataFrame([{"summary": "line one\nline | two"}])

    save_markdown_table(df, path)

    contents = path.read_text(encoding="utf-8")
    assert "| summary |" in contents
    assert "line one<br>line \\| two" in contents


def test_export_hip_progression_results_writes_markdown_tables(tmp_path):
    """HIP progression export should emit markdown tables for derived outputs, not raw artifacts."""
    artifact = make_pull_request_artifact()
    candidate = make_candidate(artifact)
    feature_vector = engineer_hip_feature_vector(candidate)
    evidence = score_hip_feature_vector(feature_vector)
    repo_status = aggregate_hip_repo_status([evidence], artifacts=[artifact])[0]

    paths = export_hip_progression_results(
        tmp_path,
        artifacts=[artifact],
        feature_vectors=[feature_vector],
        evidence_records=[evidence],
        repo_statuses=[repo_status],
    )

    assert "artifacts_markdown" not in paths
    assert not (tmp_path / "artifacts.md").exists()
    assert paths["artifact_features_markdown"].exists()
    assert paths["hip_evidence_markdown"].exists()
    assert paths["hip_repo_status_markdown"].exists()
    assert paths["pr_evaluation"].exists()
    assert paths["pr_evaluation_markdown"].exists()
    assert paths["repo_evaluation"].exists()
    assert paths["prediction_review_breakdown"].exists()
    assert paths["prediction_review_breakdown_markdown"].exists()
    assert paths["evaluation_summary"].exists()

    feature_markdown = paths["artifact_features_markdown"].read_text(encoding="utf-8")
    evidence_markdown = paths["hip_evidence_markdown"].read_text(encoding="utf-8")
    status_markdown = paths["hip_repo_status_markdown"].read_text(encoding="utf-8")
    pr_evaluation_markdown = paths["pr_evaluation_markdown"].read_text(encoding="utf-8")
    review_breakdown_markdown = paths["prediction_review_breakdown_markdown"].read_text(encoding="utf-8")
    summary_markdown = paths["evaluation_summary_markdown"].read_text(encoding="utf-8")

    assert "| repo | artifact_type | artifact_number |" in feature_markdown
    assert "| repo | hip_id | artifact_type |" in evidence_markdown
    assert "HIP-1234" in evidence_markdown
    assert "| repo | hip_id | status |" in status_markdown
    assert "artifact_link" in pr_evaluation_markdown
    assert "confirmed_non_match_rows" in review_breakdown_markdown
    assert "accuracy_percent" in summary_markdown


def test_export_hip_progression_results_preserves_manual_feedback_and_builds_review_breakdown(tmp_path):
    """Existing manual review columns should survive reruns and feed the review breakdown metrics."""
    artifact = make_pull_request_artifact()
    candidate = make_candidate(artifact)
    feature_vector = engineer_hip_feature_vector(candidate)
    evidence = score_hip_feature_vector(feature_vector)
    repo_status = aggregate_hip_repo_status([evidence], artifacts=[artifact])[0]

    first_paths = export_hip_progression_results(
        tmp_path,
        artifacts=[artifact],
        feature_vectors=[feature_vector],
        evidence_records=[evidence],
        repo_statuses=[repo_status],
    )

    pr_eval_df = pd.read_csv(first_paths["pr_evaluation"], keep_default_na=False)
    pr_eval_df.loc[0, "human_observation"] = "Confirmed by manual review."
    pr_eval_df.loc[0, "is_prediction_correct"] = "true"
    pr_eval_df.loc[0, "is_confirmed_match"] = "true"
    pr_eval_df.to_csv(first_paths["pr_evaluation"], index=False)

    issue_eval_df = pd.DataFrame(
        [
            {
                "prediction_present": False,
                "dataset_split": "test",
                "repo": "hiero-ledger/hiero-sdk-js",
                "artifact_type": "issue",
                "artifact_number": 909,
                "artifact_title": "Docs issue with no HIP relation",
                "artifact_url": "https://github.com/hiero-ledger/hiero-sdk-js/issues/909",
                "artifact_link": "[Issue #909](https://github.com/hiero-ledger/hiero-sdk-js/issues/909)",
                "hip_id": "",
                "extraction_source": "",
                "text_match_reason": "",
                "predicted_hip_candidate_score": "",
                "predicted_implementation_score": "",
                "predicted_completion_score": "",
                "predicted_confidence_level": "",
                "author_association": "NONE",
                "merged": False,
                "linked_issue_numbers": "",
                "linked_issue_urls": "",
                "linked_pr_numbers": "",
                "linked_pr_urls": "",
                "human_expected_outcome": "no_hip",
                "human_observation": "Reviewed and confirmed unrelated to any HIP.",
                "is_prediction_correct": "true",
                "is_confirmed_match": "false",
                "is_overcalled_match": "false",
                "is_missed_match": "false",
                "is_confirmed_non_match": "true",
            }
        ]
    )
    issue_eval_df.to_csv(first_paths["issue_evaluation"], index=False)

    repo_eval_df = pd.read_csv(first_paths["repo_evaluation"], keep_default_na=False)
    repo_eval_df.loc[0, "human_observation"] = "Predicted HIP is valid."
    repo_eval_df.loc[0, "is_confirmed_match"] = "true"
    repo_eval_df.loc[0, "is_prediction_correct"] = "true"
    repo_eval_df = pd.concat(
        [
            repo_eval_df,
            pd.DataFrame(
                [
                    {
                        "prediction_present": False,
                        "dataset_split": "test",
                        "repo": "hiero-ledger/hiero-sdk-js",
                        "hip_id": "HIP-9999",
                        "predicted_status": "",
                        "predicted_confidence_level": "",
                        "supporting_artifact_numbers": "",
                        "supporting_artifact_urls": "",
                        "supporting_artifact_links": "",
                        "supporting_issue_links": "",
                        "supporting_pr_links": "",
                        "has_supporting_issue": "",
                        "has_supporting_pull_request": "",
                        "has_merged_pull_request": "",
                        "has_maintainer_like_pull_request": "",
                        "has_linked_issue_pull_request_pair": "",
                        "last_evidence_at": "",
                        "rationale": "",
                        "human_expected_outcome": "completed",
                        "human_observation": "Model missed this HIP completely.",
                        "is_prediction_correct": "false",
                        "is_confirmed_match": "false",
                        "is_overcalled_match": "false",
                        "is_missed_match": "true",
                        "is_confirmed_non_match": "false",
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    repo_eval_df.to_csv(first_paths["repo_evaluation"], index=False)

    second_paths = export_hip_progression_results(
        tmp_path,
        artifacts=[artifact],
        feature_vectors=[feature_vector],
        evidence_records=[evidence],
        repo_statuses=[repo_status],
    )

    rerun_pr_eval_df = pd.read_csv(second_paths["pr_evaluation"], keep_default_na=False)
    rerun_issue_eval_df = pd.read_csv(second_paths["issue_evaluation"], keep_default_na=False)
    rerun_repo_eval_df = pd.read_csv(second_paths["repo_evaluation"], keep_default_na=False)
    review_breakdown_df = pd.read_csv(second_paths["prediction_review_breakdown"], keep_default_na=False)
    summary_df = pd.read_csv(second_paths["evaluation_summary"], keep_default_na=False)

    assert rerun_pr_eval_df.loc[0, "human_observation"] == "Confirmed by manual review."
    assert rerun_pr_eval_df.loc[0, "is_prediction_correct"] in {True, "True", "true"}
    assert rerun_pr_eval_df.loc[0, "is_confirmed_match"] in {True, "True", "true"}
    assert rerun_issue_eval_df.loc[0, "is_confirmed_non_match"] in {True, "True", "true"}

    false_negative_row = rerun_repo_eval_df.loc[rerun_repo_eval_df["hip_id"] == "HIP-9999"].iloc[0]
    assert false_negative_row["prediction_present"] in {False, "False", "false"}
    assert false_negative_row["is_missed_match"] == "true"

    issue_all_breakdown = review_breakdown_df[
        (review_breakdown_df["scope"] == "issue")
        & (review_breakdown_df["dataset_split"] == "all")
    ].iloc[0]
    assert issue_all_breakdown["confirmed_non_match_rows"] == 1
    assert float(issue_all_breakdown["accuracy_percent"]) == 100.0
    assert float(issue_all_breakdown["non_match_accuracy_percent"]) == 100.0

    repo_all_breakdown = review_breakdown_df[
        (review_breakdown_df["scope"] == "repo")
        & (review_breakdown_df["dataset_split"] == "all")
    ].iloc[0]
    assert repo_all_breakdown["confirmed_match_rows"] == 1
    assert repo_all_breakdown["missed_match_rows"] == 1
    assert float(repo_all_breakdown["accuracy_percent"]) == 50.0
    assert float(repo_all_breakdown["match_quality_percent"]) == 100.0
    assert float(repo_all_breakdown["match_coverage_percent"]) == 50.0
    assert float(repo_all_breakdown["balance_score_percent"]) == 66.67

    repo_test_summary = summary_df[
        (summary_df["scope"] == "repo")
        & (summary_df["dataset_split"] == "test")
    ].iloc[0]
    assert repo_test_summary["missed_match_rows"] == 1
