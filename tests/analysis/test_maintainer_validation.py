"""Tests for maintainer-pipeline validation helpers."""

from math import isclose, sqrt

import pandas as pd

from hiero_analytics.analysis.maintainer_validation import (
    build_actual_role_counts,
    build_role_count_comparison,
    build_role_count_differences,
    build_role_count_error_metrics,
    build_validation_summary,
)


def test_build_actual_role_counts_counts_governance_roles_per_repo():
    """Actual-count tables should reflect repo-level governance memberships."""
    repo_role_lookup = {
        "repo-a": {
            "alice": "maintainer",
            "bob": "committer",
            "carol": "triage",
        },
        "repo-b": {
            "dana": "committer",
            "erin": "committer",
        },
    }

    actual = build_actual_role_counts(repo_role_lookup)

    row_a = actual.loc[actual["repo"] == "repo-a"].iloc[0]
    row_b = actual.loc[actual["repo"] == "repo-b"].iloc[0]

    assert row_a["triage"] == 1
    assert row_a["committer"] == 1
    assert row_a["maintainer"] == 1
    assert row_b["triage"] == 0
    assert row_b["committer"] == 2
    assert row_b["maintainer"] == 0


def test_validation_tables_compare_predicted_counts_to_actual_counts():
    """Validation tables should align repos, differences, and per-repo RMSE."""
    predicted = pd.DataFrame(
        [
            {
                "repo": "repo-a",
                "general_user": 10,
                "triage": 1,
                "committer": 3,
                "maintainer": 0,
            },
            {
                "repo": "repo-c",
                "general_user": 4,
                "triage": 0,
                "committer": 1,
                "maintainer": 1,
            },
        ]
    )
    actual = pd.DataFrame(
        [
            {
                "repo": "repo-a",
                "triage": 2,
                "committer": 1,
                "maintainer": 1,
            },
            {
                "repo": "repo-b",
                "triage": 1,
                "committer": 0,
                "maintainer": 0,
            },
        ]
    )

    comparison = build_role_count_comparison(predicted, actual)
    differences = build_role_count_differences(comparison)
    errors = build_role_count_error_metrics(differences)

    repo_a = comparison.loc[comparison["repo"] == "repo-a"].iloc[0]
    repo_b = comparison.loc[comparison["repo"] == "repo-b"].iloc[0]
    repo_c = comparison.loc[comparison["repo"] == "repo-c"].iloc[0]

    assert repo_a["predicted_triage"] == 1
    assert repo_a["actual_triage"] == 2
    assert repo_b["predicted_committer"] == 0
    assert repo_b["actual_triage"] == 1
    assert repo_c["predicted_maintainer"] == 1
    assert repo_c["actual_maintainer"] == 0

    repo_a_diff = differences.loc[differences["repo"] == "repo-a"].iloc[0]
    assert repo_a_diff["triage_difference"] == -1
    assert repo_a_diff["committer_difference"] == 2
    assert repo_a_diff["maintainer_difference"] == -1

    repo_a_error = errors.loc[errors["repo"] == "repo-a"].iloc[0]
    assert repo_a_error["squared_error"] == 6
    assert isclose(repo_a_error["rmse"], sqrt(2), rel_tol=1e-9)


def test_build_validation_summary_reports_total_rmse_and_role_holder_counts():
    """Validation summaries should expose total error and unique role holders."""
    error_df = pd.DataFrame(
        [
            {"repo": "repo-a", "squared_error": 6, "rmse": sqrt(2)},
            {"repo": "repo-b", "squared_error": 1, "rmse": sqrt(1 / 3)},
        ]
    )

    summary = build_validation_summary(
        error_df,
        {
            "triage": 2,
            "committer": 5,
            "maintainer": 3,
        },
    )

    row = summary.iloc[0]
    assert row["repo_count"] == 2
    assert isclose(row["total_rmse"], sqrt(2) + sqrt(1 / 3), rel_tol=1e-9)
    assert row["actual_unique_triage_holders"] == 2
    assert row["actual_unique_committer_holders"] == 5
    assert row["actual_unique_maintainer_holders"] == 3
