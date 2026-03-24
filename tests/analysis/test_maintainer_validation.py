"""Tests for maintainer-pipeline validation helpers."""

from math import isclose, sqrt

import pandas as pd

ROLE_COLUMNS = ["triage", "committer", "maintainer"]


def build_actual_role_counts(
    repo_role_lookup: dict[str, dict[str, str]],
) -> pd.DataFrame:
    """Count actual governance role holders per repository."""
    rows = []
    for repo, user_roles in repo_role_lookup.items():
        counts = {role: 0 for role in ROLE_COLUMNS}
        for role in user_roles.values():
            if role in counts:
                counts[role] += 1
        rows.append({"repo": repo, **counts})

    if not rows:
        return pd.DataFrame(columns=["repo", *ROLE_COLUMNS])

    actual = pd.DataFrame(rows).sort_values("repo").reset_index(drop=True)
    return actual.astype({role: int for role in ROLE_COLUMNS})


def build_role_count_comparison(
    predicted_repo_df: pd.DataFrame,
    actual_role_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build a repo-level table of predicted vs actual role-holder counts."""
    predicted = (
        predicted_repo_df.reindex(columns=["repo", *ROLE_COLUMNS], fill_value=0)
        .rename(columns={role: f"predicted_{role}" for role in ROLE_COLUMNS})
        .copy()
    )
    actual = (
        actual_role_df.reindex(columns=["repo", *ROLE_COLUMNS], fill_value=0)
        .rename(columns={role: f"actual_{role}" for role in ROLE_COLUMNS})
        .copy()
    )

    comparison = (
        actual.merge(predicted, on="repo", how="outer")
        .fillna(0)
        .sort_values("repo")
        .reset_index(drop=True)
    )
    ordered_columns = ["repo"]
    for role in ROLE_COLUMNS:
        ordered_columns.extend([f"predicted_{role}", f"actual_{role}"])

    int_columns = [column for column in ordered_columns if column != "repo"]
    return comparison[ordered_columns].astype({column: int for column in int_columns})


def build_role_count_differences(comparison_df: pd.DataFrame) -> pd.DataFrame:
    """Build a repo-level table of predicted-minus-actual role differences."""
    if comparison_df.empty:
        return pd.DataFrame(
            columns=[
                "repo",
                "triage_difference",
                "committer_difference",
                "maintainer_difference",
            ]
        )

    differences = pd.DataFrame(
        {
            "repo": comparison_df["repo"],
            "triage_difference": (
                comparison_df["predicted_triage"] - comparison_df["actual_triage"]
            ),
            "committer_difference": (
                comparison_df["predicted_committer"] - comparison_df["actual_committer"]
            ),
            "maintainer_difference": (
                comparison_df["predicted_maintainer"] - comparison_df["actual_maintainer"]
            ),
        }
    )
    return differences.astype(
        {
            "triage_difference": int,
            "committer_difference": int,
            "maintainer_difference": int,
        }
    )


def build_role_count_error_metrics(difference_df: pd.DataFrame) -> pd.DataFrame:
    """Build squared-error and RMSE metrics for each repository."""
    if difference_df.empty:
        return pd.DataFrame(columns=["repo", "squared_error", "rmse"])

    error_df = difference_df.copy()
    diff_columns = [
        "triage_difference",
        "committer_difference",
        "maintainer_difference",
    ]
    error_df["squared_error"] = sum(error_df[column] ** 2 for column in diff_columns)
    error_df["rmse"] = (error_df["squared_error"] / len(diff_columns)) ** 0.5
    return error_df[["repo", "squared_error", "rmse"]]


def build_validation_summary(
    error_df: pd.DataFrame,
    distinct_role_holders: dict[str, int],
) -> pd.DataFrame:
    """Build a one-row summary table for maintainer-pipeline validation."""
    total_rmse = float(error_df["rmse"].sum()) if not error_df.empty else 0.0
    mean_repo_rmse = float(error_df["rmse"].mean()) if not error_df.empty else 0.0

    return pd.DataFrame(
        [
            {
                "repo_count": int(len(error_df)),
                "total_rmse": total_rmse,
                "mean_repo_rmse": mean_repo_rmse,
                "actual_unique_triage_holders": int(
                    distinct_role_holders.get("triage", 0)
                ),
                "actual_unique_committer_holders": int(
                    distinct_role_holders.get("committer", 0)
                ),
                "actual_unique_maintainer_holders": int(
                    distinct_role_holders.get("maintainer", 0)
                ),
            }
        ]
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
