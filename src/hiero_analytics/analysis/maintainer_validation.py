"""Validation helpers for comparing predicted pipeline roles against governance truth."""

from __future__ import annotations

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
            "triage_difference": comparison_df["predicted_triage"] - comparison_df["actual_triage"],
            "committer_difference": comparison_df["predicted_committer"] - comparison_df["actual_committer"],
            "maintainer_difference": comparison_df["predicted_maintainer"] - comparison_df["actual_maintainer"],
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
                "actual_unique_triage_holders": int(distinct_role_holders.get("triage", 0)),
                "actual_unique_committer_holders": int(distinct_role_holders.get("committer", 0)),
                "actual_unique_maintainer_holders": int(distinct_role_holders.get("maintainer", 0)),
            }
        ]
    )
