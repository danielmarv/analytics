"""Run maintainer-pipeline validation against governance truth for a GitHub organization."""

from __future__ import annotations

from hiero_analytics.analysis.maintainer_pipeline import (
    activity_to_role_dataframe,
    build_maintainer_repo_pipeline,
)
from hiero_analytics.analysis.maintainer_validation import (
    build_actual_role_counts,
    build_role_count_comparison,
    build_role_count_differences,
    build_role_count_error_metrics,
    build_validation_summary,
)
from hiero_analytics.config.paths import ORG, ensure_org_dirs
from hiero_analytics.data_sources.github_client import GitHubClient
from hiero_analytics.data_sources.github_ingest import fetch_org_contributor_activity_graphql
from hiero_analytics.data_sources.governance_config import (
    build_repo_role_lookup,
    count_distinct_role_holders_by_role,
    fetch_governance_config,
)
from hiero_analytics.export.save import save_dataframe


def main() -> None:
    """Generate validation tables comparing predicted and actual repo role counts."""
    org_data_dir, _ = ensure_org_dirs(ORG)

    print(f"Running maintainer validation for org: {ORG}")

    gov_config = fetch_governance_config()
    repo_role_lookup = build_repo_role_lookup(gov_config)
    actual_counts = build_actual_role_counts(repo_role_lookup)
    distinct_role_holders = count_distinct_role_holders_by_role(repo_role_lookup)

    client = GitHubClient()
    records = fetch_org_contributor_activity_graphql(client, org=ORG)
    stage_df = activity_to_role_dataframe(records, repo_role_lookup)
    predicted_counts = build_maintainer_repo_pipeline(stage_df)

    comparison_df = build_role_count_comparison(predicted_counts, actual_counts)
    difference_df = build_role_count_differences(comparison_df)
    error_df = build_role_count_error_metrics(difference_df)
    summary_df = build_validation_summary(error_df, distinct_role_holders)

    save_dataframe(actual_counts, org_data_dir / "maintainer_pipeline_truth_by_repo.csv")
    save_dataframe(comparison_df, org_data_dir / "maintainer_pipeline_validation_comparison.csv")
    save_dataframe(difference_df, org_data_dir / "maintainer_pipeline_validation_difference.csv")
    save_dataframe(error_df, org_data_dir / "maintainer_pipeline_validation_error.csv")
    save_dataframe(summary_df, org_data_dir / "maintainer_pipeline_validation_summary.csv")

    total_rmse = summary_df.iloc[0]["total_rmse"]
    print(f"Saved validation tables (total RMSE: {total_rmse:.3f})")


if __name__ == "__main__":
    main()
