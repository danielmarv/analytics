"""Run maintainer pipeline analytics for a GitHub organization."""

from __future__ import annotations

from hiero_analytics.analysis.maintainer_pipeline import (
    STAGE_COLUMNS,
    activity_to_stage_dataframe,
    build_maintainer_repo_pipeline,
    build_maintainer_yearly_pipeline,
)
from hiero_analytics.config.charts import MAINTAINER_PIPELINE_COLORS
from hiero_analytics.config.paths import ORG, ensure_org_dirs
from hiero_analytics.data_sources.github_client import GitHubClient
from hiero_analytics.data_sources.github_ingest import fetch_org_contributor_activity_graphql
from hiero_analytics.export.save import save_dataframe
from hiero_analytics.plotting.bars import plot_stacked_bar

STACK_LABELS = ["General Users", "Triage", "Committer/Maintainer"]


def main() -> None:
    """Run maintainer pipeline analytics for the configured organization."""
    org_data_dir, org_charts_dir = ensure_org_dirs(ORG)

    print(f"Running maintainer pipeline analytics for org: {ORG}")

    client = GitHubClient()
    records = fetch_org_contributor_activity_graphql(client, org=ORG)

    print(f"Fetched {len(records)} contributor activity records")

    stage_df = activity_to_stage_dataframe(records)
    yearly_pipeline = build_maintainer_yearly_pipeline(stage_df)
    repo_pipeline = build_maintainer_repo_pipeline(stage_df)

    save_dataframe(stage_df, org_data_dir / "maintainer_activity_events.csv")
    save_dataframe(yearly_pipeline, org_data_dir / "maintainer_pipeline_yearly.csv")
    save_dataframe(repo_pipeline, org_data_dir / "maintainer_pipeline_by_repo.csv")

    print("Saved maintainer pipeline tables")

    if not yearly_pipeline.empty:
        plot_stacked_bar(
            yearly_pipeline,
            x_col="year",
            stack_cols=STAGE_COLUMNS,
            labels=STACK_LABELS,
            colors=MAINTAINER_PIPELINE_COLORS,
            title="Maintainer Pipeline by Year",
            output_path=org_charts_dir / "maintainer_pipeline_yearly.png",
        )

    if not repo_pipeline.empty:
        plot_stacked_bar(
            repo_pipeline,
            x_col="repo",
            stack_cols=STAGE_COLUMNS,
            labels=STACK_LABELS,
            colors=MAINTAINER_PIPELINE_COLORS,
            title="Maintainer Pipeline by Repository",
            output_path=org_charts_dir / "maintainer_pipeline_by_repo.png",
            rotate_x=45,
        )

    print("Maintainer pipeline analytics complete")


if __name__ == "__main__":
    main()
