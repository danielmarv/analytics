"""
Run contributor responsibility analytics for a GitHub org.

Produces:
- Raw contributor activity log
- Actor stage journeys
- Yearly general user -> triage -> maintainer pipeline
- Repository-level highest observed stage distribution
"""

from __future__ import annotations

from hiero_analytics.analysis.maintainer_pipeline import (
    STAGE_LABELS,
    STAGE_ORDER,
    activity_records_to_dataframe,
    build_maintainer_pipeline,
    build_repo_stage_distribution,
    summarize_actor_stage_journeys,
)
from hiero_analytics.config.charts import RESPONSIBILITY_COLORS
from hiero_analytics.config.paths import ORG, ensure_org_dirs
from hiero_analytics.data_sources.github_client import GitHubClient
from hiero_analytics.data_sources.github_ingest import fetch_org_contributor_activity_graphql
from hiero_analytics.export.save import save_dataframe
from hiero_analytics.plotting.bars import plot_stacked_bar

STACK_LABELS = [STAGE_LABELS[stage] for stage in STAGE_ORDER]


def main() -> None:
    """Execute the contributor responsibility analytics pipeline."""
    org_data_dir, org_charts_dir = ensure_org_dirs(ORG)

    print(f"Running maintainer pipeline analytics for org: {ORG}")

    client = GitHubClient()
    activities = fetch_org_contributor_activity_graphql(client, org=ORG)

    print(f"Fetched {len(activities)} contributor activity records")

    activity_df = activity_records_to_dataframe(activities)
    repo_stage_journeys = summarize_actor_stage_journeys(activities, by_repo=True)
    org_stage_journeys = summarize_actor_stage_journeys(activities, by_repo=False)
    yearly_pipeline = build_maintainer_pipeline(org_stage_journeys)
    repo_stage_distribution = build_repo_stage_distribution(repo_stage_journeys)

    save_dataframe(activity_df, org_data_dir / "maintainer_activity_log.csv")
    save_dataframe(repo_stage_journeys, org_data_dir / "maintainer_stage_journeys_by_repo.csv")
    save_dataframe(org_stage_journeys, org_data_dir / "maintainer_stage_journeys_org.csv")
    save_dataframe(yearly_pipeline, org_data_dir / "maintainer_pipeline_yearly.csv")
    save_dataframe(
        repo_stage_distribution,
        org_data_dir / "maintainer_stage_distribution_by_repo.csv",
    )

    print("Saved maintainer pipeline tables")

    if not yearly_pipeline.empty:
        plot_stacked_bar(
            yearly_pipeline,
            x_col="year",
            stack_cols=STAGE_ORDER,
            labels=STACK_LABELS,
            colors=RESPONSIBILITY_COLORS,
            title="Contributor Responsibility Pipeline by Year",
            output_path=org_charts_dir / "maintainer_pipeline_yearly.png",
        )

    if not repo_stage_distribution.empty:
        plot_stacked_bar(
            repo_stage_distribution,
            x_col="repo",
            stack_cols=STAGE_ORDER,
            labels=STACK_LABELS,
            colors=RESPONSIBILITY_COLORS,
            title="Highest Observed Contributor Stage by Repository",
            output_path=org_charts_dir / "maintainer_stage_distribution_by_repo.png",
            rotate_x=45,
        )

    print("Maintainer pipeline analytics complete")


if __name__ == "__main__":
    main()
