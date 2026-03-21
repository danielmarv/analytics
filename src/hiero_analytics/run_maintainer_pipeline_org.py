"""
Run contributor responsibility analytics for a GitHub org.

Produces:
- Raw contributor activity log
- Actor stage journeys
- Yearly general user -> triage -> maintainer pipeline
- Repository-level highest observed stage distribution
"""

from __future__ import annotations

import os
from collections.abc import Callable, Sequence

from hiero_analytics.analysis.maintainer_pipeline import (
    STAGE_LABELS,
    STAGE_ORDER,
    activity_records_to_dataframe,
    build_cumulative_stage_timeline,
    build_maintainer_pipeline,
    build_repo_stage_distribution,
    build_stage_activity_timeline,
    build_stage_entry_timeline,
    summarize_actor_stage_journeys,
)
from hiero_analytics.config.charts import RESPONSIBILITY_COLORS
from hiero_analytics.config.paths import ORG, ensure_org_dirs
from hiero_analytics.data_sources.github_client import GitHubClient
from hiero_analytics.data_sources.github_ingest import (
    DEFAULT_CONTRIBUTOR_ACTIVITY_LOOKBACK_DAYS,
    fetch_org_contributor_activity_graphql,
)
from hiero_analytics.data_sources.models import ContributorActivityRecord
from hiero_analytics.export.save import save_dataframe
from hiero_analytics.plotting.bars import plot_stacked_bar
from hiero_analytics.plotting.lines import plot_multiline

STACK_LABELS = [STAGE_LABELS[stage] for stage in STAGE_ORDER]
RESPONSIBILITY_STAGE_COLORS = {
    STAGE_LABELS[stage]: RESPONSIBILITY_COLORS[STAGE_LABELS[stage]]
    for stage in STAGE_ORDER
}
DEFAULT_CONTRIBUTOR_ACTIVITY_CACHE_TTL_SECONDS = 86_400


def resolve_activity_max_workers() -> int:
    """Resolve the contributor-activity worker count from the environment."""
    raw_value = os.getenv("GITHUB_CONTRIBUTOR_ACTIVITY_MAX_WORKERS", "1")

    try:
        return max(1, int(raw_value))
    except ValueError:
        return 1


def resolve_activity_lookback_days() -> int | None:
    """Resolve the repo-relative contributor-activity lookback from the environment."""
    raw_value = os.getenv(
        "GITHUB_CONTRIBUTOR_ACTIVITY_LOOKBACK_DAYS",
        str(DEFAULT_CONTRIBUTOR_ACTIVITY_LOOKBACK_DAYS),
    )

    try:
        parsed_value = int(raw_value)
    except ValueError:
        return DEFAULT_CONTRIBUTOR_ACTIVITY_LOOKBACK_DAYS

    return parsed_value if parsed_value > 0 else None


def resolve_activity_repo_pause_seconds() -> float:
    """Resolve the pause between sequential repository fetches."""
    raw_value = os.getenv("GITHUB_CONTRIBUTOR_ACTIVITY_REPO_PAUSE_SECONDS", "5")

    try:
        return max(0.0, float(raw_value))
    except ValueError:
        return 5.0


def resolve_selected_repos() -> list[str]:
    """Resolve optional repo filters from a comma-separated environment variable."""
    raw_value = os.getenv("GITHUB_CONTRIBUTOR_ACTIVITY_REPOS", "")
    return [repo.strip() for repo in raw_value.split(",") if repo.strip()]


def resolve_activity_cache_ttl_seconds() -> int:
    """Resolve the contributor-activity cache TTL for maintainer analytics."""
    raw_value = os.getenv(
        "GITHUB_CONTRIBUTOR_ACTIVITY_CACHE_TTL_SECONDS",
        str(DEFAULT_CONTRIBUTOR_ACTIVITY_CACHE_TTL_SECONDS),
    )

    try:
        return max(0, int(raw_value))
    except ValueError:
        return DEFAULT_CONTRIBUTOR_ACTIVITY_CACHE_TTL_SECONDS


def print_maintainer_runtime_settings(
    *,
    output_fn: Callable[[str], None] = print,
    activity_max_workers: int,
    activity_lookback_days: int | None,
    activity_cache_ttl_seconds: int,
    repo_pause_seconds: float,
    selected_repos: Sequence[str],
) -> None:
    """Print the maintainer runner configuration for the current execution."""
    output_fn(f"Using contributor activity worker count: {activity_max_workers}")
    if activity_lookback_days is None:
        output_fn("Using contributor activity lookback: full history")
    else:
        output_fn(
            "Using contributor activity lookback: "
            f"{activity_lookback_days} days from each repo's latest issue or PR update"
        )
    output_fn(f"Using contributor activity cache TTL: {activity_cache_ttl_seconds}s")
    output_fn(f"Using pause between repo fetches: {repo_pause_seconds:g}s")
    if selected_repos:
        output_fn(f"Restricting contributor activity fetch to {len(selected_repos)} repo(s)")


def save_maintainer_pipeline_outputs(activities: list[ContributorActivityRecord]) -> None:
    """Build and save maintainer pipeline tables and charts from fetched activity."""
    org_data_dir, org_charts_dir = ensure_org_dirs(ORG)

    activity_df = activity_records_to_dataframe(activities)
    repo_stage_journeys = summarize_actor_stage_journeys(activities, by_repo=True)
    org_stage_journeys = summarize_actor_stage_journeys(activities, by_repo=False)
    yearly_pipeline = build_maintainer_pipeline(org_stage_journeys)
    monthly_pipeline = build_stage_entry_timeline(org_stage_journeys, frequency="month")
    cumulative_monthly_pipeline = build_cumulative_stage_timeline(
        monthly_pipeline,
        period_col="month",
    )
    monthly_stage_activity = build_stage_activity_timeline(activities, frequency="month")
    repo_stage_distribution = build_repo_stage_distribution(repo_stage_journeys)

    save_dataframe(activity_df, org_data_dir / "maintainer_activity_log.csv")
    save_dataframe(repo_stage_journeys, org_data_dir / "maintainer_stage_journeys_by_repo.csv")
    save_dataframe(org_stage_journeys, org_data_dir / "maintainer_stage_journeys_org.csv")
    save_dataframe(yearly_pipeline, org_data_dir / "maintainer_pipeline_yearly.csv")
    save_dataframe(monthly_pipeline, org_data_dir / "maintainer_pipeline_monthly.csv")
    save_dataframe(
        cumulative_monthly_pipeline,
        org_data_dir / "maintainer_pipeline_cumulative_monthly.csv",
    )
    save_dataframe(
        monthly_stage_activity,
        org_data_dir / "maintainer_stage_activity_monthly.csv",
    )
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

    if not monthly_pipeline.empty:
        monthly_pipeline_long = (
            monthly_pipeline.melt(
                id_vars=["month"],
                value_vars=STAGE_ORDER,
                var_name="stage",
                value_name="count",
            )
            .assign(stage=lambda df: df["stage"].map(STAGE_LABELS))
        )
        plot_multiline(
            monthly_pipeline_long,
            x_col="month",
            y_col="count",
            group_col="stage",
            colors=RESPONSIBILITY_STAGE_COLORS,
            title="New Responsibility Stage Entries by Month",
            output_path=org_charts_dir / "maintainer_pipeline_monthly_line.png",
            rotate_x=45,
        )

    if not cumulative_monthly_pipeline.empty:
        cumulative_monthly_pipeline_long = (
            cumulative_monthly_pipeline.melt(
                id_vars=["month"],
                value_vars=STAGE_ORDER,
                var_name="stage",
                value_name="count",
            )
            .assign(stage=lambda df: df["stage"].map(STAGE_LABELS))
        )
        plot_multiline(
            cumulative_monthly_pipeline_long,
            x_col="month",
            y_col="count",
            group_col="stage",
            colors=RESPONSIBILITY_STAGE_COLORS,
            title="Cumulative Responsibility Stage Growth by Month",
            output_path=org_charts_dir / "maintainer_pipeline_cumulative_monthly_line.png",
            rotate_x=45,
        )

    if not monthly_stage_activity.empty:
        monthly_stage_activity_long = (
            monthly_stage_activity.melt(
                id_vars=["month"],
                value_vars=STAGE_ORDER,
                var_name="stage",
                value_name="count",
            )
            .assign(stage=lambda df: df["stage"].map(STAGE_LABELS))
        )
        plot_multiline(
            monthly_stage_activity_long,
            x_col="month",
            y_col="count",
            group_col="stage",
            colors=RESPONSIBILITY_STAGE_COLORS,
            title="Active Contributors by Responsibility Signal per Month",
            output_path=org_charts_dir / "maintainer_stage_activity_monthly_line.png",
            rotate_x=45,
        )


def main() -> None:
    """Execute the contributor responsibility analytics pipeline."""
    ensure_org_dirs(ORG)
    activity_max_workers = resolve_activity_max_workers()
    activity_lookback_days = resolve_activity_lookback_days()
    activity_cache_ttl_seconds = resolve_activity_cache_ttl_seconds()
    repo_pause_seconds = resolve_activity_repo_pause_seconds()
    selected_repos = resolve_selected_repos()

    print(f"Running maintainer pipeline analytics for org: {ORG}")
    print_maintainer_runtime_settings(
        activity_max_workers=activity_max_workers,
        activity_lookback_days=activity_lookback_days,
        activity_cache_ttl_seconds=activity_cache_ttl_seconds,
        repo_pause_seconds=repo_pause_seconds,
        selected_repos=selected_repos,
    )

    client = GitHubClient()
    activities = fetch_org_contributor_activity_graphql(
        client,
        org=ORG,
        max_workers=activity_max_workers,
        repos=selected_repos or None,
        repo_pause_seconds=repo_pause_seconds,
        lookback_days=activity_lookback_days,
        cache_ttl_seconds=activity_cache_ttl_seconds,
    )

    print(f"Fetched {len(activities)} contributor activity records")
    save_maintainer_pipeline_outputs(activities)

    print("Maintainer pipeline analytics complete")


if __name__ == "__main__":
    main()
