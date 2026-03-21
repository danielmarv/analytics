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
from hiero_analytics.data_sources.github_ingest import (
    DEFAULT_CONTRIBUTOR_ACTIVITY_LOOKBACK_DAYS,
    fetch_org_contributor_activity_graphql,
)
from hiero_analytics.export.save import save_dataframe
from hiero_analytics.plotting.bars import plot_stacked_bar

STACK_LABELS = [STAGE_LABELS[stage] for stage in STAGE_ORDER]


def _activity_max_workers() -> int:
    """Resolve the contributor-activity worker count from the environment."""
    raw_value = os.getenv("GITHUB_CONTRIBUTOR_ACTIVITY_MAX_WORKERS", "1")

    try:
        return max(1, int(raw_value))
    except ValueError:
        return 1


def _activity_lookback_days() -> int | None:
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


def _activity_repo_pause_seconds() -> float:
    """Resolve the pause between sequential repository fetches."""
    raw_value = os.getenv("GITHUB_CONTRIBUTOR_ACTIVITY_REPO_PAUSE_SECONDS", "5")

    try:
        return max(0.0, float(raw_value))
    except ValueError:
        return 5.0


def _selected_repos() -> list[str]:
    """Resolve optional repo filters from a comma-separated environment variable."""
    raw_value = os.getenv("GITHUB_CONTRIBUTOR_ACTIVITY_REPOS", "")
    return [repo.strip() for repo in raw_value.split(",") if repo.strip()]


def main() -> None:
    """Execute the contributor responsibility analytics pipeline."""
    org_data_dir, org_charts_dir = ensure_org_dirs(ORG)
    activity_max_workers = _activity_max_workers()
    activity_lookback_days = _activity_lookback_days()
    repo_pause_seconds = _activity_repo_pause_seconds()
    selected_repos = _selected_repos()

    print(f"Running maintainer pipeline analytics for org: {ORG}")
    print(f"Using contributor activity worker count: {activity_max_workers}")
    if activity_lookback_days is None:
        print("Using contributor activity lookback: full history")
    else:
        print(
            "Using contributor activity lookback: "
            f"{activity_lookback_days} days from each repo's latest issue or PR update"
        )
    print(f"Using pause between repo fetches: {repo_pause_seconds:g}s")
    if selected_repos:
        print(f"Restricting contributor activity fetch to {len(selected_repos)} repo(s)")

    client = GitHubClient()
    activities = fetch_org_contributor_activity_graphql(
        client,
        org=ORG,
        max_workers=activity_max_workers,
        repos=selected_repos or None,
        repo_pause_seconds=repo_pause_seconds,
        lookback_days=activity_lookback_days,
    )

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
