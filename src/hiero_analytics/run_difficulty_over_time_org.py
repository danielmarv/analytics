"""Run historically-correct difficulty-over-time analytics for an organization."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd

from hiero_analytics.analysis.timeseries import (
    DIFFICULTY_OVER_TIME_COLUMN_ORDER,
    get_difficulty_over_time_windowed,
    issue_overlaps_window,
)
from hiero_analytics.config.charts import DIFFICULTY_COLORS
from hiero_analytics.config.paths import ORG, ensure_org_dirs
from hiero_analytics.data_sources.github_client import GitHubClient
from hiero_analytics.data_sources.github_ingest import (
    fetch_org_issues_graphql,
    fetch_repo_issue_events_for_issues_since,
)
from hiero_analytics.data_sources.github_search import search_issues
from hiero_analytics.data_sources.models import IssueRecord
from hiero_analytics.domain.labels import (
    DIFFICULTY_ADVANCED,
    DIFFICULTY_BEGINNER,
    DIFFICULTY_GOOD_FIRST_ISSUE,
    DIFFICULTY_INTERMEDIATE,
)
from hiero_analytics.export.save import save_dataframe
from hiero_analytics.plotting.lines import plot_stacked_area

WINDOW_DAYS = 365
TIMELINE_MAX_WORKERS = 3
SEARCH_MAX_PAGES = 10
DIFFICULTY_OVER_TIME_LABELS = [
    DIFFICULTY_GOOD_FIRST_ISSUE.name,
    DIFFICULTY_BEGINNER.name,
    DIFFICULTY_INTERMEDIATE.name,
    DIFFICULTY_ADVANCED.name,
]


def _parse_dt(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _issue_from_search(item: dict) -> IssueRecord | None:
    """Normalize a REST search result into an IssueRecord."""
    if not isinstance(item, dict):
        return None

    repo_url = item.get("repository_url")
    if not isinstance(repo_url, str):
        return None

    repo = "/".join(repo_url.rstrip("/").split("/")[-2:])
    number = item.get("number")
    if not isinstance(number, int):
        return None

    labels = [
        label["name"].lower()
        for label in item.get("labels", [])
        if isinstance(label, dict) and isinstance(label.get("name"), str)
    ]

    created_at = _parse_dt(item.get("created_at"))
    closed_at = _parse_dt(item.get("closed_at"))

    if created_at is None:
        return None

    return IssueRecord(
        repo=repo,
        number=number,
        title=str(item.get("title", "")),
        state=str(item.get("state", "")).upper(),
        created_at=created_at,
        closed_at=closed_at,
        labels=labels,
    )


def _search_closed_issues_windowed(
    client: GitHubClient,
    *,
    org: str,
    start_at: datetime,
    end_at: datetime,
) -> list[IssueRecord]:
    """Search for closed issues in a time window, splitting ranges to avoid API limits."""
    results: list[IssueRecord] = []

    def search_range(range_start: datetime, range_end: datetime) -> None:
        query = (
            f"org:{org} is:issue is:closed "
            f"closed:{range_start.date().isoformat()}..{range_end.date().isoformat()}"
        )
        items = search_issues(client, query, max_pages=SEARCH_MAX_PAGES)
        issues = [issue for item in items if (issue := _issue_from_search(item)) is not None]

        if len(issues) < SEARCH_MAX_PAGES * 100 or range_start.date() == range_end.date():
            results.extend(issues)
            return

        midpoint = range_start + (range_end - range_start) / 2
        mid_day = datetime(midpoint.year, midpoint.month, midpoint.day, tzinfo=UTC)
        if mid_day <= range_start:
            mid_day = range_start + timedelta(days=1)
        if mid_day >= range_end:
            results.extend(issues)
            return

        search_range(range_start, mid_day - timedelta(days=1))
        search_range(mid_day, range_end)

    search_range(start_at, end_at)
    return results


def main() -> None:
    """Generate an org-wide historical open-issue difficulty chart using timeline events."""
    org_data_dir, org_charts_dir = ensure_org_dirs(ORG)
    end_at = datetime.now(UTC)
    start_at = end_at - timedelta(days=WINDOW_DAYS)

    print(f"Running historical difficulty-over-time analytics for org: {ORG}")
    print(
        "Window: "
        f"{start_at.date().isoformat()} to {end_at.date().isoformat()}"
    )

    client = GitHubClient()
    open_issues = fetch_org_issues_graphql(client, org=ORG, states=["OPEN"])
    print(f"Fetched {len(open_issues)} open issues")

    closed_issues = _search_closed_issues_windowed(
        client,
        org=ORG,
        start_at=start_at,
        end_at=end_at,
    )
    print(f"Fetched {len(closed_issues)} closed issues since {start_at.date().isoformat()}")

    issues_by_key: dict[tuple[str, int], IssueRecord] = {}
    for issue in closed_issues + open_issues:
        issues_by_key[(issue.repo, issue.number)] = issue

    issues = list(issues_by_key.values())
    print(f"Prepared {len(issues)} total issues for windowed analysis")

    window_issues = [issue for issue in issues if issue_overlaps_window(issue, start_at, end_at)]
    print(f"Retained {len(window_issues)} issues that overlap the one-year window")

    timeline_events = fetch_repo_issue_events_for_issues_since(
        client,
        window_issues,
        since=start_at,
        max_workers=TIMELINE_MAX_WORKERS,
    )
    print(f"Fetched {len(timeline_events)} repository issue events since {start_at.date().isoformat()}")

    difficulty_over_time = pd.DataFrame(
        get_difficulty_over_time_windowed(
            window_issues,
            timeline_events,
            start_at=start_at,
            today=end_at,
        )
    )
    if difficulty_over_time.empty:
        print("No difficulty-over-time data available")
        return

    difficulty_over_time = difficulty_over_time[["date", *DIFFICULTY_OVER_TIME_COLUMN_ORDER]]

    save_dataframe(
        difficulty_over_time,
        org_data_dir / "difficulty_over_time_historical_weekly.csv",
    )

    plot_stacked_area(
        difficulty_over_time,
        x_col="date",
        stack_cols=DIFFICULTY_OVER_TIME_COLUMN_ORDER,
        labels=DIFFICULTY_OVER_TIME_LABELS,
        title="Historical Open Issues by Difficulty Over Time",
        output_path=org_charts_dir / "difficulty_over_time_historical_weekly.png",
        colors=DIFFICULTY_COLORS,
        xlabel="Date",
        ylabel="Open issues",
    )

    print("Historical difficulty-over-time analytics complete")


if __name__ == "__main__":
    main()
