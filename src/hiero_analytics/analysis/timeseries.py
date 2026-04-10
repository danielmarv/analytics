"""Time-series helpers for cumulative and difficulty-based issue trends."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd

from hiero_analytics.data_sources.models import IssueRecord
from hiero_analytics.domain.labels import (
    DIFFICULTY_ADVANCED,
    DIFFICULTY_BEGINNER,
    DIFFICULTY_GOOD_FIRST_ISSUE,
    DIFFICULTY_INTERMEDIATE,
)

DIFFICULTY_OVER_TIME_COLUMN_ORDER = [
    "advanced",
    "intermediate",
    "beginner",
    "gfi",
]

_DIFFICULTY_OVER_TIME_SPECS = (
    ("gfi", DIFFICULTY_GOOD_FIRST_ISSUE),
    ("beginner", DIFFICULTY_BEGINNER),
    ("intermediate", DIFFICULTY_INTERMEDIATE),
    ("advanced", DIFFICULTY_ADVANCED),
)


def _normalize_datetime(value: datetime | None) -> datetime | None:
    """Return a timezone-aware UTC datetime for stable comparisons."""
    if value is None:
        return None

    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)

    return value.astimezone(UTC)


def _difficulty_key(labels: list[str]) -> str | None:
    """Return the first configured difficulty key that matches an issue."""
    normalized = set(labels or [])

    for key, spec in _DIFFICULTY_OVER_TIME_SPECS:
        if spec.matches(normalized):
            return key

    return None


def _weekly_sample_points(start_at: datetime, end_at: datetime) -> list[datetime]:
    """Build inclusive weekly sample points from the first issue to now."""
    points: list[datetime] = []
    current = start_at

    while current <= end_at:
        points.append(current)
        current += timedelta(days=7)

    if not points or points[-1] < end_at:
        points.append(end_at)

    return points


def get_difficulty_over_time(
    issues: list[IssueRecord],
    *,
    today: datetime | None = None,
) -> list[dict[str, str | int]]:
    """
    Build weekly open-issue counts per difficulty label over time.

    Issues are counted at a sample point ``T`` when:
    - ``created_at <= T``
    - ``closed_at is None or closed_at > T``

    Only issues with at least one known difficulty label are included.
    If an issue has more than one difficulty label, it is assigned to the
    first configured difficulty bucket so totals stay consistent with the
    existing difficulty charts.
    """
    labeled_issues: list[tuple[str, datetime, datetime | None]] = []

    for issue in issues:
        created_at = _normalize_datetime(issue.created_at)
        difficulty = _difficulty_key(issue.labels)

        if created_at is None or difficulty is None:
            continue

        labeled_issues.append(
            (
                difficulty,
                created_at,
                _normalize_datetime(issue.closed_at),
            )
        )

    if not labeled_issues:
        return []

    end_at = _normalize_datetime(today) or datetime.now(UTC)
    start_at = min(created_at for _, created_at, _ in labeled_issues)

    if end_at < start_at:
        end_at = start_at

    series: list[dict[str, str | int]] = []

    for sample_point in _weekly_sample_points(start_at, end_at):
        row: dict[str, str | int] = {
            "date": sample_point.date().isoformat(),
            "advanced": 0,
            "intermediate": 0,
            "beginner": 0,
            "gfi": 0,
        }

        for difficulty, created_at, closed_at in labeled_issues:
            is_open = created_at <= sample_point and (
                closed_at is None or closed_at > sample_point
            )
            if is_open:
                row[difficulty] += 1

        series.append(row)

    return series


def getDifficultyOverTime(
    issues: list[IssueRecord],
    *,
    today: datetime | None = None,
) -> list[dict[str, str | int]]:
    """Compatibility wrapper for callers expecting camelCase naming."""
    return get_difficulty_over_time(issues, today=today)


def cumulative_timeseries(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """
    Build a cumulative count series over time.

    Parameters
    ----------
    df
        Input dataframe containing a datetime column.
    date_col
        Name of the datetime column to use for the timeline.

    Returns:
    -------
    pd.DataFrame
        Dataframe with:
        - ``date_col``: timeline values
        - ``count``: cumulative count
    """
    if df.empty:
        return pd.DataFrame(columns=[date_col, "count"])

    out = (
        df[[date_col]]
        .dropna()
        .sort_values(date_col)
        .assign(count=1)
    )

    out["count"] = out["count"].cumsum()

    return out.reset_index(drop=True)
