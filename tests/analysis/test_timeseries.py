"""Tests for issue time-series aggregations."""

from __future__ import annotations

from datetime import UTC, datetime

from hiero_analytics.analysis.timeseries import (
    get_difficulty_over_time,
    getDifficultyOverTime,
)
from hiero_analytics.data_sources.models import IssueRecord


def _issue(
    number: int,
    *,
    created_at: datetime,
    labels: list[str],
    closed_at: datetime | None = None,
) -> IssueRecord:
    """Create a normalized issue record for time-series tests."""
    return IssueRecord(
        repo="org/repo",
        number=number,
        title=f"Issue {number}",
        state="CLOSED" if closed_at else "OPEN",
        created_at=created_at,
        closed_at=closed_at,
        labels=labels,
    )


def test_get_difficulty_over_time_counts_open_issues_weekly() -> None:
    """Weekly samples should count only issues open at each point in time."""
    issues = [
        _issue(
            1,
            created_at=datetime(2024, 1, 1, tzinfo=UTC),
            labels=["good first issue"],
        ),
        _issue(
            2,
            created_at=datetime(2024, 1, 5, tzinfo=UTC),
            closed_at=datetime(2024, 1, 20, tzinfo=UTC),
            labels=["beginner"],
        ),
        _issue(
            3,
            created_at=datetime(2024, 1, 10, tzinfo=UTC),
            closed_at=datetime(2024, 1, 17, tzinfo=UTC),
            labels=["advanced"],
        ),
        _issue(
            4,
            created_at=datetime(2024, 1, 12, tzinfo=UTC),
            labels=["intermediate"],
        ),
        _issue(
            5,
            created_at=datetime(2024, 1, 2, tzinfo=UTC),
            labels=["bug"],
        ),
    ]

    series = get_difficulty_over_time(
        issues,
        today=datetime(2024, 1, 22, tzinfo=UTC),
    )

    assert series == [
        {
            "date": "2024-01-01",
            "advanced": 0,
            "intermediate": 0,
            "beginner": 0,
            "gfi": 1,
        },
        {
            "date": "2024-01-08",
            "advanced": 0,
            "intermediate": 0,
            "beginner": 1,
            "gfi": 1,
        },
        {
            "date": "2024-01-15",
            "advanced": 1,
            "intermediate": 1,
            "beginner": 1,
            "gfi": 1,
        },
        {
            "date": "2024-01-22",
            "advanced": 0,
            "intermediate": 1,
            "beginner": 0,
            "gfi": 1,
        },
    ]


def test_get_difficulty_over_time_uses_existing_difficulty_order() -> None:
    """Multi-labeled issues should resolve to the first configured bucket."""
    issues = [
        _issue(
            1,
            created_at=datetime(2024, 1, 1, tzinfo=UTC),
            labels=["advanced", "beginner"],
        )
    ]

    series = getDifficultyOverTime(
        issues,
        today=datetime(2024, 1, 1, tzinfo=UTC),
    )

    assert series == [
        {
            "date": "2024-01-01",
            "advanced": 0,
            "intermediate": 0,
            "beginner": 1,
            "gfi": 0,
        }
    ]


def test_get_difficulty_over_time_ignores_unlabeled_issues() -> None:
    """Issues without a known difficulty label should be excluded."""
    issues = [
        _issue(
            1,
            created_at=datetime(2024, 1, 1, tzinfo=UTC),
            labels=["bug"],
        )
    ]

    assert get_difficulty_over_time(issues, today=datetime(2024, 1, 8, tzinfo=UTC)) == []
