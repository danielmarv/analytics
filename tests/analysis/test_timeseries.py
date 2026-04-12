"""Tests for cumulative and historical difficulty-over-time series helpers."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from hiero_analytics.analysis.timeseries import (
    cumulative_timeseries,
    get_difficulty_over_time,
    get_difficulty_over_time_windowed,
)
from hiero_analytics.data_sources.models import IssueRecord, IssueTimelineEventRecord


def _issue(
    number: int,
    *,
    created_at: datetime,
    state: str = "OPEN",
    closed_at: datetime | None = None,
) -> IssueRecord:
    """Create an issue record for timeline tests."""
    return IssueRecord(
        repo="org/repo",
        number=number,
        title=f"Issue {number}",
        state=state,
        created_at=created_at,
        closed_at=closed_at,
        labels=[],
    )


def _event(
    issue_number: int,
    event_type: str,
    occurred_at: datetime,
    *,
    label: str | None = None,
) -> IssueTimelineEventRecord:
    """Create an issue timeline event for historical aggregation tests."""
    return IssueTimelineEventRecord(
        repo="org/repo",
        issue_number=issue_number,
        event_type=event_type,
        occurred_at=occurred_at,
        label=label,
    )


def test_get_difficulty_over_time_uses_historical_label_and_state_events() -> None:
    """Issues should only count after a difficulty label becomes active while open."""
    issues = [
        _issue(
            1,
            created_at=datetime(2024, 1, 1, tzinfo=UTC),
        ),
        _issue(
            2,
            created_at=datetime(2024, 1, 1, tzinfo=UTC),
            state="CLOSED",
            closed_at=datetime(2024, 1, 20, tzinfo=UTC),
        ),
        _issue(
            3,
            created_at=datetime(2024, 1, 1, tzinfo=UTC),
        ),
    ]
    events = [
        _event(1, "labeled", datetime(2024, 1, 8, tzinfo=UTC), label="beginner"),
        _event(2, "labeled", datetime(2024, 1, 1, tzinfo=UTC), label="good first issue"),
        _event(2, "closed", datetime(2024, 1, 20, tzinfo=UTC)),
        _event(3, "closed", datetime(2024, 1, 10, tzinfo=UTC)),
        _event(3, "reopened", datetime(2024, 1, 18, tzinfo=UTC)),
        _event(3, "labeled", datetime(2024, 1, 18, tzinfo=UTC), label="advanced"),
    ]

    series = get_difficulty_over_time(
        issues,
        events,
        today=datetime(2024, 1, 22, tzinfo=UTC),
    )

    assert series == [
        {
            "date": "2024-01-01",
            "gfi": 1,
            "beginner": 0,
            "intermediate": 0,
            "advanced": 0,
        },
        {
            "date": "2024-01-08",
            "gfi": 1,
            "beginner": 1,
            "intermediate": 0,
            "advanced": 0,
        },
        {
            "date": "2024-01-15",
            "gfi": 1,
            "beginner": 1,
            "intermediate": 0,
            "advanced": 0,
        },
        {
            "date": "2024-01-22",
            "gfi": 0,
            "beginner": 1,
            "intermediate": 0,
            "advanced": 1,
        },
    ]


def test_get_difficulty_over_time_excludes_issues_without_difficulty_labels() -> None:
    """Removing a difficulty label should remove an open issue from counts."""
    issues = [
        _issue(
            1,
            created_at=datetime(2024, 1, 1, tzinfo=UTC),
        )
    ]
    events = [
        _event(1, "labeled", datetime(2024, 1, 1, tzinfo=UTC), label="intermediate"),
        _event(1, "unlabeled", datetime(2024, 1, 8, tzinfo=UTC), label="intermediate"),
    ]

    series = get_difficulty_over_time(
        issues,
        events,
        today=datetime(2024, 1, 8, tzinfo=UTC),
    )

    assert series == [
        {
            "date": "2024-01-01",
            "gfi": 0,
            "beginner": 0,
            "intermediate": 1,
            "advanced": 0,
        },
        {
            "date": "2024-01-08",
            "gfi": 0,
            "beginner": 0,
            "intermediate": 0,
            "advanced": 0,
        },
    ]


def test_cumulative_timeseries_builds_cumulative_counts() -> None:
    """The cumulative helper should still behave as a simple running total."""
    df = pd.DataFrame(
        {
            "created_at": [
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 3, tzinfo=UTC),
                datetime(2024, 1, 5, tzinfo=UTC),
            ]
        }
    )

    result = cumulative_timeseries(df, "created_at")

    assert list(result["count"]) == [1, 2, 3]


def test_get_difficulty_over_time_windowed_reconstructs_start_state_from_current_state() -> None:
    """Windowed history should infer the state at window start from current issue state plus in-window events."""
    issues = [
        IssueRecord(
            repo="org/repo",
            number=1,
            title="Issue 1",
            state="OPEN",
            created_at=datetime(2024, 1, 1, tzinfo=UTC),
            closed_at=None,
            labels=["beginner"],
        ),
        IssueRecord(
            repo="org/repo",
            number=2,
            title="Issue 2",
            state="OPEN",
            created_at=datetime(2024, 1, 1, tzinfo=UTC),
            closed_at=None,
            labels=[],
        ),
    ]
    events = [
        _event(1, "labeled", datetime(2025, 7, 1, tzinfo=UTC), label="beginner"),
        _event(2, "closed", datetime(2025, 6, 1, tzinfo=UTC)),
        _event(2, "reopened", datetime(2025, 9, 1, tzinfo=UTC)),
    ]

    series = get_difficulty_over_time_windowed(
        issues,
        events,
        start_at=datetime(2025, 4, 11, tzinfo=UTC),
        today=datetime(2026, 4, 11, tzinfo=UTC),
    )

    assert series[0] == {
        "date": "2025-04-11",
        "gfi": 0,
        "beginner": 0,
        "intermediate": 0,
        "advanced": 0,
    }
    assert any(row["beginner"] == 1 for row in series if row["date"] >= "2025-07-04")
