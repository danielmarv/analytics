"""Tests for shared activity period definitions."""

from datetime import UTC, datetime

from hiero_analytics.domain.periods import ACTIVITY_PERIODS, DEFAULT_ACTIVITY_PERIOD


def test_activity_periods_have_stable_cutoffs_and_filenames():
    """Rolling periods expose a cutoff while all-time does not."""
    now = datetime(2026, 7, 21, tzinfo=UTC)
    periods = {period.key: period for period in ACTIVITY_PERIODS}

    assert periods["90d"].cutoff(now) == datetime(2026, 4, 22, tzinfo=UTC)
    assert periods["90d"].filename("team_activity_summary") == "team_activity_summary_90d.csv"
    assert periods["all"].cutoff(now) is None


def test_exactly_one_period_is_the_dashboard_default():
    """Tables open on the 90-day window, the tuned active/quiet threshold."""
    assert DEFAULT_ACTIVITY_PERIOD.key == "90d"
    assert sum(period.default for period in ACTIVITY_PERIODS) == 1
