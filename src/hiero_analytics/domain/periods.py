"""Shared time-period definitions for windowed activity analysis."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True, slots=True)
class Period:
    """A named rolling activity window; ``days=None`` means all recorded time."""

    key: str
    label: str
    days: int | None
    default: bool = False

    def cutoff(self, now: datetime) -> datetime | None:
        """Return the inclusive lower bound for this period."""
        return None if self.days is None else now - timedelta(days=self.days)

    def filename(self, stem: str) -> str:
        """Return the conventional CSV filename for this period."""
        return f"{stem}_{self.key}.csv"


ACTIVITY_PERIODS = (
    Period("30d", "30 days", 30),
    # The tuned active/quiet threshold (ROLE_ACTIVE_DAYS) and most tables' prior window,
    # so it is the tab shown first; users can switch to any other from there.
    Period("90d", "90 days", 90, default=True),
    Period("180d", "180 days", 180),
    Period("365d", "1 year", 365),
    Period("all", "All time", None),
)

# The period whose tab opens active on the dashboard.
DEFAULT_ACTIVITY_PERIOD = next(period for period in ACTIVITY_PERIODS if period.default)
