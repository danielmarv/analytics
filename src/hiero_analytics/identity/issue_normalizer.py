from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from hiero_analytics.data_sources.models import IssueRecord


def normalize_issue(issue: Dict[str, Any]) -> IssueRecord:
    """Normalize a raw GitHub issue dict into an IssueRecord."""

    labels: List[str] = [
        str(l).lower()
        for l in issue.get("labels", [])
    ]

    created_str: str = str(issue["created_at"])

    created = datetime.fromisoformat(
        created_str.replace("Z", "+00:00")
    )

    return IssueRecord(
        repo=str(issue["repo"]),
        number=int(issue["number"]),
        title=str(issue["title"]),
        state=str(issue["state"]).lower(),
        created_at=created,
        labels=labels,
    )


def normalize_issues(raw: List[Dict[str, Any]]) -> List[IssueRecord]:
    """Normalize a list of GitHub issues."""
    return [normalize_issue(i) for i in raw]