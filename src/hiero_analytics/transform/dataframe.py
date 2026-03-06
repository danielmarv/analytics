from __future__ import annotations

import pandas as pd
from hiero_analytics.data_sources.models import IssueRecord


def issues_to_dataframe(issues: list[IssueRecord]) -> pd.DataFrame:
    """
    Convert normalized IssueRecord objects into a Pandas DataFrame.
    """

    return pd.DataFrame(
        [
            {
                "repo": issue.repo,
                "number": issue.number,
                "state": issue.state,
                "created_at": issue.created_at,
                "year": issue.created_at.year,
                "labels": issue.labels,
            }
            for issue in issues
        ]
    )