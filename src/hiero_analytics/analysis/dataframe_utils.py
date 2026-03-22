"""Helpers for building analysis dataframes."""

from __future__ import annotations

import pandas as pd

from hiero_analytics.data_sources.models import ContributorActivityRecord, IssueRecord
from hiero_analytics.domain.labels import UNKNOWN_DIFFICULTY, LabelSpec


def build_difficulty_dataframe(
    df: pd.DataFrame,
    difficulty_specs: tuple[LabelSpec, ...],
    *,
    state: str | None = None,
) -> pd.DataFrame:
    """
    Build an aggregate dataframe of issue counts by difficulty level.

    Parameters
    ----------
    df
        DataFrame of issues, expected to contain at least a ``labels`` column
         (a collection of label names) and optionally a ``state`` column.
     difficulty_specs
         Ordered collection of difficulty label specifications. Each
         specification must expose a ``name`` attribute and a ``matches``
         method that accepts a sequence of labels and returns ``True`` if
         the issue belongs to that difficulty bucket.
     state
         Optional state filter. If provided, only issues whose ``state``
         column matches this value are included in the aggregation.

    Returns:
        pd.DataFrame: DataFrame with one row per difficulty level and two columns:
        ``difficulty`` for the bucket name and ``count`` for the number of
        matching issues. Issues that do not match any specification are
        grouped under ``UNKNOWN_DIFFICULTY``.
    """
    if state:
        df = df[df["state"] == state]

    rows = []

    matched_mask = pd.Series(False, index=df.index)

    for spec in difficulty_specs:
        mask = df["labels"].apply(spec.matches)

        matched_mask |= mask

        rows.append(
            {
                "difficulty": spec.name,
                "count": mask.sum(),
            }
        )

    # Unknown difficulty
    unknown_count = (~matched_mask).sum()

    rows.append(
        {
            "difficulty": UNKNOWN_DIFFICULTY,
            "count": unknown_count,
        }
    )

    return pd.DataFrame(rows)


def issues_to_dataframe(issues: list[IssueRecord]) -> pd.DataFrame:
    """
    Convert a collection of IssueRecord objects into a Pandas DataFrame.

    The resulting dataframe contains a normalized tabular representation
    of issue metadata suitable for analytical operations such as filtering,
    grouping, and aggregation.

    Columns produced:
        repo        Repository name
        number      Issue number
        state       Issue state (e.g. "open", "closed")
        created_at  Issue creation timestamp
        year        Year extracted from created_at
        labels      List of issue labels

    Parameters
    ----------
    issues
        List of IssueRecord objects retrieved from the data source layer.

    Returns:
    -------
    pd.DataFrame
        DataFrame containing one row per issue.
    """
    return pd.DataFrame(
        [
            {
                "repo": issue.repo,
                "number": issue.number,
                "state": issue.state.lower(),
                "created_at": issue.created_at,
                "year": issue.created_at.year,
                "labels": issue.labels,
            }
            for issue in issues
        ]
    )


def contributor_activity_to_dataframe(
    activities: list[ContributorActivityRecord],
) -> pd.DataFrame:
    """Convert normalized contributor activity records into a dataframe."""
    columns = [
        "repo",
        "actor",
        "occurred_at",
        "year",
        "activity_type",
        "target_type",
        "target_number",
        "target_author",
        "detail",
    ]
    if not activities:
        return pd.DataFrame(columns=columns)

    frame = pd.DataFrame(
        [
            {
                "repo": activity.repo,
                "actor": activity.actor,
                "occurred_at": activity.occurred_at,
                "year": activity.occurred_at.year,
                "activity_type": activity.activity_type,
                "target_type": activity.target_type,
                "target_number": activity.target_number,
                "target_author": activity.target_author,
                "detail": activity.detail,
            }
            for activity in activities
        ]
    )

    return frame.sort_values(["occurred_at", "repo", "actor", "activity_type"]).reset_index(drop=True)


def filter_by_labels(df: pd.DataFrame, labels: set[str]) -> pd.DataFrame:
    """
    Filter issues that contain at least one label from a given label set.

    This function performs a set intersection between the labels attached
    to each issue and the provided label set.

    Parameters
    ----------
    df
        DataFrame produced by `issues_to_dataframe`.
    labels
        Set of label names to filter for.

    Returns:
    -------
    pd.DataFrame
        Subset of the dataframe containing only issues with matching labels.
    """
    if df.empty:
        return df.copy()

    return df[df["labels"].map(lambda xs: bool(set(xs or []) & labels))]


def count_by(df: pd.DataFrame, *cols: str) -> pd.DataFrame:
    """
    Aggregate issue counts by one or more columns.

    Performs a group-by operation over the specified columns and returns
    the number of issues in each group.

    Parameters
    ----------
    df
        DataFrame produced by `issues_to_dataframe`.
    *cols
        One or more column names to group by.

    Returns:
    -------
    pd.DataFrame
        DataFrame containing the grouping columns and a `count` column
        representing the number of issues in each group.
    """
    if df.empty:
        return pd.DataFrame(columns=[*cols, "count"])

    return df.groupby(list(cols)).size().reset_index(name="count").sort_values(list(cols))
