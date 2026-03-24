from __future__ import annotations

import pandas as pd

from hiero_analytics.data_sources.models import ContributorActivityRecord

STAGE_COLUMNS = ["general_user", "triage", "committer_maintainer"]

_STAGE_BY_ACTIVITY = {
    "authored_pull_request": "general_user",
    "reviewed_pull_request": "triage",
    "merged_pull_request": "committer_maintainer",
}


def activity_to_stage_dataframe(records: list[ContributorActivityRecord]) -> pd.DataFrame:
    """Convert contributor activity records into stage-labeled rows."""
    rows = []

    for record in records:
        stage = _STAGE_BY_ACTIVITY.get(record.activity_type)
        if stage is None:
            continue

        rows.append(
            {
                "repo": record.repo.split("/")[-1],
                "actor": record.actor,
                "year": record.occurred_at.year,
                "stage": stage,
            }
        )

    if not rows:
        return pd.DataFrame(columns=["repo", "actor", "year", "stage"])

    return pd.DataFrame(rows)


def build_maintainer_yearly_pipeline(stage_df: pd.DataFrame) -> pd.DataFrame:
    """Build yearly contributor counts for each maintainer-pipeline stage."""
    if stage_df.empty:
        return pd.DataFrame(columns=["year", *STAGE_COLUMNS])

    yearly = (
        stage_df.groupby(["year", "stage"])["actor"]
        .nunique()
        .unstack(fill_value=0)
        .reindex(columns=STAGE_COLUMNS, fill_value=0)
        .reset_index()
        .sort_values("year")
    )

    return yearly.astype({column: int for column in STAGE_COLUMNS})


def build_maintainer_repo_pipeline(stage_df: pd.DataFrame) -> pd.DataFrame:
    """Build repository-level contributor counts for each maintainer-pipeline stage."""
    if stage_df.empty:
        return pd.DataFrame(columns=["repo", *STAGE_COLUMNS])

    by_repo = (
        stage_df.groupby(["repo", "stage"])["actor"]
        .nunique()
        .unstack(fill_value=0)
        .reindex(columns=STAGE_COLUMNS, fill_value=0)
        .reset_index()
    )

    by_repo["total"] = by_repo[STAGE_COLUMNS].sum(axis=1)
    by_repo = by_repo.sort_values("total", ascending=False).drop(columns=["total"])

    return by_repo.astype({column: int for column in STAGE_COLUMNS})
