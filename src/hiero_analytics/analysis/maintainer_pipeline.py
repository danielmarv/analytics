"""Contributor-responsibility pipeline analysis built from GitHub activity."""

from __future__ import annotations

import pandas as pd

from hiero_analytics.data_sources.models import ContributorActivityRecord

GENERAL_USER_STAGE = "general_user"
TRIAGE_STAGE = "triage"
MAINTAINER_STAGE = "maintainer"

STAGE_ORDER = [
    GENERAL_USER_STAGE,
    TRIAGE_STAGE,
    MAINTAINER_STAGE,
]

STAGE_LABELS = {
    GENERAL_USER_STAGE: "General users",
    TRIAGE_STAGE: "Triage contributors",
    MAINTAINER_STAGE: "Maintainers",
}

GENERAL_ACTIVITY_TYPES = {
    "authored_issue",
    "authored_pull_request",
    "commented_on_issue",
    "commented_on_pull_request",
}

TRIAGE_ACTIVITY_TYPES = {
    "assigned_issue",
    "closed_issue",
    "closed_pull_request",
    "labeled_issue",
    "labeled_pull_request",
    "reopened_issue",
    "reopened_pull_request",
    "reviewed_pull_request",
    "unlabeled_issue",
    "unlabeled_pull_request",
}

MAINTAINER_ACTIVITY_TYPES = {
    "merged_pull_request",
}


def _period_column_name(frequency: str) -> str:
    """Return the output column name for a supported timeline frequency."""
    if frequency == "year":
        return "year"
    if frequency == "month":
        return "month"

    raise ValueError(f"Unsupported frequency: {frequency}")


def _bucket_period(
    timestamps: pd.Series,
    *,
    frequency: str,
) -> pd.Series:
    """Bucket timestamps into a supported reporting period."""
    normalized = pd.to_datetime(timestamps, utc=True)
    if frequency == "year":
        return normalized.dt.year.astype(int)
    if frequency == "month":
        return normalized.dt.strftime("%Y-%m")

    raise ValueError(f"Unsupported frequency: {frequency}")


def activity_records_to_dataframe(
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


def _is_triage_signal(row: pd.Series) -> bool:
    """Treat management actions on other users' work as triage evidence."""
    if row["activity_type"] not in TRIAGE_ACTIVITY_TYPES:
        return False

    target_author = row["target_author"]
    actor = row["actor"]

    return not (isinstance(target_author, str) and target_author == actor)


def _activity_stage(row: pd.Series) -> str | None:
    """Map a normalized activity row to the highest stage it signals."""
    activity_type = row["activity_type"]

    if activity_type in MAINTAINER_ACTIVITY_TYPES:
        return MAINTAINER_STAGE
    if _is_triage_signal(row):
        return TRIAGE_STAGE
    if activity_type in GENERAL_ACTIVITY_TYPES:
        return GENERAL_USER_STAGE

    return None


def summarize_actor_stage_journeys(
    activities: list[ContributorActivityRecord],
    *,
    by_repo: bool,
) -> pd.DataFrame:
    """
    Build a stage-journey table from normalized contributor activity.

    When ``by_repo`` is True, journeys are tracked separately per repository.
    When False, the earliest signal per actor is used across the whole org.
    """
    prefix = ["repo", "actor"] if by_repo else ["actor"]
    timestamp_columns = [f"first_{stage}_at" for stage in STAGE_ORDER]
    columns = [*prefix, *timestamp_columns, "highest_stage"]

    activity_df = activity_records_to_dataframe(activities)
    if activity_df.empty:
        return pd.DataFrame(columns=columns)

    filtered = activity_df[~activity_df["actor"].astype(str).str.endswith("[bot]")].copy()
    filtered["stage"] = filtered.apply(_activity_stage, axis=1)
    filtered = filtered.dropna(subset=["stage"])

    if filtered.empty:
        return pd.DataFrame(columns=columns)

    journey_rows = (
        filtered.groupby([*prefix, "stage"], as_index=False)["occurred_at"]
        .min()
        .pivot(index=prefix, columns="stage", values="occurred_at")
        .reset_index()
    )

    journey_rows.columns.name = None

    for stage in STAGE_ORDER:
        if stage not in journey_rows.columns:
            journey_rows[stage] = pd.NaT

    for stage in STAGE_ORDER:
        journey_rows[f"first_{stage}_at"] = pd.to_datetime(journey_rows[stage], utc=True)

    def resolve_highest_stage(row: pd.Series) -> str | None:
        highest: str | None = None
        for stage in STAGE_ORDER:
            if pd.notna(row[f"first_{stage}_at"]):
                highest = stage
        return highest

    journey_rows["highest_stage"] = journey_rows.apply(resolve_highest_stage, axis=1)

    result = journey_rows[[*prefix, *timestamp_columns, "highest_stage"]].copy()

    sort_columns = ["actor"] if not by_repo else ["repo", "actor"]
    return result.sort_values(sort_columns).reset_index(drop=True)


def build_maintainer_pipeline(stage_journeys: pd.DataFrame) -> pd.DataFrame:
    """Count yearly first-entry signals for each responsibility stage."""
    return build_stage_entry_timeline(stage_journeys, frequency="year")


def build_stage_entry_timeline(
    stage_journeys: pd.DataFrame,
    *,
    frequency: str,
) -> pd.DataFrame:
    """Count first-entry stage signals by year or month."""
    period_col = _period_column_name(frequency)
    columns = [period_col, *STAGE_ORDER]
    if stage_journeys.empty:
        return pd.DataFrame(columns=columns)

    counts_by_stage: list[pd.DataFrame] = []

    for stage in STAGE_ORDER:
        stage_column = f"first_{stage}_at"
        stage_df = stage_journeys.dropna(subset=[stage_column]).copy()

        if stage_df.empty:
            counts_by_stage.append(pd.DataFrame(columns=[period_col, stage]))
            continue

        stage_df[period_col] = _bucket_period(stage_df[stage_column], frequency=frequency)
        counts = stage_df.groupby(period_col, as_index=False).size().rename(columns={"size": stage})
        counts_by_stage.append(counts)

    timeline = counts_by_stage[0]
    for counts in counts_by_stage[1:]:
        timeline = timeline.merge(counts, on=period_col, how="outer")

    timeline = (
        timeline.fillna(0)
        .astype({stage: int for stage in STAGE_ORDER})
        .sort_values(period_col)
        .reset_index(drop=True)
    )

    if frequency == "year":
        timeline[period_col] = timeline[period_col].astype(int)

    return timeline[columns]


def build_cumulative_stage_timeline(
    stage_timeline: pd.DataFrame,
    *,
    period_col: str,
) -> pd.DataFrame:
    """Convert a stage-entry timeline into cumulative stage totals."""
    columns = [period_col, *STAGE_ORDER]
    if stage_timeline.empty:
        return pd.DataFrame(columns=columns)

    cumulative = stage_timeline.sort_values(period_col).reset_index(drop=True).copy()
    cumulative[STAGE_ORDER] = cumulative[STAGE_ORDER].cumsum()
    return cumulative[columns]


def build_stage_activity_timeline(
    activities: list[ContributorActivityRecord],
    *,
    frequency: str,
) -> pd.DataFrame:
    """Count unique active contributors per stage signal by year or month."""
    period_col = _period_column_name(frequency)
    columns = [period_col, *STAGE_ORDER]

    activity_df = activity_records_to_dataframe(activities)
    if activity_df.empty:
        return pd.DataFrame(columns=columns)

    filtered = activity_df[~activity_df["actor"].astype(str).str.endswith("[bot]")].copy()
    filtered["stage"] = filtered.apply(_activity_stage, axis=1)
    filtered = filtered.dropna(subset=["stage"])

    if filtered.empty:
        return pd.DataFrame(columns=columns)

    filtered[period_col] = _bucket_period(filtered["occurred_at"], frequency=frequency)
    filtered = filtered.drop_duplicates(subset=[period_col, "stage", "actor"])

    timeline = (
        filtered.groupby([period_col, "stage"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=STAGE_ORDER, fill_value=0)
        .reset_index()
        .sort_values(period_col)
        .reset_index(drop=True)
    )

    return timeline[columns]


def build_repo_stage_distribution(stage_journeys: pd.DataFrame) -> pd.DataFrame:
    """Count contributors by their highest observed stage in each repository."""
    columns = ["repo", *STAGE_ORDER]
    if stage_journeys.empty:
        return pd.DataFrame(columns=columns)

    distribution = (
        stage_journeys.dropna(subset=["highest_stage"])
        .groupby(["repo", "highest_stage"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=STAGE_ORDER, fill_value=0)
        .reset_index()
    )

    distribution["total"] = distribution[STAGE_ORDER].sum(axis=1)
    distribution = distribution.sort_values(["total", "repo"], ascending=[False, True]).drop(columns="total")

    return distribution.reset_index(drop=True)[columns]
