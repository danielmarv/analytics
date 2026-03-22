"""Contributor responsibility pipeline analysis built from GitHub activity."""

from __future__ import annotations

import pandas as pd

from hiero_analytics.analysis.dataframe_utils import contributor_activity_to_dataframe
from hiero_analytics.data_sources.models import ContributorActivityRecord

GENERAL_USER_STAGE = "general_user"
TRIAGE_STAGE = "triage"
COMMITTER_STAGE = "committer"
MAINTAINER_STAGE = "maintainer"

STAGE_ORDER = [
    GENERAL_USER_STAGE,
    TRIAGE_STAGE,
    COMMITTER_STAGE,
    MAINTAINER_STAGE,
]

STAGE_LABELS = {
    GENERAL_USER_STAGE: "General users",
    TRIAGE_STAGE: "Triage contributors",
    COMMITTER_STAGE: "Committers",
    MAINTAINER_STAGE: "Maintainers",
}

STAGE_RANKS = {stage: index for index, stage in enumerate(STAGE_ORDER)}

GENERAL_ACTIVITY_TYPES = {
    "authored_issue",
    "authored_pull_request",
}

TRIAGE_ACTIVITY_TYPES = {
    "assigned_issue",
    "labeled_issue",
    "labeled_pull_request",
    "unlabeled_issue",
    "unlabeled_pull_request",
}

COMMITTER_ACTIVITY_TYPES = {
    "reviewed_pull_request",
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


def _explicit_activity_stage(row: pd.Series) -> str | None:
    """Map an activity row to the strongest stage that action explicitly signals."""
    activity_type = row["activity_type"]

    if activity_type in MAINTAINER_ACTIVITY_TYPES:
        return MAINTAINER_STAGE
    if activity_type in COMMITTER_ACTIVITY_TYPES:
        return COMMITTER_STAGE
    if activity_type in TRIAGE_ACTIVITY_TYPES:
        target_author = row["target_author"]
        if isinstance(target_author, str) and target_author == row["actor"]:
            return None
        return TRIAGE_STAGE
    if activity_type in GENERAL_ACTIVITY_TYPES:
        return GENERAL_USER_STAGE

    return None


def _timestamp_columns() -> list[str]:
    """Return the ordered first-seen timestamp columns for each stage."""
    return [f"first_{stage}_at" for stage in STAGE_ORDER]


def _backfill_capability_timestamps(stage_journeys: pd.DataFrame) -> pd.DataFrame:
    """Treat higher responsibility signals as implicitly granting earlier stages."""
    result = stage_journeys.copy()

    for row_index in result.index:
        inherited_timestamp = pd.NaT
        for stage in reversed(STAGE_ORDER):
            column = f"first_{stage}_at"
            value = result.at[row_index, column]
            if pd.notna(value):
                inherited_timestamp = value
                continue
            if pd.notna(inherited_timestamp):
                result.at[row_index, column] = inherited_timestamp

    return result


def _highest_stage_from_timestamps(row: pd.Series) -> str | None:
    """Return the highest stage reached by an actor."""
    highest: str | None = None
    for stage in STAGE_ORDER:
        if pd.notna(row[f"first_{stage}_at"]):
            highest = stage
    return highest


def summarize_actor_stage_journeys(
    activities: list[ContributorActivityRecord],
) -> pd.DataFrame:
    """Build an org-wide stage-journey table from normalized contributor activity."""
    columns = ["actor", *_timestamp_columns(), "highest_stage"]

    activity_df = contributor_activity_to_dataframe(activities)
    if activity_df.empty:
        return pd.DataFrame(columns=columns)

    filtered = activity_df[~activity_df["actor"].astype(str).str.endswith("[bot]")].copy()
    filtered["stage"] = filtered.apply(_explicit_activity_stage, axis=1)
    filtered = filtered.dropna(subset=["stage"])

    if filtered.empty:
        return pd.DataFrame(columns=columns)

    journey_rows = (
        filtered.groupby(["actor", "stage"], as_index=False)["occurred_at"]
        .min()
        .pivot(index="actor", columns="stage", values="occurred_at")
        .reset_index()
    )

    journey_rows.columns.name = None

    for stage in STAGE_ORDER:
        if stage not in journey_rows.columns:
            journey_rows[stage] = pd.NaT

    for stage in STAGE_ORDER:
        journey_rows[f"first_{stage}_at"] = pd.to_datetime(journey_rows[stage], utc=True)

    result = _backfill_capability_timestamps(journey_rows[["actor", *_timestamp_columns()]].copy())
    result["highest_stage"] = result.apply(_highest_stage_from_timestamps, axis=1)

    return result.sort_values("actor").reset_index(drop=True)[columns]


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
        timeline.fillna(0).astype({stage: int for stage in STAGE_ORDER}).sort_values(period_col).reset_index(drop=True)
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


def _highest_stage_reached_by_period(row: pd.Series) -> str | None:
    """Return the highest stage an actor had reached by the activity timestamp."""
    highest: str | None = None
    for stage in STAGE_ORDER:
        first_seen = row[f"first_{stage}_at"]
        if pd.notna(first_seen) and first_seen <= row["occurred_at"]:
            highest = stage
    return highest


def build_stage_activity_timeline(
    activities: list[ContributorActivityRecord],
    *,
    frequency: str,
) -> pd.DataFrame:
    """Count active contributors once per period under their highest attained stage."""
    period_col = _period_column_name(frequency)
    columns = [period_col, *STAGE_ORDER]

    activity_df = contributor_activity_to_dataframe(activities)
    if activity_df.empty:
        return pd.DataFrame(columns=columns)

    filtered = activity_df[~activity_df["actor"].astype(str).str.endswith("[bot]")].copy()
    filtered["stage"] = filtered.apply(_explicit_activity_stage, axis=1)
    filtered = filtered.dropna(subset=["stage"])

    if filtered.empty:
        return pd.DataFrame(columns=columns)

    filtered[period_col] = _bucket_period(filtered["occurred_at"], frequency=frequency)
    active_actors = filtered.groupby([period_col, "actor"], as_index=False)["occurred_at"].max()
    stage_journeys = summarize_actor_stage_journeys(activities)
    if stage_journeys.empty:
        return pd.DataFrame(columns=columns)

    active_actors = active_actors.merge(stage_journeys, on="actor", how="left")
    active_actors["stage"] = active_actors.apply(_highest_stage_reached_by_period, axis=1)
    active_actors = active_actors.dropna(subset=["stage"])

    timeline = (
        active_actors.groupby([period_col, "stage"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=STAGE_ORDER, fill_value=0)
        .reset_index()
        .sort_values(period_col)
        .reset_index(drop=True)
    )

    return timeline[columns]
