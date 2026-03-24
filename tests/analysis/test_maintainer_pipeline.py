from datetime import UTC, datetime

from hiero_analytics.analysis.maintainer_pipeline import (
    STAGE_COLUMNS,
    activity_to_stage_dataframe,
    build_maintainer_repo_pipeline,
    build_maintainer_yearly_pipeline,
)
from hiero_analytics.data_sources.models import ContributorActivityRecord


def _record(activity_type: str, actor: str, repo: str, year: int) -> ContributorActivityRecord:
    return ContributorActivityRecord(
        repo=repo,
        activity_type=activity_type,
        actor=actor,
        occurred_at=datetime(year, 1, 1, tzinfo=UTC),
        target_type="pull_request",
        target_number=1,
    )


def test_activity_to_stage_dataframe_filters_unknown_types():
    records = [
        _record("authored_pull_request", "alice", "org/repo-a", 2024),
        _record("reviewed_pull_request", "bob", "org/repo-a", 2024),
        _record("merged_pull_request", "carol", "org/repo-a", 2024),
        _record("ignored_event", "dave", "org/repo-a", 2024),
    ]

    df = activity_to_stage_dataframe(records)

    assert set(df["stage"]) == set(STAGE_COLUMNS)
    assert len(df) == 3
    assert set(df["repo"]) == {"repo-a"}


def test_build_maintainer_yearly_pipeline_counts_unique_actors_per_stage():
    records = [
        _record("authored_pull_request", "alice", "org/repo-a", 2024),
        _record("authored_pull_request", "alice", "org/repo-a", 2024),
        _record("reviewed_pull_request", "bob", "org/repo-a", 2024),
        _record("merged_pull_request", "carol", "org/repo-a", 2024),
    ]

    stage_df = activity_to_stage_dataframe(records)
    yearly = build_maintainer_yearly_pipeline(stage_df)

    row = yearly.iloc[0]
    assert row["year"] == 2024
    assert row["general_user"] == 1
    assert row["triage"] == 1
    assert row["committer_maintainer"] == 1


def test_build_maintainer_repo_pipeline_sorts_by_total():
    records = [
        _record("authored_pull_request", "alice", "org/repo-a", 2024),
        _record("reviewed_pull_request", "bob", "org/repo-a", 2024),
        _record("merged_pull_request", "carol", "org/repo-a", 2024),
        _record("authored_pull_request", "dana", "org/repo-b", 2024),
    ]

    stage_df = activity_to_stage_dataframe(records)
    by_repo = build_maintainer_repo_pipeline(stage_df)

    assert by_repo.iloc[0]["repo"] == "repo-a"
    assert by_repo.iloc[0]["general_user"] == 1
    assert by_repo.iloc[0]["triage"] == 1
    assert by_repo.iloc[0]["committer_maintainer"] == 1
