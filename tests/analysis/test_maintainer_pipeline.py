"""Tests for contributor-responsibility pipeline analysis."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from hiero_analytics.analysis.maintainer_pipeline import (
    GENERAL_USER_STAGE,
    MAINTAINER_STAGE,
    TRIAGE_STAGE,
    build_maintainer_pipeline,
    build_cumulative_stage_timeline,
    build_repo_stage_distribution,
    build_stage_activity_timeline,
    build_stage_entry_timeline,
    summarize_actor_stage_journeys,
)
from hiero_analytics.data_sources.models import ContributorActivityRecord


def _dt(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, tzinfo=UTC)


def test_summarize_actor_stage_journeys_by_repo():
    """Journeys should ignore bots and self-management triage signals."""
    activities = [
        ContributorActivityRecord(
            repo="org/repo-one",
            actor="alice",
            occurred_at=_dt(2024, 1, 1),
            activity_type="authored_issue",
            target_type="issue",
            target_number=1,
            target_author="alice",
        ),
        ContributorActivityRecord(
            repo="org/repo-one",
            actor="alice",
            occurred_at=_dt(2025, 1, 2),
            activity_type="reviewed_pull_request",
            target_type="pull_request",
            target_number=5,
            target_author="bob",
            detail="APPROVED",
        ),
        ContributorActivityRecord(
            repo="org/repo-one",
            actor="alice",
            occurred_at=_dt(2025, 6, 1),
            activity_type="merged_pull_request",
            target_type="pull_request",
            target_number=5,
            target_author="bob",
        ),
        ContributorActivityRecord(
            repo="org/repo-one",
            actor="bob",
            occurred_at=_dt(2024, 3, 1),
            activity_type="authored_pull_request",
            target_type="pull_request",
            target_number=7,
            target_author="bob",
        ),
        ContributorActivityRecord(
            repo="org/repo-two",
            actor="cara",
            occurred_at=_dt(2024, 4, 1),
            activity_type="authored_issue",
            target_type="issue",
            target_number=9,
            target_author="cara",
        ),
        ContributorActivityRecord(
            repo="org/repo-two",
            actor="cara",
            occurred_at=_dt(2024, 4, 5),
            activity_type="labeled_issue",
            target_type="issue",
            target_number=10,
            target_author="dan",
            detail="bug",
        ),
        ContributorActivityRecord(
            repo="org/repo-two",
            actor="bob",
            occurred_at=_dt(2024, 4, 7),
            activity_type="labeled_issue",
            target_type="issue",
            target_number=7,
            target_author="bob",
            detail="help wanted",
        ),
        ContributorActivityRecord(
            repo="org/repo-two",
            actor="renovate[bot]",
            occurred_at=_dt(2024, 5, 1),
            activity_type="authored_pull_request",
            target_type="pull_request",
            target_number=12,
            target_author="renovate[bot]",
        ),
    ]

    journeys = summarize_actor_stage_journeys(activities, by_repo=True)

    assert set(journeys["actor"]) == {"alice", "bob", "cara"}

    alice = journeys[journeys["actor"] == "alice"].iloc[0]
    bob = journeys[journeys["actor"] == "bob"].iloc[0]
    cara = journeys[journeys["actor"] == "cara"].iloc[0]

    assert alice["highest_stage"] == MAINTAINER_STAGE
    assert bob["highest_stage"] == GENERAL_USER_STAGE
    assert cara["highest_stage"] == TRIAGE_STAGE
    assert pd.isna(bob["first_triage_at"])


def test_build_maintainer_pipeline_and_repo_stage_distribution():
    """The pipeline and repo distribution should reflect stage first entries."""
    activities = [
        ContributorActivityRecord(
            repo="org/repo-one",
            actor="alice",
            occurred_at=_dt(2024, 1, 1),
            activity_type="authored_issue",
            target_type="issue",
            target_number=1,
            target_author="alice",
        ),
        ContributorActivityRecord(
            repo="org/repo-one",
            actor="alice",
            occurred_at=_dt(2025, 1, 2),
            activity_type="reviewed_pull_request",
            target_type="pull_request",
            target_number=5,
            target_author="bob",
            detail="APPROVED",
        ),
        ContributorActivityRecord(
            repo="org/repo-one",
            actor="alice",
            occurred_at=_dt(2025, 6, 1),
            activity_type="merged_pull_request",
            target_type="pull_request",
            target_number=5,
            target_author="bob",
        ),
        ContributorActivityRecord(
            repo="org/repo-one",
            actor="bob",
            occurred_at=_dt(2024, 3, 1),
            activity_type="authored_pull_request",
            target_type="pull_request",
            target_number=7,
            target_author="bob",
        ),
        ContributorActivityRecord(
            repo="org/repo-two",
            actor="cara",
            occurred_at=_dt(2024, 4, 1),
            activity_type="authored_issue",
            target_type="issue",
            target_number=9,
            target_author="cara",
        ),
        ContributorActivityRecord(
            repo="org/repo-two",
            actor="cara",
            occurred_at=_dt(2024, 4, 5),
            activity_type="labeled_issue",
            target_type="issue",
            target_number=10,
            target_author="dan",
            detail="bug",
        ),
    ]

    org_journeys = summarize_actor_stage_journeys(activities, by_repo=False)
    repo_journeys = summarize_actor_stage_journeys(activities, by_repo=True)

    pipeline = build_maintainer_pipeline(org_journeys)
    repo_distribution = build_repo_stage_distribution(repo_journeys)

    pipeline_2024 = pipeline[pipeline["year"] == 2024].iloc[0]
    pipeline_2025 = pipeline[pipeline["year"] == 2025].iloc[0]

    assert pipeline_2024[GENERAL_USER_STAGE] == 3
    assert pipeline_2024[TRIAGE_STAGE] == 1
    assert pipeline_2024[MAINTAINER_STAGE] == 0
    assert pipeline_2025[GENERAL_USER_STAGE] == 0
    assert pipeline_2025[TRIAGE_STAGE] == 1
    assert pipeline_2025[MAINTAINER_STAGE] == 1

    repo_one = repo_distribution[repo_distribution["repo"] == "org/repo-one"].iloc[0]
    repo_two = repo_distribution[repo_distribution["repo"] == "org/repo-two"].iloc[0]

    assert repo_one[GENERAL_USER_STAGE] == 1
    assert repo_one[TRIAGE_STAGE] == 0
    assert repo_one[MAINTAINER_STAGE] == 1
    assert repo_two[GENERAL_USER_STAGE] == 0
    assert repo_two[TRIAGE_STAGE] == 1
    assert repo_two[MAINTAINER_STAGE] == 0


def test_build_stage_entry_and_activity_timelines():
    """Monthly timelines should expose stage entries, growth, and active contributors."""
    activities = [
        ContributorActivityRecord(
            repo="org/repo-one",
            actor="alice",
            occurred_at=_dt(2024, 1, 1),
            activity_type="authored_issue",
            target_type="issue",
            target_number=1,
            target_author="alice",
        ),
        ContributorActivityRecord(
            repo="org/repo-one",
            actor="alice",
            occurred_at=_dt(2025, 1, 2),
            activity_type="reviewed_pull_request",
            target_type="pull_request",
            target_number=5,
            target_author="bob",
            detail="APPROVED",
        ),
        ContributorActivityRecord(
            repo="org/repo-one",
            actor="alice",
            occurred_at=_dt(2025, 6, 1),
            activity_type="merged_pull_request",
            target_type="pull_request",
            target_number=5,
            target_author="bob",
        ),
        ContributorActivityRecord(
            repo="org/repo-one",
            actor="bob",
            occurred_at=_dt(2024, 3, 1),
            activity_type="authored_pull_request",
            target_type="pull_request",
            target_number=7,
            target_author="bob",
        ),
        ContributorActivityRecord(
            repo="org/repo-two",
            actor="cara",
            occurred_at=_dt(2024, 4, 1),
            activity_type="authored_issue",
            target_type="issue",
            target_number=9,
            target_author="cara",
        ),
        ContributorActivityRecord(
            repo="org/repo-two",
            actor="cara",
            occurred_at=_dt(2024, 4, 5),
            activity_type="labeled_issue",
            target_type="issue",
            target_number=10,
            target_author="dan",
            detail="bug",
        ),
    ]

    org_journeys = summarize_actor_stage_journeys(activities, by_repo=False)

    monthly_entries = build_stage_entry_timeline(org_journeys, frequency="month")
    monthly_cumulative = build_cumulative_stage_timeline(monthly_entries, period_col="month")
    monthly_activity = build_stage_activity_timeline(activities, frequency="month")

    april_2024_entries = monthly_entries[monthly_entries["month"] == "2024-04"].iloc[0]
    june_2025_cumulative = monthly_cumulative[monthly_cumulative["month"] == "2025-06"].iloc[0]
    april_2024_activity = monthly_activity[monthly_activity["month"] == "2024-04"].iloc[0]

    assert april_2024_entries[GENERAL_USER_STAGE] == 1
    assert april_2024_entries[TRIAGE_STAGE] == 1
    assert april_2024_entries[MAINTAINER_STAGE] == 0

    assert june_2025_cumulative[GENERAL_USER_STAGE] == 3
    assert june_2025_cumulative[TRIAGE_STAGE] == 2
    assert june_2025_cumulative[MAINTAINER_STAGE] == 1

    assert april_2024_activity[GENERAL_USER_STAGE] == 1
    assert april_2024_activity[TRIAGE_STAGE] == 1
    assert april_2024_activity[MAINTAINER_STAGE] == 0
