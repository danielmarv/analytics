"""Tests for the sequential maintainer pipeline runner."""

from __future__ import annotations

from datetime import datetime

import pytest

import hiero_analytics.run_maintainer_pipeline_repo_by_repo as sequential_runner
from hiero_analytics.data_sources.models import ContributorActivityRecord, RepositoryRecord


def test_filter_repositories_preserves_org_order() -> None:
    """Selected repos should be filtered without changing organization order."""
    repositories = [
        RepositoryRecord("hiero-ledger/repo-a", "repo-a", "hiero-ledger"),
        RepositoryRecord("hiero-ledger/repo-b", "repo-b", "hiero-ledger"),
        RepositoryRecord("hiero-ledger/repo-c", "repo-c", "hiero-ledger"),
    ]

    filtered = sequential_runner._filter_repositories(
        repositories,
        ["repo-c", "hiero-ledger/repo-a"],
    )

    assert [repo.full_name for repo in filtered] == [
        "hiero-ledger/repo-a",
        "hiero-ledger/repo-c",
    ]


def test_filter_repositories_rejects_unknown_repos() -> None:
    """Unknown repo filters should fail fast before any fetch work starts."""
    repositories = [RepositoryRecord("hiero-ledger/repo-a", "repo-a", "hiero-ledger")]

    with pytest.raises(ValueError, match="No repositories matched"):
        sequential_runner._filter_repositories(repositories, ["repo-missing"])


def test_main_fetches_repositories_sequentially(monkeypatch, capsys) -> None:
    """The sequential runner should fetch repos in order and save one combined output."""
    repositories = [
        RepositoryRecord("hiero-ledger/repo-a", "repo-a", "hiero-ledger"),
        RepositoryRecord("hiero-ledger/repo-b", "repo-b", "hiero-ledger"),
    ]
    fetched_repos: list[str] = []
    pause_calls: list[float] = []
    saved_activities: list[list[ContributorActivityRecord]] = []

    monkeypatch.setattr(sequential_runner, "ensure_org_dirs", lambda _org: (None, None))
    monkeypatch.setattr(sequential_runner, "resolve_activity_lookback_days", lambda: 183)
    monkeypatch.setattr(sequential_runner, "resolve_activity_cache_ttl_seconds", lambda: 86400)
    monkeypatch.setattr(sequential_runner, "resolve_activity_repo_pause_seconds", lambda: 2.0)
    monkeypatch.setattr(sequential_runner, "resolve_selected_repos", lambda: [])
    monkeypatch.setattr(sequential_runner, "print_maintainer_runtime_settings", lambda **_kwargs: None)
    monkeypatch.setattr(sequential_runner, "GitHubClient", lambda: object())
    monkeypatch.setattr(
        sequential_runner,
        "fetch_org_repos_graphql",
        lambda _client, _org, cache_ttl_seconds: (
            repositories if cache_ttl_seconds == 86400 else []
        ),
    )
    monkeypatch.setattr(sequential_runner.time, "sleep", pause_calls.append)

    def fake_fetch_repo_activity(_client, owner, repo, *, lookback_days, cache_ttl_seconds):
        fetched_repos.append(f"{owner}/{repo}")
        assert lookback_days == 183
        assert cache_ttl_seconds == 86400
        return [
            ContributorActivityRecord(
                repo=f"{owner}/{repo}",
                actor=repo,
                occurred_at=datetime(2024, 1, 1),
                activity_type="authored_issue",
                target_type="issue",
                target_number=1,
                target_author=repo,
            )
        ]

    monkeypatch.setattr(
        sequential_runner,
        "fetch_repo_contributor_activity_graphql",
        fake_fetch_repo_activity,
    )
    monkeypatch.setattr(
        sequential_runner,
        "save_maintainer_pipeline_outputs",
        lambda activities: saved_activities.append(activities),
    )

    sequential_runner.main()

    captured = capsys.readouterr()

    assert fetched_repos == ["hiero-ledger/repo-a", "hiero-ledger/repo-b"]
    assert pause_calls == [2.0]
    assert len(saved_activities) == 1
    assert [activity.repo for activity in saved_activities[0]] == fetched_repos
    assert "Processing 2 repo(s) sequentially" in captured.out
