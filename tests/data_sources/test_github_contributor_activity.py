"""Tests for contributor activity ingestion."""

from datetime import datetime
from unittest.mock import Mock

import pytest
import requests

import hiero_analytics.data_sources.github_contributor_activity as contributor_activity
from hiero_analytics.data_sources.models import ContributorActivityRecord, RepositoryRecord


@pytest.fixture
def mock_client():
    return Mock()


def test_fetch_repo_contributor_activity_graphql(mock_client):
    mock_client.graphql.side_effect = [
        {
            "data": {
                "repository": {
                    "issues": {
                        "nodes": [
                            {
                                "number": 1,
                                "createdAt": "2024-01-01T00:00:00Z",
                                "updatedAt": "2024-01-03T00:00:00Z",
                                "author": {"login": "alice"},
                                "timelineItems": {
                                    "nodes": [
                                        {
                                            "__typename": "LabeledEvent",
                                            "createdAt": "2024-01-03T00:00:00Z",
                                            "actor": {"login": "carol"},
                                            "label": {"name": "bug"},
                                        }
                                    ]
                                },
                            }
                        ],
                        "pageInfo": {
                            "hasNextPage": False,
                            "endCursor": None,
                        },
                    }
                }
            }
        },
        {
            "data": {
                "repository": {
                    "pullRequests": {
                        "nodes": [
                            {
                                "number": 10,
                                "createdAt": "2024-01-04T00:00:00Z",
                                "updatedAt": "2024-01-08T00:00:00Z",
                                "mergedAt": "2024-01-07T00:00:00Z",
                                "author": {"login": "dave"},
                                "mergedBy": {"login": "erin"},
                                "reviews": {
                                    "nodes": [
                                        {
                                            "createdAt": "2024-01-06T00:00:00Z",
                                            "author": {"login": "gina"},
                                        }
                                    ]
                                },
                                "timelineItems": {
                                    "nodes": [
                                        {
                                            "__typename": "UnlabeledEvent",
                                            "createdAt": "2024-01-08T00:00:00Z",
                                            "actor": {"login": "harry"},
                                            "label": {"name": "bug"},
                                        }
                                    ]
                                },
                            }
                        ],
                        "pageInfo": {
                            "hasNextPage": False,
                            "endCursor": None,
                        },
                    }
                }
            }
        },
    ]

    records = contributor_activity.fetch_repo_contributor_activity_graphql(
        mock_client,
        "org",
        "repo",
        use_cache=False,
    )

    activity_types = [record.activity_type for record in records]

    assert isinstance(records[0], ContributorActivityRecord)
    assert "authored_issue" in activity_types
    assert "labeled_issue" in activity_types
    assert "authored_pull_request" in activity_types
    assert "reviewed_pull_request" in activity_types
    assert "merged_pull_request" in activity_types
    assert "unlabeled_pull_request" in activity_types


def test_fetch_repo_contributor_activity_graphql_uses_repo_relative_lookback(mock_client):
    mock_client.graphql.side_effect = [
        {
            "data": {
                "repository": {
                    "issues": {
                        "nodes": [
                            {
                                "number": 1,
                                "createdAt": "2024-01-01T00:00:00Z",
                                "updatedAt": "2024-05-15T00:00:00Z",
                                "author": {"login": "alice"},
                                "timelineItems": {"nodes": []},
                            }
                        ],
                        "pageInfo": {
                            "hasNextPage": True,
                            "endCursor": "older-issues",
                        },
                    }
                }
            }
        },
        {
            "data": {
                "repository": {
                    "pullRequests": {
                        "nodes": [
                            {
                                "number": 10,
                                "createdAt": "2024-01-04T00:00:00Z",
                                "updatedAt": "2024-08-01T00:00:00Z",
                                "mergedAt": "2024-07-15T00:00:00Z",
                                "author": {"login": "dave"},
                                "mergedBy": {"login": "erin"},
                                "reviews": {"nodes": []},
                                "timelineItems": {"nodes": []},
                            }
                        ],
                        "pageInfo": {
                            "hasNextPage": False,
                            "endCursor": None,
                        },
                    }
                }
            }
        },
    ]

    records = contributor_activity.fetch_repo_contributor_activity_graphql(
        mock_client,
        "org",
        "repo",
        lookback_days=30,
        use_cache=False,
    )

    assert mock_client.graphql.call_count == 2
    assert [record.activity_type for record in records] == ["merged_pull_request"]
    assert records[0].occurred_at == datetime.fromisoformat("2024-07-15T00:00:00+00:00")


def test_fetch_org_contributor_activity_graphql(monkeypatch, mock_client):
    repos = [
        RepositoryRecord("org/repo1", "repo1", "org"),
        RepositoryRecord("org/repo2", "repo2", "org"),
    ]

    monkeypatch.setattr(
        contributor_activity,
        "fetch_org_repos_graphql",
        lambda client, org, **_kwargs: repos,
    )
    monkeypatch.setattr(
        contributor_activity,
        "fetch_repo_contributor_activity_graphql",
        lambda client, owner, repo, **_kwargs: [
            ContributorActivityRecord(
                repo=f"{owner}/{repo}",
                actor="alice",
                occurred_at=datetime(2024, 1, 1),
                activity_type="authored_issue",
                target_type="issue",
                target_number=1,
                target_author="alice",
            )
        ],
    )

    records = contributor_activity.fetch_org_contributor_activity_graphql(
        mock_client,
        "org",
        max_workers=2,
        lookback_days=90,
        use_cache=False,
    )

    assert {record.repo for record in records} == {"org/repo1", "org/repo2"}
    assert len(records) == 2


def test_fetch_org_contributor_activity_graphql_forwards_lookback_days(monkeypatch, mock_client):
    repos = [RepositoryRecord("org/repo1", "repo1", "org")]
    forwarded_lookbacks: list[int | None] = []

    monkeypatch.setattr(
        contributor_activity,
        "fetch_org_repos_graphql",
        lambda client, org, **_kwargs: repos,
    )

    def fake_fetch_repo_activity(client, owner, repo, **kwargs):
        forwarded_lookbacks.append(kwargs.get("lookback_days"))
        return [
            ContributorActivityRecord(
                repo=f"{owner}/{repo}",
                actor="alice",
                occurred_at=datetime(2024, 1, 1),
                activity_type="authored_issue",
                target_type="issue",
                target_number=1,
                target_author="alice",
            )
        ]

    monkeypatch.setattr(
        contributor_activity,
        "fetch_repo_contributor_activity_graphql",
        fake_fetch_repo_activity,
    )

    contributor_activity.fetch_org_contributor_activity_graphql(
        mock_client,
        "org",
        max_workers=1,
        lookback_days=90,
        use_cache=False,
    )

    assert forwarded_lookbacks == [90]


def test_fetch_org_contributor_activity_graphql_filters_repos_and_pauses(monkeypatch, mock_client):
    repos = [
        RepositoryRecord("org/repo1", "repo1", "org"),
        RepositoryRecord("org/repo2", "repo2", "org"),
        RepositoryRecord("org/repo3", "repo3", "org"),
    ]
    fetched_repos: list[str] = []
    pause_calls: list[float] = []

    monkeypatch.setattr(
        contributor_activity,
        "fetch_org_repos_graphql",
        lambda client, org, **_kwargs: repos,
    )

    def fake_fetch_repo_activity(client, owner, repo, **_kwargs):
        fetched_repos.append(f"{owner}/{repo}")
        return [
            ContributorActivityRecord(
                repo=f"{owner}/{repo}",
                actor="alice",
                occurred_at=datetime(2024, 1, 1),
                activity_type="authored_issue",
                target_type="issue",
                target_number=1,
                target_author="alice",
            )
        ]

    monkeypatch.setattr(
        contributor_activity,
        "fetch_repo_contributor_activity_graphql",
        fake_fetch_repo_activity,
    )
    monkeypatch.setattr(contributor_activity.time, "sleep", pause_calls.append)

    records = contributor_activity.fetch_org_contributor_activity_graphql(
        mock_client,
        "org",
        max_workers=1,
        repos=["repo1", "org/repo3"],
        repo_pause_seconds=2.5,
        use_cache=False,
    )

    assert [record.repo for record in records] == ["org/repo1", "org/repo3"]
    assert fetched_repos == ["org/repo1", "org/repo3"]
    assert pause_calls == [2.5]


def test_fetch_org_contributor_activity_graphql_rejects_unknown_repo_filters(monkeypatch, mock_client):
    monkeypatch.setattr(
        contributor_activity,
        "fetch_org_repos_graphql",
        lambda client, org, **_kwargs: [RepositoryRecord("org/repo1", "repo1", "org")],
    )

    with pytest.raises(ValueError, match="No repositories matched"):
        contributor_activity.fetch_org_contributor_activity_graphql(
            mock_client,
            "org",
            max_workers=1,
            repos=["repo-missing"],
            use_cache=False,
        )


def test_fetch_org_contributor_activity_graphql_summarizes_partial_failures(
    monkeypatch,
    mock_client,
    caplog,
):
    repos = [
        RepositoryRecord("org/repo1", "repo1", "org"),
        RepositoryRecord("org/repo2", "repo2", "org"),
    ]

    monkeypatch.setattr(
        contributor_activity,
        "fetch_org_repos_graphql",
        lambda client, org, **_kwargs: repos,
    )

    def fake_fetch_repo_activity(client, owner, repo, **_kwargs):
        if repo == "repo1":
            return [
                ContributorActivityRecord(
                    repo=f"{owner}/{repo}",
                    actor="alice",
                    occurred_at=datetime(2024, 1, 1),
                    activity_type="authored_issue",
                    target_type="issue",
                    target_number=1,
                    target_author="alice",
                )
            ]

        response = Mock(status_code=403)
        response.json.return_value = {"message": "You have exceeded a secondary rate limit."}
        raise requests.HTTPError("403 Client Error", response=response)

    monkeypatch.setattr(
        contributor_activity,
        "fetch_repo_contributor_activity_graphql",
        fake_fetch_repo_activity,
    )

    with caplog.at_level("WARNING"):
        records = contributor_activity.fetch_org_contributor_activity_graphql(
            mock_client,
            "org",
            max_workers=1,
            lookback_days=90,
            use_cache=False,
        )

    assert len(records) == 1
    assert "skipped 1 repositories" in caplog.text.lower()
