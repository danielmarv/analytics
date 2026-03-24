from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest

import hiero_analytics.data_sources.github_ingest as ingest
from hiero_analytics.data_sources.models import ContributorActivityRecord, RepositoryRecord


@pytest.fixture
def mock_client():
    return Mock()


@pytest.fixture
def bypass_pagination(monkeypatch):
    monkeypatch.setattr(
        ingest,
        "paginate_cursor",
        lambda f: f(None)[0],
    )


def _to_iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def test_fetch_repo_contributor_activity_graphql(mock_client, bypass_pagination):
    now = datetime.now(UTC)
    created_at = _to_iso(now - timedelta(days=5))
    reviewed_at = _to_iso(now - timedelta(days=4))
    merged_at = _to_iso(now - timedelta(days=3))

    mock_client.graphql.return_value = {
        "data": {
            "repository": {
                "pullRequests": {
                    "nodes": [
                        {
                            "number": 10,
                            "createdAt": created_at,
                            "updatedAt": merged_at,
                            "mergedAt": merged_at,
                            "author": {"login": "alice"},
                            "mergedBy": {"login": "carol"},
                            "reviews": {
                                "nodes": [
                                    {
                                        "state": "APPROVED",
                                        "submittedAt": reviewed_at,
                                        "author": {"login": "bob"},
                                    }
                                ]
                            },
                        }
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }
        }
    }

    records = ingest.fetch_repo_contributor_activity_graphql(
        mock_client,
        "org",
        "repo",
        lookback_days=30,
        use_cache=False,
    )

    assert len(records) == 3
    assert all(isinstance(record, ContributorActivityRecord) for record in records)
    assert {record.activity_type for record in records} == {
        "authored_pull_request",
        "reviewed_pull_request",
        "merged_pull_request",
    }


def test_fetch_org_contributor_activity_graphql(monkeypatch, mock_client):
    repos = [
        RepositoryRecord("org/repo1", "repo1", "org"),
        RepositoryRecord("org/repo2", "repo2", "org"),
    ]

    monkeypatch.setattr(
        ingest,
        "fetch_org_repos_graphql",
        lambda client, org, **kwargs: repos,
    )

    monkeypatch.setattr(
        ingest,
        "fetch_repo_contributor_activity_graphql",
        lambda client, owner, repo, **kwargs: [
            ContributorActivityRecord(
                repo=f"{owner}/{repo}",
                activity_type="authored_pull_request",
                actor="alice",
                occurred_at=datetime.now(UTC),
                target_type="pull_request",
                target_number=1,
            )
        ],
    )

    records = ingest.fetch_org_contributor_activity_graphql(
        mock_client,
        "org",
        max_workers=2,
        use_cache=False,
    )

    assert len(records) == 2
    assert {record.repo for record in records} == {"org/repo1", "org/repo2"}
