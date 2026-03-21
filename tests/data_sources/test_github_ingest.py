from datetime import datetime
from unittest.mock import Mock

import pytest
import requests

import hiero_analytics.data_sources.github_ingest as ingest
from hiero_analytics.data_sources.models import (
    ContributorActivityRecord,
    IssueRecord,
    PullRequestDifficultyRecord,
    RepositoryRecord,
)

# ---------------------------------------------------------
# helpers
# ---------------------------------------------------------

@pytest.fixture
def mock_client():
    return Mock()


@pytest.fixture
def bypass_pagination(monkeypatch):
    """
    Replace paginate_cursor with a single-page execution.
    """
    monkeypatch.setattr(
        ingest,
        "paginate_cursor",
        lambda f: f(None)[0],
    )


# ---------------------------------------------------------
# parse datetime
# ---------------------------------------------------------

def test_parse_dt():
    value = "2024-01-01T00:00:00Z"

    dt = ingest._parse_dt(value)

    assert isinstance(dt, datetime)
    assert dt.year == 2024


def test_parse_dt_none():
    assert ingest._parse_dt(None) is None


# ---------------------------------------------------------
# repositories
# ---------------------------------------------------------

def test_fetch_org_repos_graphql(mock_client, bypass_pagination):

    mock_client.graphql.return_value = {
        "data": {
            "organization": {
                "repositories": {
                    "nodes": [
                        {"name": "repo1"},
                        {"name": "repo2"},
                    ],
                    "pageInfo": {
                        "hasNextPage": False,
                        "endCursor": None,
                    },
                }
            }
        }
    }

    repos = ingest.fetch_org_repos_graphql(mock_client, "org")

    assert len(repos) == 2
    assert repos[0].full_name == "org/repo1"
    assert repos[1].name == "repo2"


# ---------------------------------------------------------
# repository issues
# ---------------------------------------------------------

def test_fetch_repo_issues_graphql(mock_client, bypass_pagination):

    mock_client.graphql.return_value = {
        "data": {
            "repository": {
                "issues": {
                    "nodes": [
                        {
                            "number": 1,
                            "title": "Issue A",
                            "state": "OPEN",
                            "createdAt": "2024-01-01T00:00:00Z",
                            "closedAt": None,
                            "labels": {
                                "nodes": [{"name": "bug"}],
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
    }

    issues = ingest.fetch_repo_issues_graphql(mock_client, "org", "repo")

    assert len(issues) == 1

    issue = issues[0]

    assert isinstance(issue, IssueRecord)
    assert issue.repo == "org/repo"
    assert issue.number == 1
    assert issue.labels == ["bug"]


def test_fetch_repo_issues_normalizes_states(mock_client, bypass_pagination):

    mock_client.graphql.return_value = {
        "data": {
            "repository": {
                "issues": {
                    "nodes": [],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }
        }
    }

    ingest.fetch_repo_issues_graphql(
        mock_client,
        "org",
        "repo",
        states=["open"],
    )

    args, _ = mock_client.graphql.call_args

    variables = args[1]

    assert variables["states"] == ["OPEN"]


# ---------------------------------------------------------
# org issues parallel
# ---------------------------------------------------------

def test_fetch_org_issues_graphql_parallel(monkeypatch, mock_client):

    repos = [
        RepositoryRecord("org/repo1", "repo1", "org"),
        RepositoryRecord("org/repo2", "repo2", "org"),
    ]

    monkeypatch.setattr(
        ingest,
        "fetch_org_repos_graphql",
        lambda client, org: repos,
    )

    monkeypatch.setattr(
        ingest,
        "fetch_repo_issues_graphql",
        lambda client, owner, repo, states=None: [
            IssueRecord(
                repo=f"{owner}/{repo}",
                number=1,
                title="Issue",
                state="OPEN",
                created_at=None,
                closed_at=None,
                labels=[],
            )
        ],
    )

    issues = ingest.fetch_org_issues_graphql(mock_client, "org", max_workers=2)

    repos_returned = {i.repo for i in issues}

    assert repos_returned == {"org/repo1", "org/repo2"}
    assert len(issues) == 2


# ---------------------------------------------------------
# merged PR difficulty
# ---------------------------------------------------------

def test_fetch_repo_merged_pr_difficulty_graphql(mock_client, bypass_pagination):

    mock_client.graphql.return_value = {
        "data": {
            "repository": {
                "pullRequests": {
                    "nodes": [
                        {
                            "number": 10,
                            "createdAt": "2024-01-01T00:00:00Z",
                            "mergedAt": "2024-01-02T00:00:00Z",
                            "additions": 5,
                            "deletions": 3,
                            "changedFiles": 2,
                            "closingIssuesReferences": {
                                "nodes": [
                                    {
                                        "number": 1,
                                        "labels": {
                                            "nodes": [{"name": "good first issue"}]
                                        },
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
    }

    records = ingest.fetch_repo_merged_pr_difficulty_graphql(
        mock_client,
        "org",
        "repo",
    )

    assert len(records) == 1

    record = records[0]

    assert isinstance(record, PullRequestDifficultyRecord)
    assert record.repo == "org/repo"
    assert record.pr_number == 10
    assert record.issue_number == 1


# ---------------------------------------------------------
# merged PR org parallel
# ---------------------------------------------------------

def test_fetch_org_merged_pr_difficulty_graphql(monkeypatch, mock_client):

    repos = [
        RepositoryRecord("org/repo1", "repo1", "org"),
        RepositoryRecord("org/repo2", "repo2", "org"),
    ]

    monkeypatch.setattr(
        ingest,
        "fetch_org_repos_graphql",
        lambda client, org: repos,
    )

    monkeypatch.setattr(
        ingest,
        "fetch_repo_merged_pr_difficulty_graphql",
        lambda client, owner, repo: [
            PullRequestDifficultyRecord(
                repo=f"{owner}/{repo}",
                pr_number=1,
                pr_created_at=None,
                pr_merged_at=None,
                pr_additions=1,
                pr_deletions=1,
                pr_changed_files=1,
                issue_number=1,
                issue_labels=[],
            )
        ],
    )

    records = ingest.fetch_org_merged_pr_difficulty_graphql(
        mock_client,
        "org",
        max_workers=2,
    )

    repos_returned = {r.repo for r in records}

    assert repos_returned == {"org/repo1", "org/repo2"}
    assert len(records) == 2


# ---------------------------------------------------------
# contributor activity
# ---------------------------------------------------------

def test_fetch_repo_contributor_activity_graphql(mock_client, bypass_pagination):

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
                                "comments": {
                                    "nodes": [
                                        {
                                            "createdAt": "2024-01-02T00:00:00Z",
                                            "author": {"login": "bob"},
                                        }
                                    ]
                                },
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
                                "comments": {
                                    "nodes": [
                                        {
                                            "createdAt": "2024-01-05T00:00:00Z",
                                            "author": {"login": "frank"},
                                        }
                                    ]
                                },
                                "reviews": {
                                    "nodes": [
                                        {
                                            "createdAt": "2024-01-06T00:00:00Z",
                                            "state": "APPROVED",
                                            "author": {"login": "gina"},
                                        }
                                    ]
                                },
                                "timelineItems": {
                                    "nodes": [
                                        {
                                            "__typename": "ClosedEvent",
                                            "createdAt": "2024-01-08T00:00:00Z",
                                            "actor": {"login": "harry"},
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

    records = ingest.fetch_repo_contributor_activity_graphql(
        mock_client,
        "org",
        "repo",
        use_cache=False,
    )

    activity_types = [record.activity_type for record in records]

    assert isinstance(records[0], ContributorActivityRecord)
    assert "authored_issue" in activity_types
    assert "commented_on_issue" in activity_types
    assert "labeled_issue" in activity_types
    assert "authored_pull_request" in activity_types
    assert "commented_on_pull_request" in activity_types
    assert "reviewed_pull_request" in activity_types
    assert "merged_pull_request" in activity_types
    assert "closed_pull_request" in activity_types


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
                                "comments": {"nodes": []},
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
                                "comments": {"nodes": []},
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

    records = ingest.fetch_repo_contributor_activity_graphql(
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
        ingest,
        "fetch_org_repos_graphql",
        lambda client, org, **_kwargs: repos,
    )

    monkeypatch.setattr(
        ingest,
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

    records = ingest.fetch_org_contributor_activity_graphql(
        mock_client,
        "org",
        max_workers=2,
        lookback_days=90,
        use_cache=False,
    )

    repos_returned = {record.repo for record in records}

    assert repos_returned == {"org/repo1", "org/repo2"}
    assert len(records) == 2


def test_fetch_org_contributor_activity_graphql_forwards_lookback_days(
    monkeypatch,
    mock_client,
):

    repos = [RepositoryRecord("org/repo1", "repo1", "org")]
    forwarded_lookbacks: list[int | None] = []

    monkeypatch.setattr(
        ingest,
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
        ingest,
        "fetch_repo_contributor_activity_graphql",
        fake_fetch_repo_activity,
    )

    ingest.fetch_org_contributor_activity_graphql(
        mock_client,
        "org",
        max_workers=1,
        lookback_days=90,
        use_cache=False,
    )

    assert forwarded_lookbacks == [90]


def test_fetch_org_contributor_activity_graphql_filters_repos_and_pauses(
    monkeypatch,
    mock_client,
):

    repos = [
        RepositoryRecord("org/repo1", "repo1", "org"),
        RepositoryRecord("org/repo2", "repo2", "org"),
        RepositoryRecord("org/repo3", "repo3", "org"),
    ]
    fetched_repos: list[str] = []
    pause_calls: list[float] = []

    monkeypatch.setattr(
        ingest,
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
        ingest,
        "fetch_repo_contributor_activity_graphql",
        fake_fetch_repo_activity,
    )
    monkeypatch.setattr(ingest.time, "sleep", pause_calls.append)

    records = ingest.fetch_org_contributor_activity_graphql(
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


def test_fetch_org_contributor_activity_graphql_rejects_unknown_repo_filters(
    monkeypatch,
    mock_client,
):

    monkeypatch.setattr(
        ingest,
        "fetch_org_repos_graphql",
        lambda client, org, **_kwargs: [RepositoryRecord("org/repo1", "repo1", "org")],
    )

    with pytest.raises(ValueError, match="No repositories matched"):
        ingest.fetch_org_contributor_activity_graphql(
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
        ingest,
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
        ingest,
        "fetch_repo_contributor_activity_graphql",
        fake_fetch_repo_activity,
    )

    with caplog.at_level("WARNING"):
        records = ingest.fetch_org_contributor_activity_graphql(
            mock_client,
            "org",
            max_workers=1,
            lookback_days=90,
            use_cache=False,
        )

    assert len(records) == 1
    assert "skipped 1 repositories" in caplog.text.lower()
