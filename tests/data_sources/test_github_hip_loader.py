"""Tests for GitHub HIP artifact loading helpers."""

from unittest.mock import Mock

import hiero_analytics.data_sources.github_hip_loader as loader
from hiero_analytics.domain.hip_progression_models import HipArtifact
from tests.hip_progression_fixtures import make_issue_artifact, make_pull_request_artifact


def test_fetch_issue_summaries_filters_pull_requests(monkeypatch):
    """Issue summary fetches should exclude pull request payloads from the issues endpoint."""
    monkeypatch.setattr(
        loader,
        "_fetch_rest_collection",
        lambda *_args, **_kwargs: [
            {"number": 1, "title": "Issue only"},
            {"number": 2, "title": "Actually a PR", "pull_request": {"url": "x"}},
        ],
    )

    summaries = loader._fetch_issue_summaries(Mock(), "org", "repo")

    assert summaries == [{"number": 1, "title": "Issue only"}]


def test_filter_hip_artifacts_by_author_scope():
    """Author scope filtering should preserve maintainers within committer scope."""
    artifacts = [
        make_issue_artifact(author_association="NONE"),
        make_pull_request_artifact(number=2, author_association="COLLABORATOR"),
        make_pull_request_artifact(number=3, author_association="MEMBER"),
    ]

    maintainer_only = loader.filter_hip_artifacts_by_author_scope(artifacts, "maintainers")
    committer_scope = loader.filter_hip_artifacts_by_author_scope(artifacts, "committers")

    assert [artifact.number for artifact in maintainer_only] == [3]
    assert [artifact.number for artifact in committer_scope] == [2, 3]


def test_fetch_repo_hip_artifacts_applies_limit_after_combining_newest_artifacts(monkeypatch):
    """Newest issue and PR summaries should be combined before the overall limit is applied."""
    issue_summary = {"number": 10, "updated_at": "2025-01-01T00:00:00Z"}
    pr_summary = {"number": 20, "updated_at": "2025-01-02T00:00:00Z"}

    monkeypatch.setattr(loader, "load_records_cache", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(loader, "save_records_cache", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(loader, "_fetch_issue_summaries", lambda *_args, **_kwargs: [issue_summary])
    monkeypatch.setattr(loader, "_fetch_pull_request_summaries", lambda *_args, **_kwargs: [pr_summary])
    monkeypatch.setattr(loader, "_issue_from_summary", lambda *_args, **_kwargs: make_issue_artifact(number=10))
    monkeypatch.setattr(
        loader,
        "_pull_request_from_summary",
        lambda *_args, **_kwargs: make_pull_request_artifact(number=20),
    )

    artifacts = loader.fetch_repo_hip_artifacts(
        Mock(),
        "org",
        "repo",
        include_issues=True,
        include_prs=True,
        limit=1,
    )

    assert len(artifacts) == 1
    assert isinstance(artifacts[0], HipArtifact)
    assert artifacts[0].artifact_type == "pull_request"
    assert artifacts[0].number == 20
