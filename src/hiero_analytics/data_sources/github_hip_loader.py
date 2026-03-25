"""GitHub ingestion for repo-scoped HIP progression artifacts."""

from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Any

from hiero_analytics.config.github import BASE_URL
from hiero_analytics.domain.hip_progression_models import (
    AuthorScope,
    ChangedFileStatus,
    HipArtifact,
    author_matches_scope,
    build_changed_file,
)

from .cache import load_records_cache, save_records_cache
from .github_client import GitHubClient
from .pagination import paginate_page_number

logger = logging.getLogger(__name__)

REST_PAGE_SIZE = 100


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _normalize_text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _normalize_labels(raw_labels: object) -> list[str]:
    if not isinstance(raw_labels, list):
        return []
    labels: list[str] = []
    for label in raw_labels:
        if not isinstance(label, dict):
            continue
        name = label.get("name")
        if isinstance(name, str) and name:
            labels.append(name.lower())
    return labels


def _normalize_changed_file_status(value: object) -> ChangedFileStatus:
    normalized = str(value or "modified").lower()
    if normalized in {"added", "modified", "removed", "renamed"}:
        return normalized
    return "modified"


def _max_pages_for_limit(limit: int | None) -> int | None:
    if limit is None:
        return None
    return max(1, math.ceil(limit / REST_PAGE_SIZE))


def _fetch_rest_collection(
    client: GitHubClient,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    max_pages: int | None = None,
) -> list[dict[str, Any]]:
    base_params = dict(params or {})

    def page(page_number: int) -> list[dict[str, Any]]:
        request_params = {
            **base_params,
            "per_page": REST_PAGE_SIZE,
            "page": page_number,
        }
        data = client.get(url, params=request_params)
        if not isinstance(data, list):
            return []
        return [item for item in data if isinstance(item, dict)]

    return paginate_page_number(page, page_size=REST_PAGE_SIZE, max_pages=max_pages)


def _join_text_blocks(values: list[str]) -> str:
    return "\n\n".join(value for value in values if value)


def _fetch_issue_comments_text(
    client: GitHubClient,
    owner: str,
    repo: str,
    issue_number: int,
) -> str:
    comments = _fetch_rest_collection(
        client,
        f"{BASE_URL}/repos/{owner}/{repo}/issues/{issue_number}/comments",
    )
    return _join_text_blocks([_normalize_text(comment.get("body")) for comment in comments])


def _fetch_pull_request_review_comments_text(
    client: GitHubClient,
    owner: str,
    repo: str,
    pull_number: int,
) -> str:
    comments = _fetch_rest_collection(
        client,
        f"{BASE_URL}/repos/{owner}/{repo}/pulls/{pull_number}/comments",
    )
    return _join_text_blocks([_normalize_text(comment.get("body")) for comment in comments])


def _fetch_pull_request_commit_messages_text(
    client: GitHubClient,
    owner: str,
    repo: str,
    pull_number: int,
) -> str:
    commits = _fetch_rest_collection(
        client,
        f"{BASE_URL}/repos/{owner}/{repo}/pulls/{pull_number}/commits",
    )
    messages = []
    for commit in commits:
        commit_data = commit.get("commit")
        if not isinstance(commit_data, dict):
            continue
        messages.append(_normalize_text(commit_data.get("message")))
    return _join_text_blocks(messages)


def _fetch_pull_request_files(
    client: GitHubClient,
    owner: str,
    repo: str,
    pull_number: int,
) -> list:
    files = _fetch_rest_collection(
        client,
        f"{BASE_URL}/repos/{owner}/{repo}/pulls/{pull_number}/files",
    )
    changed_files = []
    for file_data in files:
        path = file_data.get("filename")
        if not isinstance(path, str) or not path:
            continue
        changed_files.append(
            build_changed_file(
                path,
                additions=int(file_data.get("additions", 0) or 0),
                deletions=int(file_data.get("deletions", 0) or 0),
                status=_normalize_changed_file_status(file_data.get("status")),
            )
        )
    return changed_files


def _issue_from_summary(
    client: GitHubClient,
    owner: str,
    repo: str,
    summary: dict[str, Any],
) -> HipArtifact:
    number = int(summary["number"])
    comments_count = int(summary.get("comments", 0) or 0)
    comments_text = ""
    if comments_count > 0:
        comments_text = _fetch_issue_comments_text(client, owner, repo, number)

    return HipArtifact(
        repo=f"{owner}/{repo}",
        artifact_type="issue",
        number=number,
        title=_normalize_text(summary.get("title")),
        body=_normalize_text(summary.get("body")),
        comments_text=comments_text,
        commit_messages_text="",
        author_login=_normalize_text((summary.get("user") or {}).get("login")),
        author_association=str(summary.get("author_association") or "NONE").upper(),
        state=str(summary.get("state") or "open").lower(),
        merged=False,
        created_at=_parse_datetime(summary.get("created_at")),
        updated_at=_parse_datetime(summary.get("updated_at")),
        closed_at=_parse_datetime(summary.get("closed_at")),
        changed_files=[],
        additions=0,
        deletions=0,
        labels=_normalize_labels(summary.get("labels")),
        url=_normalize_text(summary.get("html_url")),
    )


def _pull_request_from_summary(
    client: GitHubClient,
    owner: str,
    repo: str,
    summary: dict[str, Any],
) -> HipArtifact:
    number = int(summary["number"])
    detail = client.get(f"{BASE_URL}/repos/{owner}/{repo}/pulls/{number}")

    issue_comment_count = int(detail.get("comments", 0) or 0)
    review_comment_count = int(detail.get("review_comments", 0) or 0)
    comments_blocks: list[str] = []
    if issue_comment_count > 0:
        comments_blocks.append(_fetch_issue_comments_text(client, owner, repo, number))
    if review_comment_count > 0:
        comments_blocks.append(_fetch_pull_request_review_comments_text(client, owner, repo, number))

    commit_messages_text = ""
    if int(detail.get("commits", 0) or 0) > 0:
        commit_messages_text = _fetch_pull_request_commit_messages_text(client, owner, repo, number)

    changed_files = []
    if int(detail.get("changed_files", 0) or 0) > 0:
        changed_files = _fetch_pull_request_files(client, owner, repo, number)

    return HipArtifact(
        repo=f"{owner}/{repo}",
        artifact_type="pull_request",
        number=number,
        title=_normalize_text(detail.get("title") or summary.get("title")),
        body=_normalize_text(detail.get("body") or summary.get("body")),
        comments_text=_join_text_blocks(comments_blocks),
        commit_messages_text=commit_messages_text,
        author_login=_normalize_text((detail.get("user") or summary.get("user") or {}).get("login")),
        author_association=str(detail.get("author_association") or summary.get("author_association") or "NONE").upper(),
        state=str(detail.get("state") or summary.get("state") or "open").lower(),
        merged=bool(detail.get("merged_at") or detail.get("merged")),
        created_at=_parse_datetime(detail.get("created_at") or summary.get("created_at")),
        updated_at=_parse_datetime(detail.get("updated_at") or summary.get("updated_at")),
        closed_at=_parse_datetime(detail.get("closed_at") or summary.get("closed_at")),
        changed_files=changed_files,
        additions=int(detail.get("additions", 0) or 0),
        deletions=int(detail.get("deletions", 0) or 0),
        labels=_normalize_labels(detail.get("labels") or summary.get("labels")),
        url=_normalize_text(detail.get("html_url") or summary.get("html_url")),
    )


def _fetch_issue_summaries(
    client: GitHubClient,
    owner: str,
    repo: str,
    *,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    items = _fetch_rest_collection(
        client,
        f"{BASE_URL}/repos/{owner}/{repo}/issues",
        params={"state": "all", "sort": "updated", "direction": "desc"},
        max_pages=_max_pages_for_limit(limit),
    )
    issues = [item for item in items if "pull_request" not in item]
    return issues[:limit] if limit is not None else issues


def _fetch_pull_request_summaries(
    client: GitHubClient,
    owner: str,
    repo: str,
    *,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    items = _fetch_rest_collection(
        client,
        f"{BASE_URL}/repos/{owner}/{repo}/pulls",
        params={"state": "all", "sort": "updated", "direction": "desc"},
        max_pages=_max_pages_for_limit(limit),
    )
    return items[:limit] if limit is not None else items


def filter_hip_artifacts_by_author_scope(
    artifacts: list[HipArtifact],
    author_scope: AuthorScope,
) -> list[HipArtifact]:
    """Filter normalized artifacts to the requested author scope."""
    return [
        artifact
        for artifact in artifacts
        if author_matches_scope(artifact.author_association, author_scope)
    ]


def fetch_repo_hip_artifacts(
    client: GitHubClient,
    owner: str,
    repo: str,
    *,
    include_issues: bool = True,
    include_prs: bool = True,
    limit: int | None = None,
    use_cache: bool | None = None,
    cache_ttl_seconds: int | None = None,
    refresh: bool = False,
) -> list[HipArtifact]:
    """Fetch normalized issues and pull requests for HIP progression analysis."""
    if not include_issues and not include_prs:
        return []

    scope = f"{owner}_{repo}"
    cache_parameters = {
        "owner": owner,
        "repo": repo,
        "include_issues": include_issues,
        "include_prs": include_prs,
        "limit": limit,
    }
    cached = load_records_cache(
        "repo_hip_artifacts",
        scope,
        cache_parameters,
        HipArtifact,
        use_cache=use_cache,
        ttl_seconds=cache_ttl_seconds,
        refresh=refresh,
    )
    if cached is not None:
        return cached

    selected_summaries: list[tuple[str, dict[str, Any]]] = []
    if include_issues:
        selected_summaries.extend(
            ("issue", summary)
            for summary in _fetch_issue_summaries(client, owner, repo, limit=limit)
        )
    if include_prs:
        selected_summaries.extend(
            ("pull_request", summary)
            for summary in _fetch_pull_request_summaries(client, owner, repo, limit=limit)
        )

    selected_summaries.sort(
        key=lambda item: str(item[1].get("updated_at") or item[1].get("created_at") or ""),
        reverse=True,
    )
    if limit is not None:
        selected_summaries = selected_summaries[:limit]

    artifacts: list[HipArtifact] = []
    for artifact_type, summary in selected_summaries:
        logger.info("Fetching %s #%s for %s/%s", artifact_type, summary.get("number"), owner, repo)
        if artifact_type == "issue":
            artifacts.append(_issue_from_summary(client, owner, repo, summary))
        else:
            artifacts.append(_pull_request_from_summary(client, owner, repo, summary))

    save_records_cache(
        "repo_hip_artifacts",
        scope,
        cache_parameters,
        HipArtifact,
        artifacts,
        use_cache=use_cache,
    )
    return artifacts
