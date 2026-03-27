"""GitHub ingestion for repo-scoped HIP progression artifacts."""

from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Any

from hiero_analytics.config.github import BASE_URL
from hiero_analytics.domain.hip_progression_models import (
    ArtifactComment,
    ArtifactCommit,
    AuthorScope,
    ChangedFileStatus,
    HipArtifact,
    author_matches_scope,
    build_changed_file,
    extract_artifact_reference_numbers,
    flatten_text,
)

from .cache import load_records_cache, save_records_cache
from .github_client import GitHubClient
from .pagination import paginate_page_number

logger = logging.getLogger(__name__)

REST_PAGE_SIZE = 100
BOT_USER_TYPES = {"bot"}


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


def _is_bot_payload(user_payload: object) -> bool:
    if not isinstance(user_payload, dict):
        return False
    login = str(user_payload.get("login") or "").lower()
    user_type = str(user_payload.get("type") or "").lower()
    return user_type in BOT_USER_TYPES or login.endswith("[bot]") or "bot" in login


def _comment_from_payload(
    payload: dict[str, Any],
    *,
    source_kind: str,
) -> ArtifactComment:
    user_payload = payload.get("user") if isinstance(payload.get("user"), dict) else {}
    return ArtifactComment(
        body=_normalize_text(payload.get("body")),
        source_kind=source_kind,  # type: ignore[arg-type]
        author_login=_normalize_text(user_payload.get("login")),
        author_association=str(payload.get("author_association") or "NONE").upper(),
        created_at=_parse_datetime(payload.get("created_at")),
        url=_normalize_text(payload.get("html_url")),
        is_bot=_is_bot_payload(user_payload),
    )


def _commit_from_payload(payload: dict[str, Any]) -> ArtifactCommit:
    commit_payload = payload.get("commit") if isinstance(payload.get("commit"), dict) else {}
    author_payload = commit_payload.get("author") if isinstance(commit_payload.get("author"), dict) else {}
    return ArtifactCommit(
        message=_normalize_text(commit_payload.get("message")),
        sha=_normalize_text(payload.get("sha")),
        authored_at=_parse_datetime(author_payload.get("date")),
    )


def _fetch_issue_comments(
    client: GitHubClient,
    owner: str,
    repo: str,
    issue_number: int,
) -> list[ArtifactComment]:
    comments = _fetch_rest_collection(
        client,
        f"{BASE_URL}/repos/{owner}/{repo}/issues/{issue_number}/comments",
    )
    return [_comment_from_payload(comment, source_kind="issue_comment") for comment in comments]


def _fetch_pull_request_review_comments(
    client: GitHubClient,
    owner: str,
    repo: str,
    pull_number: int,
) -> list[ArtifactComment]:
    comments = _fetch_rest_collection(
        client,
        f"{BASE_URL}/repos/{owner}/{repo}/pulls/{pull_number}/comments",
    )
    return [_comment_from_payload(comment, source_kind="review_comment") for comment in comments]


def _fetch_pull_request_commits(
    client: GitHubClient,
    owner: str,
    repo: str,
    pull_number: int,
) -> list[ArtifactCommit]:
    commits = _fetch_rest_collection(
        client,
        f"{BASE_URL}/repos/{owner}/{repo}/pulls/{pull_number}/commits",
    )
    return [_commit_from_payload(commit) for commit in commits]


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


def _artifact_linked_numbers(
    title: str,
    body: str,
    comments_text: str,
    commit_messages_text: str,
) -> list[int]:
    return extract_artifact_reference_numbers(title, body, comments_text, commit_messages_text)


def _issue_from_summary(
    client: GitHubClient,
    owner: str,
    repo: str,
    summary: dict[str, Any],
) -> HipArtifact:
    number = int(summary["number"])
    comments = _fetch_issue_comments(client, owner, repo, number) if int(summary.get("comments", 0) or 0) > 0 else []
    comments_text = flatten_text([comment.body for comment in comments])
    title = _normalize_text(summary.get("title"))
    body = _normalize_text(summary.get("body"))

    return HipArtifact(
        repo=f"{owner}/{repo}",
        artifact_type="issue",
        number=number,
        title=title,
        body=body,
        comments_text=comments_text,
        commit_messages_text="",
        comments=comments,
        commits=[],
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
        linked_artifact_numbers=_artifact_linked_numbers(title, body, comments_text, ""),
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

    issue_comments = (
        _fetch_issue_comments(client, owner, repo, number)
        if int(detail.get("comments", 0) or 0) > 0
        else []
    )
    review_comments = (
        _fetch_pull_request_review_comments(client, owner, repo, number)
        if int(detail.get("review_comments", 0) or 0) > 0
        else []
    )
    comments = [*issue_comments, *review_comments]
    commits = (
        _fetch_pull_request_commits(client, owner, repo, number)
        if int(detail.get("commits", 0) or 0) > 0
        else []
    )
    changed_files = (
        _fetch_pull_request_files(client, owner, repo, number)
        if int(detail.get("changed_files", 0) or 0) > 0
        else []
    )
    comments_text = flatten_text([comment.body for comment in comments])
    commit_messages_text = flatten_text([commit.message for commit in commits])
    title = _normalize_text(detail.get("title") or summary.get("title"))
    body = _normalize_text(detail.get("body") or summary.get("body"))

    return HipArtifact(
        repo=f"{owner}/{repo}",
        artifact_type="pull_request",
        number=number,
        title=title,
        body=body,
        comments_text=comments_text,
        commit_messages_text=commit_messages_text,
        comments=comments,
        commits=commits,
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
        linked_artifact_numbers=_artifact_linked_numbers(title, body, comments_text, commit_messages_text),
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
