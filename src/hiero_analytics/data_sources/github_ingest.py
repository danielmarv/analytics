"""
GitHub data ingestion utilities using the GraphQL API.

This module provides functions for retrieving repositories, issues, and
merged pull request metadata from GitHub. Data is fetched using cursor-
based pagination and can be aggregated across an organization with
parallel requests to improve ingestion speed.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from .cache import load_records_cache, save_records_cache
from .github_client import GitHubClient
from .github_queries import ISSUES_QUERY, MERGED_PR_QUERY, REPOS_QUERY
from .models import IssueRecord, PullRequestDifficultyRecord, RepositoryRecord
from .pagination import paginate_cursor

logger = logging.getLogger(__name__)


def _cache_kwargs(
    use_cache: bool | None,
    cache_ttl_seconds: int | None,
    refresh: bool,
) -> dict[str, object]:
    """Build keyword arguments for nested cache-aware fetch calls."""
    kwargs: dict[str, object] = {}

    if use_cache is not None:
        kwargs["use_cache"] = use_cache
    if cache_ttl_seconds is not None:
        kwargs["cache_ttl_seconds"] = cache_ttl_seconds
    if refresh:
        kwargs["refresh"] = True

    return kwargs


def _parse_dt(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def fetch_org_repos_graphql(
    client: GitHubClient,
    org: str,
    *,
    use_cache: bool | None = None,
    cache_ttl_seconds: int | None = None,
    refresh: bool = False,
) -> list[RepositoryRecord]:
    """Fetch all repository full names for an organization using GraphQL."""
    cache_parameters = {"org": org}
    cached = load_records_cache(
        "org_repos",
        org,
        cache_parameters,
        RepositoryRecord,
        use_cache=use_cache,
        ttl_seconds=cache_ttl_seconds,
        refresh=refresh,
    )
    if cached is not None:
        return cached

    def page(cursor: str | None) -> tuple[list[RepositoryRecord], str | None, bool]:
        data = client.graphql(
            REPOS_QUERY,
            {"org": org, "cursor": cursor},
        )

        repo_data = data["data"]["organization"]["repositories"]
        items = [
            RepositoryRecord(
                full_name=f"{org}/{repo['name']}",
                name=repo["name"],
                owner=org,
            )
            for repo in repo_data["nodes"]
        ]

        next_cursor = repo_data["pageInfo"]["endCursor"]
        has_next = repo_data["pageInfo"]["hasNextPage"]

        return items, next_cursor, has_next

    records = paginate_cursor(page)
    save_records_cache(
        "org_repos",
        org,
        cache_parameters,
        RepositoryRecord,
        records,
        use_cache=use_cache,
    )
    return records


def fetch_repo_issues_graphql(
    client: GitHubClient,
    owner: str,
    repo: str,
    states: list[str] | None = None,
    *,
    use_cache: bool | None = None,
    cache_ttl_seconds: int | None = None,
    refresh: bool = False,
) -> list[IssueRecord]:
    """Fetch all issues for a repository using GraphQL."""
    normalized_states = None
    if states:
        normalized_states = [state.upper() for state in states]

    scope = f"{owner}_{repo}"
    cache_parameters = {
        "owner": owner,
        "repo": repo,
        "states": sorted(normalized_states or []),
    }
    cached = load_records_cache(
        "repo_issues",
        scope,
        cache_parameters,
        IssueRecord,
        use_cache=use_cache,
        ttl_seconds=cache_ttl_seconds,
        refresh=refresh,
    )
    if cached is not None:
        return cached

    def page(cursor: str | None) -> tuple[list[IssueRecord], str | None, bool]:
        data = client.graphql(
            ISSUES_QUERY,
            {
                "owner": owner,
                "repo": repo,
                "cursor": cursor,
                "states": normalized_states,
            },
        )

        issue_data = data["data"]["repository"]["issues"]
        items = [
            IssueRecord(
                repo=f"{owner}/{repo}",
                number=issue["number"],
                title=issue["title"],
                state=issue["state"],
                created_at=_parse_dt(issue["createdAt"]),  # type: ignore[arg-type]
                closed_at=_parse_dt(issue["closedAt"]),  # type: ignore[arg-type]
                labels=[label["name"].lower() for label in issue["labels"]["nodes"]],
            )
            for issue in issue_data["nodes"]
        ]

        next_cursor = issue_data["pageInfo"]["endCursor"]
        has_next = issue_data["pageInfo"]["hasNextPage"]

        return items, next_cursor, has_next

    records = paginate_cursor(page)
    save_records_cache(
        "repo_issues",
        scope,
        cache_parameters,
        IssueRecord,
        records,
        use_cache=use_cache,
    )
    return records


def fetch_org_issues_graphql(
    client: GitHubClient,
    org: str,
    states: list[str] | None = None,
    max_workers: int = 5,
    *,
    use_cache: bool | None = None,
    cache_ttl_seconds: int | None = None,
    refresh: bool = False,
) -> list[IssueRecord]:
    """Fetch all issues across all repositories in an organization."""
    logger.info(
        "Fetching organization issues for %s (states=%s, max_workers=%d)",
        org,
        states or "ALL",
        max_workers,
    )
    normalized_states = sorted(state.upper() for state in states) if states else []
    cache_parameters = {
        "org": org,
        "states": normalized_states,
    }
    cached = load_records_cache(
        "org_issues",
        org,
        cache_parameters,
        IssueRecord,
        use_cache=use_cache,
        ttl_seconds=cache_ttl_seconds,
        refresh=refresh,
    )
    if cached is not None:
        return cached

    repositories = fetch_org_repos_graphql(
        client,
        org,
        **_cache_kwargs(use_cache, cache_ttl_seconds, refresh),
    )
    logger.info("Found %d repositories in %s", len(repositories), org)

    all_issues: list[IssueRecord] = []

    def fetch(repository: RepositoryRecord) -> list[IssueRecord]:
        logger.info("Scanning repository %s", repository.full_name)
        return fetch_repo_issues_graphql(
            client,
            owner=repository.owner,
            repo=repository.name,
            states=states,
            **_cache_kwargs(use_cache, cache_ttl_seconds, refresh),
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch, repo): repo for repo in repositories}

        for future in as_completed(futures):
            repository = futures[future]

            try:
                repo_issues = future.result()
                all_issues.extend(repo_issues)
            except Exception as exc:  # pragma: no cover - logging only
                logger.exception(
                    "Failed fetching issues for %s: %s",
                    repository.full_name,
                    exc,
                )

    logger.info("Collected %d issues across %s", len(all_issues), org)
    save_records_cache(
        "org_issues",
        org,
        cache_parameters,
        IssueRecord,
        all_issues,
        use_cache=use_cache,
    )
    return all_issues


def fetch_repo_merged_pr_difficulty_graphql(
    client: GitHubClient,
    owner: str,
    repo: str,
    *,
    use_cache: bool | None = None,
    cache_ttl_seconds: int | None = None,
    refresh: bool = False,
) -> list[PullRequestDifficultyRecord]:
    """Fetch merged pull requests and their linked closing issues for a repository."""
    scope = f"{owner}_{repo}"
    cache_parameters = {
        "owner": owner,
        "repo": repo,
    }
    cached = load_records_cache(
        "repo_merged_pr_difficulty",
        scope,
        cache_parameters,
        PullRequestDifficultyRecord,
        use_cache=use_cache,
        ttl_seconds=cache_ttl_seconds,
        refresh=refresh,
    )
    if cached is not None:
        return cached

    def page(cursor: str | None) -> tuple[list[PullRequestDifficultyRecord], str | None, bool]:
        data = client.graphql(
            MERGED_PR_QUERY,
            {
                "owner": owner,
                "repo": repo,
                "cursor": cursor,
            },
        )

        pr_data = data["data"]["repository"]["pullRequests"]
        items: list[PullRequestDifficultyRecord] = []

        for pr in pr_data["nodes"]:
            issues = pr["closingIssuesReferences"]["nodes"]

            for issue in issues:
                labels = [label["name"] for label in issue["labels"]["nodes"]]
                items.append(
                    PullRequestDifficultyRecord(
                        repo=f"{owner}/{repo}",
                        pr_number=pr["number"],
                        pr_created_at=_parse_dt(pr["createdAt"]),  # type: ignore[arg-type]
                        pr_merged_at=_parse_dt(pr["mergedAt"]),  # type: ignore[arg-type]
                        pr_additions=pr["additions"],
                        pr_deletions=pr["deletions"],
                        pr_changed_files=pr["changedFiles"],
                        issue_number=issue["number"],
                        issue_labels=labels,
                    )
                )

        next_cursor = pr_data["pageInfo"]["endCursor"]
        has_next = pr_data["pageInfo"]["hasNextPage"]

        return items, next_cursor, has_next

    records = paginate_cursor(page)
    save_records_cache(
        "repo_merged_pr_difficulty",
        scope,
        cache_parameters,
        PullRequestDifficultyRecord,
        records,
        use_cache=use_cache,
    )
    return records


def fetch_org_merged_pr_difficulty_graphql(
    client: GitHubClient,
    org: str,
    max_workers: int = 5,
    *,
    use_cache: bool | None = None,
    cache_ttl_seconds: int | None = None,
    refresh: bool = False,
) -> list[PullRequestDifficultyRecord]:
    """Fetch merged pull request difficulty records across an organization."""
    logger.info(
        "Fetching merged PR difficulty records for %s (max_workers=%d)",
        org,
        max_workers,
    )
    cache_parameters = {"org": org}
    cached = load_records_cache(
        "org_merged_pr_difficulty",
        org,
        cache_parameters,
        PullRequestDifficultyRecord,
        use_cache=use_cache,
        ttl_seconds=cache_ttl_seconds,
        refresh=refresh,
    )
    if cached is not None:
        return cached

    repositories = fetch_org_repos_graphql(
        client,
        org,
        **_cache_kwargs(use_cache, cache_ttl_seconds, refresh),
    )
    logger.info("Found %d repositories in %s", len(repositories), org)
    all_records: list[PullRequestDifficultyRecord] = []

    def fetch(repository: RepositoryRecord) -> list[PullRequestDifficultyRecord]:
        logger.info("Scanning merged PRs for repository %s", repository.full_name)
        return fetch_repo_merged_pr_difficulty_graphql(
            client,
            owner=repository.owner,
            repo=repository.name,
            **_cache_kwargs(use_cache, cache_ttl_seconds, refresh),
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch, repo): repo for repo in repositories}

        for future in as_completed(futures):
            repository = futures[future]

            try:
                all_records.extend(future.result())
            except Exception as exc:  # pragma: no cover - logging only
                logger.exception(
                    "Failed fetching merged PRs for %s: %s",
                    repository.full_name,
                    exc,
                )

    logger.info("Collected %d merged PR difficulty records across %s", len(all_records), org)
    save_records_cache(
        "org_merged_pr_difficulty",
        org,
        cache_parameters,
        PullRequestDifficultyRecord,
        all_records,
        use_cache=use_cache,
    )
    return all_records
