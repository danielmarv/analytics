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
from datetime import UTC, datetime, timedelta

from .cache import (
    load_records_cache,
    save_records_cache,
)
from .github_client import GitHubClient
from .github_queries import (
    CONTRIBUTOR_ACTIVITY_QUERY,
    ISSUES_QUERY,
    MERGED_PR_QUERY,
    REPOS_QUERY,
)
from .models import (
    ContributorActivityRecord,
    IssueRecord,
    PullRequestDifficultyRecord,
    RepositoryRecord,
)
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


# --------------------------------------------------------
# FETCH REPOSITORIES
# --------------------------------------------------------


def fetch_org_repos_graphql(
    client: GitHubClient,
    org: str,
    *,
    use_cache: bool | None = None,
    cache_ttl_seconds: int | None = None,
    refresh: bool = False,
) -> list[RepositoryRecord]:
    """
    Fetch all repository full names for an organization using GraphQL.

    Args:
        client: Authenticated GitHub client.
        org: GitHub organization name.
        use_cache: Optional override for enabling or disabling fetch caching.
        cache_ttl_seconds: Optional cache TTL override in seconds.
        refresh: When True, bypass any existing cache entry and rewrite it.

    Returns:
        A list of repository full names, for example:
        ["hiero-ledger/analytics", "hiero-ledger/sdk"]
    """
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


# --------------------------------------------------------
# FETCH ISSUES FOR ONE REPOSITORY
# --------------------------------------------------------


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
    """
    Fetch all issues for a repository using GraphQL.

    Args:
        client: Authenticated GitHub client.
        owner: Repository owner or organization name.
        repo: Repository name only, not full_name.
        states: Optional list of states (e.g. ["OPEN","CLOSED"])
        use_cache: Optional override for enabling or disabling fetch caching.
        cache_ttl_seconds: Optional cache TTL override in seconds.
        refresh: When True, bypass any existing cache entry and rewrite it.

    Returns:
        A list of normalized issue records.
    """
    normalized_states = None
    if states:
        normalized_states = [s.upper() for s in states]

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
                created_at=_parse_dt(issue["createdAt"]),  # type: ignore
                closed_at=_parse_dt(issue["closedAt"]),  # type: ignore
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


# --------------------------------------------------------
# FETCH ALL ISSUES ACROSS AN ORG (PARALLEL)
# --------------------------------------------------------


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
    """
    Fetch all issues across all repositories in an organization.

    Args:
        client: Authenticated GitHub client.
        org: GitHub organization login.
        states: Optional issue states filter.
        max_workers: Number of worker threads for parallel repository fetches.
        use_cache: Optional override for enabling or disabling fetch caching.
        cache_ttl_seconds: Optional cache TTL override in seconds.
        refresh: When True, bypass any existing cache entry and rewrite it.

    Returns:
        A combined list of issue records across the organization.
    """
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

    repos = fetch_org_repos_graphql(
        client,
        org,
        **_cache_kwargs(use_cache, cache_ttl_seconds, refresh),
    )
    logger.info("Found %d repositories in %s", len(repos), org)

    all_issues: list[IssueRecord] = []

    def fetch(repo: RepositoryRecord) -> list[IssueRecord]:
        logger.info("Scanning repository %s", repo.full_name)

        return fetch_repo_issues_graphql(
            client,
            owner=repo.owner,
            repo=repo.name,
            states=states,
            **_cache_kwargs(use_cache, cache_ttl_seconds, refresh),
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch, repo): repo for repo in repos}

        for future in as_completed(futures):
            repo = futures[future]

            try:
                repo_issues = future.result()
                all_issues.extend(repo_issues)

            except Exception as e:
                logger.exception(
                    "Failed fetching issues for %s: %s",
                    repo.full_name,
                    e,
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


# --------------------------------------------------------
# FETCH MERGED PR DIFFICULTY FOR ONE REPOSITORY
# --------------------------------------------------------


def fetch_repo_merged_pr_difficulty_graphql(
    client: GitHubClient,
    owner: str,
    repo: str,
    *,
    use_cache: bool | None = None,
    cache_ttl_seconds: int | None = None,
    refresh: bool = False,
) -> list[PullRequestDifficultyRecord]:
    """
    Fetch merged pull requests and their linked closing issues for a repository.

    Args:
        client: Authenticated GitHub client.
        owner: Repository owner or organization name.
        repo: Repository name only, not full_name.
        use_cache: Optional override for enabling or disabling fetch caching.
        cache_ttl_seconds: Optional cache TTL override in seconds.
        refresh: When True, bypass any existing cache entry and rewrite it.

    Returns:
        A list of normalized records linking merged PRs to issues they close.
    """
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
                        pr_created_at=_parse_dt(pr["createdAt"]),  # type: ignore
                        pr_merged_at=_parse_dt(pr["mergedAt"]),  # type: ignore
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


# --------------------------------------------------------
# FETCH MERGED PR DIFFICULTY ACROSS AN ORG (PARALLEL)
# --------------------------------------------------------


def fetch_org_merged_pr_difficulty_graphql(
    client: GitHubClient,
    org: str,
    max_workers: int = 5,
    *,
    use_cache: bool | None = None,
    cache_ttl_seconds: int | None = None,
    refresh: bool = False,
) -> list[PullRequestDifficultyRecord]:
    """
    Fetch merged pull request difficulty records across all repositories in an organization.

    Args:
        client: Authenticated GitHub client.
        org: GitHub organization login.
        max_workers: Number of worker threads for parallel repository fetches.
        use_cache: Optional override for enabling or disabling fetch caching.
        cache_ttl_seconds: Optional cache TTL override in seconds.
        refresh: When True, bypass any existing cache entry and rewrite it.

    Returns:
        A combined list of merged PR difficulty records.
    """
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

    repos = fetch_org_repos_graphql(
        client,
        org,
        **_cache_kwargs(use_cache, cache_ttl_seconds, refresh),
    )
    logger.info("Found %d repositories in %s", len(repos), org)
    all_records: list[PullRequestDifficultyRecord] = []

    def fetch(repo: RepositoryRecord) -> list[PullRequestDifficultyRecord]:
        logger.info("Scanning merged PRs for repository %s", repo.full_name)
        return fetch_repo_merged_pr_difficulty_graphql(
            client,
            owner=repo.owner,
            repo=repo.name,
            **_cache_kwargs(use_cache, cache_ttl_seconds, refresh),
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch, repo): repo for repo in repos}

        for future in as_completed(futures):
            repo = futures[future]

            try:
                all_records.extend(future.result())

            except Exception as exc:
                logger.exception(
                    "Failed fetching merged PRs for %s: %s",
                    repo.full_name,
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


# --------------------------------------------------------
# FETCH CONTRIBUTOR ACTIVITY FOR ONE REPOSITORY
# --------------------------------------------------------


def fetch_repo_contributor_activity_graphql(
    client: GitHubClient,
    owner: str,
    repo: str,
    *,
    lookback_days: int = 183,
    use_cache: bool | None = None,
    cache_ttl_seconds: int | None = None,
    refresh: bool = False,
) -> list[ContributorActivityRecord]:
    """
    Fetch contributor activity signals from pull request lifecycle data.

    Signals include:
    - authored_pull_request
    - reviewed_pull_request
    - merged_pull_request

    Args:
        client: Authenticated GitHub client.
        owner: Repository owner or organization name.
        repo: Repository name only, not full_name.
        lookback_days: Number of days to include from the present.
        use_cache: Optional override for enabling or disabling fetch caching.
        cache_ttl_seconds: Optional cache TTL override in seconds.
        refresh: When True, bypass any existing cache entry and rewrite it.

    Returns:
        A list of normalized contributor activity records.
    """
    scope = f"{owner}_{repo}"
    cache_parameters = {
        "owner": owner,
        "repo": repo,
        "lookback_days": lookback_days,
    }
    cached = load_records_cache(
        "repo_contributor_activity",
        scope,
        cache_parameters,
        ContributorActivityRecord,
        use_cache=use_cache,
        ttl_seconds=cache_ttl_seconds,
        refresh=refresh,
    )
    if cached is not None:
        return cached

    cutoff = datetime.now(UTC) - timedelta(days=lookback_days)

    def page(cursor: str | None) -> tuple[list[ContributorActivityRecord], str | None, bool]:
        data = client.graphql(
            CONTRIBUTOR_ACTIVITY_QUERY,
            {
                "owner": owner,
                "repo": repo,
                "cursor": cursor,
            },
        )

        pr_data = data["data"]["repository"]["pullRequests"]
        pr_nodes = pr_data["nodes"]
        items: list[ContributorActivityRecord] = []

        for pr in pr_nodes:
            pr_number = pr["number"]
            repo_name = f"{owner}/{repo}"

            pr_author = None
            if pr.get("author"):
                pr_author = pr["author"].get("login")

            pr_created_at = _parse_dt(pr.get("createdAt"))
            if pr_created_at and pr_created_at >= cutoff and pr_author:
                items.append(
                    ContributorActivityRecord(
                        repo=repo_name,
                        activity_type="authored_pull_request",
                        actor=pr_author,
                        occurred_at=pr_created_at,
                        target_type="pull_request",
                        target_number=pr_number,
                        target_author=pr_author,
                    )
                )

            for review in pr["reviews"]["nodes"]:
                review_author = None
                if review.get("author"):
                    review_author = review["author"].get("login")

                reviewed_at = _parse_dt(review.get("submittedAt"))
                if reviewed_at and reviewed_at >= cutoff and review_author:
                    items.append(
                        ContributorActivityRecord(
                            repo=repo_name,
                            activity_type="reviewed_pull_request",
                            actor=review_author,
                            occurred_at=reviewed_at,
                            target_type="pull_request",
                            target_number=pr_number,
                            target_author=pr_author,
                            detail=review.get("state"),
                        )
                    )

            merged_at = _parse_dt(pr.get("mergedAt"))
            merged_by = None
            if pr.get("mergedBy"):
                merged_by = pr["mergedBy"].get("login")

            if merged_at and merged_at >= cutoff and merged_by:
                items.append(
                    ContributorActivityRecord(
                        repo=repo_name,
                        activity_type="merged_pull_request",
                        actor=merged_by,
                        occurred_at=merged_at,
                        target_type="pull_request",
                        target_number=pr_number,
                        target_author=pr_author,
                    )
                )

        next_cursor = pr_data["pageInfo"]["endCursor"]
        has_next = pr_data["pageInfo"]["hasNextPage"]

        if pr_nodes:
            oldest_updated = _parse_dt(pr_nodes[-1].get("updatedAt"))
            if oldest_updated and oldest_updated < cutoff:
                has_next = False

        return items, next_cursor, has_next

    records = paginate_cursor(page)
    save_records_cache(
        "repo_contributor_activity",
        scope,
        cache_parameters,
        ContributorActivityRecord,
        records,
        use_cache=use_cache,
    )
    return records


# --------------------------------------------------------
# FETCH CONTRIBUTOR ACTIVITY ACROSS AN ORG (PARALLEL)
# --------------------------------------------------------


def fetch_org_contributor_activity_graphql(
    client: GitHubClient,
    org: str,
    max_workers: int = 5,
    *,
    repos: list[str] | None = None,
    lookback_days: int = 183,
    use_cache: bool | None = None,
    cache_ttl_seconds: int | None = None,
    refresh: bool = False,
) -> list[ContributorActivityRecord]:
    """
    Fetch contributor activity records across all repositories in an organization.

    Args:
        client: Authenticated GitHub client.
        org: GitHub organization login.
        max_workers: Number of worker threads for parallel repository fetches.
        repos: Optional list of repositories to include (name or full_name).
        lookback_days: Number of days to include from the present.
        use_cache: Optional override for enabling or disabling fetch caching.
        cache_ttl_seconds: Optional cache TTL override in seconds.
        refresh: When True, bypass any existing cache entry and rewrite it.

    Returns:
        A combined list of contributor activity records.
    """
    normalized_repos = sorted(repos) if repos else []
    cache_parameters = {
        "org": org,
        "repos": normalized_repos,
        "lookback_days": lookback_days,
    }
    cached = load_records_cache(
        "org_contributor_activity",
        org,
        cache_parameters,
        ContributorActivityRecord,
        use_cache=use_cache,
        ttl_seconds=cache_ttl_seconds,
        refresh=refresh,
    )
    if cached is not None:
        return cached

    all_repos = fetch_org_repos_graphql(
        client,
        org,
        **_cache_kwargs(use_cache, cache_ttl_seconds, refresh),
    )

    if repos:
        allowed = set(repos)
        all_repos = [
            repo
            for repo in all_repos
            if repo.full_name in allowed or repo.name in allowed
        ]

    all_records: list[ContributorActivityRecord] = []

    def fetch(repo_record: RepositoryRecord) -> list[ContributorActivityRecord]:
        logger.info("Scanning contributor activity for %s", repo_record.full_name)
        return fetch_repo_contributor_activity_graphql(
            client,
            owner=repo_record.owner,
            repo=repo_record.name,
            lookback_days=lookback_days,
            **_cache_kwargs(use_cache, cache_ttl_seconds, refresh),
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch, repo): repo for repo in all_repos}

        for future in as_completed(futures):
            repo = futures[future]

            try:
                all_records.extend(future.result())

            except Exception as exc:
                logger.exception(
                    "Failed fetching contributor activity for %s: %s",
                    repo.full_name,
                    exc,
                )

    save_records_cache(
        "org_contributor_activity",
        org,
        cache_parameters,
        ContributorActivityRecord,
        all_records,
        use_cache=use_cache,
    )
    return all_records
