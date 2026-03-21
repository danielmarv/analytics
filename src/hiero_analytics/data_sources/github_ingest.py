"""
GitHub data ingestion utilities using the GraphQL API.

This module provides functions for retrieving repositories, issues, and
merged pull request metadata from GitHub. Data is fetched using cursor-
based pagination and can be aggregated across an organization with
parallel requests to improve ingestion speed.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import requests

from .cache import (
    load_records_cache,
    save_records_cache,
)
from .github_client import GitHubClient
from .github_queries import (
    CONTRIBUTOR_ISSUE_ACTIVITY_QUERY,
    CONTRIBUTOR_PULL_REQUEST_ACTIVITY_QUERY,
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
DEFAULT_CONTRIBUTOR_ACTIVITY_LOOKBACK_DAYS = 183


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


def _normalize_lookback_days(lookback_days: int | None) -> int | None:
    """Treat non-positive lookback windows as a request for full history."""
    if lookback_days is None or lookback_days <= 0:
        return None

    return lookback_days


def _normalize_repo_filters(org: str, repos: list[str] | None) -> list[str]:
    """Normalize repo filters to sorted full names for stable cache keys."""
    if not repos:
        return []

    normalized = {
        repo_name.strip() if "/" in repo_name.strip() else f"{org}/{repo_name.strip()}"
        for repo_name in repos
        if repo_name.strip()
    }
    return sorted(normalized)


def _connection_nodes(connection: dict[str, object]) -> list[dict[str, object]]:
    """Return only dictionary nodes from a GraphQL connection payload."""
    raw_nodes = connection.get("nodes", [])
    if not isinstance(raw_nodes, list):
        return []

    return [node for node in raw_nodes if isinstance(node, dict)]


def _connection_page_info(connection: dict[str, object]) -> tuple[str | None, bool]:
    """Extract pagination metadata from a GraphQL connection payload."""
    page_info = connection.get("pageInfo", {})
    if not isinstance(page_info, dict):
        return None, False

    next_cursor = page_info.get("endCursor")
    has_next = page_info.get("hasNextPage")
    return (next_cursor if isinstance(next_cursor, str) else None, bool(has_next))


def _node_datetime(node: dict[str, object], field_name: str) -> datetime | None:
    """Parse a datetime field from a GraphQL node when present."""
    raw_value = node.get(field_name)
    if not isinstance(raw_value, str):
        return None

    return _parse_dt(raw_value)


def _latest_node_datetime(
    *node_groups: list[dict[str, object]],
    field_name: str = "updatedAt",
) -> datetime | None:
    """Return the newest datetime found across multiple node groups."""
    datetimes = [
        occurred_at
        for nodes in node_groups
        for node in nodes
        for occurred_at in [_node_datetime(node, field_name)]
        if occurred_at is not None
    ]
    if not datetimes:
        return None

    return max(datetimes)


def _filter_nodes_by_cutoff(
    nodes: list[dict[str, object]],
    cutoff: datetime | None,
    *,
    field_name: str = "updatedAt",
) -> tuple[list[dict[str, object]], bool]:
    """Keep only nodes at or newer than the cutoff and flag when it is crossed."""
    if cutoff is None:
        return nodes, False

    filtered_nodes: list[dict[str, object]] = []
    crossed_cutoff = False

    for node in nodes:
        updated_at = _node_datetime(node, field_name)
        if updated_at is None or updated_at >= cutoff:
            filtered_nodes.append(node)
        else:
            crossed_cutoff = True

    return filtered_nodes, crossed_cutoff


def _filter_activity_records_by_cutoff(
    records: list[ContributorActivityRecord],
    cutoff: datetime | None,
) -> list[ContributorActivityRecord]:
    """Drop normalized activity that falls outside the requested lookback window."""
    if cutoff is None:
        return records

    return [record for record in records if record.occurred_at >= cutoff]


def _collect_contributor_activity(
    *,
    owner: str,
    repo: str,
    first_connection: dict[str, object],
    fetch_connection: Callable[[str | None], dict[str, object]],
    parse_nodes: Callable[[str, str, list[dict[str, object]]], list[ContributorActivityRecord]],
    cutoff: datetime | None,
) -> list[ContributorActivityRecord]:
    """Collect contributor activity pages until the lookback cutoff is crossed."""
    records: list[ContributorActivityRecord] = []
    connection = first_connection

    while True:
        nodes = _connection_nodes(connection)
        recent_nodes, crossed_cutoff = _filter_nodes_by_cutoff(nodes, cutoff)
        parsed_records = parse_nodes(owner, repo, recent_nodes)
        records.extend(_filter_activity_records_by_cutoff(parsed_records, cutoff))

        next_cursor, has_next = _connection_page_info(connection)
        if crossed_cutoff or not has_next or next_cursor is None:
            break

        connection = fetch_connection(next_cursor)

    return records


def _extract_login(node: dict[str, object] | None) -> str | None:
    """Safely read a GitHub actor/login field from a GraphQL node."""
    if not isinstance(node, dict):
        return None

    login = node.get("login")
    if isinstance(login, str) and login.strip():
        return login

    return None


def _is_human_actor(login: str | None) -> bool:
    """Return True for non-empty human logins and False for bot actors."""
    if not login:
        return False

    return not login.lower().endswith("[bot]")


def _append_activity(
    records: list[ContributorActivityRecord],
    *,
    repo: str,
    actor: str | None,
    occurred_at: datetime | None,
    activity_type: str,
    target_type: str,
    target_number: int,
    target_author: str | None,
    detail: str | None = None,
) -> None:
    """Append a normalized activity record when the actor and timestamp exist."""
    if not _is_human_actor(actor) or occurred_at is None:
        return

    records.append(
        ContributorActivityRecord(
            repo=repo,
            actor=actor,
            occurred_at=occurred_at,
            activity_type=activity_type,
            target_type=target_type,
            target_number=target_number,
            target_author=target_author,
            detail=detail,
        )
    )


def _http_error_message(exc: requests.HTTPError) -> str:
    """Extract a concise message from a requests HTTPError."""
    response = exc.response
    if response is None:
        return str(exc)

    try:
        payload = response.json()
    except ValueError:
        text = response.text.strip()
        message = text[:200] if text else str(exc)
    else:
        message = payload.get("message", str(exc)) if isinstance(payload, dict) else str(exc)

    return f"{response.status_code} {message}"


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
# FETCH CONTRIBUTOR ACTIVITY FOR ONE REPOSITORY
# --------------------------------------------------------


def _parse_issue_activity_nodes(
    owner: str,
    repo: str,
    issue_nodes: list[dict[str, object]],
) -> list[ContributorActivityRecord]:
    """Normalize issue authorship, comments, and issue-management events."""
    full_name = f"{owner}/{repo}"
    records: list[ContributorActivityRecord] = []

    for issue in issue_nodes:
        issue_number = int(issue["number"])
        issue_author = _extract_login(issue.get("author"))

        _append_activity(
            records,
            repo=full_name,
            actor=issue_author,
            occurred_at=_parse_dt(issue.get("createdAt")),
            activity_type="authored_issue",
            target_type="issue",
            target_number=issue_number,
            target_author=issue_author,
        )

        comments = issue.get("comments", {})
        if isinstance(comments, dict):
            for comment in comments.get("nodes", []):
                if not isinstance(comment, dict):
                    continue

                _append_activity(
                    records,
                    repo=full_name,
                    actor=_extract_login(comment.get("author")),
                    occurred_at=_parse_dt(comment.get("createdAt")),
                    activity_type="commented_on_issue",
                    target_type="issue",
                    target_number=issue_number,
                    target_author=issue_author,
                )

        timeline = issue.get("timelineItems", {})
        if not isinstance(timeline, dict):
            continue

        for item in timeline.get("nodes", []):
            if not isinstance(item, dict):
                continue

            typename = item.get("__typename")
            detail: str | None = None

            if typename == "LabeledEvent":
                activity_type = "labeled_issue"
                label = item.get("label")
                detail = label.get("name") if isinstance(label, dict) else None
            elif typename == "UnlabeledEvent":
                activity_type = "unlabeled_issue"
                label = item.get("label")
                detail = label.get("name") if isinstance(label, dict) else None
            elif typename == "ClosedEvent":
                activity_type = "closed_issue"
            elif typename == "ReopenedEvent":
                activity_type = "reopened_issue"
            elif typename == "AssignedEvent":
                activity_type = "assigned_issue"
                detail = _extract_login(item.get("assignee"))
            else:
                continue

            _append_activity(
                records,
                repo=full_name,
                actor=_extract_login(item.get("actor")),
                occurred_at=_parse_dt(item.get("createdAt")),
                activity_type=activity_type,
                target_type="issue",
                target_number=issue_number,
                target_author=issue_author,
                detail=detail,
            )

    return records


def _parse_pull_request_activity_nodes(
    owner: str,
    repo: str,
    pr_nodes: list[dict[str, object]],
) -> list[ContributorActivityRecord]:
    """Normalize pull-request authorship, reviews, comments, and merges."""
    full_name = f"{owner}/{repo}"
    records: list[ContributorActivityRecord] = []

    for pull_request in pr_nodes:
        pr_number = int(pull_request["number"])
        pr_author = _extract_login(pull_request.get("author"))

        _append_activity(
            records,
            repo=full_name,
            actor=pr_author,
            occurred_at=_parse_dt(pull_request.get("createdAt")),
            activity_type="authored_pull_request",
            target_type="pull_request",
            target_number=pr_number,
            target_author=pr_author,
        )

        comments = pull_request.get("comments", {})
        if isinstance(comments, dict):
            for comment in comments.get("nodes", []):
                if not isinstance(comment, dict):
                    continue

                _append_activity(
                    records,
                    repo=full_name,
                    actor=_extract_login(comment.get("author")),
                    occurred_at=_parse_dt(comment.get("createdAt")),
                    activity_type="commented_on_pull_request",
                    target_type="pull_request",
                    target_number=pr_number,
                    target_author=pr_author,
                )

        reviews = pull_request.get("reviews", {})
        if isinstance(reviews, dict):
            for review in reviews.get("nodes", []):
                if not isinstance(review, dict):
                    continue

                _append_activity(
                    records,
                    repo=full_name,
                    actor=_extract_login(review.get("author")),
                    occurred_at=_parse_dt(review.get("createdAt")),
                    activity_type="reviewed_pull_request",
                    target_type="pull_request",
                    target_number=pr_number,
                    target_author=pr_author,
                    detail=str(review.get("state")) if review.get("state") is not None else None,
                )

        _append_activity(
            records,
            repo=full_name,
            actor=_extract_login(pull_request.get("mergedBy")),
            occurred_at=_parse_dt(pull_request.get("mergedAt")),
            activity_type="merged_pull_request",
            target_type="pull_request",
            target_number=pr_number,
            target_author=pr_author,
        )

        timeline = pull_request.get("timelineItems", {})
        if not isinstance(timeline, dict):
            continue

        for item in timeline.get("nodes", []):
            if not isinstance(item, dict):
                continue

            typename = item.get("__typename")
            detail: str | None = None

            if typename == "LabeledEvent":
                activity_type = "labeled_pull_request"
                label = item.get("label")
                detail = label.get("name") if isinstance(label, dict) else None
            elif typename == "UnlabeledEvent":
                activity_type = "unlabeled_pull_request"
                label = item.get("label")
                detail = label.get("name") if isinstance(label, dict) else None
            elif typename == "ClosedEvent":
                activity_type = "closed_pull_request"
            elif typename == "ReopenedEvent":
                activity_type = "reopened_pull_request"
            else:
                continue

            _append_activity(
                records,
                repo=full_name,
                actor=_extract_login(item.get("actor")),
                occurred_at=_parse_dt(item.get("createdAt")),
                activity_type=activity_type,
                target_type="pull_request",
                target_number=pr_number,
                target_author=pr_author,
                detail=detail,
            )

    return records


def fetch_repo_contributor_activity_graphql(
    client: GitHubClient,
    owner: str,
    repo: str,
    *,
    lookback_days: int | None = DEFAULT_CONTRIBUTOR_ACTIVITY_LOOKBACK_DAYS,
    use_cache: bool | None = None,
    cache_ttl_seconds: int | None = None,
    refresh: bool = False,
) -> list[ContributorActivityRecord]:
    """
    Fetch normalized contributor activity for a repository.

    The fetch includes issue authorship and comments, issue-management events,
    pull request authorship and comments, pull request reviews, and merge
    events. Nested activity connections are capped at 100 items per issue or
    pull request to keep repository scans tractable.
    """
    normalized_lookback_days = _normalize_lookback_days(lookback_days)
    scope = f"{owner}_{repo}"
    cache_parameters = {
        "owner": owner,
        "repo": repo,
        "lookback_days": normalized_lookback_days,
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

    def fetch_issue_connection(cursor: str | None) -> dict[str, object]:
        data = client.graphql(
            CONTRIBUTOR_ISSUE_ACTIVITY_QUERY,
            {
                "owner": owner,
                "repo": repo,
                "cursor": cursor,
            },
        )

        return data["data"]["repository"]["issues"]

    def fetch_pull_request_connection(cursor: str | None) -> dict[str, object]:
        data = client.graphql(
            CONTRIBUTOR_PULL_REQUEST_ACTIVITY_QUERY,
            {
                "owner": owner,
                "repo": repo,
                "cursor": cursor,
            },
        )

        return data["data"]["repository"]["pullRequests"]

    first_issue_connection = fetch_issue_connection(None)
    first_pull_request_connection = fetch_pull_request_connection(None)
    latest_updated_at = _latest_node_datetime(
        _connection_nodes(first_issue_connection),
        _connection_nodes(first_pull_request_connection),
    )
    cutoff = (
        latest_updated_at - timedelta(days=normalized_lookback_days)
        if latest_updated_at is not None and normalized_lookback_days is not None
        else None
    )

    records = _collect_contributor_activity(
        owner=owner,
        repo=repo,
        first_connection=first_issue_connection,
        fetch_connection=fetch_issue_connection,
        parse_nodes=_parse_issue_activity_nodes,
        cutoff=cutoff,
    )
    records.extend(
        _collect_contributor_activity(
            owner=owner,
            repo=repo,
            first_connection=first_pull_request_connection,
            fetch_connection=fetch_pull_request_connection,
            parse_nodes=_parse_pull_request_activity_nodes,
            cutoff=cutoff,
        )
    )
    records.sort(key=lambda record: (record.occurred_at, record.actor, record.activity_type))

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
    max_workers: int = 2,
    *,
    repos: list[str] | None = None,
    repo_pause_seconds: float = 0.0,
    lookback_days: int | None = DEFAULT_CONTRIBUTOR_ACTIVITY_LOOKBACK_DAYS,
    use_cache: bool | None = None,
    cache_ttl_seconds: int | None = None,
    refresh: bool = False,
) -> list[ContributorActivityRecord]:
    """Fetch normalized contributor activity across all repositories in an org."""
    normalized_lookback_days = _normalize_lookback_days(lookback_days)
    normalized_repo_filters = _normalize_repo_filters(org, repos)
    pause_seconds = max(0.0, repo_pause_seconds)
    logger.info(
        "Fetching contributor activity for %s (max_workers=%d, lookback_days=%s, repos=%d, repo_pause_seconds=%.1f)",
        org,
        max_workers,
        normalized_lookback_days if normalized_lookback_days is not None else "FULL",
        len(normalized_repo_filters),
        pause_seconds,
    )
    cache_parameters = {
        "org": org,
        "lookback_days": normalized_lookback_days,
        "repos": normalized_repo_filters,
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

    repos = fetch_org_repos_graphql(
        client,
        org,
        **_cache_kwargs(use_cache, cache_ttl_seconds, refresh),
    )
    if normalized_repo_filters:
        selected_repo_names = set(normalized_repo_filters)
        repos = [repo for repo in repos if repo.full_name in selected_repo_names]
        if not repos:
            requested = ", ".join(normalized_repo_filters)
            raise ValueError(f"No repositories matched the requested filters: {requested}")
    logger.info("Found %d repositories in %s", len(repos), org)
    all_records: list[ContributorActivityRecord] = []
    failures: list[tuple[str, str]] = []

    def fetch(repository: RepositoryRecord) -> list[ContributorActivityRecord]:
        logger.info("Scanning contributor activity for repository %s", repository.full_name)
        return fetch_repo_contributor_activity_graphql(
            client,
            owner=repository.owner,
            repo=repository.name,
            lookback_days=normalized_lookback_days,
            **_cache_kwargs(use_cache, cache_ttl_seconds, refresh),
        )

    if max_workers <= 1:
        total_repos = len(repos)
        for index, repository in enumerate(repos, start=1):
            logger.info(
                "Scanning contributor activity for repository %s (%d/%d)",
                repository.full_name,
                index,
                total_repos,
            )
            try:
                all_records.extend(fetch(repository))
            except requests.HTTPError as exc:
                failures.append((repository.full_name, _http_error_message(exc)))
            except Exception as exc:
                failures.append((repository.full_name, str(exc)))

            if pause_seconds > 0 and index < total_repos:
                logger.info(
                    "Pausing %.1f seconds before the next contributor-activity fetch",
                    pause_seconds,
                )
                time.sleep(pause_seconds)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch, repo): repo for repo in repos}

            for future in as_completed(futures):
                repo = futures[future]

                try:
                    all_records.extend(future.result())
                except requests.HTTPError as exc:
                    failures.append((repo.full_name, _http_error_message(exc)))
                except Exception as exc:
                    failures.append((repo.full_name, str(exc)))

    if failures:
        preview = ", ".join(f"{repo} ({reason})" for repo, reason in failures[:5])
        if all_records:
            logger.warning(
                "Contributor activity fetch skipped %d repositories in %s. "
                "First failures: %s",
                len(failures),
                org,
                preview,
            )
        else:
            raise RuntimeError(
                "Failed fetching contributor activity for every repository in "
                f"{org}. First failures: {preview}"
            )

    all_records.sort(key=lambda record: (record.occurred_at, record.repo, record.actor))
    logger.info("Collected %d contributor activity records across %s", len(all_records), org)
    save_records_cache(
        "org_contributor_activity",
        org,
        cache_parameters,
        ContributorActivityRecord,
        all_records,
        use_cache=use_cache,
    )
    return all_records


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
