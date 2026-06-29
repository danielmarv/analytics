"""GitHub data ingestion utilities using the GraphQL API.

This module provides functions for retrieving repositories, issues, and
merged pull request metadata from GitHub. Data is fetched using cursor-
based pagination and can be aggregated across an organization with
parallel requests to improve ingestion speed.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from typing import TypeVar

import requests

from hiero_analytics.config.github import BASE_URL
from hiero_analytics.config.paths import load_query

from .cache import FetchCacheOptions, GitHubRecordCache
from .github_client import GitHubClient
from .models import (
    BaseRecord,
    ContributorActivityRecord,
    ContributorMergedPRCountRecord,
    IssueRecord,
    IssueTimelineEventRecord,
    PullRequestDifficultyRecord,
    RepositoryRecord,
)
from .pagination import extract_graphql_cursor_page, paginate_cursor

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseRecord)

class _CacheProxy:
    def resolve_cache_options(self, options: FetchCacheOptions | None) -> FetchCacheOptions:
        return GitHubRecordCache().resolve_cache_options(options)

    def load_records(self, *args, **kwargs):
        return GitHubRecordCache().load_records(*args, **kwargs)

    def save_records(self, *args, **kwargs):
        return GitHubRecordCache().save_records(*args, **kwargs)


cache = _CacheProxy()

_CONTRIBUTOR_ACTIVITY_TYPES = [
    "authored_issue",
    "authored_pull_request",
    "reviewed_pull_request",
    "merged_pull_request",
]
_ISSUE_TIMELINE_HEADERS = {
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

# --------------------------------------------------------
# GENERIC RESOURCE FETCHER ENGINE
# --------------------------------------------------------


def fetch_github_resource(  # noqa: UP047
    client: GitHubClient,
    query: str,
    variables: dict,
    model_class: type[T],
    nodes_path: list[str],
    *,
    cache_key: str,
    cache_scope: str,
    cache_parameters: dict[str, object],
    context_builder: Callable[[dict], dict] | None = None,
    cache_options: FetchCacheOptions | None = None,
    ) -> list[T]:
    """Generic engine for fetching paginated GitHub resources."""
    opts = cache.resolve_cache_options(cache_options)
    cached = cache.load_records(
        cache_key,
        cache_scope,
        cache_parameters,
        model_class,
        use_cache=opts.use_cache,
        ttl_seconds=opts.cache_ttl_seconds,
        refresh=opts.refresh,
    )
    if cached is not None:
        return cached

    def page(cursor: str | None) -> tuple[list[T], str | None, bool]:
        """Fetch a single page of GraphQL results."""
        paginated_vars = dict(variables)
        paginated_vars["cursor"] = cursor

        data = client.graphql(query, paginated_vars)
        nodes, next_cursor, has_next = extract_graphql_cursor_page(data, nodes_path)

        items = []
        for node in nodes:
            context = context_builder(node) if context_builder else {}
            result = model_class.from_github_node(node, context)
            items.extend(result)

        return items, next_cursor, has_next

    records = paginate_cursor(page)
    cache.save_records(
        cache_key, cache_scope, cache_parameters, model_class,
        records,
        use_cache=opts.use_cache,
    )
    return records


def fetch_org_resource_parallel(  # noqa: UP047
    client: GitHubClient,
    org: str,
    fetch_repo_func: Callable,
    model_class: type[T],
    max_workers: int,
    cache_key: str,
    cache_parameters: dict[str, object],
    repos: list[str] | None = None,
    *,
    cache_options: FetchCacheOptions | None = None,
    task_desc: str = "records",
) -> list[T]:
    """Generic engine for orchestrating parallel organization repository fetches."""
    opts = cache.resolve_cache_options(cache_options)
    cached = cache.load_records(
        cache_key,
        org,
        cache_parameters,
        model_class,
        use_cache=opts.use_cache,
        ttl_seconds=opts.cache_ttl_seconds,
        refresh=opts.refresh,
    )
    if cached is not None:
        return cached

    logger.info("Fetching %s across %s (max_workers=%d)", task_desc, org, max_workers)

    all_repos = fetch_org_repos_graphql(client, org, cache_options=cache_options)

    if repos:
        allowed = set(repos)
        all_repos = [r for r in all_repos if r.full_name in allowed or r.name in allowed]

    all_records = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_repo_func, repo): repo for repo in all_repos}
        for future in as_completed(futures):
            repo = futures[future]
            try:
                result = future.result()
                if isinstance(result, list):
                    all_records.extend(result)
                else:
                    all_records.append(result)
            except (requests.HTTPError, ValueError) as exc:  # noqa: BLE001
                logger.exception("Failed fetching %s for %s: %s", task_desc, repo.full_name, exc)

    logger.info("Collected %d %s across %s", len(all_records), task_desc, org)
    cache.save_records(cache_key, org, cache_parameters, model_class,
                       all_records, use_cache=opts.use_cache,
    )
    return all_records


# --------------------------------------------------------
# FETCH REPOSITORIES
# --------------------------------------------------------

def fetch_org_repos_graphql(
    client: GitHubClient,
    org: str,
    *,
    cache_options: FetchCacheOptions | None = None,
    ) -> list[RepositoryRecord]:
    """Fetch all repository full names for an organization using GraphQL."""
    REPOS_QUERY = load_query("repos")
    return fetch_github_resource(
        client, REPOS_QUERY, {"org": org}, RepositoryRecord, ["organization", "repositories"],
        cache_key="org_repos", cache_scope=org, cache_parameters={"org": org},
        context_builder=lambda _node: {"owner": org},
        cache_options=cache_options,
    )

# --------------------------------------------------------
# FETCH ISSUES
# --------------------------------------------------------

def fetch_repo_issues_graphql(
    client: GitHubClient,
    owner: str,
    repo: str,
    states: list[str] | None = None,
    *,
    cache_options: FetchCacheOptions | None = None,
) -> list[IssueRecord]:
    """Fetch all issues for a repository using GraphQL."""
    ISSUES_QUERY = load_query("issues")
    norm_states = [s.upper() for s in states] if states else None
    return fetch_github_resource(
        client, ISSUES_QUERY, {"owner": owner, "repo": repo, "states": norm_states}, IssueRecord, ["repository", "issues"],
        cache_key="repo_issues", cache_scope=f"{owner}_{repo}",
        cache_parameters={"owner": owner, "repo": repo, "states": sorted(norm_states or [])},
        context_builder=lambda _node: {"owner": owner, "repo": repo},
        cache_options=cache_options,
    )

def fetch_org_issues_graphql(
    client: GitHubClient,
    org: str,
    states: list[str] | None = None,
    max_workers: int = 5,
    *,
    cache_options: FetchCacheOptions | None = None,
    ) -> list[IssueRecord]:
    """Fetch all issues across all repositories in an organization."""
    def fetch_func(repo):
        """Fetch issues for a single repository."""
        return fetch_repo_issues_graphql(client, repo.owner, repo.name, states=states, cache_options=cache_options)
    return fetch_org_resource_parallel(
        client, org, fetch_func, IssueRecord, max_workers, "org_issues",
        {"org": org, "states": sorted(s.upper() for s in states) if states else []},
        task_desc="organization issues",
        cache_options=cache_options,
    )


def fetch_repo_issue_label_events_graphql(
    client: GitHubClient,
    owner: str,
    repo: str,
    states: list[str] | None = None,
    *,
    cache_options: FetchCacheOptions | None = None,
) -> list[IssueTimelineEventRecord]:
    """Fetch label add/remove events for a repo's issues via GraphQL ``timelineItems``.

    Unlike the repo-wide ``/issues/events`` REST endpoint (which streams every
    event type for every issue and is page-capped), this requests only
    ``LABELED_EVENT``/``UNLABELED_EVENT`` items inline with the issue list, so it
    transfers a fraction of the data, never truncates, and is cached on a stable
    key (owner/repo/states) rather than a per-run ``since`` timestamp.
    """
    query = load_query("issue_label_events")
    norm_states = [s.upper() for s in states] if states else None
    return fetch_github_resource(
        client,
        query,
        {"owner": owner, "repo": repo, "states": norm_states},
        IssueTimelineEventRecord,
        ["repository", "issues"],
        cache_key="repo_issue_label_events",
        cache_scope=f"{owner}_{repo}",
        cache_parameters={"owner": owner, "repo": repo, "states": sorted(norm_states or [])},
        context_builder=lambda _node: {"owner": owner, "repo": repo},
        cache_options=cache_options,

    )


def fetch_org_issue_label_events_graphql(
    client: GitHubClient,
    org: str,
    states: list[str] | None = None,
    max_workers: int = 5,
    *,
    cache_options: FetchCacheOptions | None = None,
) -> list[IssueTimelineEventRecord]:
    """Fetch label add/remove events for all issues across an organization via GraphQL."""
    def fetch_func(repo):
        """Fetch label events for a single repository."""
        return fetch_repo_issue_label_events_graphql(
            client, repo.owner, repo.name, states=states,
            cache_options=cache_options
        )

    return fetch_org_resource_parallel(
        client, org, fetch_func, IssueTimelineEventRecord, max_workers, "org_issue_label_events",
        {"org": org, "states": sorted(s.upper() for s in states) if states else []},
        task_desc="organization issue label events",
        cache_options=cache_options,
    )


def fetch_repo_issue_events_rest_since(
    client: GitHubClient,
    owner: str,
    repo: str,
    *,
    since: datetime,
    cache_options: FetchCacheOptions | None = None,
) -> list[IssueTimelineEventRecord]:
    """Fetch repository issue events since a cutoff date."""
    max_pages = 300
    cutoff = since.astimezone(UTC)
    cutoff_iso = cutoff.isoformat()
    cache_scope = f"{owner}_{repo}"
    cache_parameters = {
        "owner": owner,
        "repo": repo,
        "since": cutoff_iso,
    }
    opts = cache.resolve_cache_options(cache_options)
    cached = cache.load_records(
        "repo_issue_events_since",
        cache_scope,
        cache_parameters,
        IssueTimelineEventRecord,
        use_cache=opts.use_cache,
        ttl_seconds=opts.cache_ttl_seconds,
        refresh=opts.refresh,
    )
    if cached is not None:
        return cached

    records: list[IssueTimelineEventRecord] = []
    page = 1

    while True:
        if page > max_pages:
            logger.warning(
                "Stopping issue event pagination for %s/%s after %d pages",
                owner,
                repo,
                max_pages,
            )
            break

        try:
            payload = client.get(
                f"{BASE_URL}/repos/{owner}/{repo}/issues/events",
                params={"per_page": 100, "page": page},
                headers=_ISSUE_TIMELINE_HEADERS,
            )
        except requests.HTTPError as exc:
            response = exc.response
            if response is not None and response.status_code == 422:
                logger.warning(
                    "Stopping issue event pagination for %s/%s at page %d due to 422",
                    owner,
                    repo,
                    page,
                )
                break
            raise

        if not isinstance(payload, list):
            raise ValueError("Repository issue events payload must be a list")

        page_has_older_events = False

        for event in payload:
            if not isinstance(event, dict):
                continue

            issue_node = event.get("issue")
            if not isinstance(issue_node, dict):
                continue

            issue_number = issue_node.get("number")
            if not isinstance(issue_number, int):
                continue

            record = IssueTimelineEventRecord.from_rest_event(
                event,
                owner=owner,
                repo=repo,
                issue_number=issue_number,
            )
            if record is None:
                continue

            occurred_at = record.occurred_at.astimezone(UTC)
            if occurred_at < cutoff:
                page_has_older_events = True
                continue

            records.append(record)

        if len(payload) < 100 or page_has_older_events:
            break

        page += 1

    cache.save_records(
        "repo_issue_events_since",
        cache_scope,
        cache_parameters,
        IssueTimelineEventRecord,
        records,
        use_cache=opts.use_cache,
    )
    return records


def fetch_repo_issue_timeline_events_graphql(
    client: GitHubClient,
    owner: str,
    repo: str,
    issue_number: int,
    *,
    since: datetime | None = None,
    cache_options: FetchCacheOptions | None = None,
) -> list[IssueTimelineEventRecord]:
    """Fetch issue timeline events for one issue using GraphQL."""
    query = load_query("issue_timeline")
    since_value = since.astimezone(UTC).isoformat() if since is not None else None
    cache_scope = f"{owner}_{repo}_{issue_number}"
    cache_parameters = {
        "owner": owner,
        "repo": repo,
        "issue_number": issue_number,
        "since": since_value,
    }
    opts = cache.resolve_cache_options(cache_options)
    cached = cache.load_records(
        "repo_issue_timeline_events_graphql",
        cache_scope,
        cache_parameters,
        IssueTimelineEventRecord,
        use_cache=opts.use_cache,
        ttl_seconds=opts.cache_ttl_seconds,
        refresh=opts.refresh,
    )
    if cached is not None:
        return cached

    def page(cursor: str | None) -> tuple[list[IssueTimelineEventRecord], str | None, bool]:
        data = client.graphql(
            query,
            {
                "owner": owner,
                "repo": repo,
                "number": issue_number,
                "cursor": cursor,
            },
        )
        nodes, next_cursor, has_next = extract_graphql_cursor_page(
            data, ["repository", "issue", "timelineItems"]
        )

        records: list[IssueTimelineEventRecord] = []
        context = {"owner": owner, "repo": repo, "issue_number": issue_number, "since": since}
        for node in nodes:
            if not isinstance(node, dict):
                continue
            records.extend(IssueTimelineEventRecord.from_timeline_item(node, context))

        return records, next_cursor, has_next

    records = paginate_cursor(page)
    cache.save_records(
        "repo_issue_timeline_events_graphql",
        cache_scope,
        cache_parameters,
        IssueTimelineEventRecord,
        records,
        use_cache=opts.use_cache,
    )
    return records


def fetch_repo_issue_events_for_issues_since(
    client: GitHubClient,
    issues: list[IssueRecord],
    *,
    since: datetime,
    max_workers: int = 5,
    cache_options: FetchCacheOptions | None = None,
) -> list[IssueTimelineEventRecord]:
    """Fetch repository-level issue events since a cutoff for repos present in the issue set."""
    repos = sorted({issue.repo for issue in issues})

    def fetch_func(full_repo: str) -> list[IssueTimelineEventRecord]:
        """Fetch timeline events for all issues in a repository."""
        owner, repo = full_repo.split("/", maxsplit=1)
        return fetch_repo_issue_events_rest_since(
            client,
            owner,
            repo,
            since=since,
            cache_options=cache_options,
        )

    records: list[IssueTimelineEventRecord] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_func, full_repo): full_repo for full_repo in repos}
        for future in as_completed(futures):
            full_repo = futures[future]
            try:
                records.extend(future.result())
            except Exception as exc:
                logger.exception(
                    "Failed fetching repository issue events for %s since %s: %s",
                    full_repo,
                    since.isoformat(),
                    exc,
                )

    return records



# --------------------------------------------------------
# FETCH MERGED PR DIFFICULTY
# --------------------------------------------------------
def fetch_repo_merged_pr_difficulty_graphql(
    client: GitHubClient,
    owner: str,
    repo: str,
    *,
    cache_options: FetchCacheOptions | None = None,
    ) -> list[PullRequestDifficultyRecord]:
    """Fetch merged pull requests and their linked closing issues for a repository."""
    MERGED_PR_QUERY = load_query("merged_pr")
    return fetch_github_resource(
        client, MERGED_PR_QUERY, {"owner": owner, "repo": repo}, PullRequestDifficultyRecord, ["repository", "pullRequests"],
        cache_key="repo_merged_pr_difficulty", cache_scope=f"{owner}_{repo}", 
        cache_parameters={"owner": owner, "repo": repo},
        context_builder=lambda _node: {"owner": owner, "repo": repo},
        cache_options=cache_options,
    )

def fetch_org_merged_pr_difficulty_graphql(
    client: GitHubClient,
    org: str,
    max_workers: int = 5,
    *,
    cache_options: FetchCacheOptions | None = None
    ) -> list[PullRequestDifficultyRecord]:
    """Fetch merged pull request difficulty records across all repositories in an organization."""
    def fetch_func(repo):
        """Fetch merged PR difficulty metrics for a repository."""
        return fetch_repo_merged_pr_difficulty_graphql(client,
        repo.owner, repo.name, cache_options=cache_options)
    return fetch_org_resource_parallel(
        client, org, fetch_func, PullRequestDifficultyRecord, max_workers, "org_merged_pr_difficulty",
        {"org": org}, task_desc="merged PR difficulty records",
        cache_options=cache_options,
    )

# --------------------------------------------------------
# FETCH CONTRIBUTOR ACTIVITY
# --------------------------------------------------------


def fetch_repo_contributor_activity_graphql(
    client: GitHubClient,
    owner: str,
    repo: str,
    *,
    lookback_days: int = 183,
    cache_options: FetchCacheOptions | None = None,
    ) -> list[ContributorActivityRecord]:
    """
    Fetch contributor activity signals from pull request and issue lifecycle data.

    Signals include:
    - authored_pull_request
    - reviewed_pull_request
    - merged_pull_request
    - created_issue
    """
    opts = cache.resolve_cache_options(cache_options)
    cached = cache.load_records(
        "repo_contributor_activity",
        f"{owner}_{repo}",
        {"owner": owner, "repo": repo, "lookback_days": lookback_days},
        ContributorActivityRecord,
        use_cache=opts.use_cache,
        ttl_seconds=opts.cache_ttl_seconds,
        refresh=opts.refresh,
    )
    if cached is not None:
        return cached

    cutoff = (
        datetime.now(UTC) - timedelta(days=lookback_days)
        if lookback_days is not None
        else None
    )
    pr_records = fetch_github_resource(
        client,
        load_query("contributor_activity"),
        {"owner": owner, "repo": repo},
        ContributorActivityRecord,
        ["repository", "pullRequests"],
        cache_key="repo_contributor_activity_prs",
        cache_scope=f"{owner}_{repo}",
        cache_parameters={"owner": owner, "repo": repo, "lookback_days": lookback_days},
        context_builder=lambda node: {
            "owner": owner,
            "repo": repo,
            "cutoff": cutoff,
            "activity_source": "pull_request",
        },
        cache_options=FetchCacheOptions(use_cache=False),
    )

    issue_records = fetch_github_resource(
        client,
        load_query("contributor_issue_activity"),
        {"owner": owner, "repo": repo},
        ContributorActivityRecord,
        ["repository", "issues"],
        cache_key="repo_contributor_activity_issues",
        cache_scope=f"{owner}_{repo}",
        cache_parameters={"owner": owner, "repo": repo, "lookback_days": lookback_days},
        context_builder=lambda node: {
            "owner": owner,
            "repo": repo,
            "cutoff": cutoff,
            "activity_source": "issue",
        },
        cache_options=FetchCacheOptions(use_cache=False),
    )

    records = [*pr_records, *issue_records]
    return records

def fetch_org_contributor_activity_graphql(
    client: GitHubClient,
    org: str,
    max_workers: int = 5,
    *,
    repos: list[str] | None = None,
    lookback_days: int | None = 183,
    cache_options: FetchCacheOptions | None = None,
    ) -> list[ContributorActivityRecord]:
    """Fetch contributor activity records across all repositories in an organization.

    Pass ``lookback_days=None`` to retrieve the full history, which is
    necessary for yearly aggregation charts where past counts must remain
    stable across refreshes.
    """
    def fetch_func(repo):
        """Fetch contributor activity for a repository."""
        return fetch_repo_contributor_activity_graphql(client, repo.owner, repo.name, lookback_days=lookback_days, cache_options=cache_options)
    return fetch_org_resource_parallel(
        client, org, fetch_func, ContributorActivityRecord, max_workers, "org_contributor_activity",
        {
            "org": org,
            "repos": sorted(repos) if repos else [],
            "lookback_days": lookback_days,
            "activity_types": _CONTRIBUTOR_ACTIVITY_TYPES,
        }, repos=repos,
        task_desc="contributor activity",
        cache_options=cache_options,
    )

# --------------------------------------------------------
# FETCH CONTRIBUTOR MERGED PR COUNT
# --------------------------------------------------------

def fetch_repo_contributor_merged_pr_count_graphql(
    client: GitHubClient,
    owner: str,
    repo: str,
    login: str,
    *,
    cache_options: FetchCacheOptions | None = None
    ) -> ContributorMergedPRCountRecord:
    """Fetch contributor merged pull request count for a specific user in a repository."""
    CONTRIBUTOR_MERGED_PRS_COUNT_QUERY = load_query("contributor_merged_prs_count")
    records = fetch_github_resource(
        client, CONTRIBUTOR_MERGED_PRS_COUNT_QUERY, {"searchQuery": f"is:pr is:merged author:{login} repo:{owner}/{repo}"},
        ContributorMergedPRCountRecord, ["search"],
        cache_key="repo_contributor_merged_pr_count", cache_scope=f"{owner}_{repo}_{login}",
        cache_parameters={"owner": owner, "repo": repo, "login": login},
        context_builder=lambda _node: {"owner": owner, "repo": repo, "login": login},
        cache_options=cache_options,
    )
    return records[0] if records else ContributorMergedPRCountRecord(repo=f"{owner}/{repo}", login=login, merged_pr_count=0)

def fetch_org_contributor_merged_pr_count_graphql(
    client: GitHubClient,
    org: str,
    login: str,
    repos: list[str] | None = None,
    max_workers: int = 5,
    *,
   cache_options: FetchCacheOptions | None = None,
) -> list[ContributorMergedPRCountRecord]:
    """Fetch contributor merged pull request count for a specific user in an org."""
    def fetch_func(repo):
        """Fetch merged PR counts for a contributor in a repository."""
        return fetch_repo_contributor_merged_pr_count_graphql(client,
        repo.owner, repo.name, login=login,
        cache_options=cache_options)
    return fetch_org_resource_parallel(
        client, org, fetch_func, ContributorMergedPRCountRecord,
        max_workers, "org_contributor_merged_pr_count",
        {"org": org, "login": login, "repos": sorted(repos) if repos else []}, repos=repos,
        task_desc=f"merged PR count for {login}",
         cache_options=cache_options,
    )
