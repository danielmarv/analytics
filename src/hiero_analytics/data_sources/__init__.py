"""Public data-source exports for analytics ingestion."""

from .github_client import GitHubClient
from .github_contributor_activity import (
    DEFAULT_CONTRIBUTOR_ACTIVITY_LOOKBACK_DAYS,
    fetch_org_contributor_activity_graphql,
    fetch_repo_contributor_activity_graphql,
)
from .github_contributor_queries import (
    CONTRIBUTOR_ISSUE_ACTIVITY_QUERY,
    CONTRIBUTOR_PULL_REQUEST_ACTIVITY_QUERY,
)
from .github_ingest import (
    fetch_org_issues_graphql,
    fetch_org_merged_pr_difficulty_graphql,
    fetch_org_repos_graphql,
    fetch_repo_issues_graphql,
    fetch_repo_merged_pr_difficulty_graphql,
)
from .github_queries import ISSUES_QUERY, MERGED_PR_QUERY, REPOS_QUERY
from .github_search import (
    search_issues,
)
from .models import (
    ContributorActivityRecord,
    IssueRecord,
    PullRequestDifficultyRecord,
    RepositoryRecord,
)
from .pagination import paginate_cursor

__all__ = [
    "GitHubClient",
    "search_issues",
    "DEFAULT_CONTRIBUTOR_ACTIVITY_LOOKBACK_DAYS",
    "REPOS_QUERY",
    "ISSUES_QUERY",
    "MERGED_PR_QUERY",
    "CONTRIBUTOR_ISSUE_ACTIVITY_QUERY",
    "CONTRIBUTOR_PULL_REQUEST_ACTIVITY_QUERY",
    "fetch_org_issues_graphql",
    "fetch_org_repos_graphql",
    "fetch_org_merged_pr_difficulty_graphql",
    "fetch_org_contributor_activity_graphql",
    "fetch_repo_merged_pr_difficulty_graphql",
    "fetch_repo_issues_graphql",
    "fetch_repo_contributor_activity_graphql",
    "RepositoryRecord",
    "IssueRecord",
    "PullRequestDifficultyRecord",
    "ContributorActivityRecord",
    "paginate_cursor",
]
