"""Example script for searching GitHub issues."""

from hiero_analytics.config.logging import setup_logging
from hiero_analytics.data_sources.github_client import GitHubClient
from hiero_analytics.data_sources.github_search import search_issues

setup_logging(
    modules=(
        "hiero_analytics.data_sources.github_client",
        "hiero_analytics.data_sources.github_search",
        "hiero_analytics.data_sources.pagination",
        "hiero_analytics.data_sources.rate_limit",
    )
)


def main() -> None:
    """Run a sample issue search and print the first page of matches."""
    client = GitHubClient()

    query = "org:hiero-ledger is:issue label:bug state:open"
    results = search_issues(client, query)

    print(f"Query: {query}")
    print(f"Found {len(results)} matching issues\n")

    for issue in results[:5]:
        print(
            issue["html_url"],
            issue["number"],
            issue["title"],
        )


if __name__ == "__main__":
    main()

