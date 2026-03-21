"""Run maintainer analytics sequentially, one repository at a time."""

from __future__ import annotations

import time
from collections.abc import Sequence

import requests

from hiero_analytics.config.paths import ORG, ensure_org_dirs
from hiero_analytics.data_sources.github_client import GitHubClient
from hiero_analytics.data_sources.github_ingest import (
    fetch_org_repos_graphql,
    fetch_repo_contributor_activity_graphql,
)
from hiero_analytics.data_sources.models import ContributorActivityRecord, RepositoryRecord
from hiero_analytics.run_maintainer_pipeline_org import (
    print_maintainer_runtime_settings,
    resolve_activity_lookback_days,
    resolve_activity_repo_pause_seconds,
    resolve_selected_repos,
    save_maintainer_pipeline_outputs,
)


def _normalize_selected_repo_filters(selected_repos: Sequence[str]) -> list[str]:
    """Normalize selected repo filters to full repository names."""
    normalized: list[str] = []
    seen: set[str] = set()

    for repo_name in selected_repos:
        stripped = repo_name.strip()
        if not stripped:
            continue

        full_name = stripped if "/" in stripped else f"{ORG}/{stripped}"
        if full_name in seen:
            continue

        seen.add(full_name)
        normalized.append(full_name)

    return normalized


def _filter_repositories(
    repositories: Sequence[RepositoryRecord],
    selected_repos: Sequence[str],
) -> list[RepositoryRecord]:
    """Filter repositories in org order when explicit selections are provided."""
    normalized_filters = _normalize_selected_repo_filters(selected_repos)
    if not normalized_filters:
        return list(repositories)

    selected_names = set(normalized_filters)
    filtered = [repo for repo in repositories if repo.full_name in selected_names]
    if not filtered:
        requested = ", ".join(normalized_filters)
        raise ValueError(f"No repositories matched the requested filters: {requested}")

    return filtered


def main() -> None:
    """Fetch maintainer activity sequentially per repo before building outputs."""
    ensure_org_dirs(ORG)
    activity_lookback_days = resolve_activity_lookback_days()
    repo_pause_seconds = resolve_activity_repo_pause_seconds()
    selected_repos = resolve_selected_repos()

    print(f"Running maintainer pipeline analytics repo by repo for org: {ORG}")
    print_maintainer_runtime_settings(
        activity_max_workers=1,
        activity_lookback_days=activity_lookback_days,
        repo_pause_seconds=repo_pause_seconds,
        selected_repos=selected_repos,
    )

    client = GitHubClient()
    repositories = _filter_repositories(fetch_org_repos_graphql(client, ORG), selected_repos)
    total_repos = len(repositories)
    all_activities: list[ContributorActivityRecord] = []
    failures: list[tuple[str, str]] = []

    print(f"Processing {total_repos} repo(s) sequentially")

    for index, repository in enumerate(repositories, start=1):
        print(f"[{index}/{total_repos}] Fetching contributor activity for {repository.full_name}")

        try:
            repo_activities = fetch_repo_contributor_activity_graphql(
                client,
                owner=repository.owner,
                repo=repository.name,
                lookback_days=activity_lookback_days,
            )
        except requests.HTTPError as exc:
            failures.append((repository.full_name, str(exc)))
            print(f"Skipping {repository.full_name} after GitHub error: {exc}")
        except Exception as exc:
            failures.append((repository.full_name, str(exc)))
            print(f"Skipping {repository.full_name} after error: {exc}")
        else:
            all_activities.extend(repo_activities)
            print(f"Collected {len(repo_activities)} activity records for {repository.full_name}")

        if repo_pause_seconds > 0 and index < total_repos:
            print(f"Waiting {repo_pause_seconds:g}s before the next repository")
            time.sleep(repo_pause_seconds)

    if failures:
        preview = ", ".join(f"{repo} ({reason})" for repo, reason in failures[:5])
        if not all_activities:
            raise RuntimeError(
                "Failed fetching contributor activity for every repository. "
                f"First failures: {preview}"
            )
        print(f"Skipped {len(failures)} repo(s). First failures: {preview}")

    all_activities.sort(key=lambda record: (record.occurred_at, record.repo, record.actor))
    print(f"Fetched {len(all_activities)} contributor activity records across {total_repos} repo(s)")
    save_maintainer_pipeline_outputs(all_activities)
    print("Maintainer pipeline analytics complete")


if __name__ == "__main__":
    main()
