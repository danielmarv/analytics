"""Shared HIP progression pipeline orchestration."""

from __future__ import annotations

from dataclasses import dataclass

from hiero_analytics.analysis.hip_candidate_extraction import extract_hip_candidates
from hiero_analytics.analysis.hip_scoring import score_candidates
from hiero_analytics.analysis.hip_status_aggregation import aggregate_hip_repo_status
from hiero_analytics.config.hip_progression import (
    DEFAULT_HIP_PROGRESSION_CONFIG,
    HipProgressionConfig,
)
from hiero_analytics.data_sources.github_hip_catalog import fetch_official_hip_catalog
from hiero_analytics.data_sources.github_hip_loader import fetch_repo_hip_artifacts
from hiero_analytics.data_sources.governance_config import fetch_governance_config
from hiero_analytics.data_sources.github_client import GitHubClient
from hiero_analytics.domain.hip_progression_models import (
    ArtifactHipAssessment,
    HipArtifact,
    HipCatalogEntry,
    HipCandidate,
    HipRepoStatus,
    RepositoryTargetConfig,
    hip_sort_key,
)


@dataclass(slots=True)
class HipProgressionRunResult:
    """All derived outputs from one HIP progression pipeline run."""

    catalog_entries: list[HipCatalogEntry]
    artifacts: list[HipArtifact]
    candidates: list[HipCandidate]
    assessments: list[ArtifactHipAssessment]
    repo_statuses: list[HipRepoStatus]


def select_catalog_entries(
    catalog_entries: list[HipCatalogEntry],
    *,
    latest_hip_limit: int | None = None,
) -> list[HipCatalogEntry]:
    """Return the newest official HIP catalog entries in descending HIP-number order."""
    if latest_hip_limit is not None and latest_hip_limit <= 0:
        raise ValueError("latest_hip_limit must be positive when provided.")

    deduped = {e.hip_id: e for e in sorted(catalog_entries, key=lambda e: hip_sort_key(e.hip_id))}
    newest = sorted(deduped.values(), key=lambda e: hip_sort_key(e.hip_id), reverse=True)
    if latest_hip_limit is None:
        return newest
    return newest[:latest_hip_limit]


def resolve_default_repository_targets(
    *,
    owner: str,
    config: HipProgressionConfig = DEFAULT_HIP_PROGRESSION_CONFIG,
) -> list[RepositoryTargetConfig]:
    """Build default batch repo targets from governance config."""
    governance_config = fetch_governance_config()
    repositories = governance_config.get("repositories", [])
    targets: list[RepositoryTargetConfig] = []
    for repo_payload in repositories:
        if not isinstance(repo_payload, dict):
            continue
        repo_name = repo_payload.get("name")
        if not isinstance(repo_name, str):
            continue
        if not config.repository_matches_default_scope(repo_name):
            continue
        override = config.repo_overrides.get(f"{owner}/{repo_name}")
        if override is not None:
            targets.append(override)
            continue
        targets.append(RepositoryTargetConfig(owner=owner, repo=repo_name))
    return sorted(targets, key=lambda t: t.full_name)


def run_hip_progression_pipeline(
    *,
    artifacts: list[HipArtifact],
    catalog_entries: list[HipCatalogEntry],
    repos: list[str],
    latest_hip_limit: int | None = None,
) -> HipProgressionRunResult:
    """Execute the end-to-end HIP progression pipeline on already-loaded data."""
    scoped_catalog = select_catalog_entries(catalog_entries, latest_hip_limit=latest_hip_limit)
    scoped_hip_ids = {e.hip_id for e in scoped_catalog}

    candidates = extract_hip_candidates(artifacts)
    if scoped_hip_ids:
        candidates = [c for c in candidates if c.hip_id in scoped_hip_ids]
    assessments = score_candidates(candidates)
    repo_statuses = aggregate_hip_repo_status(
        assessments,
        artifacts=artifacts,
        catalog_entries=scoped_catalog,
        repos=repos,
    )
    return HipProgressionRunResult(
        catalog_entries=scoped_catalog,
        artifacts=artifacts,
        candidates=candidates,
        assessments=assessments,
        repo_statuses=repo_statuses,
    )


def run_pipeline_for_targets(
    *,
    client: GitHubClient,
    targets: list[RepositoryTargetConfig],
    limit: int | None = None,
    latest_hip_limit: int | None = None,
) -> HipProgressionRunResult:
    """Run the HIP progression pipeline across multiple repositories."""
    catalog_entries = fetch_official_hip_catalog(client)
    all_artifacts: list[HipArtifact] = []
    repos: list[str] = []
    for target in targets:
        repos.append(target.full_name)
        all_artifacts.extend(
            fetch_repo_hip_artifacts(
                client,
                target.owner,
                target.repo,
                include_issues=target.include_issues,
                include_prs=target.include_prs,
                limit=limit,
            )
        )
    return run_hip_progression_pipeline(
        artifacts=all_artifacts,
        catalog_entries=catalog_entries,
        repos=repos,
        latest_hip_limit=latest_hip_limit,
    )
