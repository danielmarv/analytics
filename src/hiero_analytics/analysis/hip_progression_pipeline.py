"""Shared HIP progression pipeline orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from hiero_analytics.analysis.hip_candidate_extraction import extract_hip_candidates
from hiero_analytics.analysis.hip_evaluation import (
    evaluate_status_predictions,
    load_benchmark_dataset,
)
from hiero_analytics.analysis.hip_feature_engineering import engineer_hip_feature_vectors
from hiero_analytics.analysis.hip_scoring import score_hip_feature_vectors
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
    HipArtifact,
    HipCatalogEntry,
    HipEvidence,
    HipRepoStatus,
    RepositoryTargetConfig,
    hip_sort_key,
)


@dataclass(slots=True)
class HipProgressionRunResult:
    """All derived outputs from one HIP progression pipeline run."""

    catalog_entries: list[HipCatalogEntry]
    artifacts: list[HipArtifact]
    candidates: list
    feature_vectors: list
    artifact_assessments: list[HipEvidence]
    repo_statuses: list[HipRepoStatus]


def select_catalog_entries(
    catalog_entries: list[HipCatalogEntry],
    *,
    latest_hip_limit: int | None = None,
) -> list[HipCatalogEntry]:
    """Return the newest official HIP catalog entries in descending HIP-number order."""
    if latest_hip_limit is not None and latest_hip_limit <= 0:
        raise ValueError("latest_hip_limit must be positive when provided.")

    deduped_entries = {
        entry.hip_id: entry
        for entry in sorted(catalog_entries, key=lambda entry: hip_sort_key(entry.hip_id))
    }
    newest_entries = sorted(
        deduped_entries.values(),
        key=lambda entry: hip_sort_key(entry.hip_id),
        reverse=True,
    )
    if latest_hip_limit is None:
        return newest_entries
    return newest_entries[:latest_hip_limit]


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
    return sorted(targets, key=lambda target: target.full_name)


def run_hip_progression_pipeline(
    *,
    artifacts: list[HipArtifact],
    catalog_entries: list[HipCatalogEntry],
    repos: list[str],
    latest_hip_limit: int | None = None,
    config: HipProgressionConfig = DEFAULT_HIP_PROGRESSION_CONFIG,
) -> HipProgressionRunResult:
    """Execute the end-to-end HIP progression pipeline on already-loaded data."""
    scoped_catalog_entries = select_catalog_entries(
        catalog_entries,
        latest_hip_limit=latest_hip_limit,
    )
    scoped_hip_ids = {entry.hip_id for entry in scoped_catalog_entries}

    candidates = extract_hip_candidates(artifacts, config=config)
    if scoped_hip_ids:
        candidates = [candidate for candidate in candidates if candidate.hip_id in scoped_hip_ids]
    feature_vectors = engineer_hip_feature_vectors(candidates, artifacts=artifacts, config=config)
    artifact_assessments = score_hip_feature_vectors(feature_vectors, config=config)
    repo_statuses = aggregate_hip_repo_status(
        artifact_assessments,
        artifacts=artifacts,
        catalog_entries=scoped_catalog_entries,
        repos=repos,
        config=config,
    )
    return HipProgressionRunResult(
        catalog_entries=scoped_catalog_entries,
        artifacts=artifacts,
        candidates=candidates,
        feature_vectors=feature_vectors,
        artifact_assessments=artifact_assessments,
        repo_statuses=repo_statuses,
    )


def run_pipeline_for_repo(
    *,
    client: GitHubClient,
    owner: str,
    repo: str,
    include_issues: bool,
    include_prs: bool,
    limit: int | None,
    latest_hip_limit: int | None = None,
    config: HipProgressionConfig = DEFAULT_HIP_PROGRESSION_CONFIG,
) -> HipProgressionRunResult:
    """Fetch catalog and artifacts, then run the pipeline for one repository."""
    catalog_entries = fetch_official_hip_catalog(client)
    artifacts = fetch_repo_hip_artifacts(
        client,
        owner,
        repo,
        include_issues=include_issues,
        include_prs=include_prs,
        limit=limit,
    )
    return run_hip_progression_pipeline(
        artifacts=artifacts,
        catalog_entries=catalog_entries,
        repos=[f"{owner}/{repo}"],
        latest_hip_limit=latest_hip_limit,
        config=config,
    )


def run_pipeline_for_targets(
    *,
    client: GitHubClient,
    targets: list[RepositoryTargetConfig],
    limit: int | None = None,
    latest_hip_limit: int | None = None,
    config: HipProgressionConfig = DEFAULT_HIP_PROGRESSION_CONFIG,
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
        config=config,
    )


def run_benchmark_evaluation(
    benchmark_dir: Path,
    *,
    config: HipProgressionConfig = DEFAULT_HIP_PROGRESSION_CONFIG,
) -> dict[str, list[dict[str, object]]]:
    """Run the pipeline on the curated benchmark dataset and score the predictions."""
    catalog_entries, artifacts, artifact_expectations, repo_expectations = load_benchmark_dataset(benchmark_dir)
    repos = sorted({artifact.repo for artifact in artifacts})
    result = run_hip_progression_pipeline(
        artifacts=artifacts,
        catalog_entries=catalog_entries,
        repos=repos,
        config=config,
    )
    artifact_predictions = {
        ((assessment.repo, f"{assessment.artifact_type}:{assessment.artifact_number}"), assessment.hip_id): assessment.status
        for assessment in result.artifact_assessments
    }
    repo_predictions = {
        (assessment.repo, assessment.hip_id): assessment.status
        for assessment in result.repo_statuses
    }
    artifact_expectation_pairs = [
        (((expectation.repo, f"{expectation.artifact_type}:{expectation.artifact_number}"), expectation.hip_id), expectation.expected_status)
        for expectation in artifact_expectations
    ]
    artifact_metrics, artifact_confusion, artifact_per_status = evaluate_status_predictions(
        scope="artifact",
        expectations=artifact_expectation_pairs,
        predictions=artifact_predictions,
    )
    repo_metrics, repo_confusion, repo_per_status = evaluate_status_predictions(
        scope="repo",
        expectations=[((expectation.repo, expectation.hip_id), expectation.expected_status) for expectation in repo_expectations],
        predictions=repo_predictions,
    )
    return {
        "artifact_metrics": artifact_metrics,
        "artifact_confusion": artifact_confusion,
        "artifact_per_status": artifact_per_status,
        "repo_metrics": repo_metrics,
        "repo_confusion": repo_confusion,
        "repo_per_status": repo_per_status,
    }
