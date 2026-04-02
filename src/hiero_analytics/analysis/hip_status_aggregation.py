"""Repository-level aggregation for HIP progression assessments."""

from __future__ import annotations

from collections import defaultdict

from hiero_analytics.domain.hip_progression_models import (
    ArtifactHipAssessment,
    DevelopmentStatus,
    HipArtifact,
    HipCatalogEntry,
    HipRepoStatus,
    hip_sort_key,
)

# Priority order: completed > in_progress > raised > not_raised
_STATUS_RANK: dict[str, int] = {
    "not_raised": 0,
    "raised": 1,
    "in_progress": 2,
    "completed": 3,
}


def _best_status(statuses: list[DevelopmentStatus]) -> DevelopmentStatus:
    """Pick the most advanced status from a list of artifact statuses."""
    if not statuses:
        return "not_raised"
    return max(statuses, key=lambda s: _STATUS_RANK.get(s, 0))


def _best_confidence(assessments: list[ArtifactHipAssessment]) -> str:
    """Pick high if any assessment is high, otherwise low."""
    if any(a.confidence == "high" for a in assessments):
        return "high"
    return "low"


def _artifact_label(artifact: HipArtifact) -> str:
    prefix = "PR" if artifact.artifact_type == "pull_request" else "Issue"
    return f"{prefix} #{artifact.number}"


def aggregate_hip_repo_status(
    assessments: list[ArtifactHipAssessment],
    *,
    artifacts: list[HipArtifact] | None = None,
    catalog_entries: list[HipCatalogEntry] | None = None,
    repos: list[str] | None = None,
) -> list[HipRepoStatus]:
    """Aggregate artifact-level assessments into repo-level HIP statuses."""
    artifact_lookup: dict[tuple[str, int], HipArtifact] = {}
    if artifacts:
        artifact_lookup = {(a.repo, a.number): a for a in artifacts}

    repo_names = set(repos or [])
    repo_names.update(a.repo for a in assessments)

    grouped: dict[tuple[str, str], list[ArtifactHipAssessment]] = defaultdict(list)
    for a in assessments:
        grouped[(a.repo, a.hip_id)].append(a)

    hip_ids = (
        [e.hip_id for e in catalog_entries]
        if catalog_entries
        else sorted({a.hip_id for a in assessments}, key=hip_sort_key)
    )

    results: list[HipRepoStatus] = []
    for repo in sorted(repo_names):
        for hip_id in sorted(hip_ids, key=hip_sort_key, reverse=True):
            group = grouped.get((repo, hip_id), [])
            status = _best_status([a.status for a in group]) if group else "not_raised"
            confidence = _best_confidence(group) if group else "low"

            # Supporting artifacts: prefer PRs over issues, merged over open.
            supporting = sorted(
                group,
                key=lambda a: (
                    1 if a.artifact_type == "pull_request" else 0,
                    1 if a.merged else 0,
                    a.artifact_number,
                ),
                reverse=True,
            )[:3]
            supporting_numbers = [a.artifact_number for a in supporting]
            top_artifacts = [
                _artifact_label(artifact_lookup[a.repo, a.artifact_number])
                for a in supporting
                if (a.repo, a.artifact_number) in artifact_lookup
            ]

            last_evidence_at = None
            for a in supporting:
                art = artifact_lookup.get((a.repo, a.artifact_number))
                if art is None:
                    continue
                ts = art.activity_timestamp()
                if ts and (last_evidence_at is None or ts > last_evidence_at):
                    last_evidence_at = ts

            results.append(
                HipRepoStatus(
                    repo=repo,
                    hip_id=hip_id,
                    status=status,
                    confidence=confidence,
                    supporting_artifact_numbers=supporting_numbers,
                    top_artifacts=top_artifacts,
                    last_evidence_at=last_evidence_at,
                )
            )
    return results
