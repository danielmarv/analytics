"""Simple status and confidence inference for HIP progression candidates."""

from __future__ import annotations

from hiero_analytics.domain.hip_progression_models import (
    ArtifactHipAssessment,
    ConfidenceLevel,
    DevelopmentStatus,
    HipCandidate,
    is_committer_like_author,
)


def _infer_status(candidate: HipCandidate) -> DevelopmentStatus:
    """Infer development status from artifact signals."""
    artifact = candidate.artifact

    if artifact.artifact_type == "pull_request":
        if artifact.merged and artifact.has_code_changes() and artifact.has_test_changes():
            return "completed"
        if artifact.has_code_changes() or artifact.has_test_changes():
            return "in_progress"
        return "raised"

    # Issues can only ever be "raised" — they don't carry code.
    return "raised"


def _infer_confidence(candidate: HipCandidate) -> ConfidenceLevel:
    """High when author is committer/maintainer AND supporting code signals exist."""
    artifact = candidate.artifact
    is_committer = is_committer_like_author(artifact.author_association)
    has_strong_signal = (
        artifact.has_code_changes()
        or artifact.has_test_changes()
        or artifact.has_substantial_delta()
    )
    if is_committer and has_strong_signal:
        return "high"
    return "low"


def score_candidate(candidate: HipCandidate) -> ArtifactHipAssessment:
    """Score one HIP candidate into an artifact-level assessment."""
    artifact = candidate.artifact
    return ArtifactHipAssessment(
        repo=artifact.repo,
        hip_id=candidate.hip_id,
        artifact_type=artifact.artifact_type,
        artifact_number=artifact.number,
        status=_infer_status(candidate),
        confidence=_infer_confidence(candidate),
        has_code=artifact.has_code_changes(),
        has_tests=artifact.has_test_changes(),
        merged=artifact.merged,
        is_committer=is_committer_like_author(artifact.author_association),
    )


def score_candidates(candidates: list[HipCandidate]) -> list[ArtifactHipAssessment]:
    """Score a batch of HIP candidates."""
    return [score_candidate(c) for c in candidates]
