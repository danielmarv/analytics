"""Conservative repository-level aggregation for HIP progression evidence."""

from __future__ import annotations

from collections import defaultdict

from hiero_analytics.domain.hip_progression_models import HipArtifact, HipEvidence, HipRepoStatus

COMPLETED_COMPLETION_THRESHOLD = 75.0
COMPLETED_IMPLEMENTATION_THRESHOLD = 60.0
STRONG_TEXT_ONLY_CONFLICT_THRESHOLD = 75.0
STRONG_TEXT_ONLY_IMPLEMENTATION_MAX = 20.0
IN_PROGRESS_IMPLEMENTATION_THRESHOLD = 40.0
IN_PROGRESS_COMPLETION_THRESHOLD = 35.0
UNKNOWN_CANDIDATE_THRESHOLD = 40.0


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _resolve_status(group: list[HipEvidence]) -> str:
    max_candidate = max(evidence.hip_candidate_score for evidence in group)
    max_implementation = max(evidence.implementation_score for evidence in group)
    max_completion = max(evidence.completion_score for evidence in group)

    has_completed_signal = any(
        evidence.completion_score >= COMPLETED_COMPLETION_THRESHOLD
        and evidence.implementation_score >= COMPLETED_IMPLEMENTATION_THRESHOLD
        for evidence in group
    )
    has_strong_text_without_code = any(
        evidence.hip_candidate_score >= STRONG_TEXT_ONLY_CONFLICT_THRESHOLD
        and evidence.implementation_score < STRONG_TEXT_ONLY_IMPLEMENTATION_MAX
        and evidence.completion_score < IN_PROGRESS_COMPLETION_THRESHOLD
        for evidence in group
    )

    if has_completed_signal and not has_strong_text_without_code:
        return "completed"
    if has_strong_text_without_code:
        return "conflicting"
    if max_implementation >= IN_PROGRESS_IMPLEMENTATION_THRESHOLD or max_completion >= IN_PROGRESS_COMPLETION_THRESHOLD:
        return "in_progress"
    if max_candidate >= UNKNOWN_CANDIDATE_THRESHOLD:
        return "unknown"
    return "not_started"


def _resolve_confidence(group: list[HipEvidence], status: str) -> str:
    max_candidate = max(evidence.hip_candidate_score for evidence in group)
    max_implementation = max(evidence.implementation_score for evidence in group)
    max_completion = max(evidence.completion_score for evidence in group)

    if status == "completed" and max_completion >= 85:
        return "high"
    if status == "conflicting" and max_candidate >= 80:
        return "high"
    if max_completion >= 55 or max_implementation >= 55:
        return "medium"
    if max_candidate >= 45:
        return "medium"
    return "low"


def _select_supporting_artifacts(group: list[HipEvidence]) -> list[int]:
    scored = sorted(
        group,
        key=lambda evidence: (
            evidence.completion_score,
            evidence.implementation_score,
            evidence.hip_candidate_score,
        ),
        reverse=True,
    )
    selected = [
        evidence.artifact_number
        for evidence in scored
        if evidence.completion_score >= 30
        or evidence.implementation_score >= 30
        or evidence.hip_candidate_score >= 40
    ]
    if not selected and scored:
        selected = [scored[0].artifact_number]
    return list(dict.fromkeys(selected))


def _build_rationale(group: list[HipEvidence], status: str) -> list[str]:
    max_candidate = max(evidence.hip_candidate_score for evidence in group)
    max_implementation = max(evidence.implementation_score for evidence in group)
    max_completion = max(evidence.completion_score for evidence in group)

    if status == "completed":
        rationale = ["strong merged implementation evidence with code and tests"]
    elif status == "in_progress":
        rationale = ["implementation evidence exists but completion evidence remains partial"]
    elif status == "conflicting":
        rationale = ["strong textual HIP references are not backed by equally strong code-change evidence"]
    elif status == "unknown":
        rationale = ["HIP mentions exist, but evidence is too sparse or ambiguous to classify confidently"]
    else:
        rationale = ["no meaningful implementation evidence detected"]

    rationale.extend(
        [
            f"max candidate score: {max_candidate:.0f}",
            f"max implementation score: {max_implementation:.0f}",
            f"max completion score: {max_completion:.0f}",
        ]
    )

    top_evidence = sorted(
        group,
        key=lambda evidence: (
            evidence.completion_score,
            evidence.implementation_score,
            evidence.hip_candidate_score,
        ),
        reverse=True,
    )[:2]
    for evidence in top_evidence:
        rationale.extend(evidence.reasons[:3])

    return _dedupe_preserve_order(rationale)


def aggregate_hip_repo_status(
    evidence_records: list[HipEvidence],
    *,
    artifacts: list[HipArtifact] | None = None,
) -> list[HipRepoStatus]:
    """Aggregate artifact-level HIP evidence into conservative repo-level statuses."""
    artifact_lookup = {}
    if artifacts is not None:
        artifact_lookup = {
            (artifact.repo, artifact.number): artifact
            for artifact in artifacts
        }

    grouped: dict[tuple[str, str], list[HipEvidence]] = defaultdict(list)
    for evidence in evidence_records:
        grouped[(evidence.repo, evidence.hip_id)].append(evidence)

    repo_statuses: list[HipRepoStatus] = []
    for (repo, hip_id), group in sorted(grouped.items()):
        status = _resolve_status(group)
        confidence_level = _resolve_confidence(group, status)
        last_evidence_at = None
        for evidence in group:
            artifact = artifact_lookup.get((repo, evidence.artifact_number))
            if artifact is None:
                continue
            artifact_timestamp = artifact.updated_at or artifact.closed_at or artifact.created_at
            if artifact_timestamp is not None and (last_evidence_at is None or artifact_timestamp > last_evidence_at):
                last_evidence_at = artifact_timestamp

        repo_statuses.append(
            HipRepoStatus(
                repo=repo,
                hip_id=hip_id,
                status=status,
                confidence_level=confidence_level,
                supporting_artifact_numbers=_select_supporting_artifacts(group),
                rationale=_build_rationale(group, status),
                last_evidence_at=last_evidence_at,
            )
        )

    return repo_statuses
