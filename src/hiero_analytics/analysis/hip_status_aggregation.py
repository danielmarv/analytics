"""Conservative repository-level aggregation for HIP progression evidence."""

from __future__ import annotations

from collections import defaultdict

from hiero_analytics.config.hip_progression import (
    DEFAULT_HIP_PROGRESSION_CONFIG,
    HipProgressionConfig,
)
from hiero_analytics.domain.hip_progression_models import (
    HipArtifact,
    HipCatalogEntry,
    HipEvidence,
    HipRepoStatus,
    flatten_text,
    hip_sort_key,
)


def _artifact_label(artifact: HipArtifact) -> str:
    prefix = "PR" if artifact.artifact_type == "pull_request" else "Issue"
    return f"{prefix} #{artifact.number}"


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def _cluster_ids(artifacts: list[HipArtifact]) -> dict[tuple[str, int], str]:
    parent: dict[tuple[str, int], tuple[str, int]] = {
        (artifact.repo, artifact.number): (artifact.repo, artifact.number)
        for artifact in artifacts
    }

    def find(key: tuple[str, int]) -> tuple[str, int]:
        parent.setdefault(key, key)
        if parent[key] != key:
            parent[key] = find(parent[key])
        return parent[key]

    def union(left: tuple[str, int], right: tuple[str, int]) -> None:
        if left not in parent or right not in parent:
            return
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    for artifact in artifacts:
        artifact_key = (artifact.repo, artifact.number)
        for linked_number in artifact.linked_artifact_numbers:
            linked_key = (artifact.repo, linked_number)
            if linked_key in parent:
                union(artifact_key, linked_key)

    return {
        key: f"{root[0]}:{root[1]}"
        for key, root in ((key, find(key)) for key in parent)
    }


def _cluster_summary(
    cluster_assessments: list[HipEvidence],
    artifact_lookup: dict[tuple[str, int], HipArtifact],
) -> dict[str, object]:
    evidence_records = list(
        dict.fromkeys(
            evidence_record.fingerprint
            for assessment in cluster_assessments
            for evidence_record in assessment.evidence_records
        )
    )
    evidence_by_type = {
        evidence_record.evidence_type
        for assessment in cluster_assessments
        for evidence_record in assessment.evidence_records
    }
    artifact_numbers = [assessment.artifact_number for assessment in cluster_assessments]
    cluster_artifacts = [
        artifact_lookup[(cluster_assessments[0].repo, number)]
        for number in artifact_numbers
        if (cluster_assessments[0].repo, number) in artifact_lookup
    ]
    has_merged_pr = any(artifact.artifact_type == "pull_request" and artifact.merged for artifact in cluster_artifacts)
    has_tests = any(
        any(record.evidence_type == "test_support" for record in assessment.evidence_records)
        for assessment in cluster_assessments
    )
    has_code = any(
        any(record.evidence_type == "code_pattern" for record in assessment.evidence_records)
        for assessment in cluster_assessments
    )
    has_corroborating_tier4 = any(
        record.evidence_type in {"closed_linked_issue", "docs_update", "changelog_update", "linked_merged_pull_request"}
        for assessment in cluster_assessments
        for record in assessment.evidence_records
    )
    has_strong_negative = any(
        record.evidence_type in {"blocked", "reverted", "follow_up"}
        for assessment in cluster_assessments
        for record in assessment.evidence_records
    )
    top_reasons = _dedupe_preserve_order(
        [reason for assessment in cluster_assessments for reason in assessment.top_reasons]
    )[:6]
    uncertainty_reasons = _dedupe_preserve_order(
        [reason for assessment in cluster_assessments for reason in assessment.uncertainty_reasons]
    )[:6]
    cluster_score = max(assessment.confidence_score for assessment in cluster_assessments)
    if len(cluster_assessments) > 1:
        cluster_score += 5.0
    if any(artifact.artifact_type == "issue" for artifact in cluster_artifacts) and any(
        artifact.artifact_type == "pull_request" for artifact in cluster_artifacts
    ):
        cluster_score += 5.0
    cluster_score = max(0.0, min(100.0, round(cluster_score, 2)))
    return {
        "artifact_numbers": sorted(set(artifact_numbers)),
        "cluster_artifacts": cluster_artifacts,
        "cluster_score": cluster_score,
        "evidence_count": len(evidence_records),
        "has_code": has_code,
        "has_tests": has_tests,
        "has_merged_pr": has_merged_pr,
        "has_corroborating_tier4": has_corroborating_tier4,
        "has_strong_negative": has_strong_negative,
        "evidence_by_type": evidence_by_type,
        "top_reasons": top_reasons,
        "uncertainty_reasons": uncertainty_reasons,
    }


def _rag_label(status: str) -> str:
    if status == "completed":
        return "green"
    if status in {"in_progress", "unknown"}:
        return "amber"
    return "red"


def _resolve_repo_status(cluster_summaries: list[dict[str, object]]) -> str:
    if not cluster_summaries:
        return "not_started"
    has_completed = any(
        summary["has_code"]
        and summary["has_tests"]
        and summary["has_merged_pr"]
        and summary["has_corroborating_tier4"]
        and not summary["has_strong_negative"]
        for summary in cluster_summaries
    )
    has_in_progress = any(summary["has_code"] or summary["has_tests"] for summary in cluster_summaries)
    has_conflict = any(summary["has_strong_negative"] for summary in cluster_summaries) and has_in_progress
    if has_completed and not has_conflict:
        return "completed"
    if has_conflict:
        return "conflicting"
    if has_in_progress:
        return "in_progress"
    if any(summary["evidence_count"] > 0 for summary in cluster_summaries):
        ambiguous_only = all(not summary["has_code"] and not summary["has_tests"] for summary in cluster_summaries)
        return "unknown" if ambiguous_only else "not_started"
    return "not_started"


def _resolve_repo_confidence(
    cluster_summaries: list[dict[str, object]],
    *,
    status: str,
    config: HipProgressionConfig,
) -> tuple[float, str]:
    if not cluster_summaries:
        return (0.0, "low")
    strongest_cluster = max(cluster_summaries, key=lambda summary: float(summary["cluster_score"]))
    score = float(strongest_cluster["cluster_score"])
    if len(cluster_summaries) > 1 and status in {"in_progress", "completed"}:
        score += 5.0
    if status == "completed" and strongest_cluster["has_corroborating_tier4"]:
        score += 10.0
    if strongest_cluster["has_strong_negative"]:
        score -= 15.0
    if status in {"not_started", "unknown"}:
        score = min(score, 54.0)
    score = max(0.0, min(100.0, round(score, 2)))
    level = config.confidence.resolve_level(score)
    if level == "high" and strongest_cluster["has_strong_negative"]:
        level = "medium"
    return score, level


def _supporting_artifacts(
    cluster_summaries: list[dict[str, object]],
    *,
    artifact_lookup: dict[tuple[str, int], HipArtifact],
    repo: str,
    limit: int,
) -> tuple[list[int], list[str]]:
    if not cluster_summaries:
        return ([], [])
    ordered_clusters = sorted(
        cluster_summaries,
        key=lambda summary: (
            float(summary["cluster_score"]),
            bool(summary["has_tests"]),
            bool(summary["has_merged_pr"]),
            bool(summary["has_code"]),
        ),
        reverse=True,
    )
    numbers: list[int] = []
    labels: list[str] = []
    for cluster in ordered_clusters:
        ordered_numbers = sorted(
            cluster["artifact_numbers"],
            key=lambda artifact_number: _artifact_priority(
                artifact_lookup.get((repo, artifact_number))
            ),
            reverse=True,
        )
        for artifact_number in ordered_numbers:
            if artifact_number in numbers:
                continue
            numbers.append(artifact_number)
            artifact = artifact_lookup.get((repo, artifact_number))
            if artifact is not None:
                labels.append(_artifact_label(artifact))
            if len(numbers) >= limit:
                return numbers, labels
    return numbers, labels


def _artifact_priority(artifact: HipArtifact | None) -> tuple[int, int, int]:
    if artifact is None:
        return (0, 0, 0)
    return (
        1 if artifact.artifact_type == "pull_request" else 0,
        1 if artifact.merged else 0,
        artifact.number,
    )


def aggregate_hip_repo_status(
    evidence_records: list[HipEvidence],
    *,
    artifacts: list[HipArtifact] | None = None,
    catalog_entries: list[HipCatalogEntry] | None = None,
    repos: list[str] | None = None,
    config: HipProgressionConfig = DEFAULT_HIP_PROGRESSION_CONFIG,
) -> list[HipRepoStatus]:
    """Aggregate artifact-level HIP evidence into conservative repo-level statuses."""
    artifact_lookup: dict[tuple[str, int], HipArtifact] = {}
    if artifacts is not None:
        artifact_lookup = {
            (artifact.repo, artifact.number): artifact
            for artifact in artifacts
        }

    repo_names = set(repos or [])
    repo_names.update(evidence.repo for evidence in evidence_records)
    repo_names.update(artifact.repo for artifact in artifacts or [])

    cluster_lookup = _cluster_ids(artifacts or [])
    grouped: dict[tuple[str, str], list[HipEvidence]] = defaultdict(list)
    for evidence in evidence_records:
        grouped[(evidence.repo, evidence.hip_id)].append(evidence)

    hip_ids = [entry.hip_id for entry in catalog_entries] if catalog_entries else sorted({evidence.hip_id for evidence in evidence_records}, key=hip_sort_key)
    repo_statuses: list[HipRepoStatus] = []

    for repo in sorted(repo_names):
        for hip_id in sorted(hip_ids, key=hip_sort_key, reverse=True):
            group = grouped.get((repo, hip_id), [])
            clusters: dict[str, list[HipEvidence]] = defaultdict(list)
            for assessment in group:
                cluster_key = cluster_lookup.get((repo, assessment.artifact_number), f"{repo}:{assessment.artifact_number}")
                clusters[cluster_key].append(assessment)
            cluster_summaries = [
                _cluster_summary(cluster_assessments, artifact_lookup)
                for cluster_assessments in clusters.values()
            ]
            status = _resolve_repo_status(cluster_summaries)
            confidence_score, confidence_level = _resolve_repo_confidence(
                cluster_summaries,
                status=status,
                config=config,
            )
            supporting_numbers, top_artifacts = _supporting_artifacts(
                cluster_summaries,
                artifact_lookup=artifact_lookup,
                repo=repo,
                limit=config.status_rules.supporting_artifact_limit,
            )
            top_reasons = _dedupe_preserve_order(
                [
                    reason
                    for summary in sorted(
                        cluster_summaries,
                        key=lambda item: float(item["cluster_score"]),
                        reverse=True,
                    )
                    for reason in summary["top_reasons"]
                ]
            )[:6]
            uncertainty_reasons = _dedupe_preserve_order(
                [
                    reason
                    for summary in sorted(
                        cluster_summaries,
                        key=lambda item: float(item["cluster_score"]),
                        reverse=True,
                    )
                    for reason in summary["uncertainty_reasons"]
                ]
            )[:6]
            if not top_reasons:
                top_reasons = ["No repo evidence found for this official HIP."]
            rationale = _dedupe_preserve_order([*top_reasons, *uncertainty_reasons])
            last_evidence_at = None
            for artifact_number in supporting_numbers:
                artifact = artifact_lookup.get((repo, artifact_number))
                if artifact is None:
                    continue
                timestamp = artifact.activity_timestamp()
                if timestamp is not None and (last_evidence_at is None or timestamp > last_evidence_at):
                    last_evidence_at = timestamp

            repo_statuses.append(
                HipRepoStatus(
                    repo=repo,
                    hip_id=hip_id,
                    status=status,  # type: ignore[arg-type]
                    rag_label=_rag_label(status),
                    confidence_score=confidence_score,
                    confidence_level=confidence_level,  # type: ignore[arg-type]
                    evidence_count=sum(int(summary["evidence_count"]) for summary in cluster_summaries),
                    top_artifacts=top_artifacts,
                    supporting_artifact_numbers=supporting_numbers,
                    top_reasons=top_reasons,
                    uncertainty_reasons=uncertainty_reasons,
                    reviewer_notes="",
                    rationale=rationale,
                    last_evidence_at=last_evidence_at,
                )
            )

    return repo_statuses
