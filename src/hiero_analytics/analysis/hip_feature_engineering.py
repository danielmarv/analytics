"""Feature engineering for HIP progression candidates."""

from __future__ import annotations

from collections import Counter

from hiero_analytics.analysis.hip_evidence_collection import collect_artifact_evidence
from hiero_analytics.config.hip_progression import (
    DEFAULT_HIP_PROGRESSION_CONFIG,
    HipProgressionConfig,
)
from hiero_analytics.domain.hip_progression_models import (
    HipArtifact,
    HipCandidate,
    HipFeatureVector,
)


def engineer_hip_feature_vector(
    candidate: HipCandidate,
    *,
    artifact_lookup: dict[tuple[str, int], HipArtifact] | None = None,
    config: HipProgressionConfig = DEFAULT_HIP_PROGRESSION_CONFIG,
) -> HipFeatureVector:
    """Build explainable deterministic features for one HIP candidate."""
    artifact = candidate.artifact
    evidence_records = collect_artifact_evidence(
        candidate,
        artifact_lookup=artifact_lookup,
        config=config,
    )
    tier_counts = Counter(evidence.evidence_tier for evidence in evidence_records)
    polarity_counts = Counter(evidence.polarity for evidence in evidence_records)
    src_files = [changed_file for changed_file in artifact.changed_files if changed_file.is_src]
    test_files = [changed_file for changed_file in artifact.changed_files if changed_file.is_test]
    integration_tests = [
        changed_file for changed_file in artifact.changed_files if changed_file.is_integration_test
    ]
    docs_files = [changed_file for changed_file in artifact.changed_files if changed_file.is_docs]
    changelog_files = [changed_file for changed_file in artifact.changed_files if changed_file.is_changelog]
    top_evidence_types = [
        evidence.evidence_type
        for evidence in sorted(
            evidence_records,
            key=lambda evidence: abs(evidence.confidence_contribution),
            reverse=True,
        )[:5]
    ]

    return HipFeatureVector(
        repo=artifact.repo,
        hip_id=candidate.hip_id,
        artifact_type=artifact.artifact_type,
        artifact_number=artifact.number,
        evidence_count=len(evidence_records),
        positive_evidence_count=int(polarity_counts["positive"]),
        negative_evidence_count=int(polarity_counts["negative"]),
        tier_1_count=int(tier_counts["tier_1"]),
        tier_2_count=int(tier_counts["tier_2"]),
        tier_3_count=int(tier_counts["tier_3"]),
        tier_4_count=int(tier_counts["tier_4"]),
        tier_5_count=int(tier_counts["tier_5"]),
        direct_mention_count=sum(
            1 for mention in candidate.mentions if mention.is_explicit_match and mention.source_kind != "linked_artifact"
        ),
        semantic_phrase_count=sum(1 for mention in candidate.mentions if mention.is_semantic_match),
        propagated_mention_count=sum(1 for mention in candidate.mentions if mention.source_kind == "linked_artifact"),
        bot_mention_count=sum(1 for mention in candidate.mentions if mention.is_bot),
        src_files_changed_count=len(src_files),
        test_files_changed_count=len(test_files),
        integration_test_files_changed_count=len(integration_tests),
        docs_files_changed_count=len(docs_files),
        changelog_files_changed_count=len(changelog_files),
        new_src_files_count=sum(1 for changed_file in src_files if changed_file.status == "added"),
        new_test_files_count=sum(1 for changed_file in test_files if changed_file.status == "added"),
        merged=artifact.merged,
        linked_artifact_numbers=list(dict.fromkeys(candidate.linked_artifact_numbers)),
        has_direct_reference=any(
            mention.is_explicit_match and mention.source_kind != "linked_artifact"
            for mention in candidate.mentions
        ),
        has_code_evidence=bool(src_files),
        has_test_evidence=bool(test_files),
        has_docs_only_change=bool(docs_files) and not src_files and not test_files,
        has_changelog_update=bool(changelog_files),
        has_negative_blocked="blocked" in candidate.negative_context_flags,
        has_negative_follow_up="follow_up" in candidate.negative_context_flags,
        has_negative_prep="prep" in candidate.negative_context_flags,
        has_negative_refactor_only="refactor_only" in candidate.negative_context_flags,
        has_negative_cleanup_only="cleanup_only" in candidate.negative_context_flags,
        has_negative_reverted="reverted" in candidate.negative_context_flags,
        evidence_records=evidence_records,
        top_evidence_types=list(dict.fromkeys(top_evidence_types)),
    )


def engineer_hip_feature_vectors(
    candidates: list[HipCandidate],
    *,
    artifacts: list[HipArtifact] | None = None,
    config: HipProgressionConfig = DEFAULT_HIP_PROGRESSION_CONFIG,
) -> list[HipFeatureVector]:
    """Build feature vectors for a list of HIP candidates."""
    artifact_lookup = {}
    if artifacts is not None:
        artifact_lookup = {(artifact.repo, artifact.number): artifact for artifact in artifacts}
    return [
        engineer_hip_feature_vector(
            candidate,
            artifact_lookup=artifact_lookup,
            config=config,
        )
        for candidate in candidates
    ]
