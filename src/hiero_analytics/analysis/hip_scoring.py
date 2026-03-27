"""Deterministic status inference for HIP progression evidence."""

from __future__ import annotations

from hiero_analytics.config.hip_progression import (
    DEFAULT_HIP_PROGRESSION_CONFIG,
    HipProgressionConfig,
)
from hiero_analytics.domain.hip_progression_models import (
    ArtifactHipAssessment,
    ConfidenceBreakdown,
    HipFeatureVector,
)


def _clamp_score(score: float) -> float:
    return max(0.0, min(100.0, round(score, 2)))


def _positive_tier_count(feature_vector: HipFeatureVector) -> int:
    return sum(
        1
        for value in [
            feature_vector.tier_1_count,
            feature_vector.tier_2_count,
            feature_vector.tier_3_count,
            feature_vector.tier_4_count,
        ]
        if value > 0
    )


def _build_confidence_breakdown(
    feature_vector: HipFeatureVector,
    *,
    config: HipProgressionConfig,
) -> ConfidenceBreakdown:
    positive_total = sum(
        evidence.confidence_contribution
        for evidence in feature_vector.evidence_records
        if evidence.polarity == "positive"
    )
    negative_total = abs(
        sum(
            evidence.confidence_contribution
            for evidence in feature_vector.evidence_records
            if evidence.polarity == "negative"
        )
    )
    score = positive_total - negative_total
    if _positive_tier_count(feature_vector) >= 3:
        score += config.weights.multi_tier_agreement_bonus
    if feature_vector.positive_evidence_count > 0 and feature_vector.negative_evidence_count > 0:
        score -= config.weights.contradiction_penalty
    score = _clamp_score(score)
    top_reasons = [
        evidence.short_rationale
        for evidence in sorted(
            [evidence for evidence in feature_vector.evidence_records if evidence.polarity == "positive"],
            key=lambda evidence: evidence.confidence_contribution,
            reverse=True,
        )[:4]
    ]
    uncertainty_reasons = list(
        dict.fromkeys(
            reason
            for evidence in feature_vector.evidence_records
            for reason in evidence.uncertainty_reasons
        )
    )
    if not feature_vector.has_code_evidence:
        uncertainty_reasons.append("No implementation file changes were detected.")
    if feature_vector.has_code_evidence and not feature_vector.has_test_evidence:
        uncertainty_reasons.append("Implementation exists, but test corroboration is missing.")
    if not feature_vector.merged and feature_vector.has_code_evidence:
        uncertainty_reasons.append("Code evidence is not merged yet.")
    if feature_vector.has_docs_only_change:
        uncertainty_reasons.append("Docs-only changes do not prove implementation.")
    uncertainty_reasons = list(dict.fromkeys(uncertainty_reasons))

    level = config.confidence.resolve_level(score)
    if level == "high":
        if (
            _positive_tier_count(feature_vector) < config.confidence.high_min_positive_tiers
            or feature_vector.negative_evidence_count > config.confidence.high_max_negative_evidence
        ):
            level = "medium"

    return ConfidenceBreakdown(
        confidence_score=score,
        confidence_level=level,
        top_reasons=list(dict.fromkeys(top_reasons)),
        uncertainty_reasons=uncertainty_reasons,
    )


def _progress_stage(feature_vector: HipFeatureVector) -> str:
    if feature_vector.has_code_evidence and feature_vector.has_test_evidence and feature_vector.merged:
        return "implementation_with_merge_and_tests"
    if feature_vector.has_code_evidence and feature_vector.has_test_evidence:
        return "implementation_with_tests"
    if feature_vector.has_code_evidence:
        return "partial_implementation"
    if feature_vector.has_test_evidence:
        return "tests_only"
    if (
        feature_vector.has_negative_blocked
        or feature_vector.has_negative_follow_up
        or feature_vector.has_negative_prep
        or feature_vector.has_negative_refactor_only
        or feature_vector.has_negative_cleanup_only
    ):
        return "planning_or_follow_up"
    if feature_vector.direct_mention_count > 0:
        return "mention_only"
    return "ambiguous"


def _infer_artifact_status(
    feature_vector: HipFeatureVector,
    *,
    confidence_score: float,
    config: HipProgressionConfig,
) -> str:
    strong_contradiction = (
        feature_vector.has_negative_blocked
        or feature_vector.has_negative_reverted
        or feature_vector.has_negative_follow_up
    )
    if strong_contradiction and (feature_vector.has_code_evidence or feature_vector.has_test_evidence or feature_vector.merged):
        return "conflicting"
    if (
        feature_vector.has_code_evidence
        and feature_vector.has_test_evidence
        and feature_vector.merged
        and feature_vector.tier_4_count > 0
        and confidence_score >= config.status_rules.completed_min_score
        and not strong_contradiction
    ):
        return "completed"
    if feature_vector.has_code_evidence or feature_vector.has_test_evidence:
        return "in_progress"
    if feature_vector.direct_mention_count > 0:
        if (
            feature_vector.has_negative_blocked
            or feature_vector.has_negative_follow_up
            or feature_vector.has_negative_prep
            or feature_vector.has_negative_refactor_only
            or feature_vector.has_negative_cleanup_only
        ):
            return "not_started"
        if feature_vector.bot_mention_count > 0 or feature_vector.has_docs_only_change:
            return "unknown"
        return "not_started"
    if confidence_score >= config.status_rules.unknown_min_score:
        return "unknown"
    return "not_started"


def score_hip_feature_vector(
    feature_vector: HipFeatureVector,
    *,
    config: HipProgressionConfig = DEFAULT_HIP_PROGRESSION_CONFIG,
) -> ArtifactHipAssessment:
    """Score one feature vector into an explainable artifact-level HIP assessment."""
    confidence = _build_confidence_breakdown(feature_vector, config=config)
    status = _infer_artifact_status(
        feature_vector,
        confidence_score=confidence.confidence_score,
        config=config,
    )
    return ArtifactHipAssessment(
        repo=feature_vector.repo,
        hip_id=feature_vector.hip_id,
        artifact_type=feature_vector.artifact_type,
        artifact_number=feature_vector.artifact_number,
        status=status,  # type: ignore[arg-type]
        progress_stage=_progress_stage(feature_vector),
        confidence_score=confidence.confidence_score,
        confidence_level=confidence.confidence_level,
        evidence_count=feature_vector.evidence_count,
        positive_evidence_count=feature_vector.positive_evidence_count,
        negative_evidence_count=feature_vector.negative_evidence_count,
        evidence_records=feature_vector.evidence_records,
        top_reasons=confidence.top_reasons,
        uncertainty_reasons=confidence.uncertainty_reasons,
    )


def score_hip_feature_vectors(
    feature_vectors: list[HipFeatureVector],
    *,
    config: HipProgressionConfig = DEFAULT_HIP_PROGRESSION_CONFIG,
) -> list[ArtifactHipAssessment]:
    """Score a batch of HIP feature vectors."""
    return [
        score_hip_feature_vector(
            feature_vector,
            config=config,
        )
        for feature_vector in feature_vectors
    ]
