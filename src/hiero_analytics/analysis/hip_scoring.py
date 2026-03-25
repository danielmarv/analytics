"""Deterministic scoring for HIP progression evidence."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from hiero_analytics.domain.hip_progression_models import (
    ConfidenceLevel,
    HipEvidence,
    HipFeatureVector,
)

NEGATIVE_FLAG_PENALTIES = {
    "unblock": 12.0,
    "blocked": 18.0,
    "follow_up": 10.0,
    "prep": 10.0,
    "refactor_only": 15.0,
    "cleanup_only": 15.0,
}


@dataclass(frozen=True, slots=True)
class HipScoringConfig:
    """Tunable deterministic weights for the HIP progression pilot."""

    candidate_explicit_mention: float = 20.0
    candidate_title_bonus: float = 30.0
    candidate_body_bonus: float = 15.0
    candidate_comments_bonus: float = 10.0
    candidate_commit_bonus: float = 10.0
    implementation_src_bonus: float = 30.0
    implementation_large_src_bonus: float = 10.0
    implementation_new_src_bonus: float = 10.0
    implementation_test_bonus: float = 15.0
    implementation_integration_bonus: float = 10.0
    implementation_new_test_bonus: float = 10.0
    implementation_merged_bonus: float = 20.0
    implementation_keyword_bonus: float = 10.0
    implementation_support_bonus: float = 8.0
    implementation_feat_bonus: float = 6.0
    implementation_maintainer_bonus: float = 8.0
    implementation_committer_bonus: float = 5.0
    implementation_large_diff_bonus: float = 10.0
    implementation_no_code_penalty: float = 20.0
    implementation_issue_without_code_penalty: float = 15.0
    completion_merged_bonus: float = 30.0
    completion_src_bonus: float = 25.0
    completion_test_bonus: float = 20.0
    completion_integration_bonus: float = 10.0
    completion_new_src_bonus: float = 8.0
    completion_new_test_bonus: float = 8.0
    completion_keyword_bonus: float = 6.0
    completion_support_bonus: float = 6.0
    completion_maintainer_bonus: float = 4.0
    completion_committer_bonus: float = 2.0
    completion_missing_tests_penalty: float = 15.0
    completion_not_merged_penalty: float = 20.0
    completion_no_code_penalty: float = 25.0
    completion_issue_without_code_penalty: float = 20.0


DEFAULT_SCORING_CONFIG = HipScoringConfig()
ExtraSignalHook = Callable[[HipFeatureVector], tuple[float, float, float, list[str]] | None]


def _clamp_score(score: float) -> float:
    return max(0.0, min(100.0, round(score, 2)))


def _add_reason(reasons: list[str], reason: str) -> None:
    if reason not in reasons:
        reasons.append(reason)


def _apply_negative_penalties(
    score: float,
    reasons: list[str],
    negative_context_flags: list[str],
) -> float:
    for flag in negative_context_flags:
        penalty = NEGATIVE_FLAG_PENALTIES.get(flag, 0.0)
        if penalty <= 0:
            continue
        score -= penalty
        _add_reason(reasons, f"negative context: {flag.replace('_', ' ')}")
    return score


def _score_candidate(
    feature_vector: HipFeatureVector,
    config: HipScoringConfig,
) -> tuple[float, list[str]]:
    score = 0.0
    reasons: list[str] = []

    if feature_vector.explicit_hip_mention:
        score += config.candidate_explicit_mention
    if feature_vector.hip_in_title:
        score += config.candidate_title_bonus
        _add_reason(reasons, "explicit HIP mention in PR title" if feature_vector.artifact_type == "pull_request" else "explicit HIP mention in issue title")
    if feature_vector.hip_in_body:
        score += config.candidate_body_bonus
        _add_reason(reasons, "explicit HIP mention in body")
    if feature_vector.hip_in_comments:
        score += config.candidate_comments_bonus
        _add_reason(reasons, "explicit HIP mention in comments")
    if feature_vector.hip_in_commit_messages:
        score += config.candidate_commit_bonus
        _add_reason(reasons, "explicit HIP mention in commit messages")

    score = _apply_negative_penalties(score, reasons, feature_vector.negative_context_flags)
    return _clamp_score(score), reasons


def _score_implementation(
    feature_vector: HipFeatureVector,
    config: HipScoringConfig,
) -> tuple[float, list[str]]:
    score = 0.0
    reasons: list[str] = []
    has_code_evidence = (
        feature_vector.src_files_changed_count > 0
        or feature_vector.test_files_changed_count > 0
        or feature_vector.integration_test_files_changed_count > 0
    )

    if feature_vector.src_files_changed_count > 0:
        score += config.implementation_src_bonus
        _add_reason(reasons, "source files changed")
    if feature_vector.src_files_changed_count >= 3:
        score += config.implementation_large_src_bonus
    if feature_vector.new_src_files_count > 0:
        score += config.implementation_new_src_bonus
        _add_reason(reasons, "source files added")
    if feature_vector.test_files_changed_count > 0:
        score += config.implementation_test_bonus
        _add_reason(reasons, "unit tests changed")
    if feature_vector.integration_test_files_changed_count > 0:
        score += config.implementation_integration_bonus
        _add_reason(reasons, "integration tests changed")
    if feature_vector.new_test_files_count > 0:
        score += config.implementation_new_test_bonus
        _add_reason(reasons, "unit tests added")
    if feature_vector.merged:
        score += config.implementation_merged_bonus
        _add_reason(reasons, "merged PR")
    if feature_vector.has_implement_keyword:
        score += config.implementation_keyword_bonus
        _add_reason(reasons, "implementation keyword present")
    if feature_vector.has_support_keyword:
        score += config.implementation_support_bonus
        _add_reason(reasons, "support keyword present")
    if feature_vector.has_feat_keyword:
        score += config.implementation_feat_bonus
        _add_reason(reasons, "feature keyword present")
    if feature_vector.author_is_maintainer_like:
        score += config.implementation_maintainer_bonus
        _add_reason(reasons, "maintainer-like author")
    elif feature_vector.author_is_committer_like:
        score += config.implementation_committer_bonus
        _add_reason(reasons, "committer-like author")

    if feature_vector.total_additions + feature_vector.total_deletions >= 50:
        score += config.implementation_large_diff_bonus

    if not has_code_evidence:
        score -= config.implementation_no_code_penalty
        _add_reason(reasons, "no code-change evidence")

    if feature_vector.artifact_type == "issue" and not has_code_evidence:
        score -= config.implementation_issue_without_code_penalty

    score = _apply_negative_penalties(score, reasons, feature_vector.negative_context_flags)
    return _clamp_score(score), reasons


def _score_completion(
    feature_vector: HipFeatureVector,
    config: HipScoringConfig,
) -> tuple[float, list[str]]:
    score = 0.0
    reasons: list[str] = []
    has_src = feature_vector.src_files_changed_count > 0
    has_tests = feature_vector.test_files_changed_count > 0
    has_code_evidence = has_src or has_tests or feature_vector.integration_test_files_changed_count > 0

    if feature_vector.merged:
        score += config.completion_merged_bonus
        _add_reason(reasons, "merged PR")
    else:
        score -= config.completion_not_merged_penalty

    if has_src:
        score += config.completion_src_bonus
        _add_reason(reasons, "source files changed")
    if has_tests:
        score += config.completion_test_bonus
        _add_reason(reasons, "unit tests changed")
    if feature_vector.integration_test_files_changed_count > 0:
        score += config.completion_integration_bonus
        _add_reason(reasons, "integration tests changed")
    if feature_vector.new_src_files_count > 0:
        score += config.completion_new_src_bonus
        _add_reason(reasons, "source files added")
    if feature_vector.new_test_files_count > 0:
        score += config.completion_new_test_bonus
        _add_reason(reasons, "unit tests added")
    if feature_vector.has_implement_keyword:
        score += config.completion_keyword_bonus
    if feature_vector.has_support_keyword:
        score += config.completion_support_bonus
    if feature_vector.author_is_maintainer_like:
        score += config.completion_maintainer_bonus
    elif feature_vector.author_is_committer_like:
        score += config.completion_committer_bonus

    if feature_vector.merged and has_src and not has_tests:
        score -= config.completion_missing_tests_penalty
        _add_reason(reasons, "missing test evidence")

    if not has_code_evidence:
        score -= config.completion_no_code_penalty
        _add_reason(reasons, "no code-change evidence")

    if feature_vector.artifact_type == "issue" and not has_code_evidence:
        score -= config.completion_issue_without_code_penalty

    score = _apply_negative_penalties(score, reasons, feature_vector.negative_context_flags)
    return _clamp_score(score), reasons


def _resolve_confidence_level(
    candidate_score: float,
    implementation_score: float,
    completion_score: float,
) -> ConfidenceLevel:
    if completion_score >= 75 or (candidate_score >= 60 and implementation_score >= 60):
        return "high"
    if implementation_score >= 35 or (candidate_score >= 70 and implementation_score >= 20):
        return "medium"
    return "low"


def score_hip_feature_vector(
    feature_vector: HipFeatureVector,
    *,
    config: HipScoringConfig = DEFAULT_SCORING_CONFIG,
    extra_signal_hook: ExtraSignalHook | None = None,
) -> HipEvidence:
    """Score one feature vector into explainable HIP evidence."""
    candidate_score, candidate_reasons = _score_candidate(feature_vector, config)
    implementation_score, implementation_reasons = _score_implementation(feature_vector, config)
    completion_score, completion_reasons = _score_completion(feature_vector, config)

    reasons = []
    for reason in [*candidate_reasons, *implementation_reasons, *completion_reasons]:
        _add_reason(reasons, reason)

    if extra_signal_hook is not None:
        extra_scores = extra_signal_hook(feature_vector)
        if extra_scores is not None:
            candidate_delta, implementation_delta, completion_delta, extra_reasons = extra_scores
            candidate_score = _clamp_score(candidate_score + candidate_delta)
            implementation_score = _clamp_score(implementation_score + implementation_delta)
            completion_score = _clamp_score(completion_score + completion_delta)
            for reason in extra_reasons:
                _add_reason(reasons, reason)

    confidence_level = _resolve_confidence_level(
        candidate_score,
        implementation_score,
        completion_score,
    )

    return HipEvidence(
        repo=feature_vector.repo,
        hip_id=feature_vector.hip_id,
        artifact_type=feature_vector.artifact_type,
        artifact_number=feature_vector.artifact_number,
        hip_candidate_score=candidate_score,
        implementation_score=implementation_score,
        completion_score=completion_score,
        confidence_level=confidence_level,
        reasons=reasons,
    )


def score_hip_feature_vectors(
    feature_vectors: list[HipFeatureVector],
    *,
    config: HipScoringConfig = DEFAULT_SCORING_CONFIG,
    extra_signal_hook: ExtraSignalHook | None = None,
) -> list[HipEvidence]:
    """Score a batch of HIP feature vectors."""
    return [
        score_hip_feature_vector(
            feature_vector,
            config=config,
            extra_signal_hook=extra_signal_hook,
        )
        for feature_vector in feature_vectors
    ]
