"""Feature engineering for HIP progression candidates."""

from __future__ import annotations

import re

from hiero_analytics.domain.hip_progression_models import (
    HipCandidate,
    HipFeatureVector,
    is_committer_like_author,
    is_maintainer_like_author,
)

FEAT_KEYWORD_PATTERN = re.compile(r"\bfeat(?:ure)?\b", re.IGNORECASE)
IMPLEMENT_KEYWORD_PATTERN = re.compile(
    r"\bimplement(?:ed|ing|s)?\b|\bintroduc(?:e|ed|es|ing)\b|\badd(?:ed|ing|s)?\b",
    re.IGNORECASE,
)
SUPPORT_KEYWORD_PATTERN = re.compile(r"\bsupport(?:ed|ing|s)?\b", re.IGNORECASE)


def engineer_hip_feature_vector(candidate: HipCandidate) -> HipFeatureVector:
    """Build explainable deterministic features for one HIP candidate."""
    artifact = candidate.artifact
    combined_text = "\n\n".join(
        value
        for value in (
            artifact.title,
            artifact.body,
            artifact.comments_text,
            artifact.commit_messages_text,
        )
        if value
    )

    src_files_changed_count = sum(1 for changed_file in artifact.changed_files if changed_file.is_src)
    test_files_changed_count = sum(1 for changed_file in artifact.changed_files if changed_file.is_test)
    integration_test_files_changed_count = sum(
        1 for changed_file in artifact.changed_files if changed_file.is_integration_test
    )
    new_src_files_count = sum(
        1
        for changed_file in artifact.changed_files
        if changed_file.is_src and changed_file.status == "added"
    )
    new_test_files_count = sum(
        1
        for changed_file in artifact.changed_files
        if changed_file.is_test and changed_file.status == "added"
    )

    has_feat_keyword = bool(FEAT_KEYWORD_PATTERN.search(combined_text))
    has_implement_keyword = bool(IMPLEMENT_KEYWORD_PATTERN.search(combined_text))
    has_support_keyword = bool(SUPPORT_KEYWORD_PATTERN.search(combined_text))
    author_is_maintainer_like = is_maintainer_like_author(artifact.author_association)
    author_is_committer_like = is_committer_like_author(artifact.author_association)

    implementation_inputs: list[str] = []
    if src_files_changed_count > 0:
        implementation_inputs.append("source files changed")
    if test_files_changed_count > 0:
        implementation_inputs.append("unit tests changed")
    if integration_test_files_changed_count > 0:
        implementation_inputs.append("integration tests changed")
    if new_src_files_count > 0:
        implementation_inputs.append("source files added")
    if new_test_files_count > 0:
        implementation_inputs.append("unit tests added")
    if artifact.merged:
        implementation_inputs.append("merged pull request")
    if has_feat_keyword:
        implementation_inputs.append("feature keyword present")
    if has_implement_keyword:
        implementation_inputs.append("implementation keyword present")
    if has_support_keyword:
        implementation_inputs.append("support keyword present")
    if author_is_maintainer_like:
        implementation_inputs.append("maintainer-like author")
    elif author_is_committer_like:
        implementation_inputs.append("committer-like author")
    implementation_inputs.extend(
        f"negative context: {flag}"
        for flag in candidate.negative_context_flags
    )

    return HipFeatureVector(
        repo=artifact.repo,
        artifact_type=artifact.artifact_type,
        artifact_number=artifact.number,
        hip_id=candidate.hip_id,
        extraction_source=candidate.extraction_source,
        text_match_reason=candidate.text_match_reason,
        explicit_hip_mention=True,
        hip_in_title="title" in candidate.matched_sources,
        hip_in_body="body" in candidate.matched_sources,
        hip_in_comments="comments" in candidate.matched_sources,
        hip_in_commit_messages="commit_messages" in candidate.matched_sources,
        negative_phrase_unblock="unblock" in candidate.negative_context_flags,
        negative_phrase_blocked="blocked" in candidate.negative_context_flags,
        negative_phrase_follow_up="follow_up" in candidate.negative_context_flags,
        negative_phrase_prep="prep" in candidate.negative_context_flags,
        negative_phrase_refactor_only="refactor_only" in candidate.negative_context_flags,
        negative_phrase_cleanup_only="cleanup_only" in candidate.negative_context_flags,
        has_feat_keyword=has_feat_keyword,
        has_implement_keyword=has_implement_keyword,
        has_support_keyword=has_support_keyword,
        src_files_changed_count=src_files_changed_count,
        test_files_changed_count=test_files_changed_count,
        integration_test_files_changed_count=integration_test_files_changed_count,
        new_src_files_count=new_src_files_count,
        new_test_files_count=new_test_files_count,
        total_additions=artifact.additions,
        total_deletions=artifact.deletions,
        merged=artifact.merged,
        author_is_maintainer_like=author_is_maintainer_like,
        author_is_committer_like=author_is_committer_like,
        negative_context_flags=list(candidate.negative_context_flags),
        implementation_score_inputs=implementation_inputs,
    )


def engineer_hip_feature_vectors(candidates: list[HipCandidate]) -> list[HipFeatureVector]:
    """Build feature vectors for a list of HIP candidates."""
    return [engineer_hip_feature_vector(candidate) for candidate in candidates]
