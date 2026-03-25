"""Tests for deterministic HIP scoring behavior."""

from hiero_analytics.analysis.hip_feature_engineering import engineer_hip_feature_vector
from hiero_analytics.analysis.hip_scoring import score_hip_feature_vector
from tests.hip_progression_fixtures import (
    make_candidate,
    make_issue_artifact,
    make_pull_request_artifact,
)


def test_score_strong_implementation_pull_request():
    """Merged source-plus-test work should score as strong implementation evidence."""
    artifact = make_pull_request_artifact()
    candidate = make_candidate(
        artifact,
        matched_sources=["title", "body", "commit_messages"],
    )

    evidence = score_hip_feature_vector(engineer_hip_feature_vector(candidate))

    assert evidence.hip_candidate_score >= 70
    assert evidence.implementation_score >= 80
    assert evidence.completion_score >= 80
    assert evidence.confidence_level == "high"
    assert "merged PR" in evidence.reasons
    assert "source files changed" in evidence.reasons
    assert "unit tests added" in evidence.reasons


def test_score_weak_unblock_pull_request():
    """Preparatory unblock PRs without code should stay weak and low-confidence."""
    artifact = make_pull_request_artifact(
        title="Unblock HIP-1234 release prep",
        body="Prep for HIP-1234 follow-up only.",
        commit_messages_text="chore: unblock HIP-1234",
        merged=False,
        changed_files=[],
        additions=4,
        deletions=1,
        author_association="NONE",
    )
    candidate = make_candidate(
        artifact,
        matched_sources=["title", "body", "commit_messages"],
        negative_context_flags=["unblock", "follow_up", "prep"],
    )

    evidence = score_hip_feature_vector(engineer_hip_feature_vector(candidate))

    assert evidence.hip_candidate_score < 70
    assert evidence.implementation_score < 20
    assert evidence.completion_score < 20
    assert evidence.confidence_level == "low"
    assert "negative context: unblock" in evidence.reasons
    assert "no code-change evidence" in evidence.reasons


def test_score_issue_mention_without_code_stays_weak():
    """Issue-only mentions should not look complete without code evidence."""
    artifact = make_issue_artifact()
    candidate = make_candidate(artifact, matched_sources=["title", "body"])

    evidence = score_hip_feature_vector(engineer_hip_feature_vector(candidate))

    assert evidence.hip_candidate_score >= 60
    assert evidence.implementation_score == 0
    assert evidence.completion_score == 0
    assert evidence.confidence_level == "low"
    assert "no code-change evidence" in evidence.reasons
