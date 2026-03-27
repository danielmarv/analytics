"""Tests for deterministic HIP scoring behavior."""

from hiero_analytics.analysis.hip_feature_engineering import engineer_hip_feature_vector
from hiero_analytics.analysis.hip_scoring import score_hip_feature_vector
from hiero_analytics.domain.hip_progression_models import build_changed_file
from tests.hip_progression_fixtures import make_candidate, make_issue_artifact, make_pull_request_artifact


def test_score_strong_completed_pull_request_requires_completion_corroboration():
    """Merged source-plus-test work with changelog evidence should score as completed."""
    artifact = make_pull_request_artifact(
        changed_files=[
            build_changed_file("src/client/hip1234.ts", additions=80, deletions=5, status="added"),
            build_changed_file("tests/unit/hip1234.test.ts", additions=25, deletions=0, status="added"),
            build_changed_file("CHANGELOG.md", additions=4, deletions=0, status="modified"),
        ],
    )
    candidate = make_candidate(
        artifact,
        matched_sources=["title", "body", "commit_message"],
    )

    evidence = score_hip_feature_vector(engineer_hip_feature_vector(candidate))

    assert evidence.status == "completed"
    assert evidence.confidence_level == "high"
    assert evidence.confidence_score >= 80
    assert "Tests changed in PR #101." in evidence.top_reasons


def test_score_weak_unblock_pull_request_stays_not_started():
    """Preparatory follow-up PRs without code should stay weak and low-confidence."""
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
        matched_sources=["title", "body", "commit_message"],
        negative_context_flags=["follow_up", "prep"],
    )

    evidence = score_hip_feature_vector(engineer_hip_feature_vector(candidate))

    assert evidence.status == "not_started"
    assert evidence.confidence_level == "low"
    assert "No implementation file changes were detected." in evidence.uncertainty_reasons


def test_score_issue_with_bot_only_mention_becomes_unknown():
    """Bot-only comment evidence should remain ambiguous rather than progress."""
    artifact = make_issue_artifact(
        title="Workflow issue",
        body="General workflow issue with no direct plan.",
        comments_text="WorkflowBot: HIP-1234 cannot be merged until checks pass.",
        author_association="NONE",
    )
    candidate = make_candidate(artifact, matched_sources=["issue_comment"])
    candidate.mentions[0].source_kind = "issue_comment"
    candidate.mentions[0].is_bot = True

    evidence = score_hip_feature_vector(engineer_hip_feature_vector(candidate))

    assert evidence.status == "unknown"
    assert evidence.confidence_level == "low"
