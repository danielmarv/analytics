"""Tests for HIP repo-status aggregation."""

from hiero_analytics.analysis.hip_feature_engineering import engineer_hip_feature_vector
from hiero_analytics.analysis.hip_scoring import score_hip_feature_vector
from hiero_analytics.analysis.hip_status_aggregation import aggregate_hip_repo_status
from tests.hip_progression_fixtures import (
    make_candidate,
    make_issue_artifact,
    make_pull_request_artifact,
)


def test_aggregation_marks_completed_when_strong_merged_code_and_tests_exist():
    """Multiple supporting artifacts should aggregate to completed when one is clearly strong."""
    strong_pr = make_pull_request_artifact(number=201)
    follow_up_pr = make_pull_request_artifact(
        number=202,
        title="Follow-up for HIP-1234",
        body="Additional support for HIP-1234",
        additions=30,
        deletions=5,
    )
    evidence_records = [
        score_hip_feature_vector(engineer_hip_feature_vector(make_candidate(strong_pr))),
        score_hip_feature_vector(engineer_hip_feature_vector(make_candidate(follow_up_pr))),
    ]

    repo_statuses = aggregate_hip_repo_status(evidence_records, artifacts=[strong_pr, follow_up_pr])

    assert len(repo_statuses) == 1
    repo_status = repo_statuses[0]
    assert repo_status.status == "completed"
    assert repo_status.confidence_level == "high"
    assert repo_status.supporting_artifact_numbers == [201, 202]
    assert repo_status.last_evidence_at == follow_up_pr.updated_at


def test_aggregation_marks_unknown_for_issue_only_mentions():
    """Text-only issue evidence should remain unknown rather than complete."""
    issue = make_issue_artifact(number=301)
    evidence_records = [
        score_hip_feature_vector(engineer_hip_feature_vector(make_candidate(issue))),
    ]

    repo_statuses = aggregate_hip_repo_status(evidence_records, artifacts=[issue])

    assert repo_statuses[0].status == "unknown"
    assert repo_statuses[0].confidence_level == "low"


def test_aggregation_marks_conflicting_for_strong_text_without_code():
    """Strong textual claims without code support should be treated as conflicting."""
    issue = make_issue_artifact(
        number=401,
        title="HIP-1234 implementation complete",
        body="HIP-1234 is done and HIP-1234 support is available now.",
        comments_text="HIP-1234 shipped.",
    )
    evidence_records = [
        score_hip_feature_vector(
            engineer_hip_feature_vector(
                make_candidate(issue, matched_sources=["title", "body", "comments"])
            )
        )
    ]

    repo_statuses = aggregate_hip_repo_status(evidence_records, artifacts=[issue])

    assert repo_statuses[0].status == "conflicting"
    assert repo_statuses[0].confidence_level == "low"
    assert "strong textual HIP references" in repo_statuses[0].rationale[0]


def test_aggregation_marks_linked_maintainer_issue_and_pr_as_high_confidence():
    """Linked issue-plus-PR HIP evidence should elevate confidence when a maintainer merged the PR."""
    issue = make_issue_artifact(
        number=501,
        title="Track HIP-1234 support",
        body="Tracking issue for HIP-1234 implementation.",
    )
    pr = make_pull_request_artifact(
        number=502,
        title="Implement HIP-1234 support",
        body="Closes #501 and adds full HIP-1234 support.",
        author_association="MEMBER",
    )
    evidence_records = [
        score_hip_feature_vector(engineer_hip_feature_vector(make_candidate(issue))),
        score_hip_feature_vector(engineer_hip_feature_vector(make_candidate(pr))),
    ]

    repo_statuses = aggregate_hip_repo_status(evidence_records, artifacts=[issue, pr])

    assert repo_statuses[0].status == "completed"
    assert repo_statuses[0].confidence_level == "high"
    assert "linked issue and PR evidence reinforce the same HIP" in repo_statuses[0].rationale
