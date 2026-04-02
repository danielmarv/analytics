"""Tests for deterministic HIP scoring behavior."""

from hiero_analytics.analysis.hip_scoring import score_candidate
from hiero_analytics.domain.hip_progression_models import build_changed_file
from tests.hip_progression_fixtures import make_candidate, make_issue_artifact, make_pull_request_artifact


def test_score_merged_pr_with_code_and_tests_is_completed_high():
    """Merged PR with source + test changes from a committer should score completed/high."""
    artifact = make_pull_request_artifact(
        changed_files=[
            build_changed_file("src/client/hip1234.ts", additions=80, deletions=5, status="added"),
            build_changed_file("tests/unit/hip1234.test.ts", additions=25, deletions=0, status="added"),
        ],
    )
    candidate = make_candidate(artifact)

    assessment = score_candidate(candidate)

    assert assessment.status == "completed"
    assert assessment.confidence == "high"
    assert assessment.has_code is True
    assert assessment.has_tests is True
    assert assessment.merged is True


def test_score_unmerged_pr_with_code_is_in_progress():
    """Open PR with code changes should be in_progress."""
    artifact = make_pull_request_artifact(
        merged=False,
        changed_files=[
            build_changed_file("src/client/hip1234.ts", additions=20, deletions=0, status="added"),
        ],
        author_association="NONE",
    )
    candidate = make_candidate(artifact)

    assessment = score_candidate(candidate)

    assert assessment.status == "in_progress"
    assert assessment.confidence == "low"


def test_score_pr_without_code_is_raised():
    """PR without code or test changes should be raised."""
    artifact = make_pull_request_artifact(
        merged=False,
        changed_files=[],
        additions=4,
        deletions=1,
        author_association="NONE",
    )
    candidate = make_candidate(artifact)

    assessment = score_candidate(candidate)

    assert assessment.status == "raised"
    assert assessment.confidence == "low"


def test_score_issue_is_always_raised():
    """Issues should always score as raised."""
    artifact = make_issue_artifact()
    candidate = make_candidate(artifact)

    assessment = score_candidate(candidate)

    assert assessment.status == "raised"
