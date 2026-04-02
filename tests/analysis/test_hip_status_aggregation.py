"""Tests for HIP repo-status aggregation."""

from hiero_analytics.analysis.hip_scoring import score_candidate
from hiero_analytics.analysis.hip_status_aggregation import aggregate_hip_repo_status
from hiero_analytics.domain.hip_progression_models import build_changed_file
from tests.hip_progression_fixtures import (
    make_candidate,
    make_catalog_entries,
    make_issue_artifact,
    make_pull_request_artifact,
)


def test_aggregation_marks_completed_when_merged_code_and_tests():
    """Repo status should become completed when a merged PR has code + tests."""
    issue = make_issue_artifact(number=201, title="Track HIP-1234 support", body="Tracking HIP-1234.", state="closed", linked_artifact_numbers=[202])
    strong_pr = make_pull_request_artifact(
        number=202,
        body="Closes #201 and completes HIP-1234 compliance.",
        linked_artifact_numbers=[201],
        changed_files=[
            build_changed_file("src/client/hip1234.ts", additions=80, deletions=5, status="added"),
            build_changed_file("tests/unit/hip1234.test.ts", additions=25, deletions=0, status="added"),
        ],
    )
    assessments = [
        score_candidate(make_candidate(issue)),
        score_candidate(make_candidate(strong_pr)),
    ]

    repo_statuses = aggregate_hip_repo_status(
        assessments,
        artifacts=[issue, strong_pr],
        catalog_entries=make_catalog_entries("HIP-1234"),
        repos=["hiero-ledger/hiero-sdk-js"],
    )

    assert len(repo_statuses) == 1
    repo_status = repo_statuses[0]
    assert repo_status.status == "completed"
    assert repo_status.confidence == "high"
    assert 202 in repo_status.supporting_artifact_numbers


def test_aggregation_marks_not_raised_for_catalog_hip_without_evidence():
    """Official HIPs with no repo evidence should still emit not_raised rows."""
    repo_statuses = aggregate_hip_repo_status(
        [],
        artifacts=[],
        catalog_entries=make_catalog_entries("HIP-1234"),
        repos=["hiero-ledger/hiero-sdk-js"],
    )

    assert repo_statuses[0].status == "not_raised"
    assert repo_statuses[0].confidence == "low"


def test_aggregation_picks_best_status_across_artifacts():
    """Best status wins when multiple artifacts reference the same HIP."""
    issue = make_issue_artifact(number=401, title="Track HIP-1234", body="HIP-1234 tracking issue.")
    pr = make_pull_request_artifact(
        number=402,
        title="Implement HIP-1234",
        body="HIP-1234 implementation.",
        changed_files=[
            build_changed_file("src/client/hip1234.ts", additions=80, deletions=5, status="added"),
        ],
        merged=False,
    )
    assessments = [
        score_candidate(make_candidate(issue)),
        score_candidate(make_candidate(pr)),
    ]

    repo_statuses = aggregate_hip_repo_status(
        assessments,
        artifacts=[issue, pr],
        catalog_entries=make_catalog_entries("HIP-1234"),
        repos=["hiero-ledger/hiero-sdk-js"],
    )

    assert repo_statuses[0].status == "in_progress"


def test_aggregation_caps_top_supporting_artifacts():
    """Repo summary should keep at most 3 supporting artifacts."""
    issue = make_issue_artifact(number=501, title="Track HIP-1234 support", body="HIP-1234", linked_artifact_numbers=[502, 503, 504], state="closed")
    prs = [
        make_pull_request_artifact(number=502, body="Closes #501 and implements HIP-1234.", linked_artifact_numbers=[501]),
        make_pull_request_artifact(number=503, body="Follow-up for HIP-1234.", linked_artifact_numbers=[501]),
        make_pull_request_artifact(number=504, body="Docs for HIP-1234.", linked_artifact_numbers=[501], changed_files=[build_changed_file("docs/hip1234.md", additions=10, deletions=0, status="added")]),
    ]
    assessments = [score_candidate(make_candidate(issue))]
    assessments.extend(score_candidate(make_candidate(pr)) for pr in prs)

    repo_statuses = aggregate_hip_repo_status(
        assessments,
        artifacts=[issue, *prs],
        catalog_entries=make_catalog_entries("HIP-1234"),
        repos=["hiero-ledger/hiero-sdk-js"],
    )

    assert len(repo_statuses[0].supporting_artifact_numbers) <= 3
