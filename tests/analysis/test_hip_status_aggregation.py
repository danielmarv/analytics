"""Tests for HIP repo-status aggregation."""

from hiero_analytics.analysis.hip_feature_engineering import engineer_hip_feature_vector
from hiero_analytics.analysis.hip_scoring import score_hip_feature_vector
from hiero_analytics.analysis.hip_status_aggregation import aggregate_hip_repo_status
from hiero_analytics.domain.hip_progression_models import build_changed_file
from tests.hip_progression_fixtures import (
    make_candidate,
    make_catalog_entries,
    make_issue_artifact,
    make_pull_request_artifact,
)


def test_aggregation_marks_completed_when_merged_code_tests_and_corrob_exist():
    """Repo status should become completed only when corroborating Tier 4 evidence exists."""
    issue = make_issue_artifact(number=201, title="Track HIP-1234 support", body="Tracking HIP-1234.", state="closed", linked_artifact_numbers=[202])
    strong_pr = make_pull_request_artifact(
        number=202,
        body="Closes #201 and completes HIP-1234 compliance.",
        linked_artifact_numbers=[201],
        changed_files=[
            build_changed_file("src/client/hip1234.ts", additions=80, deletions=5, status="added"),
            build_changed_file("tests/unit/hip1234.test.ts", additions=25, deletions=0, status="added"),
            build_changed_file("CHANGELOG.md", additions=4, deletions=0, status="modified"),
        ],
    )
    evidence_records = [
        score_hip_feature_vector(engineer_hip_feature_vector(make_candidate(issue))),
        score_hip_feature_vector(engineer_hip_feature_vector(make_candidate(strong_pr))),
    ]

    repo_statuses = aggregate_hip_repo_status(
        evidence_records,
        artifacts=[issue, strong_pr],
        catalog_entries=make_catalog_entries("HIP-1234"),
        repos=["hiero-ledger/hiero-sdk-js"],
    )

    assert len(repo_statuses) == 1
    repo_status = repo_statuses[0]
    assert repo_status.status == "completed"
    assert repo_status.confidence_level == "high"
    assert repo_status.supporting_artifact_numbers == [202, 201]


def test_aggregation_marks_not_started_for_catalog_hip_without_repo_evidence():
    """Official HIPs with no repo evidence should still emit not_started rows."""
    repo_statuses = aggregate_hip_repo_status(
        [],
        artifacts=[],
        catalog_entries=make_catalog_entries("HIP-1234"),
        repos=["hiero-ledger/hiero-sdk-js"],
    )

    assert repo_statuses[0].status == "not_started"
    assert repo_statuses[0].confidence_level == "low"
    assert repo_statuses[0].top_reasons == ["No repo evidence found for this official HIP."]


def test_aggregation_marks_conflicting_for_positive_and_negative_evidence():
    """Positive implementation evidence with revert/follow-up signals should conflict."""
    issue = make_issue_artifact(
        number=401,
        title="Revert HIP-1234 rollout",
        body="HIP-1234 was reverted and needs follow-up.",
        state="closed",
        linked_artifact_numbers=[402],
    )
    pr = make_pull_request_artifact(
        number=402,
        title="Follow-up for HIP-1234 after revert",
        body="Follow-up for HIP-1234 after revert.",
        linked_artifact_numbers=[401],
    )
    issue_candidate = make_candidate(issue, matched_sources=["title", "body"], negative_context_flags=["reverted", "follow_up"])
    pr_candidate = make_candidate(pr, matched_sources=["title", "body", "commit_message"], negative_context_flags=["reverted", "follow_up"])
    evidence_records = [
        score_hip_feature_vector(engineer_hip_feature_vector(issue_candidate)),
        score_hip_feature_vector(engineer_hip_feature_vector(pr_candidate)),
    ]

    repo_statuses = aggregate_hip_repo_status(
        evidence_records,
        artifacts=[issue, pr],
        catalog_entries=make_catalog_entries("HIP-1234"),
        repos=["hiero-ledger/hiero-sdk-js"],
    )

    assert repo_statuses[0].status == "conflicting"
    assert repo_statuses[0].rag_label == "red"


def test_aggregation_caps_top_supporting_artifacts_and_dedupes_labels():
    """Repo summary should keep a short, deduped artifact list."""
    issue = make_issue_artifact(number=501, title="Track HIP-1234 support", body="HIP-1234", linked_artifact_numbers=[502, 503, 504], state="closed")
    prs = [
        make_pull_request_artifact(number=502, body="Closes #501 and implements HIP-1234.", linked_artifact_numbers=[501]),
        make_pull_request_artifact(number=503, body="Follow-up for HIP-1234.", linked_artifact_numbers=[501]),
        make_pull_request_artifact(number=504, body="Docs for HIP-1234.", linked_artifact_numbers=[501], changed_files=[build_changed_file("docs/hip1234.md", additions=10, deletions=0, status="added")]),
    ]
    evidence_records = [score_hip_feature_vector(engineer_hip_feature_vector(make_candidate(issue)))]
    evidence_records.extend(score_hip_feature_vector(engineer_hip_feature_vector(make_candidate(pr))) for pr in prs)

    repo_statuses = aggregate_hip_repo_status(
        evidence_records,
        artifacts=[issue, *prs],
        catalog_entries=make_catalog_entries("HIP-1234"),
        repos=["hiero-ledger/hiero-sdk-js"],
    )

    assert len(repo_statuses[0].top_artifacts) <= 3
    assert len(repo_statuses[0].supporting_artifact_numbers) <= 3
