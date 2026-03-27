"""Tests for HIP artifact evidence collection."""

from hiero_analytics.analysis.hip_evidence_collection import collect_artifact_evidence
from tests.hip_progression_fixtures import make_candidate, make_issue_artifact, make_pull_request_artifact


def test_collect_artifact_evidence_adds_maintainer_linked_signal_for_prs():
    """Maintainer-authored PRs linked to related artifacts should add an explicit confidence signal."""
    issue = make_issue_artifact(number=77, title="Track HIP-1234 support", body="HIP-1234 tracking issue.")
    pull_request = make_pull_request_artifact(number=101, linked_artifact_numbers=[77], author_association="MEMBER")
    candidate = make_candidate(pull_request)

    evidence_records = collect_artifact_evidence(
        candidate,
        artifact_lookup={
            (issue.repo, issue.number): issue,
            (pull_request.repo, pull_request.number): pull_request,
        },
    )

    assert any(record.evidence_type == "maintainer_linked_artifact" for record in evidence_records)


def test_collect_artifact_evidence_rewards_strong_pr_language_and_shape():
    """Direct HIP PR references with feat-style language and new src/test files should earn stronger evidence."""
    pull_request = make_pull_request_artifact(
        title="feat: introduces HIP-1234 transaction costing",
        body="This PR introduces HIP-1234 support.",
        commit_messages_text="feat: add HIP-1234 support",
        additions=540,
        deletions=40,
    )
    candidate = make_candidate(pull_request, matched_sources=["title", "body", "commit_message"])

    evidence_records = collect_artifact_evidence(candidate)
    evidence_types = {record.evidence_type for record in evidence_records}

    assert "pull_request_valid_hip_reference" in evidence_types
    assert "substantial_change_shape" in evidence_types
    assert "implementation_language" in evidence_types
    assert "implementation_shape" in evidence_types
