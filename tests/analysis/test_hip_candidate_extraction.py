"""Tests for HIP candidate extraction and normalization."""

from hiero_analytics.analysis.hip_candidate_extraction import (
    extract_hip_candidates,
    extract_hip_ids,
)
from hiero_analytics.domain.hip_progression_models import build_changed_file, normalize_hip_id
from tests.hip_progression_fixtures import make_issue_artifact, make_pull_request_artifact


def test_normalize_hip_id_variants():
    """HIP identifiers should normalize across common formatting variants."""
    assert normalize_hip_id("HIP-1234") == "HIP-1234"
    assert normalize_hip_id("hip-1234") == "HIP-1234"
    assert normalize_hip_id("HIP 1234") == "HIP-1234"
    assert normalize_hip_id("hip 1234") == "HIP-1234"


def test_extract_hip_ids_deduplicates_and_preserves_order():
    """Repeated HIP mentions should collapse to a stable ordered list."""
    text = "HIP-1001 fixes HIP 2002 and hip-1001 stays referenced."
    assert extract_hip_ids(text) == ["HIP-1001", "HIP-2002"]


def test_extract_hip_candidates_from_title_and_body():
    """Candidate extraction should find HIP references in title and body."""
    artifact = make_pull_request_artifact(
        title="Implement HIP-1234",
        body="This PR adds HIP-1234 support.",
    )

    candidates = extract_hip_candidates([artifact])

    hip_candidates = [c for c in candidates if c.hip_id == "HIP-1234"]
    assert len(hip_candidates) == 1
    assert hip_candidates[0].source == "title_or_body"
    assert not hip_candidates[0].is_propagated


def test_extract_hip_candidates_propagates_from_linked_issue_to_pr():
    """Linked issue evidence should propagate to a PR that closes the issue."""
    issue = make_issue_artifact(number=7001, title="Track HIP-1234 support", body="HIP-1234 backlog item.")
    pr = make_pull_request_artifact(
        number=7002,
        title="Wire up transport",
        body="Closes #7001",
        linked_artifact_numbers=[7001],
        commit_messages_text="",
        changed_files=[build_changed_file("src/client/transport.ts", additions=20, deletions=0, status="added")],
    )

    candidates = extract_hip_candidates([issue, pr])

    propagated = [c for c in candidates if c.artifact.number == 7002 and c.hip_id == "HIP-1234"]
    assert len(propagated) == 1
    assert propagated[0].is_propagated
    assert "linked" in propagated[0].source
