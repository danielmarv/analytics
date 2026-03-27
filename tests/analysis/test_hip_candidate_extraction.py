"""Tests for HIP candidate extraction and normalization."""

from hiero_analytics.analysis.hip_candidate_extraction import (
    extract_hip_candidates,
    extract_hip_candidates_from_artifact,
    extract_hip_ids,
    find_negative_context_flags,
    normalize_hip_id,
)
from hiero_analytics.domain.hip_progression_models import build_changed_file
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


def test_find_negative_context_flags_detects_preparatory_language():
    """Preparatory and blocker phrases should be captured as weakening context."""
    text = "Prep for HIP-1234 follow-up. Blocked by refactor only cleanup only. Later reverted and unblocks rollout."
    assert find_negative_context_flags(text) == [
        "blocked",
        "follow_up",
        "prep",
        "refactor_only",
        "cleanup_only",
        "reverted",
    ]


def test_extract_hip_candidates_from_artifact_tracks_structured_sources_and_flags():
    """Candidate extraction should explain where the HIP reference came from."""
    artifact = make_pull_request_artifact(
        title="Implement HIP-1234",
        body="Prep work for HIP 1234",
        comments_text="Blocked by HIP-1234 follow-up work",
        commit_messages_text="feat: support hip-1234",
    )

    candidates = extract_hip_candidates_from_artifact(artifact)

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.hip_id == "HIP-1234"
    assert "title" in candidate.matched_sources
    assert "body" in candidate.matched_sources
    assert "review_comment" in candidate.matched_sources
    assert "commit_message" in candidate.matched_sources
    assert "changed_file" in candidate.matched_sources
    assert set(candidate.negative_context_flags) >= {"blocked", "follow_up", "prep"}


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

    propagated = [candidate for candidate in candidates if candidate.artifact.number == 7002 and candidate.hip_id == "HIP-1234"]
    assert len(propagated) == 1
    assert propagated[0].extraction_source == "linked_artifact"
    assert propagated[0].propagated_from_artifacts == [7001]
