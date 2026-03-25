"""Tests for HIP candidate extraction and normalization."""

from hiero_analytics.analysis.hip_candidate_extraction import (
    extract_hip_candidates_from_artifact,
    extract_hip_ids,
    find_negative_context_flags,
    normalize_hip_id,
)
from tests.hip_progression_fixtures import make_pull_request_artifact


def test_normalize_hip_id_variants():
    """HIP identifiers should normalize across common formatting variants."""
    assert normalize_hip_id("HIP-1234") == "HIP-1234"
    assert normalize_hip_id("hip-1234") == "HIP-1234"
    assert normalize_hip_id("HIP 1234") == "HIP-1234"
    assert normalize_hip_id("hip 1234") == "HIP-1234"


def test_extract_hip_ids_deduplicates_and_preserves_order():
    """Repeated HIP mentions should collapse to a stable ordered list."""
    text = "HIP-1001 fixes HIP 2002 and hip-1001 stays referenced."

    hip_ids = extract_hip_ids(text)

    assert hip_ids == ["HIP-1001", "HIP-2002"]


def test_find_negative_context_flags_detects_preparatory_language():
    """Preparatory and blocker phrases should be captured as weakening context."""
    text = "Prep for HIP-1234 follow-up. Blocked by refactor only cleanup only."

    flags = find_negative_context_flags(text)

    assert flags == ["blocked", "follow_up", "prep", "refactor_only", "cleanup_only"]


def test_extract_hip_candidates_from_artifact_tracks_sources_and_negative_flags():
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
    assert candidate.matched_sources == ["title", "body", "comments", "commit_messages"]
    assert candidate.extraction_source == "title, body, comments, commit_messages"
    assert candidate.negative_context_flags == ["blocked", "follow_up", "prep"]
