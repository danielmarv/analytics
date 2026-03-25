"""Tests for HIP feature engineering heuristics."""

from hiero_analytics.analysis.hip_feature_engineering import engineer_hip_feature_vector
from hiero_analytics.domain.hip_progression_models import classify_changed_file_path
from tests.hip_progression_fixtures import make_candidate, make_pull_request_artifact


def test_classify_changed_file_path_covers_source_unit_and_integration_files():
    """Path heuristics should separate implementation and test evidence."""
    assert classify_changed_file_path("src/client/feature.ts") == (True, False, False)
    assert classify_changed_file_path("tests/unit/feature.test.ts") == (False, True, False)
    assert classify_changed_file_path("tests/integration/feature.e2e.ts") == (False, True, True)


def test_engineer_hip_feature_vector_from_strong_pull_request():
    """Feature engineering should convert changed files and text cues into counts and flags."""
    artifact = make_pull_request_artifact()
    candidate = make_candidate(
        artifact,
        matched_sources=["title", "body", "comments", "commit_messages"],
    )

    feature_vector = engineer_hip_feature_vector(candidate)

    assert feature_vector.repo == "hiero-ledger/hiero-sdk-js"
    assert feature_vector.hip_in_title is True
    assert feature_vector.hip_in_body is True
    assert feature_vector.hip_in_comments is True
    assert feature_vector.hip_in_commit_messages is True
    assert feature_vector.has_feat_keyword is True
    assert feature_vector.has_implement_keyword is True
    assert feature_vector.has_support_keyword is True
    assert feature_vector.src_files_changed_count == 1
    assert feature_vector.test_files_changed_count == 2
    assert feature_vector.integration_test_files_changed_count == 1
    assert feature_vector.new_src_files_count == 1
    assert feature_vector.new_test_files_count == 2
    assert feature_vector.author_is_maintainer_like is True
    assert "source files changed" in feature_vector.implementation_score_inputs
    assert "unit tests added" in feature_vector.implementation_score_inputs
