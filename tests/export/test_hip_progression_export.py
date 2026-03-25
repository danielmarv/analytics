"""Tests for HIP progression markdown exports."""

from __future__ import annotations

import pandas as pd

from hiero_analytics.analysis.hip_feature_engineering import engineer_hip_feature_vector
from hiero_analytics.analysis.hip_scoring import score_hip_feature_vector
from hiero_analytics.analysis.hip_status_aggregation import aggregate_hip_repo_status
from hiero_analytics.export.hip_progression_export import export_hip_progression_results
from hiero_analytics.export.save import save_markdown_table
from tests.hip_progression_fixtures import make_candidate, make_pull_request_artifact


def test_save_markdown_table_escapes_pipes_and_newlines(tmp_path):
    """Markdown export should escape pipes and preserve line breaks safely."""
    path = tmp_path / "table.md"
    df = pd.DataFrame([{"summary": "line one\nline | two"}])

    save_markdown_table(df, path)

    contents = path.read_text(encoding="utf-8")
    assert "| summary |" in contents
    assert "line one<br>line \\| two" in contents


def test_export_hip_progression_results_writes_markdown_tables(tmp_path):
    """HIP progression export should emit markdown tables for derived outputs, not raw artifacts."""
    artifact = make_pull_request_artifact()
    candidate = make_candidate(artifact)
    feature_vector = engineer_hip_feature_vector(candidate)
    evidence = score_hip_feature_vector(feature_vector)
    repo_status = aggregate_hip_repo_status([evidence], artifacts=[artifact])[0]

    paths = export_hip_progression_results(
        tmp_path,
        artifacts=[artifact],
        feature_vectors=[feature_vector],
        evidence_records=[evidence],
        repo_statuses=[repo_status],
    )

    assert "artifacts_markdown" not in paths
    assert not (tmp_path / "artifacts.md").exists()
    assert paths["artifact_features_markdown"].exists()
    assert paths["hip_evidence_markdown"].exists()
    assert paths["hip_repo_status_markdown"].exists()

    feature_markdown = paths["artifact_features_markdown"].read_text(encoding="utf-8")
    evidence_markdown = paths["hip_evidence_markdown"].read_text(encoding="utf-8")
    status_markdown = paths["hip_repo_status_markdown"].read_text(encoding="utf-8")

    assert "| repo | artifact_type | artifact_number |" in feature_markdown
    assert "| repo | hip_id | artifact_type |" in evidence_markdown
    assert "HIP-1234" in evidence_markdown
    assert "| repo | hip_id | status |" in status_markdown
