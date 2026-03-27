"""Tests for HIP progression pipeline catalog scoping."""

from hiero_analytics.analysis.hip_progression_pipeline import (
    run_hip_progression_pipeline,
    select_catalog_entries,
)
from hiero_analytics.domain.hip_progression_models import build_changed_file
from tests.hip_progression_fixtures import make_catalog_entries, make_pull_request_artifact


def test_select_catalog_entries_limits_to_latest_hips_in_descending_order():
    """Catalog scoping should keep the newest HIPs first."""
    catalog_entries = make_catalog_entries("HIP-10", "HIP-20", "HIP-30")

    scoped_entries = select_catalog_entries(catalog_entries, latest_hip_limit=2)

    assert [entry.hip_id for entry in scoped_entries] == ["HIP-30", "HIP-20"]


def test_run_pipeline_scopes_predictions_to_latest_catalog_entries():
    """Pipeline output should only include HIPs that remain inside the configured scope."""
    artifact = make_pull_request_artifact(
        title="Implement HIP-100 support",
        body="This adds HIP-100 support.",
        commit_messages_text="feat: support HIP-100",
        changed_files=[build_changed_file("src/client/hip100.ts", additions=25, deletions=0, status="added")],
    )

    result = run_hip_progression_pipeline(
        artifacts=[artifact],
        catalog_entries=make_catalog_entries("HIP-100", "HIP-200"),
        repos=["hiero-ledger/hiero-sdk-js"],
        latest_hip_limit=1,
    )

    assert [entry.hip_id for entry in result.catalog_entries] == ["HIP-200"]
    assert result.candidates == []
    assert [repo_status.hip_id for repo_status in result.repo_statuses] == ["HIP-200"]
    assert result.repo_statuses[0].status == "not_started"
