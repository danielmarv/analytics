"""Tests for assembling generated CSVs into dashboard sections."""

from __future__ import annotations

import pandas as pd

from hiero_analytics import run_dashboard
from hiero_analytics.dashboard_spec import SECTION_SPECS
from hiero_analytics.domain.periods import ACTIVITY_PERIODS


def test_load_period_variants_preserves_existing_empty_csv(tmp_path):
    """A generated zero-row period is a valid view, not a missing variant."""
    spec = {
        "periods": [
            ("30 days", "activity_30d.csv"),
            ("90 days", "activity_90d.csv"),
            ("All time", "activity_all.csv"),
        ]
    }
    pd.DataFrame(columns=["user", "actions"]).to_csv(tmp_path / "activity_30d.csv", index=False)
    pd.DataFrame([{"user": "alice", "actions": 3}]).to_csv(tmp_path / "activity_all.csv", index=False)

    variants = run_dashboard._load_period_variants(spec, tmp_path)

    assert [variant["label"] for variant in variants] == ["30 days", "All time"]
    assert variants[0]["data"].empty
    assert variants[1]["data"].to_dict("records") == [{"user": "alice", "actions": 3}]


def test_activity_specs_use_the_shared_period_set():
    """Every tabbed activity table uses the same ordered periods and filenames."""
    tabbed = {spec["id"]: spec for spec in SECTION_SPECS if spec.get("periods")}
    expected_labels = [period.label for period in ACTIVITY_PERIODS]

    assert set(tabbed) == {"profiles", "repoactivity", "understaffed", "loadshare", "repo", "teams"}
    assert all([label for label, _filename in spec["periods"]] == expected_labels for spec in tabbed.values())
