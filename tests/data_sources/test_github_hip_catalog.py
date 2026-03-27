"""Tests for the official HIP catalog loader."""

from pathlib import Path

from hiero_analytics.data_sources.github_hip_catalog import (
    _entry_from_front_matter,
    _front_matter_and_body,
    load_catalog_snapshot,
)


def test_entry_from_front_matter_normalizes_numeric_hip_ids():
    """Catalog entries should accept the upstream integer HIP format."""
    entry = _entry_from_front_matter(
        {
            "hip": 1,
            "title": "Publish initial specification",
            "status": "Final",
            "type": "Standards Track",
            "category": "Core",
        },
        path_label="hip-1.md",
    )

    assert entry.hip_id == "HIP-1"
    assert entry.number == 1
    assert entry.title == "Publish initial specification"


def test_front_matter_parser_returns_body_without_yaml_markers():
    """The front-matter parser should separate the YAML header from the markdown body."""
    front_matter, body = _front_matter_and_body(
        "---\nhip: 7\ntitle: HIP title\nstatus: Draft\n---\n# Heading\nBody text.\n"
    )

    assert front_matter["hip"] == 7
    assert front_matter["title"] == "HIP title"
    assert body == "# Heading\nBody text.\n"


def test_load_catalog_snapshot_reads_json_payload(tmp_path: Path):
    """Benchmark catalog snapshots should load from checked-in JSON fixtures."""
    snapshot = tmp_path / "catalog_snapshot.json"
    snapshot.write_text(
        '[{"hip_id": "1", "number": 1, "title": "HIP one", "status": "Draft", "hip_type": "Core", "category": "Core"}]',
        encoding="utf-8",
    )

    entries = load_catalog_snapshot(snapshot)

    assert [entry.hip_id for entry in entries] == ["HIP-1"]
    assert entries[0].number == 1
