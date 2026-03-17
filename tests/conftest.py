"""Shared pytest configuration for the analytics test suite."""

import sys
from pathlib import Path

import pytest

import hiero_analytics.data_sources.cache as cache

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture(autouse=True)
def isolate_github_cache(monkeypatch, tmp_path):
    """Keep tests isolated from any real on-disk GitHub cache state."""
    monkeypatch.setattr(cache, "GITHUB_CACHE_DIR", tmp_path / "github")
