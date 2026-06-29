"""Shared pytest configuration for the analytics test suite."""

import pytest

import hiero_analytics.data_sources.cache as cache
import hiero_analytics.config.paths as paths


@pytest.fixture(autouse=True)
def isolate_github_cache(monkeypatch, tmp_path):
    """Keep tests isolated from any real on-disk GitHub cache state by
    redirecting the analytics `OUTPUTS_DIR` to a temporary location.
    """
    monkeypatch.setattr(paths, "OUTPUTS_DIR", tmp_path)
