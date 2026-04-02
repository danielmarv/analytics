"""Central configuration for HIP progression analysis."""

from __future__ import annotations

from dataclasses import dataclass, field

from hiero_analytics.domain.hip_progression_models import RepositoryTargetConfig

OFFICIAL_HIP_REPOSITORY = "hiero-ledger/hiero-improvement-proposals"
OFFICIAL_HIP_DIRECTORY = "HIP"
OFFICIAL_HIP_BASE_URL = "https://github.com/hiero-ledger/hiero-improvement-proposals/blob/main/HIP"


@dataclass(frozen=True, slots=True)
class HipProgressionConfig:
    """Top-level HIP progression configuration."""

    sdk_repo_name_substrings: tuple[str, ...] = ("sdk",)
    repo_overrides: dict[str, RepositoryTargetConfig] = field(default_factory=dict)

    def repository_matches_default_scope(self, repo_name: str) -> bool:
        """Return True when a repo should be included in default SDK batch scope."""
        lowered = repo_name.lower()
        return any(fragment in lowered for fragment in self.sdk_repo_name_substrings)


DEFAULT_HIP_PROGRESSION_CONFIG = HipProgressionConfig()
