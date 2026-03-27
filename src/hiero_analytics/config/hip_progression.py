"""Central configuration for HIP progression analysis."""

from __future__ import annotations

from dataclasses import dataclass, field

from hiero_analytics.domain.hip_progression_models import (
    ConfidenceLevel,
    RepositoryTargetConfig,
)

OFFICIAL_HIP_REPOSITORY = "hiero-ledger/hiero-improvement-proposals"
OFFICIAL_HIP_DIRECTORY = "HIP"
OFFICIAL_HIP_BASE_URL = "https://github.com/hiero-ledger/hiero-improvement-proposals/blob/main/HIP"


@dataclass(frozen=True, slots=True)
class EvidenceWeightConfig:
    """Deterministic confidence contributions for each evidence tier."""

    tier_1_direct_mention: float = 15.0
    pull_request_hip_bonus: float = 8.0
    implementation_language_bonus: float = 8.0
    implementation_shape_bonus: float = 10.0
    substantial_delta_bonus: float = 5.0
    tier_2_code_pattern: float = 30.0
    tier_3_test_support: float = 20.0
    tier_4_completion: float = 25.0
    tier_5_blocked: float = -35.0
    tier_5_reverted: float = -35.0
    tier_5_follow_up: float = -20.0
    tier_5_prep: float = -20.0
    tier_5_refactor_only: float = -20.0
    tier_5_cleanup_only: float = -20.0
    docs_only_penalty: float = -15.0
    bot_signal_penalty: float = -10.0
    propagated_link_penalty: float = -5.0
    linked_issue_bonus: float = 5.0
    linked_pr_bonus: float = 5.0
    maintainer_linked_bonus: float = 5.0
    merged_bonus: float = 10.0
    integration_test_bonus: float = 5.0
    changelog_bonus: float = 5.0
    issue_closed_bonus: float = 5.0
    multi_tier_agreement_bonus: float = 10.0
    contradiction_penalty: float = 15.0
    missing_test_penalty: float = -10.0


@dataclass(frozen=True, slots=True)
class ConfidenceThresholdConfig:
    """Central thresholds for numeric confidence -> level mapping."""

    low_max: float = 54.99
    medium_max: float = 79.99
    high_min: float = 80.0
    high_min_positive_tiers: int = 3
    high_max_negative_evidence: int = 0

    def resolve_level(self, score: float) -> ConfidenceLevel:
        """Map a score to a confidence label."""
        if score >= self.high_min:
            return "high"
        if score > self.low_max:
            return "medium"
        return "low"


@dataclass(frozen=True, slots=True)
class StatusRuleConfig:
    """Thresholds controlling conservative status inference."""

    supporting_artifact_limit: int = 3
    unknown_min_score: float = 15.0
    in_progress_min_score: float = 45.0
    completed_min_score: float = 80.0
    completion_requires_tier4: bool = True
    completion_requires_tests: bool = True
    completion_requires_merged: bool = True


@dataclass(frozen=True, slots=True)
class HipProgressionConfig:
    """Top-level HIP progression configuration."""

    weights: EvidenceWeightConfig = EvidenceWeightConfig()
    confidence: ConfidenceThresholdConfig = ConfidenceThresholdConfig()
    status_rules: StatusRuleConfig = StatusRuleConfig()
    semantic_positive_terms: tuple[str, ...] = (
        "implement",
        "implements",
        "implemented",
        "introduce",
        "introduces",
        "introduced",
        "feat",
        "adds",
        "added",
        "support",
        "supports",
        "supported",
        "compliance",
        "compliant",
        "enable",
        "enabled",
        "ship",
        "shipped",
        "complete",
        "completed",
        "close",
        "closes",
        "closing",
    )
    semantic_negative_terms: tuple[str, ...] = (
        "blocked",
        "blocker",
        "follow-up",
        "follow up",
        "prep",
        "preparatory",
        "refactor only",
        "cleanup only",
        "revert",
        "reverted",
        "rollback",
    )
    bot_login_markers: tuple[str, ...] = ("bot", "[bot]", "workflowbot", "coderabbit")
    workflow_noise_markers: tuple[str, ...] = (
        "cannot be merged",
        "workflow checks",
        "auto-generated comment",
        "codelint",
        "suggested fix",
    )
    sdk_repo_name_substrings: tuple[str, ...] = ("sdk",)
    substantial_delta_min: int = 500
    repo_overrides: dict[str, RepositoryTargetConfig] = field(default_factory=dict)

    def is_bot_login(self, login: str) -> bool:
        """Return True when a login looks bot-generated."""
        lowered = login.lower()
        return any(marker in lowered for marker in self.bot_login_markers)

    def looks_like_workflow_noise(self, text: str) -> bool:
        """Return True when free-form text resembles workflow noise rather than HIP evidence."""
        lowered = text.lower()
        return any(marker in lowered for marker in self.workflow_noise_markers)

    def repository_matches_default_scope(self, repo_name: str) -> bool:
        """Return True when a repo should be included in default SDK batch scope."""
        lowered = repo_name.lower()
        return any(fragment in lowered for fragment in self.sdk_repo_name_substrings)


DEFAULT_HIP_PROGRESSION_CONFIG = HipProgressionConfig()
