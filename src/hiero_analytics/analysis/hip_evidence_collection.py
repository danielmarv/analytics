"""Evidence collection for artifact-to-HIP associations."""

from __future__ import annotations

from collections import OrderedDict
import re

from hiero_analytics.config.hip_progression import (
    DEFAULT_HIP_PROGRESSION_CONFIG,
    HipProgressionConfig,
)
from hiero_analytics.domain.hip_progression_models import (
    ArtifactEvidence,
    EvidencePolarity,
    EvidenceTier,
    HipArtifact,
    HipCandidate,
    is_maintainer_like_author,
)


def _artifact_label(artifact: HipArtifact) -> str:
    prefix = "PR" if artifact.artifact_type == "pull_request" else "Issue"
    return f"{prefix} #{artifact.number}"


def _fingerprint(
    artifact: HipArtifact,
    hip_id: str,
    evidence_type: str,
    source_kind: str,
    short_rationale: str,
) -> str:
    return "|".join([artifact.repo, hip_id, artifact.artifact_type, str(artifact.number), evidence_type, source_kind, short_rationale])


def _contribution_for_negative_flag(flag: str, config: HipProgressionConfig) -> float:
    weights = config.weights
    return {
        "blocked": weights.tier_5_blocked,
        "reverted": weights.tier_5_reverted,
        "follow_up": weights.tier_5_follow_up,
        "prep": weights.tier_5_prep,
        "refactor_only": weights.tier_5_refactor_only,
        "cleanup_only": weights.tier_5_cleanup_only,
    }.get(flag, weights.tier_5_follow_up)


IMPLEMENTATION_LANGUAGE_PATTERN = re.compile(
    r"\bfeat\b[:\-]?|\bintroduce(?:s|d)?\b|\badd(?:s|ed)?\b",
    re.IGNORECASE,
)


def _has_direct_non_linked_mention(candidate: HipCandidate) -> bool:
    return any(mention.source_kind != "linked_artifact" for mention in candidate.mentions)


def _has_implementation_language(artifact: HipArtifact) -> bool:
    texts = [artifact.title, artifact.body, artifact.commit_messages_text]
    texts.extend(commit.message for commit in artifact.commits)
    return any(IMPLEMENTATION_LANGUAGE_PATTERN.search(text or "") for text in texts)


def _has_substantial_delta(artifact: HipArtifact, *, config: HipProgressionConfig) -> bool:
    return (artifact.additions + artifact.deletions) >= config.substantial_delta_min


def _add_evidence(
    evidence_map: OrderedDict[str, ArtifactEvidence],
    *,
    artifact: HipArtifact,
    hip_id: str,
    evidence_type: str,
    evidence_tier: EvidenceTier,
    source_kind: str,
    short_rationale: str,
    polarity: EvidencePolarity,
    confidence_contribution: float,
    top_reasons: list[str] | None = None,
    uncertainty_reasons: list[str] | None = None,
) -> None:
    fingerprint = _fingerprint(artifact, hip_id, evidence_type, source_kind, short_rationale)
    if fingerprint in evidence_map:
        return
    evidence_map[fingerprint] = ArtifactEvidence(
        hip_id=hip_id,
        artifact_type=artifact.artifact_type,
        artifact_number=artifact.number,
        source_artifact=_artifact_label(artifact),
        evidence_type=evidence_type,
        evidence_tier=evidence_tier,
        source_kind=source_kind,
        short_rationale=short_rationale,
        polarity=polarity,
        confidence_contribution=round(confidence_contribution, 2),
        top_reasons=list(top_reasons or [short_rationale]),
        uncertainty_reasons=list(uncertainty_reasons or []),
        fingerprint=fingerprint,
    )


def collect_artifact_evidence(
    candidate: HipCandidate,
    *,
    artifact_lookup: dict[tuple[str, int], HipArtifact] | None = None,
    config: HipProgressionConfig = DEFAULT_HIP_PROGRESSION_CONFIG,
) -> list[ArtifactEvidence]:
    """Build first-class evidence records for one artifact-to-HIP candidate."""
    artifact = candidate.artifact
    weights = config.weights
    evidence_map: OrderedDict[str, ArtifactEvidence] = OrderedDict()
    direct_mentions = 0
    bot_mentions = 0

    for mention in candidate.mentions:
        if mention.source_kind == "linked_artifact":
            _add_evidence(
                evidence_map,
                artifact=artifact,
                hip_id=candidate.hip_id,
                evidence_type="linked_artifact_propagation",
                evidence_tier="tier_1",
                source_kind=mention.source_kind,
                short_rationale=f"Linked artifact propagates {candidate.hip_id} into this artifact.",
                polarity="positive",
                confidence_contribution=weights.tier_1_direct_mention + weights.propagated_link_penalty,
                uncertainty_reasons=["HIP association is inferred from a linked artifact, not a direct mention."],
            )
            continue

        direct_mentions += 1
        if mention.is_bot or config.looks_like_workflow_noise(mention.phrase_context):
            bot_mentions += 1
        uncertainty_reasons: list[str] = []
        contribution = weights.tier_1_direct_mention
        if mention.is_bot or config.looks_like_workflow_noise(mention.phrase_context):
            contribution += weights.bot_signal_penalty
            uncertainty_reasons.append("Mention appears in bot or workflow-generated text.")
        if mention.source_kind in {"issue_comment", "review_comment"}:
            uncertainty_reasons.append("Mention appears in comments, which do not prove implementation by themselves.")
        if mention.is_semantic_match:
            short_rationale = f"Semantic phrase near {candidate.hip_id} in {mention.source_kind.replace('_', ' ')}."
        else:
            short_rationale = f"Direct {candidate.hip_id} mention in {mention.source_kind.replace('_', ' ')}."
        _add_evidence(
            evidence_map,
            artifact=artifact,
            hip_id=candidate.hip_id,
            evidence_type="direct_mention",
            evidence_tier="tier_1",
            source_kind=mention.source_kind,
            short_rationale=short_rationale,
            polarity="positive",
            confidence_contribution=contribution,
            uncertainty_reasons=uncertainty_reasons,
        )

    src_files = [changed_file for changed_file in artifact.changed_files if changed_file.is_src]
    test_files = [changed_file for changed_file in artifact.changed_files if changed_file.is_test]
    docs_files = [changed_file for changed_file in artifact.changed_files if changed_file.is_docs]
    changelog_files = [changed_file for changed_file in artifact.changed_files if changed_file.is_changelog]
    hip_file_matches = [
        changed_file
        for changed_file in artifact.changed_files
        if candidate.hip_id.lower().replace("-", "") in changed_file.path.lower().replace("-", "").replace("_", "")
    ]

    if src_files:
        short_rationale = f"Source files changed in {_artifact_label(artifact)}."
        if hip_file_matches:
            short_rationale = f"Source changes include HIP-shaped file paths like {hip_file_matches[0].path}."
        _add_evidence(
            evidence_map,
            artifact=artifact,
            hip_id=candidate.hip_id,
            evidence_type="code_pattern",
            evidence_tier="tier_2",
            source_kind="changed_file",
            short_rationale=short_rationale,
            polarity="positive",
            confidence_contribution=weights.tier_2_code_pattern,
        )

    if artifact.artifact_type == "pull_request" and _has_direct_non_linked_mention(candidate):
        _add_evidence(
            evidence_map,
            artifact=artifact,
            hip_id=candidate.hip_id,
            evidence_type="pull_request_valid_hip_reference",
            evidence_tier="tier_1",
            source_kind="title",
            short_rationale="Pull request contains a direct valid HIP identifier.",
            polarity="positive",
            confidence_contribution=weights.pull_request_hip_bonus,
        )

    if (
        artifact.artifact_type == "pull_request"
        and _has_direct_non_linked_mention(candidate)
        and src_files
        and _has_substantial_delta(artifact, config=config)
    ):
        _add_evidence(
            evidence_map,
            artifact=artifact,
            hip_id=candidate.hip_id,
            evidence_type="substantial_change_shape",
            evidence_tier="tier_2",
            source_kind="changed_file",
            short_rationale="PR has a direct HIP reference and a substantial implementation delta in source files.",
            polarity="positive",
            confidence_contribution=weights.substantial_delta_bonus,
            uncertainty_reasons=["Large code deltas can strengthen confidence, but size alone does not prove HIP completion."],
        )

    if artifact.artifact_type == "pull_request" and _has_direct_non_linked_mention(candidate) and _has_implementation_language(artifact):
        _add_evidence(
            evidence_map,
            artifact=artifact,
            hip_id=candidate.hip_id,
            evidence_type="implementation_language",
            evidence_tier="tier_2",
            source_kind="title",
            short_rationale="Implementation language such as feat, adds, or introduces appears alongside the HIP reference.",
            polarity="positive",
            confidence_contribution=weights.implementation_language_bonus,
        )

    if test_files:
        contribution = weights.tier_3_test_support
        if any(changed_file.is_integration_test for changed_file in test_files):
            contribution += weights.integration_test_bonus
        _add_evidence(
            evidence_map,
            artifact=artifact,
            hip_id=candidate.hip_id,
            evidence_type="test_support",
            evidence_tier="tier_3",
            source_kind="changed_file",
            short_rationale=f"Tests changed in {_artifact_label(artifact)}.",
            polarity="positive",
            confidence_contribution=contribution,
        )

    if (
        artifact.artifact_type == "pull_request"
        and src_files
        and test_files
        and any(changed_file.status == "added" for changed_file in src_files)
        and any(changed_file.status == "added" for changed_file in test_files)
    ):
        _add_evidence(
            evidence_map,
            artifact=artifact,
            hip_id=candidate.hip_id,
            evidence_type="implementation_shape",
            evidence_tier="tier_3",
            source_kind="changed_file",
            short_rationale="PR adds new source and test files, matching a stronger HIP implementation shape.",
            polarity="positive",
            confidence_contribution=weights.implementation_shape_bonus,
        )

    if artifact.merged:
        _add_evidence(
            evidence_map,
            artifact=artifact,
            hip_id=candidate.hip_id,
            evidence_type="merged_pull_request",
            evidence_tier="tier_4",
            source_kind="linked_artifact" if artifact.artifact_type == "issue" else "body",
            short_rationale=f"{_artifact_label(artifact)} is merged.",
            polarity="positive",
            confidence_contribution=weights.tier_4_completion + weights.merged_bonus,
            uncertainty_reasons=["Merge state alone does not prove a HIP is fully completed."],
        )

    if changelog_files:
        _add_evidence(
            evidence_map,
            artifact=artifact,
            hip_id=candidate.hip_id,
            evidence_type="changelog_update",
            evidence_tier="tier_4",
            source_kind="changed_file",
            short_rationale="Changelog or release-note files were updated.",
            polarity="positive",
            confidence_contribution=weights.tier_4_completion + weights.changelog_bonus,
            uncertainty_reasons=["Documentation updates corroborate implementation but do not replace code evidence."],
        )
    elif docs_files:
        _add_evidence(
            evidence_map,
            artifact=artifact,
            hip_id=candidate.hip_id,
            evidence_type="docs_update",
            evidence_tier="tier_4",
            source_kind="changed_file",
            short_rationale="Documentation files were updated.",
            polarity="positive",
            confidence_contribution=weights.tier_4_completion,
            uncertainty_reasons=["Docs updates are corroborating evidence, not standalone completion proof."],
        )

    if artifact_lookup:
        linked_artifacts = [
            artifact_lookup[(artifact.repo, number)]
            for number in candidate.linked_artifact_numbers
            if (artifact.repo, number) in artifact_lookup
        ]
        if any(linked.artifact_type == "issue" and linked.state == "closed" for linked in linked_artifacts):
            _add_evidence(
                evidence_map,
                artifact=artifact,
                hip_id=candidate.hip_id,
                evidence_type="closed_linked_issue",
                evidence_tier="tier_4",
                source_kind="linked_artifact",
                short_rationale="Linked issue is closed.",
                polarity="positive",
                confidence_contribution=weights.tier_4_completion + weights.issue_closed_bonus,
                uncertainty_reasons=["A closed issue helps corroborate progress but is not sufficient on its own."],
            )
        if any(linked.artifact_type == "pull_request" and linked.merged for linked in linked_artifacts):
            _add_evidence(
                evidence_map,
                artifact=artifact,
                hip_id=candidate.hip_id,
                evidence_type="linked_merged_pull_request",
                evidence_tier="tier_4",
                source_kind="linked_artifact",
                short_rationale="Linked pull request is merged.",
                polarity="positive",
                confidence_contribution=weights.tier_4_completion,
                uncertainty_reasons=["Linked merge evidence is weaker than merged code in the current artifact."],
            )
        if (
            artifact.artifact_type == "pull_request"
            and linked_artifacts
            and is_maintainer_like_author(artifact.author_association)
        ):
            _add_evidence(
                evidence_map,
                artifact=artifact,
                hip_id=candidate.hip_id,
                evidence_type="maintainer_linked_artifact",
                evidence_tier="tier_4",
                source_kind="linked_artifact",
                short_rationale="Maintainer-authored PR links to related issue or PR.",
                polarity="positive",
                confidence_contribution=weights.maintainer_linked_bonus,
                uncertainty_reasons=["Maintainer ownership strengthens confidence but does not prove completion alone."],
            )

    if direct_mentions == 0 and candidate.propagated_from_artifacts:
        _add_evidence(
            evidence_map,
            artifact=artifact,
            hip_id=candidate.hip_id,
            evidence_type="linked_without_direct_reference",
            evidence_tier="tier_5",
            source_kind="linked_artifact",
            short_rationale="HIP association depends on linked artifacts rather than a direct reference.",
            polarity="negative",
            confidence_contribution=weights.propagated_link_penalty,
            uncertainty_reasons=["Direct mention is absent in this artifact."],
        )

    for flag in candidate.negative_context_flags:
        short_rationale = f"Negative context indicates {flag.replace('_', ' ')} work."
        _add_evidence(
            evidence_map,
            artifact=artifact,
            hip_id=candidate.hip_id,
            evidence_type=flag,
            evidence_tier="tier_5",
            source_kind="body",
            short_rationale=short_rationale,
            polarity="negative",
            confidence_contribution=_contribution_for_negative_flag(flag, config),
        )

    if docs_files and not src_files and not test_files:
        _add_evidence(
            evidence_map,
            artifact=artifact,
            hip_id=candidate.hip_id,
            evidence_type="docs_only_change",
            evidence_tier="tier_5",
            source_kind="changed_file",
            short_rationale="Only docs-like files changed.",
            polarity="negative",
            confidence_contribution=weights.docs_only_penalty,
            uncertainty_reasons=["Docs-only changes do not prove HIP implementation."],
        )

    if src_files and not test_files:
        _add_evidence(
            evidence_map,
            artifact=artifact,
            hip_id=candidate.hip_id,
            evidence_type="missing_test_evidence",
            evidence_tier="tier_5",
            source_kind="changed_file",
            short_rationale="Code changes are not backed by test changes.",
            polarity="negative",
            confidence_contribution=weights.missing_test_penalty,
            uncertainty_reasons=["Implementation exists, but test corroboration is missing."],
        )

    if bot_mentions and bot_mentions == direct_mentions:
        _add_evidence(
            evidence_map,
            artifact=artifact,
            hip_id=candidate.hip_id,
            evidence_type="bot_only_mentions",
            evidence_tier="tier_5",
            source_kind="issue_comment" if artifact.artifact_type == "issue" else "review_comment",
            short_rationale="All detected HIP mentions came from bot or workflow text.",
            polarity="negative",
            confidence_contribution=weights.bot_signal_penalty,
            uncertainty_reasons=["Bot-only mentions are weak evidence."],
        )

    return list(evidence_map.values())


def collect_artifact_evidence_batch(
    candidates: list[HipCandidate],
    *,
    artifact_lookup: dict[tuple[str, int], HipArtifact] | None = None,
    config: HipProgressionConfig = DEFAULT_HIP_PROGRESSION_CONFIG,
) -> dict[tuple[str, int, str], list[ArtifactEvidence]]:
    """Collect evidence for a batch of candidates."""
    return {
        (candidate.artifact.repo, candidate.artifact.number, candidate.hip_id): collect_artifact_evidence(
            candidate,
            artifact_lookup=artifact_lookup,
            config=config,
        )
        for candidate in candidates
    }
