"""HIP candidate extraction from structured GitHub artifacts."""

from __future__ import annotations

import re
from collections import OrderedDict, defaultdict

from hiero_analytics.config.hip_progression import (
    DEFAULT_HIP_PROGRESSION_CONFIG,
    HipProgressionConfig,
)
from hiero_analytics.domain.hip_progression_models import (
    HIP_ID_PATTERN,
    ArtifactTextSegment,
    HipArtifact,
    HipCandidate,
    HipMention,
    flatten_text,
    normalize_hip_id,
)

NEGATIVE_CONTEXT_PATTERNS: dict[str, re.Pattern[str]] = {
    "blocked": re.compile(r"\bblocked(?:\s+by)?\b|\bblockers?\b", re.IGNORECASE),
    "follow_up": re.compile(r"\bfollow[\s-]?up\b|\bunblocks?\b|\bunblocking\b", re.IGNORECASE),
    "prep": re.compile(r"\bprep\b|\bpreparatory\b", re.IGNORECASE),
    "refactor_only": re.compile(r"\brefactor(?:ing)?\s+only\b", re.IGNORECASE),
    "cleanup_only": re.compile(r"\bcleanup\s+only\b", re.IGNORECASE),
    "reverted": re.compile(r"\brevert(?:ed|ing|s)?\b|\brollback\b", re.IGNORECASE),
}


def extract_hip_ids(text: str) -> list[str]:
    """Extract canonical HIP identifiers from free-form text."""
    seen: OrderedDict[str, None] = OrderedDict()
    for match in HIP_ID_PATTERN.finditer(text or ""):
        seen[f"HIP-{int(match.group(1))}"] = None
    return list(seen)


def find_negative_context_flags(text: str) -> list[str]:
    """Find negative or preparatory phrases that weaken implementation evidence."""
    flags: list[str] = []
    for flag, pattern in NEGATIVE_CONTEXT_PATTERNS.items():
        if pattern.search(text or ""):
            flags.append(flag)
    return flags


def _segment_context(text: str, *, start: int, end: int, window: int = 80) -> str:
    left = max(0, start - window)
    right = min(len(text), end + window)
    return " ".join(text[left:right].split())


def _looks_semantic(context: str, config: HipProgressionConfig) -> bool:
    lowered = context.lower()
    return any(term in lowered for term in config.semantic_positive_terms)


def _segment_mentions(
    segment: ArtifactTextSegment,
    *,
    config: HipProgressionConfig,
) -> list[HipMention]:
    mentions: list[HipMention] = []
    for match in HIP_ID_PATTERN.finditer(segment.text or ""):
        phrase_context = _segment_context(segment.text, start=match.start(), end=match.end())
        negative_context_flags = find_negative_context_flags(phrase_context)
        mentions.append(
            HipMention(
                hip_id=normalize_hip_id(match.group(0)),
                source_kind=segment.source_kind,
                source_id=segment.source_id,
                matched_text=match.group(0),
                phrase_context=phrase_context,
                is_explicit_match=True,
                is_semantic_match=_looks_semantic(phrase_context, config=config),
                is_negative_context=bool(negative_context_flags),
                negative_context_flags=negative_context_flags,
                is_bot=segment.is_bot,
            )
        )
    return mentions


def _reason_for_mention(mention: HipMention) -> str:
    source_label = mention.source_kind.replace("_", " ")
    if mention.source_kind == "changed_file":
        return f"HIP-shaped file path hint in {mention.source_id}"
    if mention.is_semantic_match and mention.is_explicit_match:
        return f"semantic HIP mention in {source_label}"
    return f"explicit HIP mention in {source_label}"


def extract_hip_candidates_from_artifact(
    artifact: HipArtifact,
    *,
    config: HipProgressionConfig = DEFAULT_HIP_PROGRESSION_CONFIG,
) -> list[HipCandidate]:
    """Extract unique direct HIP candidates from one artifact."""
    mentions_by_hip: dict[str, list[HipMention]] = defaultdict(list)
    all_negative_flags = find_negative_context_flags(
        flatten_text([segment.text for segment in artifact.text_segments() if segment.text])
    )
    for segment in artifact.text_segments():
        for mention in _segment_mentions(segment, config=config):
            mentions_by_hip[mention.hip_id].append(mention)

    candidates: list[HipCandidate] = []
    for hip_id, mentions in mentions_by_hip.items():
        matched_sources = list(dict.fromkeys(mention.source_kind for mention in mentions))
        text_reasons = list(dict.fromkeys(_reason_for_mention(mention) for mention in mentions))
        linked_numbers = list(dict.fromkeys(artifact.linked_artifact_numbers))
        candidate_negative_flags = list(
            dict.fromkeys(
                flag
                for mention in mentions
                for flag in mention.negative_context_flags
            )
        )
        candidates.append(
            HipCandidate(
                artifact=artifact,
                hip_id=hip_id,
                extraction_source=", ".join(matched_sources),
                text_match_reason="; ".join(text_reasons),
                mentions=mentions,
                negative_context_flags=list(dict.fromkeys([*all_negative_flags, *candidate_negative_flags])),
                matched_sources=matched_sources,
                linked_artifact_numbers=linked_numbers,
            )
        )
    return candidates


def propagate_candidates_across_artifacts(
    candidates: list[HipCandidate],
    artifacts: list[HipArtifact],
) -> list[HipCandidate]:
    """Propagate HIP candidates across clearly linked issue/PR pairs."""
    direct_by_artifact: dict[tuple[str, int], list[HipCandidate]] = defaultdict(list)
    artifact_lookup = {(artifact.repo, artifact.number): artifact for artifact in artifacts}
    for candidate in candidates:
        direct_by_artifact[(candidate.artifact.repo, candidate.artifact.number)].append(candidate)

    propagated: list[HipCandidate] = list(candidates)
    existing_pairs = {(candidate.artifact.repo, candidate.artifact.number, candidate.hip_id) for candidate in candidates}
    for artifact in artifacts:
        linked_numbers = artifact.linked_artifact_numbers
        if not linked_numbers:
            continue
        for linked_number in linked_numbers:
            linked_artifact = artifact_lookup.get((artifact.repo, linked_number))
            if linked_artifact is None:
                continue
            for linked_candidate in direct_by_artifact.get((linked_artifact.repo, linked_artifact.number), []):
                key = (artifact.repo, artifact.number, linked_candidate.hip_id)
                if key in existing_pairs:
                    continue
                mention = HipMention(
                    hip_id=linked_candidate.hip_id,
                    source_kind="linked_artifact",
                    source_id=f"linked:{linked_number}",
                    matched_text=f"linked artifact #{linked_number}",
                    phrase_context=f"Linked artifact #{linked_number} carries direct {linked_candidate.hip_id} evidence.",
                    is_explicit_match=False,
                    is_semantic_match=True,
                    is_negative_context=False,
                    linked_artifact_numbers=[linked_number],
                    is_bot=False,
                )
                propagated_candidate = HipCandidate(
                    artifact=artifact,
                    hip_id=linked_candidate.hip_id,
                    extraction_source="linked_artifact",
                    text_match_reason=f"propagated from linked artifact #{linked_number}",
                    mentions=[mention],
                    negative_context_flags=[],
                    matched_sources=["linked_artifact"],
                    linked_artifact_numbers=list(dict.fromkeys([*artifact.linked_artifact_numbers, linked_number])),
                    propagated_from_artifacts=[linked_number],
                )
                propagated.append(propagated_candidate)
                existing_pairs.add(key)
    return propagated


def extract_hip_candidates(
    artifacts: list[HipArtifact],
    *,
    config: HipProgressionConfig = DEFAULT_HIP_PROGRESSION_CONFIG,
) -> list[HipCandidate]:
    """Extract HIP candidates across a list of normalized artifacts."""
    candidates: list[HipCandidate] = []
    for artifact in artifacts:
        candidates.extend(extract_hip_candidates_from_artifact(artifact, config=config))
    return propagate_candidates_across_artifacts(candidates, artifacts)
