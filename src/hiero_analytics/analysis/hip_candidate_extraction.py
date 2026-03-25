"""HIP candidate extraction from normalized GitHub artifacts."""

from __future__ import annotations

import re
from collections import OrderedDict

from hiero_analytics.domain.hip_progression_models import HipArtifact, HipCandidate

HIP_ID_PATTERN = re.compile(r"\bhip[-\s]?(\d+)\b", re.IGNORECASE)

NEGATIVE_CONTEXT_PATTERNS: dict[str, re.Pattern[str]] = {
    "unblock": re.compile(r"\bunblocks?\b", re.IGNORECASE),
    "blocked": re.compile(r"\bblocked(?:\s+by)?\b|\bblockers?\b", re.IGNORECASE),
    "follow_up": re.compile(r"\bfollow[\s-]?up\b", re.IGNORECASE),
    "prep": re.compile(r"\bprep\b|\bpreparatory\b", re.IGNORECASE),
    "refactor_only": re.compile(r"\brefactor(?:ing)?\s+only\b", re.IGNORECASE),
    "cleanup_only": re.compile(r"\bcleanup\s+only\b", re.IGNORECASE),
}


def normalize_hip_id(raw_value: str) -> str:
    """Normalize a HIP identifier to the canonical ``HIP-1234`` form."""
    match = HIP_ID_PATTERN.search(raw_value)
    if not match:
        raise ValueError(f"Unable to normalize HIP identifier from {raw_value!r}")
    return f"HIP-{match.group(1)}"


def extract_hip_ids(text: str) -> list[str]:
    """Extract canonical HIP identifiers from free-form text."""
    seen: OrderedDict[str, None] = OrderedDict()
    for match in HIP_ID_PATTERN.finditer(text or ""):
        seen[f"HIP-{match.group(1)}"] = None
    return list(seen)


def find_negative_context_flags(text: str) -> list[str]:
    """Find negative or preparatory phrases that weaken implementation evidence."""
    flags: list[str] = []
    for flag, pattern in NEGATIVE_CONTEXT_PATTERNS.items():
        if pattern.search(text or ""):
            flags.append(flag)
    return flags


def extract_hip_candidates_from_artifact(artifact: HipArtifact) -> list[HipCandidate]:
    """Extract unique HIP candidates from one artifact."""
    source_texts = {
        "title": artifact.title,
        "body": artifact.body,
        "comments": artifact.comments_text,
        "commit_messages": artifact.commit_messages_text,
    }
    all_text = "\n\n".join(value for value in source_texts.values() if value)
    negative_context_flags = find_negative_context_flags(all_text)

    matches_by_hip: OrderedDict[str, dict[str, list[str]]] = OrderedDict()
    for source, text in source_texts.items():
        for hip_id in extract_hip_ids(text):
            info = matches_by_hip.setdefault(hip_id, {"sources": [], "reasons": []})
            if source not in info["sources"]:
                info["sources"].append(source)
            info["reasons"].append(f"explicit HIP mention in {source.replace('_', ' ')}")

    candidates: list[HipCandidate] = []
    for hip_id, info in matches_by_hip.items():
        candidates.append(
            HipCandidate(
                artifact=artifact,
                hip_id=hip_id,
                extraction_source=", ".join(info["sources"]),
                text_match_reason="; ".join(info["reasons"]),
                negative_context_flags=list(negative_context_flags),
                matched_sources=list(info["sources"]),
            )
        )
    return candidates


def extract_hip_candidates(artifacts: list[HipArtifact]) -> list[HipCandidate]:
    """Extract HIP candidates across a list of normalized artifacts."""
    candidates: list[HipCandidate] = []
    for artifact in artifacts:
        candidates.extend(extract_hip_candidates_from_artifact(artifact))
    return candidates
