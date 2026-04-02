"""HIP candidate extraction from GitHub artifacts via regex + linked-artifact propagation."""

from __future__ import annotations

from collections import OrderedDict, defaultdict

from hiero_analytics.domain.hip_progression_models import (
    HIP_ID_PATTERN,
    HipArtifact,
    HipCandidate,
    normalize_hip_id,
)


def extract_hip_ids(text: str) -> list[str]:
    """Extract canonical HIP identifiers from free-form text."""
    seen: OrderedDict[str, None] = OrderedDict()
    for match in HIP_ID_PATTERN.finditer(text or ""):
        seen[f"HIP-{int(match.group(1))}"] = None
    return list(seen)


def _extract_from_artifact(artifact: HipArtifact) -> list[HipCandidate]:
    """Extract direct HIP candidates from title + body only."""
    combined = f"{artifact.title}\n{artifact.body}"
    hip_ids = extract_hip_ids(combined)
    return [
        HipCandidate(artifact=artifact, hip_id=hip_id, source="title_or_body")
        for hip_id in hip_ids
    ]


def _propagate_across_links(
    candidates: list[HipCandidate],
    artifacts: list[HipArtifact],
) -> list[HipCandidate]:
    """Propagate HIP candidates across clearly linked issue/PR pairs."""
    direct_by_key: dict[tuple[str, int], list[HipCandidate]] = defaultdict(list)
    artifact_lookup = {(a.repo, a.number): a for a in artifacts}
    for c in candidates:
        direct_by_key[(c.artifact.repo, c.artifact.number)].append(c)

    existing = {(c.artifact.repo, c.artifact.number, c.hip_id) for c in candidates}
    propagated = list(candidates)

    for artifact in artifacts:
        for linked_number in artifact.linked_artifact_numbers:
            linked = artifact_lookup.get((artifact.repo, linked_number))
            if linked is None:
                continue
            for linked_candidate in direct_by_key.get((linked.repo, linked.number), []):
                key = (artifact.repo, artifact.number, linked_candidate.hip_id)
                if key in existing:
                    continue
                propagated.append(
                    HipCandidate(
                        artifact=artifact,
                        hip_id=linked_candidate.hip_id,
                        source=f"linked:#{linked_number}",
                        is_propagated=True,
                    )
                )
                existing.add(key)
    return propagated


def extract_hip_candidates(
    artifacts: list[HipArtifact],
) -> list[HipCandidate]:
    """Extract HIP candidates across a list of normalized artifacts."""
    candidates: list[HipCandidate] = []
    for artifact in artifacts:
        candidates.extend(_extract_from_artifact(artifact))
    return _propagate_across_links(candidates, artifacts)
