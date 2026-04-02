"""Evaluation helpers for HIP progression: train/test splits and manual review rows."""

from __future__ import annotations

from collections import defaultdict

from hiero_analytics.domain.hip_progression_models import (
    ArtifactHipAssessment,
    HipArtifact,
    HipRepoStatus,
)

MANUAL_REVIEW_COLUMNS = [
    "human_observation",
    "is_prediction_correct",
    "is_confirmed_match",
    "is_overcalled_match",
    "is_missed_match",
    "is_confirmed_non_match",
]

ARTIFACT_EVALUATION_COLUMNS = [
    "prediction_present",
    "dataset_split",
    "repo",
    "artifact_type",
    "artifact_number",
    "artifact_title",
    "artifact_url",
    "hip_id",
    "predicted_status",
    "predicted_confidence",
    *MANUAL_REVIEW_COLUMNS,
]

REPO_EVALUATION_COLUMNS = [
    "prediction_present",
    "dataset_split",
    "repo",
    "hip_id",
    "predicted_status",
    "predicted_confidence",
    "supporting_artifact_numbers",
    "supporting_artifact_urls",
    *MANUAL_REVIEW_COLUMNS,
]

MANUAL_ACCURACY_COLUMNS = [
    "review_scope",
    "prediction_present",
    "dataset_split",
    "repo",
    "artifact_type",
    "artifact_number",
    "artifact_title",
    "artifact_url",
    "hip_id",
    "predicted_status",
    "predicted_confidence",
    "supporting_artifact_numbers",
    "supporting_artifact_urls",
    *MANUAL_REVIEW_COLUMNS,
]

LEGACY_MANUAL_REVIEW_COLUMN_ALIASES = {
    "is_true_positive": "is_confirmed_match",
    "is_false_positive": "is_overcalled_match",
    "is_false_negative": "is_missed_match",
    "is_true_negative": "is_confirmed_non_match",
}


def _artifact_timestamp(artifact: HipArtifact):
    return artifact.updated_at or artifact.closed_at or artifact.created_at


def _flatten(values: list[str]) -> str:
    return " | ".join(v for v in values if v)


def assign_dataset_splits(
    artifacts: list[HipArtifact],
    *,
    train_ratio: float = 0.8,
) -> dict[tuple[str, int], str]:
    """Assign a chronological 80/20 train/test split, separately for issues and PRs."""
    if not 0 < train_ratio < 1:
        raise ValueError("train_ratio must be between 0 and 1.")

    splits: dict[tuple[str, int], str] = {}
    by_type: dict[str, list[HipArtifact]] = defaultdict(list)
    for a in artifacts:
        by_type[a.artifact_type].append(a)

    for grouped in by_type.values():
        ordered = sorted(
            grouped,
            key=lambda a: (_artifact_timestamp(a) is None, _artifact_timestamp(a), a.number),
        )
        if not ordered:
            continue
        cutoff = max(1, min(len(ordered) - 1, int(len(ordered) * train_ratio)))
        for i, a in enumerate(ordered):
            splits[(a.repo, a.number)] = "train" if i < cutoff else "test"
    return splits


def build_artifact_evaluation_rows(
    *,
    artifacts: list[HipArtifact],
    assessments: list[ArtifactHipAssessment],
    dataset_splits: dict[tuple[str, int], str],
) -> list[dict[str, object]]:
    """Build manual-review rows for artifact-level HIP predictions."""
    by_artifact: dict[tuple[str, int], list[ArtifactHipAssessment]] = defaultdict(list)
    for a in assessments:
        by_artifact[(a.repo, a.artifact_number)].append(a)

    rows: list[dict[str, object]] = []
    for artifact in artifacts:
        key = (artifact.repo, artifact.number)
        split = dataset_splits.get(key, "train")
        group = sorted(by_artifact.get(key, []), key=lambda a: a.hip_id)

        if not group:
            rows.append({
                "prediction_present": False,
                "dataset_split": split,
                "repo": artifact.repo,
                "artifact_type": artifact.artifact_type,
                "artifact_number": artifact.number,
                "artifact_title": artifact.title,
                "artifact_url": artifact.url,
                "hip_id": "",
                "predicted_status": "",
                "predicted_confidence": "",
                **{col: "" for col in MANUAL_REVIEW_COLUMNS},
            })
            continue

        for assessment in group:
            rows.append({
                "prediction_present": True,
                "dataset_split": split,
                "repo": artifact.repo,
                "artifact_type": artifact.artifact_type,
                "artifact_number": artifact.number,
                "artifact_title": artifact.title,
                "artifact_url": artifact.url,
                "hip_id": assessment.hip_id,
                "predicted_status": assessment.status,
                "predicted_confidence": assessment.confidence,
                **{col: "" for col in MANUAL_REVIEW_COLUMNS},
            })
    return rows


def build_repo_evaluation_rows(
    *,
    artifacts: list[HipArtifact],
    repo_statuses: list[HipRepoStatus],
    dataset_splits: dict[tuple[str, int], str],
) -> list[dict[str, object]]:
    """Build manual-review rows for repo-level HIP status predictions."""
    artifact_lookup = {(a.repo, a.number): a for a in artifacts}
    rows: list[dict[str, object]] = []
    for rs in repo_statuses:
        supporting = [
            artifact_lookup[(rs.repo, n)]
            for n in rs.supporting_artifact_numbers
            if (rs.repo, n) in artifact_lookup
        ]
        split = "train"
        if any(dataset_splits.get((a.repo, a.number)) == "test" for a in supporting):
            split = "test"
        rows.append({
            "prediction_present": True,
            "dataset_split": split,
            "repo": rs.repo,
            "hip_id": rs.hip_id,
            "predicted_status": rs.status,
            "predicted_confidence": rs.confidence,
            "supporting_artifact_numbers": _flatten([str(n) for n in rs.supporting_artifact_numbers]),
            "supporting_artifact_urls": _flatten([a.url for a in supporting if a.url]),
            **{col: "" for col in MANUAL_REVIEW_COLUMNS},
        })
    return rows


def build_manual_accuracy_rows(
    *,
    artifacts: list[HipArtifact],
    assessments: list[ArtifactHipAssessment],
    repo_statuses: list[HipRepoStatus],
    dataset_splits: dict[tuple[str, int], str],
) -> list[dict[str, object]]:
    """Build a unified manual-review queue across PR, issue, and repo scopes."""
    artifact_rows = build_artifact_evaluation_rows(
        artifacts=artifacts, assessments=assessments, dataset_splits=dataset_splits,
    )
    repo_rows = build_repo_evaluation_rows(
        artifacts=artifacts, repo_statuses=repo_statuses, dataset_splits=dataset_splits,
    )
    combined: list[dict[str, object]] = []
    for r in artifact_rows:
        combined.append({
            "review_scope": str(r.get("artifact_type", "")),
            "prediction_present": r.get("prediction_present", ""),
            "dataset_split": r.get("dataset_split", ""),
            "repo": r.get("repo", ""),
            "artifact_type": r.get("artifact_type", ""),
            "artifact_number": r.get("artifact_number", ""),
            "artifact_title": r.get("artifact_title", ""),
            "artifact_url": r.get("artifact_url", ""),
            "hip_id": r.get("hip_id", ""),
            "predicted_status": r.get("predicted_status", ""),
            "predicted_confidence": r.get("predicted_confidence", ""),
            "supporting_artifact_numbers": "",
            "supporting_artifact_urls": "",
            **{col: r.get(col, "") for col in MANUAL_REVIEW_COLUMNS},
        })
    for r in repo_rows:
        combined.append({
            "review_scope": "repo",
            "prediction_present": r.get("prediction_present", ""),
            "dataset_split": r.get("dataset_split", ""),
            "repo": r.get("repo", ""),
            "artifact_type": "",
            "artifact_number": "",
            "artifact_title": "",
            "artifact_url": "",
            "hip_id": r.get("hip_id", ""),
            "predicted_status": r.get("predicted_status", ""),
            "predicted_confidence": r.get("predicted_confidence", ""),
            "supporting_artifact_numbers": r.get("supporting_artifact_numbers", ""),
            "supporting_artifact_urls": r.get("supporting_artifact_urls", ""),
            **{col: r.get(col, "") for col in MANUAL_REVIEW_COLUMNS},
        })
    return combined
