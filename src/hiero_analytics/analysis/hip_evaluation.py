"""Evaluation-table helpers for HIP progression feedback loops."""

from __future__ import annotations

from collections import defaultdict

from hiero_analytics.domain.hip_progression_models import (
    HipArtifact,
    HipEvidence,
    HipFeatureVector,
    HipRepoStatus,
    extract_artifact_reference_numbers,
    is_maintainer_like_author,
)

ARTIFACT_EVALUATION_COLUMNS = [
    "prediction_present",
    "dataset_split",
    "repo",
    "artifact_type",
    "artifact_number",
    "artifact_title",
    "artifact_url",
    "artifact_link",
    "hip_id",
    "extraction_source",
    "text_match_reason",
    "predicted_hip_candidate_score",
    "predicted_implementation_score",
    "predicted_completion_score",
    "predicted_confidence_level",
    "author_association",
    "merged",
    "linked_issue_numbers",
    "linked_issue_urls",
    "linked_pr_numbers",
    "linked_pr_urls",
    "human_expected_outcome",
    "human_observation",
    "is_prediction_correct",
    "is_confirmed_match",
    "is_overcalled_match",
    "is_missed_match",
    "is_confirmed_non_match",
]

REPO_EVALUATION_COLUMNS = [
    "prediction_present",
    "dataset_split",
    "repo",
    "hip_id",
    "predicted_status",
    "predicted_confidence_level",
    "supporting_artifact_numbers",
    "supporting_artifact_urls",
    "supporting_artifact_links",
    "supporting_issue_links",
    "supporting_pr_links",
    "has_supporting_issue",
    "has_supporting_pull_request",
    "has_merged_pull_request",
    "has_maintainer_like_pull_request",
    "has_linked_issue_pull_request_pair",
    "last_evidence_at",
    "rationale",
    "human_expected_outcome",
    "human_observation",
    "is_prediction_correct",
    "is_confirmed_match",
    "is_overcalled_match",
    "is_missed_match",
    "is_confirmed_non_match",
]

EVALUATION_SUMMARY_COLUMNS = [
    "scope",
    "dataset_split",
    "total_rows",
    "prediction_rows",
    "reviewed_rows",
    "review_coverage_percent",
    "evaluated_review_rows",
    "correct_reviewed_rows",
    "accuracy_percent",
    "confirmed_match_rows",
    "overcalled_match_rows",
    "missed_match_rows",
    "confirmed_non_match_rows",
    "match_quality_percent",
    "match_coverage_percent",
]

PREDICTION_REVIEW_BREAKDOWN_COLUMNS = [
    "scope",
    "dataset_split",
    "confirmed_match_rows",
    "overcalled_match_rows",
    "missed_match_rows",
    "confirmed_non_match_rows",
    "reviewed_rows",
    "unclear_review_rows",
    "accuracy_percent",
    "match_quality_percent",
    "match_coverage_percent",
    "non_match_accuracy_percent",
    "balance_score_percent",
]

MANUAL_REVIEW_COLUMNS = [
    "human_expected_outcome",
    "human_observation",
    "is_prediction_correct",
    "is_confirmed_match",
    "is_overcalled_match",
    "is_missed_match",
    "is_confirmed_non_match",
]

LEGACY_MANUAL_REVIEW_COLUMN_ALIASES = {
    "is_true_positive": "is_confirmed_match",
    "is_false_positive": "is_overcalled_match",
    "is_false_negative": "is_missed_match",
    "is_true_negative": "is_confirmed_non_match",
}


def _artifact_timestamp(artifact: HipArtifact):
    return artifact.updated_at or artifact.closed_at or artifact.created_at


def _format_datetime(value) -> str:
    return value.isoformat() if value is not None else ""


def _flatten_sequence(values: list[str]) -> str:
    return " | ".join(values)


def _artifact_link_label(artifact: HipArtifact) -> str:
    prefix = "PR" if artifact.artifact_type == "pull_request" else "Issue"
    return f"{prefix} #{artifact.number}"


def _artifact_markdown_link(artifact: HipArtifact) -> str:
    if not artifact.url:
        return _artifact_link_label(artifact)
    return f"[{_artifact_link_label(artifact)}]({artifact.url})"


def _linked_artifacts(
    artifact: HipArtifact,
    artifact_lookup: dict[tuple[str, int], HipArtifact],
) -> list[HipArtifact]:
    linked_numbers = extract_artifact_reference_numbers(
        artifact.title,
        artifact.body,
        artifact.comments_text,
        artifact.commit_messages_text,
    )
    linked_artifacts: list[HipArtifact] = []
    for number in linked_numbers:
        linked_artifact = artifact_lookup.get((artifact.repo, number))
        if linked_artifact is not None:
            linked_artifacts.append(linked_artifact)
    return linked_artifacts


def assign_dataset_splits(
    artifacts: list[HipArtifact],
    *,
    train_ratio: float = 0.8,
) -> dict[tuple[str, int], str]:
    """Assign a chronological train/test split, separately for issues and PRs."""
    if not 0 < train_ratio < 1:
        raise ValueError("train_ratio must be between 0 and 1.")

    splits: dict[tuple[str, int], str] = {}
    artifacts_by_type: dict[str, list[HipArtifact]] = defaultdict(list)
    for artifact in artifacts:
        artifacts_by_type[artifact.artifact_type].append(artifact)

    for grouped_artifacts in artifacts_by_type.values():
        ordered = sorted(
            grouped_artifacts,
            key=lambda artifact: (
                _artifact_timestamp(artifact) is None,
                _artifact_timestamp(artifact),
                artifact.number,
            ),
        )
        if not ordered:
            continue

        if len(ordered) == 1:
            train_cutoff = 1
        else:
            train_cutoff = int(len(ordered) * train_ratio)
            train_cutoff = min(len(ordered) - 1, max(1, train_cutoff))

        for index, artifact in enumerate(ordered):
            split = "train" if index < train_cutoff else "test"
            splits[(artifact.repo, artifact.number)] = split

    return splits


def build_artifact_evaluation_rows(
    *,
    artifacts: list[HipArtifact],
    feature_vectors: list[HipFeatureVector],
    evidence_records: list[HipEvidence],
    dataset_splits: dict[tuple[str, int], str],
) -> list[dict[str, object]]:
    """Build manual-review rows for artifact-level HIP predictions."""
    artifact_lookup = {
        (artifact.repo, artifact.number): artifact
        for artifact in artifacts
    }
    rows: list[dict[str, object]] = []

    for feature_vector, evidence in zip(feature_vectors, evidence_records, strict=True):
        artifact = artifact_lookup.get((evidence.repo, evidence.artifact_number))
        if artifact is None:
            continue

        linked_artifacts = _linked_artifacts(artifact, artifact_lookup)
        linked_issue_artifacts = [
            linked_artifact
            for linked_artifact in linked_artifacts
            if linked_artifact.artifact_type == "issue"
        ]
        linked_pr_artifacts = [
            linked_artifact
            for linked_artifact in linked_artifacts
            if linked_artifact.artifact_type == "pull_request"
        ]

        rows.append(
            {
                "prediction_present": True,
                "dataset_split": dataset_splits.get((artifact.repo, artifact.number), "train"),
                "repo": evidence.repo,
                "artifact_type": evidence.artifact_type,
                "artifact_number": evidence.artifact_number,
                "artifact_title": artifact.title,
                "artifact_url": artifact.url,
                "artifact_link": _artifact_markdown_link(artifact),
                "hip_id": evidence.hip_id,
                "extraction_source": feature_vector.extraction_source,
                "text_match_reason": feature_vector.text_match_reason,
                "predicted_hip_candidate_score": evidence.hip_candidate_score,
                "predicted_implementation_score": evidence.implementation_score,
                "predicted_completion_score": evidence.completion_score,
                "predicted_confidence_level": evidence.confidence_level,
                "author_association": artifact.author_association,
                "merged": artifact.merged,
                "linked_issue_numbers": _flatten_sequence(
                    [str(linked_artifact.number) for linked_artifact in linked_issue_artifacts]
                ),
                "linked_issue_urls": _flatten_sequence(
                    [linked_artifact.url for linked_artifact in linked_issue_artifacts if linked_artifact.url]
                ),
                "linked_pr_numbers": _flatten_sequence(
                    [str(linked_artifact.number) for linked_artifact in linked_pr_artifacts]
                ),
                "linked_pr_urls": _flatten_sequence(
                    [linked_artifact.url for linked_artifact in linked_pr_artifacts if linked_artifact.url]
                ),
                "human_expected_outcome": "",
                "human_observation": "",
                "is_prediction_correct": "",
                "is_confirmed_match": "",
                "is_overcalled_match": "",
                "is_missed_match": "",
                "is_confirmed_non_match": "",
            }
        )

    return rows


def build_repo_evaluation_rows(
    *,
    artifacts: list[HipArtifact],
    evidence_records: list[HipEvidence],
    repo_statuses: list[HipRepoStatus],
    dataset_splits: dict[tuple[str, int], str],
) -> list[dict[str, object]]:
    """Build manual-review rows for repo-level HIP status predictions."""
    artifact_lookup = {
        (artifact.repo, artifact.number): artifact
        for artifact in artifacts
    }
    evidence_by_hip: dict[tuple[str, str], list[HipEvidence]] = defaultdict(list)
    for evidence in evidence_records:
        evidence_by_hip[(evidence.repo, evidence.hip_id)].append(evidence)

    rows: list[dict[str, object]] = []
    for repo_status in repo_statuses:
        group = evidence_by_hip[(repo_status.repo, repo_status.hip_id)]
        group_artifacts = [
            artifact_lookup[(evidence.repo, evidence.artifact_number)]
            for evidence in group
            if (evidence.repo, evidence.artifact_number) in artifact_lookup
        ]

        supporting_artifacts = [
            artifact_lookup[(repo_status.repo, artifact_number)]
            for artifact_number in repo_status.supporting_artifact_numbers
            if (repo_status.repo, artifact_number) in artifact_lookup
        ]
        supporting_issue_artifacts = [
            artifact for artifact in supporting_artifacts if artifact.artifact_type == "issue"
        ]
        supporting_pr_artifacts = [
            artifact for artifact in supporting_artifacts if artifact.artifact_type == "pull_request"
        ]

        group_issue_numbers = {
            artifact.number
            for artifact in group_artifacts
            if artifact.artifact_type == "issue"
        }
        group_pr_numbers = {
            artifact.number
            for artifact in group_artifacts
            if artifact.artifact_type == "pull_request"
        }
        has_linked_issue_pull_request_pair = False
        for artifact in group_artifacts:
            linked_numbers = set(
                extract_artifact_reference_numbers(
                    artifact.title,
                    artifact.body,
                    artifact.comments_text,
                    artifact.commit_messages_text,
                )
            )
            if artifact.artifact_type == "pull_request" and linked_numbers & group_issue_numbers:
                has_linked_issue_pull_request_pair = True
                break
            if artifact.artifact_type == "issue" and linked_numbers & group_pr_numbers:
                has_linked_issue_pull_request_pair = True
                break

        row_split = "train"
        if any(dataset_splits.get((artifact.repo, artifact.number), "train") == "test" for artifact in group_artifacts):
            row_split = "test"

        rows.append(
            {
                "prediction_present": True,
                "dataset_split": row_split,
                "repo": repo_status.repo,
                "hip_id": repo_status.hip_id,
                "predicted_status": repo_status.status,
                "predicted_confidence_level": repo_status.confidence_level,
                "supporting_artifact_numbers": _flatten_sequence(
                    [str(number) for number in repo_status.supporting_artifact_numbers]
                ),
                "supporting_artifact_urls": _flatten_sequence(
                    [artifact.url for artifact in supporting_artifacts if artifact.url]
                ),
                "supporting_artifact_links": _flatten_sequence(
                    [_artifact_markdown_link(artifact) for artifact in supporting_artifacts]
                ),
                "supporting_issue_links": _flatten_sequence(
                    [_artifact_markdown_link(artifact) for artifact in supporting_issue_artifacts]
                ),
                "supporting_pr_links": _flatten_sequence(
                    [_artifact_markdown_link(artifact) for artifact in supporting_pr_artifacts]
                ),
                "has_supporting_issue": bool(supporting_issue_artifacts),
                "has_supporting_pull_request": bool(supporting_pr_artifacts),
                "has_merged_pull_request": any(artifact.merged for artifact in supporting_pr_artifacts),
                "has_maintainer_like_pull_request": any(
                    is_maintainer_like_author(artifact.author_association)
                    for artifact in supporting_pr_artifacts
                ),
                "has_linked_issue_pull_request_pair": has_linked_issue_pull_request_pair,
                "last_evidence_at": _format_datetime(repo_status.last_evidence_at),
                "rationale": _flatten_sequence(repo_status.rationale),
                "human_expected_outcome": "",
                "human_observation": "",
                "is_prediction_correct": "",
                "is_confirmed_match": "",
                "is_overcalled_match": "",
                "is_missed_match": "",
                "is_confirmed_non_match": "",
            }
        )

    return rows
