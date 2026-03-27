"""Evaluation helpers for HIP progression feedback loops and benchmarks."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

from hiero_analytics.domain.hip_progression_models import (
    ArtifactBenchmarkExpectation,
    ArtifactComment,
    ArtifactCommit,
    ChangedFile,
    HipArtifact,
    HipCatalogEntry,
    HipEvidence,
    HipRepoStatus,
    RepoBenchmarkExpectation,
    build_changed_file,
    extract_artifact_reference_numbers,
    normalize_hip_id,
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
    "predicted_status",
    "predicted_progress_stage",
    "predicted_confidence_score",
    "predicted_confidence_level",
    "evidence_count",
    "linked_issue_numbers",
    "linked_issue_urls",
    "linked_pr_numbers",
    "linked_pr_urls",
    "top_reasons",
    "uncertainty_reasons",
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
    "predicted_rag_label",
    "predicted_confidence_score",
    "predicted_confidence_level",
    "evidence_count",
    "supporting_artifact_numbers",
    "supporting_artifact_links",
    "last_evidence_at",
    "top_reasons",
    "uncertainty_reasons",
    "human_expected_outcome",
    "human_observation",
    "is_prediction_correct",
    "is_confirmed_match",
    "is_overcalled_match",
    "is_missed_match",
    "is_confirmed_non_match",
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
    "artifact_link",
    "hip_id",
    "predicted_status",
    "predicted_progress_stage",
    "predicted_rag_label",
    "predicted_confidence_score",
    "predicted_confidence_level",
    "evidence_count",
    "linked_issue_numbers",
    "linked_issue_urls",
    "linked_pr_numbers",
    "linked_pr_urls",
    "supporting_artifact_numbers",
    "supporting_artifact_links",
    "top_reasons",
    "uncertainty_reasons",
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

BENCHMARK_METRIC_COLUMNS = [
    "scope",
    "total_expected_rows",
    "prediction_rows",
    "coverage_percent",
    "accuracy_percent",
    "precision_macro_percent",
    "recall_macro_percent",
    "overcall_rate_percent",
    "undercall_rate_percent",
]

CONFUSION_MATRIX_COLUMNS = [
    "scope",
    "expected_status",
    "predicted_status",
    "count",
]

PER_STATUS_METRIC_COLUMNS = [
    "scope",
    "status",
    "support",
    "precision_percent",
    "recall_percent",
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

STATUS_LABELS = ["not_started", "unknown", "conflicting", "in_progress", "completed"]
STATUS_RANK = {
    "not_started": 0,
    "unknown": 1,
    "conflicting": 1,
    "in_progress": 2,
    "completed": 3,
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
    linked_numbers = list(dict.fromkeys([*artifact.linked_artifact_numbers, *extract_artifact_reference_numbers(
        artifact.title,
        artifact.body,
        artifact.comments_text,
        artifact.commit_messages_text,
    )]))
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
    artifact_assessments: list[HipEvidence],
    dataset_splits: dict[tuple[str, int], str],
) -> list[dict[str, object]]:
    """Build manual-review rows for artifact-level HIP predictions."""
    artifact_lookup = {
        (artifact.repo, artifact.number): artifact
        for artifact in artifacts
    }
    assessments_by_artifact: dict[tuple[str, int], list[HipEvidence]] = defaultdict(list)
    for assessment in artifact_assessments:
        assessments_by_artifact[(assessment.repo, assessment.artifact_number)].append(assessment)

    rows: list[dict[str, object]] = []
    for artifact in artifacts:
        linked_artifacts = _linked_artifacts(artifact, artifact_lookup)
        linked_issue_artifacts = [linked_artifact for linked_artifact in linked_artifacts if linked_artifact.artifact_type == "issue"]
        linked_pr_artifacts = [linked_artifact for linked_artifact in linked_artifacts if linked_artifact.artifact_type == "pull_request"]
        row_base = {
            "dataset_split": dataset_splits.get((artifact.repo, artifact.number), "train"),
            "repo": artifact.repo,
            "artifact_type": artifact.artifact_type,
            "artifact_number": artifact.number,
            "artifact_title": artifact.title,
            "artifact_url": artifact.url,
            "artifact_link": _artifact_markdown_link(artifact),
            "linked_issue_numbers": _flatten_sequence([str(linked.number) for linked in linked_issue_artifacts]),
            "linked_issue_urls": _flatten_sequence([linked.url for linked in linked_issue_artifacts if linked.url]),
            "linked_pr_numbers": _flatten_sequence([str(linked.number) for linked in linked_pr_artifacts]),
            "linked_pr_urls": _flatten_sequence([linked.url for linked in linked_pr_artifacts if linked.url]),
            "human_expected_outcome": "",
            "human_observation": "",
            "is_prediction_correct": "",
            "is_confirmed_match": "",
            "is_overcalled_match": "",
            "is_missed_match": "",
            "is_confirmed_non_match": "",
        }
        assessments = sorted(
            assessments_by_artifact.get((artifact.repo, artifact.number), []),
            key=lambda assessment: (assessment.hip_id, assessment.artifact_number),
        )
        if not assessments:
            rows.append(
                {
                    "prediction_present": False,
                    **row_base,
                    "hip_id": "",
                    "predicted_status": "",
                    "predicted_progress_stage": "",
                    "predicted_confidence_score": "",
                    "predicted_confidence_level": "",
                    "evidence_count": 0,
                    "top_reasons": "No HIP prediction generated for this artifact.",
                    "uncertainty_reasons": "",
                }
            )
            continue

        for assessment in assessments:
            rows.append(
                {
                    "prediction_present": True,
                    **row_base,
                    "hip_id": assessment.hip_id,
                    "predicted_status": assessment.status,
                    "predicted_progress_stage": assessment.progress_stage,
                    "predicted_confidence_score": assessment.confidence_score,
                    "predicted_confidence_level": assessment.confidence_level,
                    "evidence_count": assessment.evidence_count,
                    "top_reasons": _flatten_sequence(assessment.top_reasons),
                    "uncertainty_reasons": _flatten_sequence(assessment.uncertainty_reasons),
                }
            )
    return rows


def build_repo_evaluation_rows(
    *,
    artifacts: list[HipArtifact],
    repo_statuses: list[HipRepoStatus],
    dataset_splits: dict[tuple[str, int], str],
) -> list[dict[str, object]]:
    """Build manual-review rows for repo-level HIP status predictions."""
    artifact_lookup = {(artifact.repo, artifact.number): artifact for artifact in artifacts}
    rows: list[dict[str, object]] = []
    for repo_status in repo_statuses:
        supporting_artifacts = [
            artifact_lookup[(repo_status.repo, number)]
            for number in repo_status.supporting_artifact_numbers
            if (repo_status.repo, number) in artifact_lookup
        ]
        row_split = "train"
        if any(dataset_splits.get((artifact.repo, artifact.number), "train") == "test" for artifact in supporting_artifacts):
            row_split = "test"
        rows.append(
            {
                "prediction_present": True,
                "dataset_split": row_split,
                "repo": repo_status.repo,
                "hip_id": repo_status.hip_id,
                "predicted_status": repo_status.status,
                "predicted_rag_label": repo_status.rag_label,
                "predicted_confidence_score": repo_status.confidence_score,
                "predicted_confidence_level": repo_status.confidence_level,
                "evidence_count": repo_status.evidence_count,
                "supporting_artifact_numbers": _flatten_sequence([str(number) for number in repo_status.supporting_artifact_numbers]),
                "supporting_artifact_links": _flatten_sequence([_artifact_markdown_link(artifact) for artifact in supporting_artifacts]),
                "last_evidence_at": _format_datetime(repo_status.last_evidence_at),
                "top_reasons": _flatten_sequence(repo_status.top_reasons),
                "uncertainty_reasons": _flatten_sequence(repo_status.uncertainty_reasons),
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


def build_manual_accuracy_rows(
    *,
    artifacts: list[HipArtifact],
    artifact_assessments: list[HipEvidence],
    repo_statuses: list[HipRepoStatus],
    dataset_splits: dict[tuple[str, int], str],
) -> list[dict[str, object]]:
    """Build a unified manual-review queue across PR, issue, and repo scopes."""
    artifact_rows = build_artifact_evaluation_rows(
        artifacts=artifacts,
        artifact_assessments=artifact_assessments,
        dataset_splits=dataset_splits,
    )
    repo_rows = build_repo_evaluation_rows(
        artifacts=artifacts,
        repo_statuses=repo_statuses,
        dataset_splits=dataset_splits,
    )

    combined_rows: list[dict[str, object]] = []
    for row in artifact_rows:
        combined_rows.append(
            {
                "review_scope": str(row["artifact_type"]),
                "prediction_present": row.get("prediction_present", ""),
                "dataset_split": row.get("dataset_split", ""),
                "repo": row.get("repo", ""),
                "artifact_type": row.get("artifact_type", ""),
                "artifact_number": row.get("artifact_number", ""),
                "artifact_title": row.get("artifact_title", ""),
                "artifact_url": row.get("artifact_url", ""),
                "artifact_link": row.get("artifact_link", ""),
                "hip_id": row.get("hip_id", ""),
                "predicted_status": row.get("predicted_status", ""),
                "predicted_progress_stage": row.get("predicted_progress_stage", ""),
                "predicted_rag_label": "",
                "predicted_confidence_score": row.get("predicted_confidence_score", ""),
                "predicted_confidence_level": row.get("predicted_confidence_level", ""),
                "evidence_count": row.get("evidence_count", ""),
                "linked_issue_numbers": row.get("linked_issue_numbers", ""),
                "linked_issue_urls": row.get("linked_issue_urls", ""),
                "linked_pr_numbers": row.get("linked_pr_numbers", ""),
                "linked_pr_urls": row.get("linked_pr_urls", ""),
                "supporting_artifact_numbers": "",
                "supporting_artifact_links": "",
                "top_reasons": row.get("top_reasons", ""),
                "uncertainty_reasons": row.get("uncertainty_reasons", ""),
                "human_expected_outcome": row.get("human_expected_outcome", ""),
                "human_observation": row.get("human_observation", ""),
                "is_prediction_correct": row.get("is_prediction_correct", ""),
                "is_confirmed_match": row.get("is_confirmed_match", ""),
                "is_overcalled_match": row.get("is_overcalled_match", ""),
                "is_missed_match": row.get("is_missed_match", ""),
                "is_confirmed_non_match": row.get("is_confirmed_non_match", ""),
            }
        )

    for row in repo_rows:
        combined_rows.append(
            {
                "review_scope": "repo",
                "prediction_present": row.get("prediction_present", ""),
                "dataset_split": row.get("dataset_split", ""),
                "repo": row.get("repo", ""),
                "artifact_type": "",
                "artifact_number": "",
                "artifact_title": "",
                "artifact_url": "",
                "artifact_link": "",
                "hip_id": row.get("hip_id", ""),
                "predicted_status": row.get("predicted_status", ""),
                "predicted_progress_stage": "",
                "predicted_rag_label": row.get("predicted_rag_label", ""),
                "predicted_confidence_score": row.get("predicted_confidence_score", ""),
                "predicted_confidence_level": row.get("predicted_confidence_level", ""),
                "evidence_count": row.get("evidence_count", ""),
                "linked_issue_numbers": "",
                "linked_issue_urls": "",
                "linked_pr_numbers": "",
                "linked_pr_urls": "",
                "supporting_artifact_numbers": row.get("supporting_artifact_numbers", ""),
                "supporting_artifact_links": row.get("supporting_artifact_links", ""),
                "top_reasons": row.get("top_reasons", ""),
                "uncertainty_reasons": row.get("uncertainty_reasons", ""),
                "human_expected_outcome": row.get("human_expected_outcome", ""),
                "human_observation": row.get("human_observation", ""),
                "is_prediction_correct": row.get("is_prediction_correct", ""),
                "is_confirmed_match": row.get("is_confirmed_match", ""),
                "is_overcalled_match": row.get("is_overcalled_match", ""),
                "is_missed_match": row.get("is_missed_match", ""),
                "is_confirmed_non_match": row.get("is_confirmed_non_match", ""),
            }
        )

    return combined_rows


def _artifact_from_payload(payload: dict[str, object]) -> HipArtifact:
    comments = [
        ArtifactComment(
            body=str(comment.get("body") or ""),
            source_kind=str(comment.get("source_kind") or "issue_comment"),  # type: ignore[arg-type]
            author_login=str(comment.get("author_login") or ""),
            author_association=str(comment.get("author_association") or "NONE"),
            created_at=None,
            url=str(comment.get("url") or ""),
            is_bot=bool(comment.get("is_bot", False)),
        )
        for comment in payload.get("comments", [])
        if isinstance(comment, dict)
    ]
    commits = [
        ArtifactCommit(
            message=str(commit.get("message") or ""),
            sha=str(commit.get("sha") or ""),
            authored_at=None,
        )
        for commit in payload.get("commits", [])
        if isinstance(commit, dict)
    ]
    changed_files = [
        build_changed_file(
            str(changed_file["path"]),
            additions=int(changed_file.get("additions", 0) or 0),
            deletions=int(changed_file.get("deletions", 0) or 0),
            status=str(changed_file.get("status") or "modified"),  # type: ignore[arg-type]
        )
        for changed_file in payload.get("changed_files", [])
        if isinstance(changed_file, dict) and changed_file.get("path")
    ]
    comments_text = _flatten_sequence([comment.body for comment in comments]).replace(" | ", "\n\n")
    commit_messages_text = _flatten_sequence([commit.message for commit in commits]).replace(" | ", "\n\n")
    return HipArtifact(
        repo=str(payload["repo"]),
        artifact_type=str(payload["artifact_type"]),  # type: ignore[arg-type]
        number=int(payload["number"]),
        title=str(payload.get("title") or ""),
        body=str(payload.get("body") or ""),
        comments_text=comments_text,
        commit_messages_text=commit_messages_text,
        comments=comments,
        commits=commits,
        author_login=str(payload.get("author_login") or ""),
        author_association=str(payload.get("author_association") or "NONE"),
        state=str(payload.get("state") or "open"),
        merged=bool(payload.get("merged", False)),
        changed_files=changed_files,
        additions=int(payload.get("additions", 0) or 0),
        deletions=int(payload.get("deletions", 0) or 0),
        labels=[str(label) for label in payload.get("labels", []) if isinstance(label, str)],
        linked_artifact_numbers=[int(number) for number in payload.get("linked_artifact_numbers", [])],
        url=str(payload.get("url") or ""),
    )


def load_benchmark_dataset(
    benchmark_dir: Path,
) -> tuple[list[HipCatalogEntry], list[HipArtifact], list[ArtifactBenchmarkExpectation], list[RepoBenchmarkExpectation]]:
    """Load the checked-in benchmark dataset."""
    catalog_payload = json.loads((benchmark_dir / "catalog_snapshot.json").read_text(encoding="utf-8"))
    artifact_payload = json.loads((benchmark_dir / "artifact_expectations.json").read_text(encoding="utf-8"))
    repo_payload = json.loads((benchmark_dir / "repo_expectations.json").read_text(encoding="utf-8"))

    catalog_entries = [
        HipCatalogEntry(
            hip_id=normalize_hip_id(str(item["hip_id"])),
            number=int(item["number"]),
            title=str(item.get("title") or ""),
            status=str(item.get("status") or ""),
            hip_type=str(item.get("hip_type") or ""),
            category=str(item.get("category") or ""),
            created=str(item.get("created") or ""),
            updated=str(item.get("updated") or ""),
            discussions_to=str(item.get("discussions_to") or ""),
            requested_by=str(item.get("requested_by") or ""),
            url=str(item.get("url") or ""),
        )
        for item in catalog_payload
        if isinstance(item, dict) and item.get("hip_id")
    ]

    artifact_lookup: dict[tuple[str, int], HipArtifact] = {}
    artifact_expectations: list[ArtifactBenchmarkExpectation] = []
    for item in artifact_payload:
        if not isinstance(item, dict) or "artifact" not in item:
            continue
        artifact = _artifact_from_payload(dict(item["artifact"]))
        artifact_lookup[(artifact.repo, artifact.number)] = artifact
        artifact_expectations.append(
            ArtifactBenchmarkExpectation(
                repo=artifact.repo,
                artifact_type=artifact.artifact_type,
                artifact_number=artifact.number,
                hip_id=normalize_hip_id(str(item["hip_id"])),
                expected_status=str(item["expected_status"]),  # type: ignore[arg-type]
                rationale=str(item.get("rationale") or ""),
            )
        )

    repo_expectations = [
        RepoBenchmarkExpectation(
            repo=str(item["repo"]),
            hip_id=normalize_hip_id(str(item["hip_id"])),
            expected_status=str(item["expected_status"]),  # type: ignore[arg-type]
            rationale=str(item.get("rationale") or ""),
        )
        for item in repo_payload
        if isinstance(item, dict) and item.get("repo") and item.get("hip_id")
    ]
    return catalog_entries, list(artifact_lookup.values()), artifact_expectations, repo_expectations


def _percentage(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round((numerator / denominator) * 100, 2)


def _macro_precision(confusion: dict[tuple[str, str], int]) -> float | None:
    values: list[float] = []
    for status in STATUS_LABELS:
        true_positive = confusion.get((status, status), 0)
        predicted_positive = sum(confusion.get((expected, status), 0) for expected in STATUS_LABELS)
        if predicted_positive == 0:
            continue
        values.append(true_positive / predicted_positive)
    if not values:
        return None
    return round(sum(values) / len(values) * 100, 2)


def _macro_recall(confusion: dict[tuple[str, str], int]) -> float | None:
    values: list[float] = []
    for status in STATUS_LABELS:
        true_positive = confusion.get((status, status), 0)
        actual_positive = sum(confusion.get((status, predicted), 0) for predicted in STATUS_LABELS)
        if actual_positive == 0:
            continue
        values.append(true_positive / actual_positive)
    if not values:
        return None
    return round(sum(values) / len(values) * 100, 2)


def _per_status_metrics(scope: str, confusion: dict[tuple[str, str], int]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for status in STATUS_LABELS:
        support = sum(confusion.get((status, predicted), 0) for predicted in STATUS_LABELS)
        predicted_positive = sum(confusion.get((expected, status), 0) for expected in STATUS_LABELS)
        true_positive = confusion.get((status, status), 0)
        rows.append(
            {
                "scope": scope,
                "status": status,
                "support": support,
                "precision_percent": _percentage(true_positive, predicted_positive),
                "recall_percent": _percentage(true_positive, support),
            }
        )
    return rows


def evaluate_status_predictions(
    *,
    scope: str,
    expectations: list[tuple[object, str]],
    predictions: dict[object, str],
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    """Evaluate predicted statuses against expected labels."""
    confusion: dict[tuple[str, str], int] = Counter()
    covered = 0
    correct = 0
    overcalled = 0
    undercalled = 0
    total = len(expectations)

    for key, expected_status in expectations:
        predicted_status = predictions.get(key)
        if predicted_status is None:
            continue
        covered += 1
        confusion[(expected_status, predicted_status)] += 1
        if predicted_status == expected_status:
            correct += 1
        expected_rank = STATUS_RANK.get(expected_status, 0)
        predicted_rank = STATUS_RANK.get(predicted_status, 0)
        if predicted_status != expected_status and predicted_rank > expected_rank:
            overcalled += 1
        elif predicted_status != expected_status and predicted_rank < expected_rank:
            undercalled += 1

    for expected_status in STATUS_LABELS:
        for predicted_status in STATUS_LABELS:
            confusion.setdefault((expected_status, predicted_status), 0)

    metric_rows = [
        {
            "scope": scope,
            "total_expected_rows": total,
            "prediction_rows": covered,
            "coverage_percent": _percentage(covered, total),
            "accuracy_percent": _percentage(correct, total),
            "precision_macro_percent": _macro_precision(confusion),
            "recall_macro_percent": _macro_recall(confusion),
            "overcall_rate_percent": _percentage(overcalled, total),
            "undercall_rate_percent": _percentage(undercalled, total),
        }
    ]
    confusion_rows = [
        {
            "scope": scope,
            "expected_status": expected_status,
            "predicted_status": predicted_status,
            "count": confusion[(expected_status, predicted_status)],
        }
        for expected_status in STATUS_LABELS
        for predicted_status in STATUS_LABELS
    ]
    return metric_rows, confusion_rows, _per_status_metrics(scope, confusion)
