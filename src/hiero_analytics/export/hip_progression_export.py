"""Structured exports for HIP progression analysis outputs."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from hiero_analytics.domain.hip_progression_models import (
    HipArtifact,
    HipEvidence,
    HipFeatureVector,
    HipRepoStatus,
)
from hiero_analytics.export.save import save_dataframe, save_markdown_table


def _format_datetime(value) -> str:
    return value.isoformat() if value is not None else ""


def _flatten_sequence(values: list[str]) -> str:
    return " | ".join(values)


def _artifact_rows(artifacts: list[HipArtifact]) -> list[dict[str, object]]:
    return [
        {
            "repo": artifact.repo,
            "artifact_type": artifact.artifact_type,
            "number": artifact.number,
            "title": artifact.title,
            "body": artifact.body,
            "comments_text": artifact.comments_text,
            "commit_messages_text": artifact.commit_messages_text,
            "author_login": artifact.author_login,
            "author_association": artifact.author_association,
            "state": artifact.state,
            "merged": artifact.merged,
            "created_at": _format_datetime(artifact.created_at),
            "updated_at": _format_datetime(artifact.updated_at),
            "closed_at": _format_datetime(artifact.closed_at),
            "additions": artifact.additions,
            "deletions": artifact.deletions,
            "labels": _flatten_sequence(artifact.labels),
            "changed_files_count": len(artifact.changed_files),
            "changed_file_paths": _flatten_sequence([changed_file.path for changed_file in artifact.changed_files]),
            "src_files_changed_count": sum(1 for changed_file in artifact.changed_files if changed_file.is_src),
            "test_files_changed_count": sum(1 for changed_file in artifact.changed_files if changed_file.is_test),
            "integration_test_files_changed_count": sum(
                1 for changed_file in artifact.changed_files if changed_file.is_integration_test
            ),
            "url": artifact.url,
        }
        for artifact in artifacts
    ]


def _feature_rows(feature_vectors: list[HipFeatureVector]) -> list[dict[str, object]]:
    return [
        {
            "repo": feature_vector.repo,
            "artifact_type": feature_vector.artifact_type,
            "artifact_number": feature_vector.artifact_number,
            "hip_id": feature_vector.hip_id,
            "extraction_source": feature_vector.extraction_source,
            "text_match_reason": feature_vector.text_match_reason,
            "explicit_hip_mention": feature_vector.explicit_hip_mention,
            "hip_in_title": feature_vector.hip_in_title,
            "hip_in_body": feature_vector.hip_in_body,
            "hip_in_comments": feature_vector.hip_in_comments,
            "hip_in_commit_messages": feature_vector.hip_in_commit_messages,
            "negative_context_flags": _flatten_sequence(feature_vector.negative_context_flags),
            "negative_phrase_unblock": feature_vector.negative_phrase_unblock,
            "negative_phrase_blocked": feature_vector.negative_phrase_blocked,
            "negative_phrase_follow_up": feature_vector.negative_phrase_follow_up,
            "negative_phrase_prep": feature_vector.negative_phrase_prep,
            "negative_phrase_refactor_only": feature_vector.negative_phrase_refactor_only,
            "negative_phrase_cleanup_only": feature_vector.negative_phrase_cleanup_only,
            "has_feat_keyword": feature_vector.has_feat_keyword,
            "has_implement_keyword": feature_vector.has_implement_keyword,
            "has_support_keyword": feature_vector.has_support_keyword,
            "src_files_changed_count": feature_vector.src_files_changed_count,
            "test_files_changed_count": feature_vector.test_files_changed_count,
            "integration_test_files_changed_count": feature_vector.integration_test_files_changed_count,
            "new_src_files_count": feature_vector.new_src_files_count,
            "new_test_files_count": feature_vector.new_test_files_count,
            "total_additions": feature_vector.total_additions,
            "total_deletions": feature_vector.total_deletions,
            "merged": feature_vector.merged,
            "author_is_maintainer_like": feature_vector.author_is_maintainer_like,
            "author_is_committer_like": feature_vector.author_is_committer_like,
            "implementation_score_inputs": _flatten_sequence(feature_vector.implementation_score_inputs),
        }
        for feature_vector in feature_vectors
    ]


def _evidence_rows(evidence_records: list[HipEvidence]) -> list[dict[str, object]]:
    return [
        {
            "repo": evidence.repo,
            "hip_id": evidence.hip_id,
            "artifact_type": evidence.artifact_type,
            "artifact_number": evidence.artifact_number,
            "hip_candidate_score": evidence.hip_candidate_score,
            "implementation_score": evidence.implementation_score,
            "completion_score": evidence.completion_score,
            "confidence_level": evidence.confidence_level,
            "reasons": _flatten_sequence(evidence.reasons),
        }
        for evidence in evidence_records
    ]


def _status_rows(repo_statuses: list[HipRepoStatus]) -> list[dict[str, object]]:
    return [
        {
            "repo": repo_status.repo,
            "hip_id": repo_status.hip_id,
            "status": repo_status.status,
            "confidence_level": repo_status.confidence_level,
            "supporting_artifact_numbers": _flatten_sequence(
                [str(number) for number in repo_status.supporting_artifact_numbers]
            ),
            "rationale": _flatten_sequence(repo_status.rationale),
            "last_evidence_at": _format_datetime(repo_status.last_evidence_at),
        }
        for repo_status in repo_statuses
    ]


def export_hip_progression_results(
    output_dir: Path,
    *,
    artifacts: list[HipArtifact],
    feature_vectors: list[HipFeatureVector],
    evidence_records: list[HipEvidence],
    repo_statuses: list[HipRepoStatus],
) -> dict[str, Path]:
    """Export HIP progression outputs as review-friendly CSV tables."""
    output_dir.mkdir(parents=True, exist_ok=True)

    artifact_path = output_dir / "artifacts.csv"
    feature_path = output_dir / "artifact_features.csv"
    feature_markdown_path = output_dir / "artifact_features.md"
    evidence_path = output_dir / "hip_evidence.csv"
    evidence_markdown_path = output_dir / "hip_evidence.md"
    status_path = output_dir / "hip_repo_status.csv"
    status_markdown_path = output_dir / "hip_repo_status.md"
    artifact_markdown_path = output_dir / "artifacts.md"

    artifact_df = pd.DataFrame(_artifact_rows(artifacts))
    feature_df = pd.DataFrame(_feature_rows(feature_vectors))
    evidence_df = pd.DataFrame(_evidence_rows(evidence_records))
    status_df = pd.DataFrame(_status_rows(repo_statuses))

    save_dataframe(artifact_df, artifact_path)
    artifact_markdown_path.unlink(missing_ok=True)
    save_dataframe(feature_df, feature_path)
    save_markdown_table(feature_df, feature_markdown_path)
    save_dataframe(evidence_df, evidence_path)
    save_markdown_table(evidence_df, evidence_markdown_path)
    save_dataframe(status_df, status_path)
    save_markdown_table(status_df, status_markdown_path)

    return {
        "artifacts": artifact_path,
        "artifact_features": feature_path,
        "artifact_features_markdown": feature_markdown_path,
        "hip_evidence": evidence_path,
        "hip_evidence_markdown": evidence_markdown_path,
        "hip_repo_status": status_path,
        "hip_repo_status_markdown": status_markdown_path,
    }
