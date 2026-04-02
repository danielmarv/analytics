"""Structured exports for HIP progression analysis outputs."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pandas as pd

from hiero_analytics.analysis.hip_evaluation import (
    ARTIFACT_EVALUATION_COLUMNS,
    LEGACY_MANUAL_REVIEW_COLUMN_ALIASES,
    MANUAL_ACCURACY_COLUMNS,
    MANUAL_REVIEW_COLUMNS,
    REPO_EVALUATION_COLUMNS,
    assign_dataset_splits,
    build_artifact_evaluation_rows,
    build_manual_accuracy_rows,
    build_repo_evaluation_rows,
)
from hiero_analytics.domain.hip_progression_models import (
    ArtifactHipAssessment,
    HipArtifact,
    HipCatalogEntry,
    HipRepoStatus,
    hip_sort_key,
)
from hiero_analytics.plotting.bars import plot_bar, plot_stacked_bar
from hiero_analytics.export.save import save_dataframe

ExportProfile = Literal["review", "full"]
ExportScope = Literal["repo", "batch"]

STATUS_CHART_COLORS = {
    "completed": "#2E8B57",
    "in_progress": "#D98E04",
    "raised": "#C96A00",
    "not_raised": "#9AA0A6",
}
PRESENTATION_STATUS_ORDER = ["not_raised", "raised", "in_progress", "completed"]


def _format_datetime(value) -> str:
    return value.isoformat() if value is not None else ""


def _flatten(values: list[str]) -> str:
    return " | ".join(v for v in values if v)


def _repo_aliases(repos: list[str]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for repo in sorted(repos):
        alias = repo.split("/", maxsplit=1)[-1]
        for prefix in ("hiero-sdk-", "hedera-sdk-", "sdk-"):
            if alias.startswith(prefix):
                alias = alias[len(prefix):]
                break
        aliases[repo] = alias.replace("-", "_")
    return aliases


def _chart_scope_label(repos: list[str]) -> str:
    aliases = _repo_aliases(repos)
    ordered = [aliases[r] for r in sorted(repos)]
    if len(ordered) <= 4:
        return ", ".join(ordered)
    return f"{', '.join(ordered[:4])}, +{len(ordered) - 4} more"


# -- Per-repo CSV builders ------------------------------------------------


def _repo_status_df(repo_statuses: list[HipRepoStatus]) -> pd.DataFrame:
    rows = [
        {
            "repo": rs.repo,
            "hip_id": rs.hip_id,
            "status": rs.status,
            "confidence": rs.confidence,
            "supporting_artifacts": _flatten(rs.top_artifacts),
            "last_evidence_at": _format_datetime(rs.last_evidence_at),
        }
        for rs in sorted(repo_statuses, key=lambda r: hip_sort_key(r.hip_id), reverse=True)
    ]
    return pd.DataFrame(rows)


def _repo_issue_df(
    *,
    artifacts: list[HipArtifact],
    assessments: list[ArtifactHipAssessment],
    repo_statuses: list[HipRepoStatus],
) -> pd.DataFrame:
    repo_lookup = {(rs.repo, rs.hip_id): rs for rs in repo_statuses}
    rows: list[dict[str, object]] = []
    for a in sorted(assessments, key=lambda x: (hip_sort_key(x.hip_id), x.artifact_number), reverse=True):
        if a.artifact_type != "issue":
            continue
        artifact = next((art for art in artifacts if art.repo == a.repo and art.number == a.artifact_number), None)
        if artifact is None:
            continue
        rs = repo_lookup.get((a.repo, a.hip_id))
        rows.append({
            "repo": a.repo,
            "hip_id": a.hip_id,
            "confidence": a.confidence,
            "development_status": rs.status if rs else a.status,
            "issue_number": artifact.number,
            "issue_title": artifact.title,
            "issue_url": artifact.url,
        })
    return pd.DataFrame(rows)


def _repo_status_chart_df(repo_statuses: list[HipRepoStatus]) -> pd.DataFrame:
    rows = [
        {
            "hip_id": rs.hip_id,
            **{s: (1 if rs.status == s else 0) for s in PRESENTATION_STATUS_ORDER},
        }
        for rs in sorted(repo_statuses, key=lambda r: hip_sort_key(r.hip_id), reverse=True)
    ]
    return pd.DataFrame(rows, columns=["hip_id", *PRESENTATION_STATUS_ORDER])


# -- Batch CSV builders ----------------------------------------------------


def _sdk_status_matrix_df(
    repo_statuses: list[HipRepoStatus],
    *,
    catalog_entries: list[HipCatalogEntry],
) -> pd.DataFrame:
    repos = sorted({rs.repo for rs in repo_statuses})
    aliases = _repo_aliases(repos)
    lookup = {(rs.repo, rs.hip_id): rs.status for rs in repo_statuses}
    rows = [
        {"hip_id": e.hip_id, **{aliases[r]: lookup.get((r, e.hip_id), "not_raised") for r in repos}}
        for e in sorted(catalog_entries, key=lambda e: hip_sort_key(e.hip_id), reverse=True)
    ]
    return pd.DataFrame(rows, columns=["hip_id", *[aliases[r] for r in repos]])


def _sdk_rollup_df(
    repo_statuses: list[HipRepoStatus],
    *,
    catalog_entries: list[HipCatalogEntry],
) -> pd.DataFrame:
    grouped: dict[str, dict[str, object]] = {}
    for e in catalog_entries:
        grouped[e.hip_id] = {
            "hip_id": e.hip_id,
            "not_raised_count": 0,
            "raised_count": 0,
            "in_progress_count": 0,
            "completed_count": 0,
            "repos_with_issue_raised_count": 0,
            "completion_rate_percent": 0.0,
        }
    for rs in repo_statuses:
        row = grouped.get(rs.hip_id)
        if row is None:
            continue
        row[f"{rs.status}_count"] = int(row.get(f"{rs.status}_count", 0) or 0) + 1
        if rs.status != "not_raised":
            row["repos_with_issue_raised_count"] = int(row["repos_with_issue_raised_count"]) + 1

    result: list[dict[str, object]] = []
    for hip_id, row in sorted(grouped.items(), key=lambda i: hip_sort_key(i[0]), reverse=True):
        raised = int(row["repos_with_issue_raised_count"])
        completed = int(row["completed_count"])
        row["completion_rate_percent"] = round((completed / raised) * 100, 2) if raised else 0.0
        result.append(row)
    return pd.DataFrame(result)


def _sdk_status_chart_df(repo_statuses: list[HipRepoStatus]) -> pd.DataFrame:
    repos = sorted({rs.repo for rs in repo_statuses})
    aliases = _repo_aliases(repos)
    grouped: dict[str, dict[str, int | str]] = {
        r: {"sdk": aliases[r], **{s: 0 for s in PRESENTATION_STATUS_ORDER}} for r in repos
    }
    for rs in repo_statuses:
        row = grouped.get(rs.repo)
        if row:
            row[rs.status] = int(row[rs.status]) + 1
    return pd.DataFrame([grouped[r] for r in repos], columns=["sdk", *PRESENTATION_STATUS_ORDER])


def _sdk_completion_df(repo_statuses: list[HipRepoStatus]) -> pd.DataFrame:
    repos = sorted({rs.repo for rs in repo_statuses})
    aliases = _repo_aliases(repos)
    grouped: dict[str, dict[str, int | float | str]] = {
        r: {"sdk": aliases[r], "raised_count": 0, "completed_count": 0, "completion_rate_percent": 0.0}
        for r in repos
    }
    for rs in repo_statuses:
        row = grouped.get(rs.repo)
        if not row:
            continue
        if rs.status != "not_raised":
            row["raised_count"] = int(row["raised_count"]) + 1
        if rs.status == "completed":
            row["completed_count"] = int(row["completed_count"]) + 1
    result: list[dict[str, object]] = []
    for r in repos:
        row = grouped[r]
        raised = int(row["raised_count"])
        completed = int(row["completed_count"])
        row["completion_rate_percent"] = round((completed / raised) * 100, 2) if raised else 0.0
        result.append(row)
    return pd.DataFrame(result)


# -- Manual review feedback preservation -----------------------------------


def _key_for_row(row: dict[str, object], key_columns: list[str]) -> tuple[str, ...]:
    return tuple(str(row.get(c, "")) for c in key_columns)


def _preserve_feedback_rows(
    rows: list[dict[str, object]],
    path: Path,
    *,
    key_columns: list[str],
    columns: list[str],
    preserved_columns: list[str],
) -> pd.DataFrame:
    existing: dict[tuple[str, ...], dict[str, object]] = {}
    if path.exists():
        df = pd.read_csv(path, keep_default_na=False)
        df = df.rename(columns=LEGACY_MANUAL_REVIEW_COLUMN_ALIASES)
        for r in df.to_dict(orient="records"):
            existing[_key_for_row(r, key_columns)] = r

    merged: list[dict[str, object]] = []
    for row in rows:
        key = _key_for_row(row, key_columns)
        prev = existing.get(key, {})
        merged_row = {c: row.get(c, "") for c in columns}
        for c in preserved_columns:
            merged_row[c] = prev.get(c, merged_row.get(c, ""))
        merged.append(merged_row)
    return pd.DataFrame(merged, columns=columns)


# -- Main export function --------------------------------------------------


def export_hip_progression_results(
    output_dir: Path,
    *,
    artifacts: list[HipArtifact],
    catalog_entries: list[HipCatalogEntry],
    assessments: list[ArtifactHipAssessment],
    repo_statuses: list[HipRepoStatus],
    dataset_splits: dict[tuple[str, int], str] | None = None,
    export_profile: ExportProfile = "review",
    export_scope: ExportScope = "repo",
) -> dict[str, Path]:
    """Export HIP progression outputs: CSVs, charts, and evaluation review files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    evaluation_dir = output_dir / "evaluation"
    debug_dir = output_dir / "debug"
    dataset_splits = dataset_splits or assign_dataset_splits(artifacts)
    export_paths: dict[str, Path] = {}

    # -- Evaluation / manual review CSVs -----------------------------------
    artifact_predictions_path = evaluation_dir / "artifact_predictions.csv"
    repo_predictions_path = evaluation_dir / "repo_predictions.csv"
    manual_accuracy_path = evaluation_dir / "manual_accuracy_review.csv"

    artifact_predictions_df = _preserve_feedback_rows(
        build_artifact_evaluation_rows(artifacts=artifacts, assessments=assessments, dataset_splits=dataset_splits),
        artifact_predictions_path,
        key_columns=["repo", "hip_id", "artifact_number"],
        columns=ARTIFACT_EVALUATION_COLUMNS,
        preserved_columns=MANUAL_REVIEW_COLUMNS,
    )
    repo_predictions_df = _preserve_feedback_rows(
        build_repo_evaluation_rows(artifacts=artifacts, repo_statuses=repo_statuses, dataset_splits=dataset_splits),
        repo_predictions_path,
        key_columns=["repo", "hip_id"],
        columns=REPO_EVALUATION_COLUMNS,
        preserved_columns=MANUAL_REVIEW_COLUMNS,
    )
    manual_accuracy_df = _preserve_feedback_rows(
        build_manual_accuracy_rows(
            artifacts=artifacts, assessments=assessments,
            repo_statuses=repo_statuses, dataset_splits=dataset_splits,
        ),
        manual_accuracy_path,
        key_columns=["review_scope", "repo", "artifact_number", "hip_id"],
        columns=MANUAL_ACCURACY_COLUMNS,
        preserved_columns=MANUAL_REVIEW_COLUMNS,
    )

    save_dataframe(artifact_predictions_df, artifact_predictions_path)
    save_dataframe(repo_predictions_df, repo_predictions_path)
    save_dataframe(manual_accuracy_df, manual_accuracy_path)
    export_paths["artifact_predictions"] = artifact_predictions_path
    export_paths["repo_predictions"] = repo_predictions_path
    export_paths["manual_accuracy_review"] = manual_accuracy_path

    # -- Scope-specific outputs --------------------------------------------
    if export_scope == "repo":
        repo_status_path = output_dir / "repo_hip_status.csv"
        repo_issue_path = output_dir / "repo_hip_issues.csv"
        repo_chart_path = output_dir / "repo_hip_status.png"
        scope_label = _chart_scope_label(sorted({rs.repo for rs in repo_statuses}))

        save_dataframe(_repo_status_df(repo_statuses), repo_status_path)
        save_dataframe(
            _repo_issue_df(artifacts=artifacts, assessments=assessments, repo_statuses=repo_statuses),
            repo_issue_path,
        )
        chart_df = _repo_status_chart_df(repo_statuses)
        if not chart_df.empty:
            plot_stacked_bar(
                chart_df,
                x_col="hip_id",
                stack_cols=PRESENTATION_STATUS_ORDER,
                labels=PRESENTATION_STATUS_ORDER,
                title=f"HIP Development Status | {scope_label}",
                output_path=repo_chart_path,
                colors=STATUS_CHART_COLORS,
                rotate_x=20,
                annotate_totals=True,
                sort_categorical=False,
            )

        export_paths["repo_hip_status"] = repo_status_path
        export_paths["repo_hip_issues"] = repo_issue_path
        export_paths["repo_hip_status_chart"] = repo_chart_path
    else:
        sdk_matrix_path = output_dir / "sdk_hip_status_matrix.csv"
        sdk_rollup_path = output_dir / "sdk_hip_rollup.csv"
        sdk_status_chart_path = output_dir / "sdk_hip_development_status.png"
        sdk_completion_chart_path = output_dir / "sdk_hip_completion_rate.png"
        scope_label = _chart_scope_label(sorted({rs.repo for rs in repo_statuses}))

        save_dataframe(_sdk_status_matrix_df(repo_statuses, catalog_entries=catalog_entries), sdk_matrix_path)
        save_dataframe(_sdk_rollup_df(repo_statuses, catalog_entries=catalog_entries), sdk_rollup_path)

        status_chart_df = _sdk_status_chart_df(repo_statuses)
        if not status_chart_df.empty:
            plot_stacked_bar(
                status_chart_df,
                x_col="sdk",
                stack_cols=PRESENTATION_STATUS_ORDER,
                labels=PRESENTATION_STATUS_ORDER,
                title=f"SDK HIP Development Status | {scope_label}",
                output_path=sdk_status_chart_path,
                colors=STATUS_CHART_COLORS,
                rotate_x=20,
                annotate_totals=True,
                sort_categorical=False,
            )
        completion_df = _sdk_completion_df(repo_statuses)
        if not completion_df.empty:
            completion_df["sdk"] = pd.Categorical(completion_df["sdk"], categories=list(completion_df["sdk"]), ordered=True)
            plot_bar(
                completion_df,
                x_col="sdk",
                y_col="completion_rate_percent",
                title=f"SDK HIP Completion Rate | {scope_label}",
                output_path=sdk_completion_chart_path,
                rotate_x=20,
            )

        export_paths["sdk_hip_status_matrix"] = sdk_matrix_path
        export_paths["sdk_hip_rollup"] = sdk_rollup_path
        export_paths["sdk_hip_development_status_chart"] = sdk_status_chart_path
        export_paths["sdk_hip_completion_rate_chart"] = sdk_completion_chart_path

    # -- Debug outputs (full profile only) ---------------------------------
    if export_profile == "full":
        text_dump_path = debug_dir / "artifact_text_dump.csv"
        text_rows = [
            {
                "repo": a.repo,
                "artifact_type": a.artifact_type,
                "number": a.number,
                "title": a.title,
                "body": a.body,
                "author_login": a.author_login,
                "author_association": a.author_association,
                "state": a.state,
                "merged": a.merged,
                "url": a.url,
            }
            for a in artifacts
        ]
        save_dataframe(pd.DataFrame(text_rows), text_dump_path)
        export_paths["artifact_text_dump"] = text_dump_path

    return export_paths
