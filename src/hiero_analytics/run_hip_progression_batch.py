"""Run the HIP progression pipeline across multiple SDK-pattern repositories."""

from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path

from hiero_analytics.analysis.hip_evaluation import assign_dataset_splits
from hiero_analytics.analysis.hip_progression_pipeline import (
    resolve_default_repository_targets,
    run_benchmark_evaluation,
    run_pipeline_for_targets,
)
from hiero_analytics.config.hip_progression import DEFAULT_HIP_PROGRESSION_CONFIG
from hiero_analytics.config.logging import setup_logging
from hiero_analytics.config.paths import OUTPUTS_DIR, PROJECT_ROOT
from hiero_analytics.data_sources.github_client import GitHubClient
from hiero_analytics.domain.hip_progression_models import RepositoryTargetConfig
from hiero_analytics.export.hip_progression_export import export_hip_progression_results

DEFAULT_OWNER = "hiero-ledger"


def build_argument_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for batch HIP progression analysis."""
    parser = argparse.ArgumentParser(description="Run batch HIP progression analysis across SDK repositories.")
    parser.add_argument("--owner", default=DEFAULT_OWNER, help="GitHub repository owner.")
    parser.add_argument(
        "--repo",
        action="append",
        default=[],
        help="Optional repository name to include. Repeat for multiple repos. Defaults to all SDK-pattern repos.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on the number of newest artifacts to materialize per repo.")
    parser.add_argument("--output-dir", type=Path, default=OUTPUTS_DIR / "hip_progression" / "batch", help="Directory where batch outputs should be written.")
    parser.add_argument("--evaluate", action=argparse.BooleanOptionalAction, default=False, help="Run benchmark evaluation and export the metrics.")
    parser.add_argument(
        "--benchmark-dir",
        type=Path,
        default=PROJECT_ROOT / "tests" / "fixtures" / "hip_progression_benchmark",
        help="Benchmark fixture directory used with --evaluate.",
    )
    parser.add_argument("--train-ratio", type=float, default=0.8, help="Chronological fraction of issue and PR history reserved for training review.")
    parser.add_argument(
        "--latest-hip-limit",
        type=int,
        default=10,
        help="Overall HIP scope limit. Keeps only the newest official HIPs, ordered descending by HIP number.",
    )
    parser.add_argument(
        "--export-profile",
        choices=["review", "full"],
        default="review",
        help="Write the smaller reviewer bundle by default, or the full audit bundle when needed.",
    )
    parser.add_argument(
        "--checklist-limit",
        type=int,
        default=10,
        help="Maximum number of newest HIPs to include per repository in the checklist view.",
    )
    return parser


def _targets_from_args(owner: str, repos: list[str]) -> list[RepositoryTargetConfig]:
    if repos:
        return [RepositoryTargetConfig(owner=owner, repo=repo_name) for repo_name in sorted(set(repos))]
    return resolve_default_repository_targets(owner=owner, config=DEFAULT_HIP_PROGRESSION_CONFIG)


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for batch HIP progression analysis."""
    setup_logging()
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    targets = _targets_from_args(args.owner, args.repo)
    client = GitHubClient()
    result = run_pipeline_for_targets(
        client=client,
        targets=targets,
        limit=args.limit,
        latest_hip_limit=args.latest_hip_limit,
        config=DEFAULT_HIP_PROGRESSION_CONFIG,
    )
    benchmark_exports = None
    if args.evaluate:
        benchmark_result = run_benchmark_evaluation(args.benchmark_dir, config=DEFAULT_HIP_PROGRESSION_CONFIG)
        benchmark_exports = {
            "metrics": [*benchmark_result["artifact_metrics"], *benchmark_result["repo_metrics"]],
            "confusion": [*benchmark_result["artifact_confusion"], *benchmark_result["repo_confusion"]],
            "per_status": [*benchmark_result["artifact_per_status"], *benchmark_result["repo_per_status"]],
        }

    export_paths = export_hip_progression_results(
        args.output_dir,
        artifacts=result.artifacts,
        catalog_entries=result.catalog_entries,
        feature_vectors=result.feature_vectors,
        artifact_assessments=result.artifact_assessments,
        repo_statuses=result.repo_statuses,
        dataset_splits=assign_dataset_splits(result.artifacts, train_ratio=args.train_ratio),
        benchmark_exports=benchmark_exports,
        export_profile=args.export_profile,
        export_scope="batch",
        checklist_latest_limit=args.checklist_limit,
    )
    client.log_usage()

    status_counts = Counter(repo_status.status for repo_status in result.repo_statuses)
    print(f"HIP progression batch analysis complete for {len(targets)} repositories")
    print(f"Repositories: {', '.join(target.full_name for target in targets)}")
    print(f"Catalog HIPs in scope: {len(result.catalog_entries)}")
    print(f"Artifacts fetched: {len(result.artifacts)}")
    print(f"HIP candidates extracted: {len(result.candidates)}")
    print(f"Repo status rows produced: {len(result.repo_statuses)}")
    print(f"Latest HIP limit: {args.latest_hip_limit}")
    print(f"Status breakdown: {dict(sorted(status_counts.items()))}")
    if args.evaluate:
        print(f"Benchmark evaluation exported from: {args.benchmark_dir}")
    print(f"Outputs written to: {args.output_dir}")
    print(f"Primary summary: {export_paths['sdk_hip_status_matrix']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
