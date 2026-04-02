"""Run the repo-scoped HIP progression pipeline for one repository."""

from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path

from hiero_analytics.analysis.hip_evaluation import assign_dataset_splits
from hiero_analytics.analysis.hip_progression_pipeline import run_hip_progression_pipeline
from hiero_analytics.config.logging import setup_logging
from hiero_analytics.config.paths import OUTPUTS_DIR
from hiero_analytics.data_sources.github_client import GitHubClient
from hiero_analytics.data_sources.github_hip_catalog import fetch_official_hip_catalog
from hiero_analytics.data_sources.github_hip_loader import (
    fetch_repo_hip_artifacts,
    filter_hip_artifacts_by_author_scope,
)
from hiero_analytics.domain.hip_progression_models import AuthorScope
from hiero_analytics.export.hip_progression_export import export_hip_progression_results

DEFAULT_OWNER = "hiero-ledger"
DEFAULT_REPO = "hiero-sdk-js"


def build_argument_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for the HIP progression runner."""
    parser = argparse.ArgumentParser(description="Run repo-scoped HIP progression analysis.")
    parser.add_argument("--owner", default=DEFAULT_OWNER, help="GitHub repository owner.")
    parser.add_argument("--repo", default=DEFAULT_REPO, help="GitHub repository name.")
    parser.add_argument(
        "--author-scope",
        choices=["all", "maintainers", "committers"],
        default="all",
        help="Filter artifacts by author association before analysis.",
    )
    parser.add_argument(
        "--include-issues",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include issues in the analysis.",
    )
    parser.add_argument(
        "--include-prs",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include pull requests in the analysis.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory where HIP progression outputs should be written.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on the number of newest artifacts to materialize.",
    )
    parser.add_argument(
        "--train-ratio",
        type=float,
        default=0.8,
        help="Chronological fraction of issue and PR history reserved for training review.",
    )
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
    return parser


def _default_output_dir(owner: str, repo: str) -> Path:
    return OUTPUTS_DIR / "hip_progression" / f"{owner}_{repo}"


def run_pipeline(
    *,
    owner: str,
    repo: str,
    author_scope: AuthorScope,
    include_issues: bool,
    include_prs: bool,
    output_dir: Path,
    limit: int | None = None,
    train_ratio: float = 0.8,
    latest_hip_limit: int = 10,
    export_profile: str = "review",
) -> dict[str, object]:
    """Execute the end-to-end HIP progression pipeline."""
    client = GitHubClient()
    catalog_entries = fetch_official_hip_catalog(client)
    artifacts = fetch_repo_hip_artifacts(
        client,
        owner=owner,
        repo=repo,
        include_issues=include_issues,
        include_prs=include_prs,
        limit=limit,
    )
    scoped_artifacts = filter_hip_artifacts_by_author_scope(artifacts, author_scope)
    result = run_hip_progression_pipeline(
        artifacts=scoped_artifacts,
        catalog_entries=catalog_entries,
        repos=[f"{owner}/{repo}"],
        latest_hip_limit=latest_hip_limit,
    )
    dataset_splits = assign_dataset_splits(scoped_artifacts, train_ratio=train_ratio)
    exported_paths = export_hip_progression_results(
        output_dir,
        artifacts=scoped_artifacts,
        catalog_entries=result.catalog_entries,
        assessments=result.assessments,
        repo_statuses=result.repo_statuses,
        dataset_splits=dataset_splits,
        export_profile=export_profile,
        export_scope="repo",
    )
    client.log_usage()

    return {
        "catalog_entries": result.catalog_entries,
        "artifacts": artifacts,
        "scoped_artifacts": scoped_artifacts,
        "candidates": result.candidates,
        "assessments": result.assessments,
        "repo_statuses": result.repo_statuses,
        "dataset_splits": dataset_splits,
        "exported_paths": exported_paths,
    }


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for the HIP progression repo runner."""
    setup_logging()

    parser = build_argument_parser()
    args = parser.parse_args(argv)

    if not args.include_issues and not args.include_prs:
        parser.error("At least one of --include-issues or --include-prs must be enabled.")

    output_dir = args.output_dir or _default_output_dir(args.owner, args.repo)
    results = run_pipeline(
        owner=args.owner,
        repo=args.repo,
        author_scope=args.author_scope,
        include_issues=args.include_issues,
        include_prs=args.include_prs,
        output_dir=output_dir,
        limit=args.limit,
        train_ratio=args.train_ratio,
        latest_hip_limit=args.latest_hip_limit,
        export_profile=args.export_profile,
    )

    repo_statuses = results["repo_statuses"]
    scoped_artifacts = results["scoped_artifacts"]
    candidates = results["candidates"]
    dataset_splits = results["dataset_splits"]
    status_counts = Counter(repo_status.status for repo_status in repo_statuses)
    split_counts = Counter(dataset_splits.values())

    print(f"HIP progression analysis complete for {args.owner}/{args.repo}")
    print(f"Catalog HIPs in scope: {len(results['catalog_entries'])}")
    print(f"Artifacts fetched: {len(results['artifacts'])}")
    print(f"Artifacts kept after author scope '{args.author_scope}': {len(scoped_artifacts)}")
    print(f"HIP candidates extracted: {len(candidates)}")
    print(f"Repo statuses produced: {len(repo_statuses)}")
    print(f"Latest HIP limit: {args.latest_hip_limit}")
    if status_counts:
        print(f"Status breakdown: {dict(sorted(status_counts.items()))}")
    if split_counts:
        print(f"Train/test artifact split: {dict(sorted(split_counts.items()))}")
    print(f"Outputs written to: {output_dir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
