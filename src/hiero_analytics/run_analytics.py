"""Unified terminal launcher for the analytics pipelines."""

from __future__ import annotations

import argparse
import re
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass

from hiero_analytics import (
    run_difficulty_org_for_repo,
    run_gfic_gfi_org,
    run_maintainer_pipeline_org,
    run_maintainer_pipeline_repo_by_repo,
)

PipelineRunner = Callable[[], None]


@dataclass(frozen=True)
class PipelineSpec:
    """Describe a runnable analytics pipeline."""

    key: str
    label: str
    description: str
    runner: PipelineRunner
    aliases: tuple[str, ...] = ()


PIPELINES: tuple[PipelineSpec, ...] = (
    PipelineSpec(
        key="onboarding",
        label="Onboarding",
        description="Good first issue and candidate pipeline analytics.",
        runner=run_gfic_gfi_org.main,
        aliases=("gfi", "gfic"),
    ),
    PipelineSpec(
        key="difficulty",
        label="Difficulty",
        description="Difficulty distribution analytics for recent open issues.",
        runner=run_difficulty_org_for_repo.main,
    ),
    PipelineSpec(
        key="maintainer",
        label="Maintainer",
        description="Contributor responsibility and maintainer pipeline analytics.",
        runner=run_maintainer_pipeline_org.main,
        aliases=("responsibility", "pipeline"),
    ),
    PipelineSpec(
        key="maintainer-sequential",
        label="Maintainer Safe",
        description="Maintainer pipeline analytics fetched one repository at a time.",
        runner=run_maintainer_pipeline_repo_by_repo.main,
        aliases=("maintainer-safe", "repo-by-repo", "safe"),
    ),
)

ALL_SELECTION = "all"


def _pipeline_map() -> dict[str, PipelineSpec]:
    """Return the configured pipelines keyed by their stable command names."""
    return {pipeline.key: pipeline for pipeline in PIPELINES}


def _selection_aliases() -> dict[str, str]:
    """Build the set of accepted prompt aliases for pipeline selection."""
    aliases = {
        "a": ALL_SELECTION,
        ALL_SELECTION: ALL_SELECTION,
    }

    for index, pipeline in enumerate(PIPELINES, start=1):
        aliases[str(index)] = pipeline.key
        aliases[pipeline.key] = pipeline.key
        aliases[pipeline.key.replace("_", "-")] = pipeline.key

        for alias in pipeline.aliases:
            aliases[alias] = pipeline.key

    return aliases


def _dedupe_preserving_order(items: Iterable[str]) -> list[str]:
    """Return items without duplicates while preserving the first-seen order."""
    seen: set[str] = set()
    ordered: list[str] = []

    for item in items:
        if item in seen:
            continue
        seen.add(item)
        ordered.append(item)

    return ordered


def parse_selection(raw_selection: str) -> list[str]:
    """Parse a prompt selection string into pipeline keys."""
    tokens = [token.strip().lower() for token in re.split(r"[\s,]+", raw_selection) if token.strip()]
    if not tokens:
        raise ValueError("enter one or more pipeline names, numbers, or 'all'")

    aliases = _selection_aliases()
    resolved: list[str] = []

    for token in tokens:
        pipeline_key = aliases.get(token)
        if pipeline_key is None:
            valid = ", ".join([*(str(index) for index, _ in enumerate(PIPELINES, start=1)), ALL_SELECTION])
            raise ValueError(f"unknown selection '{token}' (valid selections: {valid})")
        if pipeline_key == ALL_SELECTION:
            return [pipeline.key for pipeline in PIPELINES]
        resolved.append(pipeline_key)

    return _dedupe_preserving_order(resolved)


def print_pipeline_menu(*, output_fn: Callable[[str], None] = print) -> None:
    """Print the interactive pipeline selection menu."""
    output_fn("Available analytics pipelines:")

    for index, pipeline in enumerate(PIPELINES, start=1):
        output_fn(f"  {index}. {pipeline.label:<12} {pipeline.description}")

    output_fn("  A. Run all      Execute every analytics pipeline.")


def prompt_for_selection(
    *,
    input_fn: Callable[[str], str] = input,
    output_fn: Callable[[str], None] = print,
) -> list[str]:
    """Prompt until the user chooses one or more valid pipelines."""
    while True:
        print_pipeline_menu(output_fn=output_fn)

        try:
            raw_selection = input_fn(
                "Select analytics to run (numbers or names, comma-separated, or 'all'): "
            )
        except EOFError as exc:
            raise SystemExit("No selection received. Re-run with --all or --run to skip the prompt.") from exc

        try:
            return parse_selection(raw_selection)
        except ValueError as exc:
            output_fn(f"Invalid selection: {exc}")


def build_parser() -> argparse.ArgumentParser:
    """Create the command-line parser for the unified analytics runner."""
    parser = argparse.ArgumentParser(
        description="Run one or more Hiero analytics pipelines from a single terminal entry point.",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--run",
        nargs="+",
        choices=sorted(_pipeline_map()),
        metavar="PIPELINE",
        help="Run one or more named pipelines without prompting.",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Run every analytics pipeline.",
    )
    group.add_argument(
        "--list",
        action="store_true",
        help="List the available analytics pipelines and exit.",
    )
    return parser


def run_selected_pipelines(
    pipeline_keys: Sequence[str],
    *,
    output_fn: Callable[[str], None] = print,
) -> None:
    """Execute the selected pipelines in menu order."""
    pipeline_map = _pipeline_map()
    selected = [pipeline_map[key] for key in pipeline_keys]

    for index, pipeline in enumerate(selected, start=1):
        output_fn(f"[{index}/{len(selected)}] Running {pipeline.label} analytics")
        pipeline.runner()


def main(argv: Sequence[str] | None = None) -> int:
    """Run the unified analytics launcher."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.list:
        print_pipeline_menu()
        return 0

    if args.all:
        selected = [pipeline.key for pipeline in PIPELINES]
    elif args.run:
        selected = _dedupe_preserving_order(args.run)
    else:
        selected = prompt_for_selection()

    run_selected_pipelines(selected)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
