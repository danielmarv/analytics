"""Tests for the unified analytics launcher."""

from __future__ import annotations

import hiero_analytics.run_analytics as run_analytics


def _build_test_pipelines(call_log: list[str]) -> tuple[run_analytics.PipelineSpec, ...]:
    """Create a small fake pipeline registry for launcher tests."""
    return (
        run_analytics.PipelineSpec(
            key="onboarding",
            label="Onboarding",
            description="Onboarding analytics.",
            runner=lambda: call_log.append("onboarding"),
            aliases=("gfi",),
        ),
        run_analytics.PipelineSpec(
            key="difficulty",
            label="Difficulty",
            description="Difficulty analytics.",
            runner=lambda: call_log.append("difficulty"),
        ),
        run_analytics.PipelineSpec(
            key="maintainer",
            label="Maintainer",
            description="Maintainer analytics.",
            runner=lambda: call_log.append("maintainer"),
        ),
    )


def test_parse_selection_supports_numbers_and_names(monkeypatch):
    """Prompt parsing should accept menu numbers and pipeline names."""
    call_log: list[str] = []
    monkeypatch.setattr(run_analytics, "PIPELINES", _build_test_pipelines(call_log))

    selection = run_analytics.parse_selection("1 maintainer")

    assert selection == ["onboarding", "maintainer"]


def test_prompt_for_selection_retries_after_invalid_input(monkeypatch):
    """Interactive prompts should loop until the user enters a valid choice."""
    call_log: list[str] = []
    monkeypatch.setattr(run_analytics, "PIPELINES", _build_test_pipelines(call_log))

    responses = iter(["wat", "2"])
    output: list[str] = []

    selection = run_analytics.prompt_for_selection(
        input_fn=lambda _prompt: next(responses),
        output_fn=output.append,
    )

    assert selection == ["difficulty"]
    assert any(message.startswith("Invalid selection:") for message in output)


def test_main_runs_selected_pipelines_once(monkeypatch, capsys):
    """Named CLI selection should run each chosen pipeline once in order."""
    call_log: list[str] = []
    monkeypatch.setattr(run_analytics, "PIPELINES", _build_test_pipelines(call_log))

    exit_code = run_analytics.main(["--run", "difficulty", "maintainer", "difficulty"])

    captured = capsys.readouterr()

    assert exit_code == 0
    assert call_log == ["difficulty", "maintainer"]
    assert "[1/2] Running Difficulty analytics" in captured.out


def test_main_all_runs_every_pipeline(monkeypatch):
    """The all flag should execute the full registry in menu order."""
    call_log: list[str] = []
    monkeypatch.setattr(run_analytics, "PIPELINES", _build_test_pipelines(call_log))

    exit_code = run_analytics.main(["--all"])

    assert exit_code == 0
    assert call_log == ["onboarding", "difficulty", "maintainer"]
