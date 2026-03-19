"""Tests for plotting primitives and shared chart styling."""

from __future__ import annotations

import matplotlib
import pandas as pd
from matplotlib.patches import FancyBboxPatch

matplotlib.use("Agg")

import matplotlib.pyplot as plt

from hiero_analytics.plotting.bars import _round_bar_patches, plot_bar
from hiero_analytics.plotting.base import create_figure, style_axes
from hiero_analytics.plotting.lines import plot_multiline
from hiero_analytics.plotting.pie import plot_pie


def test_style_axes_uses_single_axis_grid():
    """Cartesian charts should keep only the requested grid axis visible."""
    fig, ax = create_figure()
    ax.plot([2023, 2024], [3, 5])

    style_axes(ax, grid_axis="y")

    assert not any(line.get_visible() for line in ax.get_xgridlines())
    assert any(line.get_visible() for line in ax.get_ygridlines())
    assert not ax.spines["top"].get_visible()
    assert not ax.spines["right"].get_visible()

    plt.close(fig)


def test_round_bar_patches_replaces_default_rectangles():
    """Rounded bars should be rendered with rounded box patches."""
    fig, ax = create_figure()
    bars = ax.bar(["A", "B"], [4, 6])

    _round_bar_patches(ax, list(bars.patches))

    rounded_patches = [patch for patch in ax.patches if isinstance(patch, FancyBboxPatch)]
    assert len(rounded_patches) == 2
    assert not any(bar.get_visible() for bar in bars.patches)

    plt.close(fig)


def test_plotters_write_chart_files(tmp_path):
    """The main plotting helpers should export non-empty chart assets."""
    bar_df = pd.DataFrame(
        {
            "repo": ["mirror-node", "sdk-python", "solo"],
            "count": [24, 18, 12],
        }
    )
    line_df = pd.DataFrame(
        {
            "year": [2023, 2023, 2024, 2024],
            "count": [8, 3, 12, 5],
            "state": ["open", "closed", "open", "closed"],
        }
    )
    pie_df = pd.DataFrame(
        {
            "difficulty": ["Unknown", "Good First Issue", "Beginner"],
            "count": [7, 9, 4],
        }
    )

    bar_output = tmp_path / "difficulty_by_repo.png"
    line_output = tmp_path / "gfi_state_line.png"
    pie_output = tmp_path / "difficulty_donut.png"

    plot_bar(
        bar_df,
        x_col="repo",
        y_col="count",
        title="Issues by Repository",
        output_path=bar_output,
        rotate_x=30,
    )
    plot_multiline(
        line_df,
        x_col="year",
        y_col="count",
        group_col="state",
        title="Good First Issues by State",
        output_path=line_output,
    )
    plot_pie(
        pie_df,
        label_col="difficulty",
        value_col="count",
        title="Issue Difficulty Distribution",
        output_path=pie_output,
    )

    assert bar_output.exists() and bar_output.stat().st_size > 0
    assert line_output.exists() and line_output.stat().st_size > 0
    assert pie_output.exists() and pie_output.stat().st_size > 0
