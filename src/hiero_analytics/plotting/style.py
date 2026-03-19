"""
Centralized matplotlib styling for analytics charts.

This module applies a consistent visual style across all charts generated
by the analytics system. Style configuration values are sourced from
`hiero_analytics.config.charts`.
"""

from __future__ import annotations

import matplotlib.pyplot as plt

from hiero_analytics.config.charts import (
    AXIS_LINE_COLOR,
    DEFAULT_FIGSIZE,
    DEFAULT_STYLE,
    FIGURE_BACKGROUND_COLOR,
    FONT_FAMILY,
    GRID_ALPHA,
    GRID_COLOR,
    GRID_ENABLED,
    GRID_LINE_WIDTH,
    GRID_STYLE,
    LABEL_FONT_SIZE,
    LEGEND_BACKGROUND_COLOR,
    LEGEND_EDGE_COLOR,
    LEGEND_FONT_SIZE,
    MUTED_TEXT_COLOR,
    PLOT_BACKGROUND_COLOR,
    TEXT_COLOR,
    TICK_FONT_SIZE,
    TITLE_COLOR,
    TITLE_FONT_SIZE,
)

# Prevent applying style multiple times
_STYLE_APPLIED = False


def apply_style() -> None:
    """
    Apply consistent matplotlib styling for analytics charts.

    This function configures global matplotlib style parameters to ensure
    consistent appearance across all generated charts.

    It is safe to call multiple times; the style will only be applied once.
    """
    global _STYLE_APPLIED

    if _STYLE_APPLIED:
        return

    # Start from matplotlib's default theme and then layer our shared analytics
    # styling on top so every chart export looks consistent.
    plt.style.use(DEFAULT_STYLE)

    plt.rcParams.update(
        {
            "figure.figsize": DEFAULT_FIGSIZE,
            "figure.facecolor": FIGURE_BACKGROUND_COLOR,
            "savefig.facecolor": FIGURE_BACKGROUND_COLOR,
            "savefig.transparent": False,
            "axes.facecolor": PLOT_BACKGROUND_COLOR,
            "axes.titlesize": TITLE_FONT_SIZE,
            "axes.titleweight": "semibold",
            "axes.titlecolor": TITLE_COLOR,
            "axes.titlepad": 18,
            "axes.labelsize": LABEL_FONT_SIZE,
            "axes.labelcolor": MUTED_TEXT_COLOR,
            "axes.edgecolor": AXIS_LINE_COLOR,
            "axes.linewidth": 0.9,
            "axes.axisbelow": True,
            "xtick.labelsize": TICK_FONT_SIZE,
            "ytick.labelsize": TICK_FONT_SIZE,
            "xtick.color": MUTED_TEXT_COLOR,
            "ytick.color": MUTED_TEXT_COLOR,
            "xtick.major.size": 0,
            "ytick.major.size": 0,
            "text.color": TEXT_COLOR,
            "font.family": FONT_FAMILY,
            "legend.fontsize": LEGEND_FONT_SIZE,
            "legend.facecolor": LEGEND_BACKGROUND_COLOR,
            "legend.edgecolor": LEGEND_EDGE_COLOR,
            "legend.framealpha": 1.0,
            "legend.fancybox": True,
            "axes.grid": GRID_ENABLED,
            "grid.alpha": GRID_ALPHA,
            "grid.linestyle": GRID_STYLE,
            "grid.color": GRID_COLOR,
            "grid.linewidth": GRID_LINE_WIDTH,
            "axes.spines.top": False,
            "axes.spines.right": False,
        }
    )

    _STYLE_APPLIED = True
