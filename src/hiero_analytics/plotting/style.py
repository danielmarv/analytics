"""
Centralized matplotlib styling for analytics charts.

This module applies a consistent visual style across all charts generated
by the analytics system. Style configuration values are sourced from
`hiero_analytics.config.charts`.
"""

from __future__ import annotations

import matplotlib.pyplot as plt

from hiero_analytics.config.charts import (
    DEFAULT_FIGSIZE,
    DEFAULT_STYLE,
    GRID_ALPHA,
    GRID_ENABLED,
    GRID_STYLE,
    LABEL_FONT_SIZE,
    LEGEND_FONT_SIZE,
    TICK_FONT_SIZE,
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

    plt.style.use(DEFAULT_STYLE)

    plt.rcParams.update(
        {
            "figure.figsize": DEFAULT_FIGSIZE,
            "figure.autolayout": True,
            "axes.titlesize": TITLE_FONT_SIZE,
            "axes.labelsize": LABEL_FONT_SIZE,
            "xtick.labelsize": TICK_FONT_SIZE,
            "ytick.labelsize": TICK_FONT_SIZE,
            "legend.fontsize": LEGEND_FONT_SIZE,
            "axes.grid": GRID_ENABLED,
            "grid.alpha": GRID_ALPHA,
            "grid.linestyle": GRID_STYLE,
        }
    )

    _STYLE_APPLIED = True