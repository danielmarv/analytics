"""Pie and donut chart helpers for analytics summaries."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from hiero_analytics.config.charts import MUTED_TEXT_COLOR, PLOT_BACKGROUND_COLOR, TITLE_COLOR
from hiero_analytics.domain.labels import DIFFICULTY_ORDER

from .base import create_figure, finalize_chart, prepare_dataframe, style_legend


def _format_donut_pct(pct: float) -> str:
    """Show only meaningful percentage labels to reduce clutter."""
    return f"{pct:.0f}%" if pct >= 5 else ""


def _format_count(value: float) -> str:
    """Format legend counts without trailing decimals."""
    return f"{int(value):,}" if float(value).is_integer() else f"{value:,.1f}"


def plot_pie(
    df: pd.DataFrame,
    label_col: str,
    value_col: str,
    title: str,
    output_path: Path,
    colors: dict[str, str] | None = None,
) -> None:
    """
    Plot a pie chart.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing pie chart data.
    label_col : str
        Column containing labels for each slice.
    value_col : str
        Column containing numeric values for each slice.
    title : str
        Chart title.
    output_path : Path
        File path where the chart image will be saved.
    colors : dict[str, str], optional
        Mapping of label -> color.
    """
    data = prepare_dataframe(df, label_col, value_col)

    if data.empty:
        raise ValueError("No valid data available for plotting")

    if data[value_col].sum() == 0:
        raise ValueError("Pie chart values sum to zero")

    # Sort slices largest → smallest for readability
    data[label_col] = pd.Categorical(
        data[label_col],
        categories=DIFFICULTY_ORDER,
        ordered=True,
    )

    data = data.sort_values(label_col)

    slice_colors = None
    if colors:
        slice_colors = [colors.get(label) for label in data[label_col]]

    fig, ax = create_figure()

    total = float(data[value_col].sum())
    percentages = data[value_col] / total * 100
    # Put the detailed breakdown into the legend so the donut stays visually
    # clean while still showing counts and percentages.
    legend_labels = [
        f"{label}  {_format_count(value)} ({pct:.0f}%)"
        for label, value, pct in zip(data[label_col], data[value_col], percentages, strict=True)
    ]
    center_x = -0.18

    wedges, _, _ = ax.pie(
        data[value_col],
        autopct=_format_donut_pct,
        pctdistance=0.8,
        startangle=110,
        colors=slice_colors,
        radius=0.92,
        center=(center_x, 0),
        wedgeprops={
            "width": 0.34,
            "edgecolor": PLOT_BACKGROUND_COLOR,
            "linewidth": 2,
        },
        textprops={
            "color": TITLE_COLOR,
            "fontsize": 10,
            "fontweight": "semibold",
        },
    )

    legend = ax.legend(
        wedges,
        legend_labels,
        title="Category",
        loc="center left",
        bbox_to_anchor=(0.95, 0.5),
    )
    style_legend(legend)

    ax.text(
        center_x,
        0.08,
        f"{int(total):,}",
        ha="center",
        va="center",
        fontsize=20,
        fontweight="semibold",
        color=TITLE_COLOR,
    )
    ax.text(
        center_x,
        -0.11,
        "Open issues",
        ha="center",
        va="center",
        fontsize=10,
        color=MUTED_TEXT_COLOR,
    )

    ax.set_aspect("equal")
    ax.set_xlim(-1.45, 1.5)

    finalize_chart(
        fig=fig,
        ax=ax,
        title=title,
        xlabel="",
        ylabel="",
        output_path=output_path,
        grid_axis=None,
    )
