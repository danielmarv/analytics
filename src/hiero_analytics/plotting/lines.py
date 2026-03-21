"""Line chart primitives styled to match the shared analytics theme."""

from __future__ import annotations

from pathlib import Path

import matplotlib.ticker as ticker
import pandas as pd

from hiero_analytics.config.charts import FIGURE_BACKGROUND_COLOR, PRIMARY_PALETTE

from .base import create_figure, finalize_chart, prepare_dataframe
from .primitives import annotate_endpoint_badge, build_palette, format_chart_value


def _resolve_line_x_values(
    values: pd.Series,
) -> tuple[pd.Series, list[str] | None]:
    """Return numeric plotting positions and optional categorical tick labels."""
    numeric_values = pd.to_numeric(values, errors="coerce")
    if numeric_values.notna().all():
        return numeric_values.astype(float), None

    labels = values.astype(str).tolist()
    positions = pd.Series(range(len(values)), index=values.index, dtype=float)
    return positions, labels


def plot_line(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    output_path: Path,
    rotate_x: int | None = None,
) -> None:
    """Plot a single-series line chart."""
    df = prepare_dataframe(df, x_col, y_col)
    data = df.sort_values(x_col).copy()
    x_values, x_labels = _resolve_line_x_values(data[x_col])

    fig, ax = create_figure()

    ax.plot(
        x_values,
        data[y_col],
        marker="o",
        color=PRIMARY_PALETTE[2],
        linewidth=2.6,
        markersize=7,
        markeredgecolor=FIGURE_BACKGROUND_COLOR,
        markeredgewidth=2,
        solid_capstyle="round",
        zorder=3,
    )
    ax.fill_between(
        x_values,
        data[y_col],
        0,
        color=PRIMARY_PALETTE[2],
        alpha=0.08,
        zorder=2,
    )
    annotate_endpoint_badge(
        ax,
        x=float(x_values.iloc[-1]),
        y=float(data[y_col].iloc[-1]),
        text=f"{y_col} {format_chart_value(float(data[y_col].iloc[-1]))}",
        color=PRIMARY_PALETTE[2],
        y_offset=-4,
    )

    if x_labels is None:
        ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    else:
        ax.set_xticks(list(x_values))
        ax.set_xticklabels(x_labels)
    ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    ax.set_xlim(float(x_values.min()) - 0.15, float(x_values.max()) + 0.45)
    ax.margins(x=0.03, y=0.16)

    finalize_chart(
        fig=fig,
        ax=ax,
        title=title,
        xlabel=x_col,
        ylabel=y_col,
        output_path=output_path,
        rotate_x=rotate_x,
        grid_axis="y",
    )


def plot_multiline(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    group_col: str,
    title: str,
    output_path: Path,
    colors: dict[str, str] | None = None,
    rotate_x: int | None = None,
) -> None:
    """
    Plot a multi-series line chart grouped by a column.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataset.

    x_col : str
        Column used for x-axis.

    y_col : str
        Column used for y values.

    group_col : str
        Column defining the separate series.

    title : str
        Chart title.

    output_path : Path
        File path where the chart image will be saved.

    colors : dict[str, str] | None
        Optional mapping of series label -> color.

    rotate_x : int | None
        Optional x-axis label rotation.
    """
    df = prepare_dataframe(df, x_col, y_col, group_col).copy()

    pivot = df.pivot_table(index=x_col, columns=group_col, values=y_col, aggfunc="sum").sort_index()

    if pivot.empty:
        raise ValueError("Pivot produced an empty dataset")

    index_positions, x_labels = _resolve_line_x_values(pivot.index.to_series().reset_index(drop=True))
    pivot.index = pd.Index(index_positions.tolist(), dtype=float)

    fig, ax = create_figure()
    palette = build_palette(len(pivot.columns))
    endpoint_offsets = [-14, 0, 14, 28, 42]

    for index, column in enumerate(pivot.columns):
        color = colors.get(column) if colors else palette[index]
        is_total = str(column).lower() == "total"
        series = pivot[column].dropna()

        ax.plot(
            series.index,
            series,
            marker="o",
            label=str(column),
            color=color,
            linewidth=3 if is_total else 2.4,
            markersize=7,
            markeredgecolor=FIGURE_BACKGROUND_COLOR,
            markeredgewidth=2,
            solid_capstyle="round",
            zorder=3,
        )
        if is_total:
            # The total line gets a subtle area fill so it reads as the main
            # trend without overpowering the other series.
            ax.fill_between(
                series.index,
                series,
                0,
                color=color,
                alpha=0.08,
                zorder=2,
            )
        annotate_endpoint_badge(
            ax,
            x=float(series.index[-1]),
            y=float(series.iloc[-1]),
            text=f"{column} {format_chart_value(float(series.iloc[-1]))}",
            color=color,
            y_offset=endpoint_offsets[index % len(endpoint_offsets)],
        )

    if x_labels is None:
        ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    else:
        ax.set_xticks(list(pivot.index))
        ax.set_xticklabels(x_labels)
    ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    ax.set_xlim(float(pivot.index.min()) - 0.15, float(pivot.index.max()) + 0.45)
    ax.margins(x=0.03, y=0.16)

    finalize_chart(
        fig=fig,
        ax=ax,
        title=title,
        xlabel=x_col,
        ylabel=y_col,
        output_path=output_path,
        legend=True,
        rotate_x=rotate_x,
        grid_axis="y",
        # Keep the legend in the header whitespace instead of on top of data.
        legend_loc="upper right",
        legend_bbox_to_anchor=(1, 1.14),
        legend_ncol=min(len(pivot.columns), 3),
        legend_kwargs={"borderaxespad": 0.0},
        layout_rect=(0, 0, 1, 0.92),
    )
