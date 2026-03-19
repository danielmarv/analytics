"""Line chart primitives styled to match the shared analytics theme."""

from __future__ import annotations

from pathlib import Path

import matplotlib.ticker as ticker
import pandas as pd

from hiero_analytics.config.charts import FIGURE_BACKGROUND_COLOR, PRIMARY_PALETTE

from .base import create_figure, finalize_chart, prepare_dataframe


def _format_value(value: float) -> str:
    """Format line values without unnecessary decimal places."""
    return f"{int(value):,}" if float(value).is_integer() else f"{value:,.1f}"


def _annotate_endpoint(
    ax,
    *,
    x: float,
    y: float,
    label: str,
    color: str,
    y_offset: int,
) -> None:
    """Label the final point of a series with a lightweight pill."""
    # Put the series label where the reader naturally finishes scanning: the
    # latest point on the right edge of the chart.
    ax.annotate(
        f"{label} {_format_value(y)}",
        xy=(x, y),
        xytext=(10, y_offset),
        textcoords="offset points",
        ha="left",
        va="center",
        fontsize=9,
        color=color,
        bbox={
            "boxstyle": "round,pad=0.28,rounding_size=0.8",
            "fc": "#FFFFFF",
            "ec": "#DCE5EF",
            "lw": 0.9,
        },
        zorder=5,
    )


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

    # Ensure numeric x-axis values
    data[x_col] = pd.to_numeric(data[x_col], errors="coerce")
    data = data.dropna(subset=[x_col])

    if data.empty:
        raise ValueError("No valid numeric x-axis values")

    fig, ax = create_figure()

    ax.plot(
        data[x_col],
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
        data[x_col],
        data[y_col],
        0,
        color=PRIMARY_PALETTE[2],
        alpha=0.08,
        zorder=2,
    )
    _annotate_endpoint(
        ax,
        x=float(data[x_col].iloc[-1]),
        y=float(data[y_col].iloc[-1]),
        label=str(y_col),
        color=PRIMARY_PALETTE[2],
        y_offset=-4,
    )

    ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    ax.set_xlim(float(data[x_col].min()) - 0.15, float(data[x_col].max()) + 0.45)
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

    pivot = (
        df
        .pivot_table(index=x_col, columns=group_col, values=y_col, aggfunc="sum")
        .sort_index()
    )

    if pivot.empty:
        raise ValueError("Pivot produced an empty dataset")

    pivot.index = pd.to_numeric(pivot.index, errors="coerce")
    pivot = pivot.dropna(axis=0, how="all")
    pivot = pivot[~pivot.index.isna()]

    if pivot.empty:
        raise ValueError("No valid numeric x-axis values")

    fig, ax = create_figure()
    palette = [PRIMARY_PALETTE[index % len(PRIMARY_PALETTE)] for index in range(len(pivot.columns))]
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
        _annotate_endpoint(
            ax,
            x=float(series.index[-1]),
            y=float(series.iloc[-1]),
            label=str(column),
            color=color,
            y_offset=endpoint_offsets[index % len(endpoint_offsets)],
        )

    ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
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
