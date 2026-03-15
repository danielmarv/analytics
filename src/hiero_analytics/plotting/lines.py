from __future__ import annotations

from pathlib import Path

import matplotlib.ticker as ticker
import pandas as pd

from .base import create_figure, finalize_chart, prepare_dataframe


def plot_line(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    output_path: Path,
    rotate_x: int | None = None,
) -> None:
    """
    Plot a single-series line chart.
    """
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
    )

    ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))

    finalize_chart(
        fig=fig,
        ax=ax,
        title=title,
        xlabel=x_col,
        ylabel=y_col,
        output_path=output_path,
        rotate_x=rotate_x,
    )


def plot_multiline(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    group_col: str,
    title: str,
    output_path: Path,
    rotate_x: int | None = None,
) -> None:
    """
    Plot a multi-series line chart grouped by a column.
    """
    df = prepare_dataframe(df, x_col, y_col, group_col)

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

    for column in sorted(pivot.columns):
        ax.plot(
            pivot.index,
            pivot[column],
            marker="o",
            label=str(column),
        )

    ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))

    finalize_chart(
        fig=fig,
        ax=ax,
        title=title,
        xlabel=x_col,
        ylabel=y_col,
        output_path=output_path,
        legend=True,
        rotate_x=rotate_x,
    )