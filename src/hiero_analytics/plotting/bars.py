from __future__ import annotations

from pathlib import Path

import matplotlib.cm as cm
import numpy as np
import pandas as pd

from .base import create_figure, finalize_chart, prepare_dataframe


def plot_bar(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    output_path: Path,
    rotate_x: int | None = None,
) -> None:
    """
    Plot a standard bar chart.
    """
    df = prepare_dataframe(df, x_col, y_col)

    fig, ax = create_figure()

    colors = cm.tab20(np.linspace(0, 1, len(df)))

    ax.bar(
        df[x_col],
        df[y_col],
        color=colors,
    )

    finalize_chart(
        fig=fig,
        ax=ax,
        title=title,
        xlabel=x_col,
        ylabel=y_col,
        output_path=output_path,
        rotate_x=rotate_x,
    )


def plot_stacked_bar(
    df: pd.DataFrame,
    x_col: str,
    stack_cols: list[str],
    labels: list[str],
    title: str,
    output_path: Path,
    rotate_x: int | None = None,
) -> None:
    """
    Plot stacked bar chart.

    Parameters
    ----------
    df : pd.DataFrame
        Data containing categories and stacked values.

    x_col : str
        Column used for x-axis categories.

    stack_cols : List[str]
        Columns containing numeric values to stack.

    labels : List[str]
        Labels corresponding to each stacked column.

    title : str
        Chart title.

    output_path : Path
        Destination path for the saved chart.

    rotate_x : int | None
        Optional x-axis label rotation.
    """
    df = prepare_dataframe(df, x_col, *stack_cols)

    if len(stack_cols) != len(labels):
        raise ValueError("stack_cols and labels must have the same length")

    df = df.sort_values(x_col)

    fig, ax = create_figure()

    bottom = np.zeros(len(df))

    for col, label in zip(stack_cols, labels):

        ax.bar(
            df[x_col],
            df[col],
            bottom=bottom,
            label=label,
        )

        bottom += df[col].to_numpy()

    finalize_chart(
        fig=fig,
        ax=ax,
        title=title,
        xlabel=x_col,
        ylabel="count",
        output_path=output_path,
        legend=True,
        rotate_x=rotate_x,
    )